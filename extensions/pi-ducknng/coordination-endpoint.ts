import { readFile, rename, writeFile } from "node:fs/promises";
import { basename, dirname, extname, resolve } from "node:path";
import { DuckDBInstance, type DuckDBConnection } from "@duckdb/node-api";
import { PACKAGE_ROOT, ensureDucknngExtension } from "./index.ts";

const SERVICE = "pi_coordination";
const COORDINATION_ROOT = resolve(PACKAGE_ROOT, "coordination");

export type CoordinationGrant = {
  peer_identity: string;
  project_id: string;
  agent_id: string;
};

export type CoordinationTls =
  | { certKeyFile: string; caFile: string }
  | { certPem: string; keyPem: string; caPem: string };

export type CoordinationEndpointOptions = {
  /** DuckDB database file owned by this endpoint. */
  database: string;
  /** Listen URL; defaults to ipc:// beside the database. */
  listen?: string;
  /** File that receives the resolved listen URL. */
  locator?: string;
  /**
   * Mutual TLS listener material, as files or as in-memory PEM text; every
   * method then requires a verified peer.
   */
  tls?: CoordinationTls;
  /**
   * Wake-up hint listener. Defaults to ipc:// beside the database when the
   * endpoint itself listens on ipc://; pass a URL such as wss://host:port to
   * publish hints across hosts, or false to disable hints.
   */
  events?: false | { listen: string };
  /** Peer identities allowed to register as each (project, agent). */
  grants?: CoordinationGrant[];
  retentionMs?: number;
  maxPendingPerMailbox?: number;
};

export type CoordinationEndpoint = {
  url: string;
  /** Wake-up hint URL, when hints are enabled. */
  eventsUrl?: string;
  /** The host connection, for administration on the same database. */
  connection: DuckDBConnection;
  close(): Promise<void>;
};

type MethodCatalog = {
  schema_version: number;
  methods: Array<{ name: string; maintain: boolean; request_schema: unknown }>;
};

function ipcBeside(database: string, suffix: string): string {
  const socket = resolve(dirname(database), `${basename(database, extname(database))}${suffix}`);
  if (Buffer.byteLength(socket, "utf8") > 100) {
    throw new Error(`ipc socket path is too long; pass a listen URL: ${socket}`);
  }
  return `ipc://${socket}`;
}

export function defaultCoordinationListen(database: string): string {
  return ipcBeside(database, ".ipc");
}

async function tlsConfigId(connection: DuckDBConnection, tls: CoordinationTls): Promise<number> {
  const id = "certPem" in tls
    ? await scalar(connection,
      "SELECT ducknng_tls_config_from_pem($cert, $key, $ca, NULL, 2)::UBIGINT",
      { cert: tls.certPem, key: tls.keyPem, ca: tls.caPem })
    : await scalar(connection,
      "SELECT ducknng_tls_config_from_files($cert_key, $ca, NULL, 2)::UBIGINT",
      { cert_key: tls.certKeyFile, ca: tls.caFile });
  return Number(id);
}

// Socket helpers return a STRUCT with ok and error; failures are in-band.
async function socketCall(
  connection: DuckDBConnection,
  call: string,
  values: Record<string, unknown>,
): Promise<{ socket_id: unknown; url: unknown }> {
  const reader = await connection.runAndReadAll(
    `SELECT s.ok, s.error, s.socket_id, s.url FROM (SELECT ${call} AS s)`,
    values as never,
  );
  const [row] = reader.getRowObjects();
  if (!row?.ok) throw new Error(String(row?.error ?? "ducknng socket call failed"));
  return { socket_id: row.socket_id, url: row.url };
}

async function runScript(connection: DuckDBConnection, sql: string): Promise<void> {
  const statements = await connection.extractStatements(sql);
  for (let index = 0; index < statements.count; index += 1) {
    const prepared = await statements.prepare(index);
    try {
      await prepared.run();
    } finally {
      prepared.destroySync();
    }
  }
}

async function scalar(
  connection: DuckDBConnection,
  sql: string,
  values?: Record<string, unknown>,
): Promise<unknown> {
  const reader = await connection.runAndReadAll(sql, values as never);
  const [row] = reader.getRows();
  return row?.[0];
}

/**
 * Serves the coordination methods from ducknng SQL methods. The SQL files in
 * coordination/ are the method implementations; this host only owns the
 * database, the listener, and the grant table's contents.
 */
export async function startCoordinationEndpoint(
  options: CoordinationEndpointOptions,
): Promise<CoordinationEndpoint> {
  const extensionPath = await ensureDucknngExtension(PACKAGE_ROOT);
  const listen = options.listen ?? defaultCoordinationListen(options.database);
  const eventsListen = options.events === false
    ? undefined
    : options.events?.listen ??
      (listen.startsWith("ipc://") ? ipcBeside(options.database, ".events.ipc") : undefined);
  const catalog = JSON.parse(
    await readFile(resolve(COORDINATION_ROOT, "methods.json"), "utf8"),
  ) as MethodCatalog;
  const maintain = await readFile(resolve(COORDINATION_ROOT, "maintain.sql"), "utf8");
  const instance = await DuckDBInstance.create(options.database, {
    allow_unsigned_extensions: "true",
  });
  const connection = await instance.connect();
  let serving = false;
  let eventSocket: unknown;
  const shutdown = async () => {
    try {
      if (serving) await connection.run(`SELECT ducknng_stop_server('${SERVICE}')`);
      if (eventSocket !== undefined) {
        await connection.run(
          "UPDATE coordination_meta SET event_socket_id = NULL, event_url = NULL");
        await connection.run("SELECT ducknng_close_socket($id::UBIGINT)", { id: eventSocket } as never);
      }
    } finally {
      serving = false;
      connection.closeSync();
      instance.closeSync();
    }
  };
  try {
    await connection.run(`LOAD '${extensionPath.replaceAll("'", "''")}'`);
    await runScript(connection, await readFile(resolve(COORDINATION_ROOT, "schema.sql"), "utf8"));
    const version = Number(await scalar(connection,
      "SELECT schema_version FROM coordination_meta WHERE singleton"));
    if (version !== catalog.schema_version) {
      throw new Error(`unsupported coordination database schema ${version}`);
    }
    await connection.run(
      "UPDATE coordination_meta SET retention_ms = $retention, max_pending_per_mailbox = $pending",
      {
        retention: options.retentionMs ?? 2_592_000_000,
        pending: options.maxPendingPerMailbox ?? 10_000,
      },
    );
    await connection.run("DELETE FROM coordination_grants");
    for (const grant of options.grants ?? []) {
      await connection.run(
        "INSERT INTO coordination_grants VALUES ($peer, $project, $agent)",
        { peer: grant.peer_identity, project: grant.project_id, agent: grant.agent_id },
      );
    }
    for (const method of catalog.methods) {
      const body = await readFile(resolve(COORDINATION_ROOT, "methods", `${method.name}.sql`), "utf8");
      await connection.run(
        "SELECT ducknng_register_sql_method($name, $sql, $schema, $requires_auth)",
        {
          name: method.name,
          sql: method.maintain ? `${maintain}\n${body}` : body,
          schema: JSON.stringify(method.request_schema),
          requires_auth: options.tls !== undefined,
        },
      );
    }
    const tlsConfig = options.tls ? await tlsConfigId(connection, options.tls) : 0;
    let eventsUrl: string | undefined;
    await connection.run("UPDATE coordination_meta SET event_socket_id = NULL, event_url = NULL");
    if (eventsListen) {
      const opened = await socketCall(connection, "ducknng_open_socket('pub')", {});
      eventSocket = opened.socket_id;
      const bound = await socketCall(connection,
        "ducknng_listen_socket($id::UBIGINT, $url, 1048576, $tls::UBIGINT)",
        { id: eventSocket, url: eventsListen, tls: tlsConfig });
      eventsUrl = typeof bound.url === "string" && bound.url ? bound.url : eventsListen;
      await connection.run(
        "UPDATE coordination_meta SET event_socket_id = $id::UBIGINT, event_url = $url",
        { id: eventSocket, url: eventsUrl } as never,
      );
    }
    await connection.run(
      `SELECT ducknng_start_server('${SERVICE}', $listen, 8, 134217728, 300000, $tls::UBIGINT)`,
      { listen, tls: tlsConfig },
    );
    serving = true;
    const url = String(await scalar(connection,
      `SELECT listen FROM ducknng_list_servers() WHERE name = '${SERVICE}'`));
    if (options.locator) {
      const staged = `${options.locator}.${process.pid}.tmp`;
      await writeFile(staged, `${url}\n`, { mode: 0o600 });
      await rename(staged, options.locator);
    }
    return { url, eventsUrl, connection, close: shutdown };
  } catch (error) {
    await shutdown();
    throw error;
  }
}
