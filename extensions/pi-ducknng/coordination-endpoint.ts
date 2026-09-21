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

export type CoordinationEndpointOptions = {
  /** DuckDB database file owned by this endpoint. */
  database: string;
  /** Listen URL; defaults to ipc:// beside the database. */
  listen?: string;
  /** File that receives the resolved listen URL. */
  locator?: string;
  /** Mutual TLS listener material; every method then requires a verified peer. */
  tls?: { certKeyFile: string; caFile: string };
  /** Peer identities allowed to register as each (project, agent). */
  grants?: CoordinationGrant[];
  retentionMs?: number;
  maxPendingPerMailbox?: number;
};

export type CoordinationEndpoint = {
  url: string;
  /** The host connection, for administration on the same database. */
  connection: DuckDBConnection;
  close(): Promise<void>;
};

type MethodCatalog = {
  schema_version: number;
  methods: Array<{ name: string; maintain: boolean; request_schema: unknown }>;
};

export function defaultCoordinationListen(database: string): string {
  const socket = resolve(dirname(database), `${basename(database, extname(database))}.ipc`);
  if (Buffer.byteLength(socket, "utf8") > 100) {
    throw new Error(`ipc socket path is too long; pass a listen URL: ${socket}`);
  }
  return `ipc://${socket}`;
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
  const catalog = JSON.parse(
    await readFile(resolve(COORDINATION_ROOT, "methods.json"), "utf8"),
  ) as MethodCatalog;
  const maintain = await readFile(resolve(COORDINATION_ROOT, "maintain.sql"), "utf8");
  const instance = await DuckDBInstance.create(options.database, {
    allow_unsigned_extensions: "true",
  });
  const connection = await instance.connect();
  let serving = false;
  const shutdown = async () => {
    try {
      if (serving) await connection.run(`SELECT ducknng_stop_server('${SERVICE}')`);
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
    let tlsConfig = 0;
    if (options.tls) {
      tlsConfig = Number(await scalar(connection,
        "SELECT ducknng_tls_config_from_files($cert_key, $ca, NULL, 2)::UBIGINT",
        { cert_key: options.tls.certKeyFile, ca: options.tls.caFile }));
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
    return { url, connection, close: shutdown };
  } catch (error) {
    await shutdown();
    throw error;
  }
}
