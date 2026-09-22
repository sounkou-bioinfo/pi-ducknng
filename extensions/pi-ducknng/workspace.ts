import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import type { DuckDBConnection } from "@duckdb/node-api";

/** The project's SQL workspace, relative to the Pi session's working directory. */
export const WORKSPACE_FILE = ".pi/ducknng/workspace.duckdb";
export const DEFAULT_SQL_ROWS = 100;
export const MAX_SQL_ROWS = 1000;

// r_eval(code, scope, wait_ms) evaluates R on the endpoint named by the r_url
// variable and returns its value as rows. The lambda binds the reply once, so
// the request is sent once and an R error is raised with its own text.
const R_EVAL_MACRO = `
  CREATE OR REPLACE TEMP MACRO r_eval(code, scope := 'main', wait_ms := 20000) AS TABLE
  SELECT * FROM ducknng_parse_body(
    list_transform(
      [ducknng_request_raw(
        coalesce(getvariable('r_url'), error('r_eval needs the R endpoint; call persistent_r_start first')),
        ducknng_encode_rpc_call('eval',
          json_object('code', code, 'scope', scope, 'wait_ms', wait_ms)::VARCHAR),
        wait_ms + 10000,
        0::UBIGINT
      )],
      reply -> CASE WHEN ducknng_frame_type_name(reply) = 'error'
        THEN error(ducknng_frame_error_text(reply))
        ELSE ducknng_frame_payload(reply) END
    )[1],
    'application/vnd.apache.arrow.stream'
  )`;

export type SqlReply = {
  columns: Array<{ name: string; type: string }>;
  rows: unknown[];
  truncated: boolean;
};

export type SqlWorkspace = {
  path: string;
  query(
    sql: string,
    options: { maxRows?: number; rUrl?: string; signal?: AbortSignal },
  ): Promise<SqlReply>;
  close(): void;
};

/**
 * Opens the workspace database with ducknng loaded. One connection serves the
 * session and runs one statement batch at a time; an abort interrupts it.
 */
export async function openSqlWorkspace(
  cwd: string,
  open: (database: string) => Promise<{ connection: DuckDBConnection; close(): void }>,
): Promise<SqlWorkspace> {
  const path = resolve(cwd, WORKSPACE_FILE);
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  let opened;
  try {
    opened = await open(path);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`cannot open the SQL workspace ${path}: ${detail}`, { cause: error });
  }
  const { connection, close } = opened;
  try {
    await connection.run(R_EVAL_MACRO);
  } catch (error) {
    close();
    throw error;
  }
  let turn: Promise<unknown> = Promise.resolve();
  const run = async (
    sql: string,
    { maxRows = DEFAULT_SQL_ROWS, rUrl, signal }: { maxRows?: number; rUrl?: string; signal?: AbortSignal },
  ): Promise<SqlReply> => {
    signal?.throwIfAborted();
    await connection.run("SET VARIABLE r_url = $url::VARCHAR", { url: rUrl ?? null });
    const interrupt = () => connection.interrupt();
    signal?.addEventListener("abort", interrupt, { once: true });
    try {
      const reader = await connection.runAndReadUntil(sql, maxRows + 1);
      const types = reader.columnTypes();
      const rows = reader.getRowObjectsJson();
      return {
        columns: reader.columnNames().map((name, index) => ({ name, type: types[index].toString() })),
        rows: rows.slice(0, maxRows),
        truncated: rows.length > maxRows,
      };
    } finally {
      signal?.removeEventListener("abort", interrupt);
    }
  };
  return {
    path,
    query(sql, options) {
      const result = turn.then(() => run(sql, options));
      turn = result.catch(() => undefined);
      return result;
    },
    close,
  };
}
