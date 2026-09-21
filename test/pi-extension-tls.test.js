import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";

import { DuckDBInstance } from "@duckdb/node-api";

import piDucknngExtension from "../extensions/pi-ducknng/index.ts";
import { pki, withEnv } from "./support/pki.js";

const ROOT = resolve(import.meta.dirname, "..");
const EXTENSION = process.env.DUCKNNG_EXTENSION_PATH ??
  resolve(ROOT, "vendor/ducknng/build/release/ducknng.duckdb_extension");

async function mtlsServer(material) {
  const instance = await DuckDBInstance.create(":memory:", {
    allow_unsigned_extensions: "true",
  });
  const connection = await instance.connect();
  await connection.run(`LOAD '${EXTENSION.replaceAll("'", "''")}'`);
  const config = (await connection.runAndReadAll(
    "SELECT ducknng_tls_config_from_files($cert_key, $ca, NULL, 2)::UBIGINT AS id",
    { cert_key: material.server, ca: material.ca },
  )).getRowObjects()[0].id;
  await connection.run(
    `SELECT ducknng_start_server('pi_mtls', 'tls+tcp://127.0.0.1:0', 1, 134217728, 300000, ${config}::UBIGINT)`,
  );
  const url = (await connection.runAndReadAll(
    "SELECT listen FROM ducknng_list_servers() WHERE name = 'pi_mtls'",
  )).getRowObjects()[0].listen;
  return {
    url,
    connection,
    async close() {
      await connection.run("SELECT ducknng_stop_server('pi_mtls')");
      connection.closeSync();
      instance.closeSync();
    },
  };
}

function loadTools() {
  const tools = new Map();
  piDucknngExtension({
    registerTool(definition) {
      tools.set(definition.name, definition);
    },
    registerCommand() {},
    registerFlag() {},
    getFlag() {},
    on() {},
  });
  return tools;
}

test("generic tools reach mutual-TLS endpoints with injected credentials", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-tls-test-"));
  const material = await pki(work);
  const server = await mtlsServer(material);
  const tools = loadTools();
  const signal = new AbortController().signal;
  const describe = () => tools.get("ducknng_describe").execute("describe", { url: server.url }, signal);
  try {
    await withEnv({ PI_DUCKNNG_TLS_CA_FILE: undefined, PI_DUCKNNG_TLS_CERT_KEY_FILE: undefined }, async () => {
      await assert.rejects(describe(), /PI_DUCKNNG_TLS_CA_FILE/);
    });
    await withEnv({ PI_DUCKNNG_TLS_CA_FILE: material.ca, PI_DUCKNNG_TLS_CERT_KEY_FILE: undefined }, async () => {
      await assert.rejects(describe(), "a listener requiring mTLS rejects a client without a certificate");
    });
    await withEnv({ PI_DUCKNNG_TLS_CA_FILE: material.ca, PI_DUCKNNG_TLS_CERT_KEY_FILE: material["pi-agent"] }, async () => {
      const described = await describe();
      assert.ok(described.details.manifest.methods.some(({ name }) => name === "manifest"));
      assert.ok(!JSON.stringify(described).includes(work), "credential paths stay out of results");
      await server.connection.run(
        "SELECT ducknng_register_sql_method('whoami', 'SELECT to_json(s) FROM ducknng_request_subject() s', '{}', true)",
      );
      await describe();
      const called = await tools.get("ducknng_call").execute(
        "whoami",
        { url: server.url, method: "whoami", arguments: {} },
        signal,
      );
      assert.equal(called.details.result.authenticated, true);
      assert.match(called.details.result.peer_identity, /pi-agent/);
    });
  } finally {
    await server.close();
    await rm(work, { recursive: true, force: true });
  }
});
