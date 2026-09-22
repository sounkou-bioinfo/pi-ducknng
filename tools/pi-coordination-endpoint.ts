import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import {
  type CoordinationGrant,
  startCoordinationEndpoint,
} from "../extensions/pi-ducknng/coordination-endpoint.ts";

const USAGE = [
  "usage: node tools/pi-coordination-endpoint.ts LOCATOR_FILE DATABASE_FILE [LISTEN_URL]",
  "  [--tls-cert-key FILE --tls-ca FILE --grants FILE] [--events URL|off] [--sse URL]",
].join("\n");

async function grants(path: string): Promise<CoordinationGrant[]> {
  const value: unknown = JSON.parse(await readFile(path, "utf8"));
  const valid = Array.isArray(value) && value.every((grant) =>
    typeof grant === "object" && grant !== null &&
    ["peer_identity", "project_id", "agent_id"].every((key) =>
      typeof (grant as Record<string, unknown>)[key] === "string"));
  if (!valid) {
    throw new Error("grants file must be a JSON array of {peer_identity, project_id, agent_id}");
  }
  return value as CoordinationGrant[];
}

async function main(): Promise<void> {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      "tls-cert-key": { type: "string" },
      "tls-ca": { type: "string" },
      grants: { type: "string" },
      events: { type: "string" },
      sse: { type: "string" },
    },
  });
  if (positionals.length < 2 || positionals.length > 3) throw new Error(USAGE);
  const [locator, database, listen] = positionals;
  const certKeyFile = values["tls-cert-key"];
  const caFile = values["tls-ca"];
  if ((certKeyFile === undefined) !== (caFile === undefined)) {
    throw new Error("mutual TLS needs both --tls-cert-key and --tls-ca");
  }
  const tls = certKeyFile && caFile ? { certKeyFile, caFile } : undefined;
  if (tls && !values.grants) throw new Error("a mutual-TLS endpoint needs --grants");
  // The ipc socket and locator are created readable only by this user.
  process.umask(0o077);
  const endpoint = await startCoordinationEndpoint({
    database,
    locator,
    listen,
    tls,
    grants: values.grants ? await grants(values.grants) : [],
    events: values.events === "off" ? false : values.events ? { listen: values.events } : undefined,
    sse: values.sse ? { listen: values.sse } : undefined,
  });
  if (endpoint.sseUrl) console.log(`server-sent events: ${endpoint.sseUrl}?topic=<topic>`);
  const keepAlive = setInterval(() => {}, 2 ** 30);
  const stop = async () => {
    clearInterval(keepAlive);
    await endpoint.close();
    process.exit(0);
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
