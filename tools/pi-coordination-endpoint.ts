import { readFile } from "node:fs/promises";
import {
  type CoordinationGrant,
  startCoordinationEndpoint,
} from "../extensions/pi-ducknng/coordination-endpoint.ts";

const USAGE = "usage: node tools/pi-coordination-endpoint.ts LOCATOR_FILE DATABASE_FILE [LISTEN_URL]";

function environment(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value || undefined;
}

async function grants(path: string | undefined): Promise<CoordinationGrant[]> {
  if (!path) return [];
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
  const args = process.argv.slice(2);
  if (args.length < 2 || args.length > 3) throw new Error(USAGE);
  const [locator, database, listen] = args;
  const certKeyFile = environment("PI_DUCKNNG_COORDINATION_TLS_CERT_KEY_FILE");
  const caFile = environment("PI_DUCKNNG_COORDINATION_TLS_CA_FILE");
  if ((certKeyFile === undefined) !== (caFile === undefined)) {
    throw new Error(
      "mutual TLS needs both PI_DUCKNNG_COORDINATION_TLS_CERT_KEY_FILE and PI_DUCKNNG_COORDINATION_TLS_CA_FILE",
    );
  }
  const granted = await grants(environment("PI_DUCKNNG_COORDINATION_GRANTS_FILE"));
  if (certKeyFile && granted.length === 0) {
    throw new Error("a mutual-TLS endpoint needs PI_DUCKNNG_COORDINATION_GRANTS_FILE");
  }
  // The ipc socket and locator are created readable only by this user.
  process.umask(0o077);
  const endpoint = await startCoordinationEndpoint({
    database,
    locator,
    listen,
    tls: certKeyFile && caFile ? { certKeyFile, caFile } : undefined,
    grants: granted,
  });
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
