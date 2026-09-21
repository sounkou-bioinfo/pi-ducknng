import {
  coordinationErrorCode,
} from "../extensions/pi-ducknng/coordination.ts";
import { ducknngCoordinationClient as client } from "../extensions/pi-ducknng/index.ts";

// Calls one coordination method from the shell and prints its JSON reply.
// TLS material comes from the same PI_DUCKNNG_TLS_* variables as Pi.
const USAGE = "usage: node tools/coordination-call.ts URL METHOD [JSON_ARGUMENTS [FIELDS]]";

// FIELDS is a comma-separated list of dotted paths to keep from the reply.
function project(value: unknown, fields: string | undefined): unknown {
  if (!fields) return value;
  const picked: Record<string, unknown> = {};
  for (const path of fields.split(",")) {
    picked[path] = path.split(".").reduce<unknown>(
      (node, key) => (node as Record<string, unknown> | undefined)?.[key],
      value,
    );
  }
  return picked;
}

async function main(): Promise<void> {
  const [url, method, json, fields] = process.argv.slice(2);
  if (!url || !method || process.argv.length > 6) throw new Error(USAGE);
  const args = json ? JSON.parse(json) : {};
  await client.describe(url);
  console.log(JSON.stringify(project(await client.call(url, method, args), fields)));
}

main().then(() => process.exit(0), (error) => {
  const message = error instanceof Error ? error.message : String(error);
  const code = coordinationErrorCode(error);
  console.error(code ? message.slice(message.indexOf(`${code}: `)) : message);
  process.exit(1);
});
