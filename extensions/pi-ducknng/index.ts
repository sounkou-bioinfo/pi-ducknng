import { execFile } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const execFileAsync = promisify(execFile);
const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

export async function runPersistentRProof(root = PACKAGE_ROOT): Promise<string> {
  const { stdout, stderr } = await execFileAsync(
    "make",
    ["persistent-r-proof"],
    {
      cwd: root,
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
      timeout: 120_000,
    },
  );
  return `${stdout}${stderr}`.trim();
}

export default function piDucknngExtension(pi: ExtensionAPI): void {
  pi.registerCommand("ducknng-proof", {
    description: "Prove persistent R state survives a host reconnect",
    handler: async (_args, ctx) => {
      if (ctx.hasUI) {
        ctx.ui.notify("Running the persistent R reconnect proof...", "info");
      }
      try {
        const output = await runPersistentRProof();
        if (ctx.hasUI) {
          ctx.ui.notify(output, "info");
        }
      } catch (error) {
        if (ctx.hasUI) {
          ctx.ui.notify(
            error instanceof Error ? error.message : String(error),
            "error",
          );
        }
        throw error;
      }
    },
  });
}
