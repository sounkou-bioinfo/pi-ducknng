import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const child = spawn(
  "pi",
  ["--mode", "rpc", "--no-session", "-e", root],
  { cwd: root, stdio: ["pipe", "pipe", "pipe"] },
);

let stdoutBuffer = "";
let stderr = "";
let proofOutput;
let commandName;
let commandsConfirmed = false;
let promptConfirmed = false;

const finish = new Promise((resolveFinish, rejectFinish) => {
  const timer = setTimeout(() => {
    child.kill();
    rejectFinish(new Error(`Pi RPC proof timed out: ${stderr}`));
  }, 12 * 60 * 1000);

  const fail = (error) => {
    clearTimeout(timer);
    child.kill();
    rejectFinish(error);
  };

  const maybeFinish = () => {
    if (proofOutput && promptConfirmed) {
      clearTimeout(timer);
      child.stdin.end();
      resolveFinish(proofOutput);
    }
  };

  const handleRecord = (record) => {
    if (record.type === "extension_error") {
      fail(new Error(record.error ?? record.message ?? "Pi extension failed"));
      return;
    }
    if (record.type === "response" && record.id === "commands") {
      if (!record.success) {
        fail(new Error(record.error ?? "Pi get_commands failed"));
        return;
      }
      const commands = record.data?.commands ?? [];
      const localPath = resolve(root, "extensions/pi-ducknng/index.ts");
      const command = commands.find(
        (candidate) =>
          candidate.source === "extension" &&
          candidate.sourceInfo?.path === localPath,
      );
      if (!command) {
        fail(new Error("Pi did not load the local ducknng-proof extension command"));
        return;
      }
      commandName = command.name;
      commandsConfirmed = true;
      child.stdin.write(
        `${JSON.stringify({
          id: "proof",
          type: "prompt",
          message: `/${commandName}`,
        })}\n`,
      );
      return;
    }
    if (record.type === "response" && record.id === "proof") {
      if (!record.success) {
        fail(new Error(record.error ?? "Pi rejected /ducknng-proof"));
        return;
      }
      promptConfirmed = true;
      maybeFinish();
      return;
    }
    if (
      record.type === "extension_ui_request" &&
      record.method === "notify" &&
      record.message?.startsWith("Pi extension: loaded DuckDB API")
    ) {
      proofOutput = record.message;
      maybeFinish();
    }
  };

  child.stdout.on("data", (chunk) => {
    stdoutBuffer += chunk.toString();
    for (;;) {
      const newline = stdoutBuffer.indexOf("\n");
      if (newline < 0) break;
      const line = stdoutBuffer.slice(0, newline).replace(/\r$/, "");
      stdoutBuffer = stdoutBuffer.slice(newline + 1);
      if (!line) continue;
      try {
        handleRecord(JSON.parse(line));
      } catch (error) {
        fail(new Error(`invalid Pi RPC record: ${line}\n${String(error)}`));
      }
    }
  });

  child.stderr.on("data", (chunk) => {
    stderr = `${stderr}${chunk.toString()}`.slice(-1024 * 1024);
  });

  child.once("error", fail);
  child.once("exit", (code) => {
    if (!proofOutput || !promptConfirmed) {
      fail(new Error(`Pi RPC exited with status ${code}: ${stderr}`));
    }
  });
});

child.stdin.write(`${JSON.stringify({ id: "commands", type: "get_commands" })}\n`);
const output = await finish;
if (!commandsConfirmed) throw new Error("Pi command discovery was not confirmed");
console.log("Pi RPC: discovered and invoked the local /ducknng-proof command");
console.log(output);
