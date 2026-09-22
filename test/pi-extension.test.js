import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import piDucknngExtension from "../extensions/pi-ducknng/index.ts";

function loadTools() {
  const tools = new Map();
  const handlers = new Map();
  piDucknngExtension({
    registerTool(definition) {
      tools.set(definition.name, definition);
    },
    registerCommand() {},
    registerFlag() {},
    getFlag() {},
    on(event, handler) {
      const registered = handlers.get(event) ?? [];
      registered.push(handler);
      handlers.set(event, registered);
    },
  });
  return {
    tools,
    shutdown: async () => {
      for (const handler of handlers.get("session_shutdown") ?? []) {
        await handler();
      }
    },
  };
}

test("package manifest declares the Pi extension", async () => {
  const manifest = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  );
  assert.ok(manifest.keywords.includes("pi-package"));
  assert.deepEqual(manifest.pi.extensions, ["./extensions/pi-ducknng/index.ts"]);
});

test("R adapter spawn failure returns promptly", async () => {
  const empty = await mkdtemp(resolve(tmpdir(), "pi-ducknng-no-r-"));
  const previous = process.env.PATH;
  process.env.PATH = empty;
  const { tools, shutdown } = loadTools();
  const started = Date.now();
  try {
    await assert.rejects(
      tools.get("persistent_r_start").execute(
        "start",
        {},
        new AbortController().signal,
      ),
      /persistent_r_start needs R, but Rscript is not on PATH/,
    );
    assert.ok(Date.now() - started < 5000);
  } finally {
    process.env.PATH = previous;
    await shutdown();
    await rm(empty, { recursive: true, force: true });
  }
});

test("model tools discover and call the ducknng manifest", async () => {
  const { tools, shutdown } = loadTools();
  assert.deepEqual(
    [...tools.keys()],
    [
      "persistent_r_start",
      "ducknng_describe",
      "ducknng_call",
      "duckdb_sql",
      "coordination_send",
      "coordination_inbox",
      "coordination_agents",
      "coordination_reserve",
      "coordination_release",
    ],
  );
  const signal = new AbortController().signal;
  try {
    const started = await tools.get("persistent_r_start").execute(
      "start",
      {},
      signal,
    );
    const { url } = started.details;
    const described = await tools.get("ducknng_describe").execute(
      "describe",
      { url },
      signal,
    );
    const manifestReceipt = JSON.parse(described.content[0].text);
    assert.equal(manifestReceipt.manifest.server.protocol_version, 1);
    assert.deepEqual(
      manifestReceipt.manifest.methods.map(({ name }) => name),
      ["eval", "submit", "job", "result", "scopes", "reset", "interrupt", "jobs", "close"],
    );
    const evalMethod = manifestReceipt.manifest.methods.find(
      ({ name }) => name === "eval",
    );
    assert.match(
      evalMethod.request_schema.properties.code.examples[0],
      /datasets::mtcars/,
    );

    const call = (method, args) => tools.get("ducknng_call").execute(
      method, { url, method, arguments: args }, signal);
    const set = await call("eval", { code: "x <- 41L" });
    const bound = await call("eval", {
      scope: "analysis",
      code: [
        "makeActiveBinding('answer', function(value) {",
        "  if (!missing(value)) stop('answer is read-only')",
        "  42L",
        "}, environment())",
        "exists('x')",
      ].join("\n"),
    });
    const evaluated = await call("eval", { scope: "analysis", code: "answer" });
    assert.equal(set.details.result, 41);
    assert.equal(bound.details.result, false, "scopes do not see each other's objects");
    assert.equal(evaluated.details.result, 42);
    assert.equal(set.details.endpoint_process, evaluated.details.endpoint_process);
    await assert.rejects(call("eval", { code: "1", envir: "analysis" }),
      /unknown argument envir/);
    await assert.rejects(call("eval", { code: "1", scope: "not a name" }),
      /invalid_argument: scope must match/);

    const listed = await call("scopes", {});
    assert.deepEqual(listed.details.result.scopes, [
      { scope: "analysis", objects: [{ name: "answer", class: ["active_binding"] }] },
      { scope: "main", objects: [{ name: "x", class: ["integer"] }] },
    ]);
    assert.deepEqual((await call("reset", { scope: "analysis" })).details.result,
      { scope: "analysis", existed: true });
    assert.equal((await call("eval", { scope: "analysis", code: "exists('answer')" }))
      .details.result, false);
    assert.equal((await call("eval", { code: "x" })).details.result, 41,
      "resetting one scope keeps the others");

    const limited = await call("eval", { code: "datasets::mtcars[, 1:2]", limit: 2 });
    assert.equal(limited.details.result.length, 2);

    const table = await tools.get("ducknng_call").execute(
      "table",
      {
        url,
        method: "eval",
        arguments: {
          code: [
            "mpg_by_cyl <- aggregate(mpg ~ cyl,",
            "data = datasets::mtcars, FUN = mean); mpg_by_cyl",
          ].join(" "),
        },
      },
      signal,
    );
    assert.deepEqual(
      table.details.result.map(({ cyl }) => cyl),
      [4, 6, 8],
    );
    assert.ok(Math.abs(table.details.result[0].mpg - 26.66364) < 1e-5);

    const transformed = await tools.get("ducknng_call").execute(
      "transform",
      {
        url,
        method: "eval",
        arguments: {
          code: [
            "transform(mpg_by_cyl,",
            "  delta_from_4cyl = mpg - mpg[cyl == 4])",
          ].join(" "),
        },
      },
      signal,
    );
    assert.equal(transformed.details.result[0].delta_from_4cyl, 0);
    assert.ok(
      Math.abs(transformed.details.result[2].delta_from_4cyl + 11.56364) < 1e-5,
    );

    const increments = await Promise.all([
      tools.get("ducknng_call").execute(
        "increment-a",
        {
          url,
          method: "eval",
          arguments: {
            code: "current <- x; Sys.sleep(0.1); x <- current + 1L; x",
          },
        },
        signal,
      ),
      tools.get("ducknng_call").execute(
        "increment-b",
        {
          url,
          method: "eval",
          arguments: {
            code: "current <- x; Sys.sleep(0.1); x <- current + 1L; x",
          },
        },
        signal,
      ),
    ]);
    assert.deepEqual(increments.map(({ details }) => details.result), [42, 43]);

    await assert.rejects(
      tools.get("ducknng_call").execute(
        "missing",
        { url, method: "missing", arguments: {} },
        signal,
      ),
      /not declared by the endpoint manifest/,
    );

    const closed = await tools.get("ducknng_call").execute(
      "close",
      { url, method: "close", arguments: {} },
      signal,
    );
    assert.deepEqual(closed.details.result, { closed: true });
  } finally {
    await shutdown();
  }
});

test("long evaluations run as jobs that stream, fail with conditions, and stop on abort", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-jobs-test-"));
  const previous = process.env.PI_DUCKNNG_R_LOCATOR;
  process.env.PI_DUCKNNG_R_LOCATOR = resolve(work, "r.url");
  const { tools, shutdown } = loadTools();
  const signal = new AbortController().signal;
  const call = (method, args, options = {}) => tools.get("ducknng_call").execute(
    method, { url, method, arguments: args, ...options }, options.signal ?? signal);
  let url;
  try {
    ({ url } = (await tools.get("persistent_r_start").execute("start", {}, signal)).details);
    assert.match(url, /^ipc:\/\//);
    assert.equal((await readFile(resolve(work, "r.url"), "utf8")).trim(), url);
    await tools.get("ducknng_describe").execute("describe", { url }, signal);

    const timedOut = await call("eval", { code: "Sys.sleep(1.5); 7L", wait_ms: 200 })
      .catch((error) => error);
    const [, id] = /r_timeout: evaluation continues as job (\d+)/.exec(timedOut.message);
    const finished = await call("job", { job_id: Number(id), wait_ms: 10000 });
    assert.equal(finished.details.result.state, "succeeded");
    assert.equal((await call("result", { job_id: Number(id) })).details.result, 7);

    const ticking = (await call("submit", {
      code: "for (i in 1:3) { cat('tick', i, '\\n'); Sys.sleep(0.3) }; warning('late'); i",
    })).details.result;
    let output = "";
    let offset = 0;
    let conditions = [];
    let report;
    do {
      report = (await call("job", {
        job_id: ticking.job_id, wait_ms: 5000, output_offset: offset,
        conditions_offset: conditions.length,
      })).details.result;
      output += report.output;
      offset = report.output_offset;
      conditions = conditions.concat(report.conditions);
    } while (report.state === "queued" || report.state === "running");
    assert.equal(output, "tick 1 \ntick 2 \ntick 3 \n[1] 3\n");
    assert.deepEqual(conditions.map(({ type, message }) => [type, message]), [["warning", "late"]]);
    assert.deepEqual(report.value.class, ["integer"]);

    await assert.rejects(call("eval", { code: "f <- function() stop('boom'); f()" }),
      /r_error: job \d+: boom in f\(\) \(class simpleError, error, condition\)/);

    // A second client on the same URL sees the agent's job and interrupts it.
    const running = (await call("submit", {
      code: "for (k in 1:300) { counter <- k; Sys.sleep(0.1) }",
    })).details.result;
    await promisify(execFile)("Rscript", ["--vanilla", "-e", [
      "source('tools/ducknng-rpc.R')",
      `url <- '${url}'`,
      "Sys.sleep(0.5)",
      "jobs <- rpc_call(url, 'jobs')$jobs",
      `stopifnot(any(vapply(jobs, function(job) job$job_id == ${running.job_id} && job$state == 'running', TRUE)))`,
      `invisible(rpc_call(url, 'interrupt', list(job_id = ${running.job_id})))`,
    ].join("; ")], { cwd: resolve(import.meta.dirname, "..") });
    const stopped = (await call("job", { job_id: running.job_id })).details.result;
    assert.equal(stopped.state, "interrupted");

    const aborter = new AbortController();
    const started = Date.now();
    const aborted = call("eval", { code: "Sys.sleep(30)", wait_ms: 40000 },
      { signal: aborter.signal, timeout_ms: 45000 });
    setTimeout(() => aborter.abort(), 500);
    await assert.rejects(aborted, /r_interrupted|abort/i);
    assert.ok(Date.now() - started < 5000, "abort did not interrupt the evaluation");
    const kept = await call("eval", { code: "counter > 0" });
    assert.equal(kept.details.result, true);
  } finally {
    await shutdown();
    if (previous === undefined) delete process.env.PI_DUCKNNG_R_LOCATOR;
    else process.env.PI_DUCKNNG_R_LOCATOR = previous;
    await assert.rejects(readFile(resolve(work, "r.url"), "utf8"), /ENOENT/);
    await rm(work, { recursive: true, force: true });
  }
});

function exited(child) {
  return new Promise((resolveExit) => {
    if (child.exitCode !== null || child.signalCode !== null) resolveExit();
    else child.once("exit", resolveExit);
  });
}

test("R endpoint outlives idle polls and exits with its parent", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-parent-test-"));
  const parent = spawn("sleep", ["60"], { stdio: "ignore" });
  const endpoint = spawn(
    "Rscript",
    ["--vanilla", "tools/pi-r-endpoint.R", resolve(work, "endpoint.url"), `--parent=${parent.pid}`],
    { cwd: resolve(import.meta.dirname, ".."), stdio: "ignore" },
  );
  try {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      const url = await readFile(resolve(work, "endpoint.url"), "utf8").catch(() => "");
      if (url.trim()) break;
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 3000));
    assert.equal(endpoint.exitCode, null, "endpoint exited while its parent lived");
    parent.kill();
    await exited(parent);
    const stopped = Date.now();
    await Promise.race([
      exited(endpoint),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("endpoint outlived its parent")), 5000)),
    ]);
    assert.ok(Date.now() - stopped < 5000);
  } finally {
    parent.kill();
    endpoint.kill("SIGKILL");
    await rm(work, { recursive: true, force: true });
  }
});

test("duckdb_sql keeps tables in the project workspace and reads R values into them", async () => {
  const cwd = await mkdtemp(resolve(tmpdir(), "pi-ducknng-workspace-test-"));
  const signal = new AbortController().signal;
  const session = () => {
    const loaded = loadTools();
    const sql = async (text, options = {}) => (await loaded.tools.get("duckdb_sql").execute(
      "sql", { sql: text, ...options }, signal, undefined, { cwd })).details;
    return { ...loaded, sql };
  };
  const first = session();
  try {
    await assert.rejects(first.sql("FROM r_eval('1')"), /call persistent_r_start first/);
    const bounded = await first.sql("FROM range(10)", { max_rows: 3 });
    assert.deepEqual(bounded.columns, [{ name: "range", type: "BIGINT" }]);
    assert.equal(bounded.rows.length, 3);
    assert.equal(bounded.truncated, true);
    assert.equal(bounded.database, ".pi/ducknng/workspace.duckdb");

    await first.tools.get("persistent_r_start").execute("start", {}, signal);
    const saved = await first.sql([
      "CREATE TABLE mpg_by_cyl AS",
      "FROM r_eval('aggregate(mpg ~ cyl, data = datasets::mtcars, FUN = mean)')",
      "; SELECT count(*) AS groups FROM mpg_by_cyl",
    ].join(" "));
    assert.deepEqual(saved.rows, [{ groups: "3" }]);
    await first.sql("FROM r_eval('marker <- TRUE; 1', scope := 'kept')");
    assert.deepEqual((await first.sql("FROM r_eval('exists(\"marker\")')")).rows, [{ value: false }]);
    await assert.rejects(first.sql("FROM r_eval('stop(\"boom\")')"), /r_error: job \d+: boom/);
  } finally {
    await first.shutdown();
  }

  const second = session();
  try {
    const reopened = await second.sql("SELECT cyl, round(mpg, 2) AS mpg FROM mpg_by_cyl ORDER BY cyl");
    assert.deepEqual(reopened.rows.map(({ cyl }) => cyl), [4, 6, 8]);
    await readFile(resolve(cwd, ".pi/ducknng/workspace.duckdb"));
  } finally {
    await second.shutdown();
    await rm(cwd, { recursive: true, force: true });
  }
});
