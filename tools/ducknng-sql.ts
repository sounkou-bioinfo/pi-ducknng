import { readFile } from "node:fs/promises";
import { StatementType } from "@duckdb/node-api";
import {
  PACKAGE_ROOT,
  closeDucknngConnection,
  resolveDucknngExtension,
  openDucknngConnection,
} from "../extensions/pi-ducknng/index.ts";

// Runs a SQL file in an in-memory DuckDB with the pinned ducknng loaded and
// prints each SELECT result as a table. NAME=value arguments become SQL variables,
// read with getvariable('NAME'), and are bound rather than spliced into SQL.
const USAGE = "usage: node tools/ducknng-sql.ts FILE.sql [NAME=value ...]";

function cell(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "object") return JSON.stringify(value, (_key, item) =>
    typeof item === "bigint" ? item.toString() : item);
  return String(value);
}

function table(columns: string[], rows: unknown[][]): string {
  const text = rows.map((row) => row.map(cell));
  const widths = columns.map((name, index) =>
    Math.max(name.length, ...text.map((row) => row[index].length)));
  const line = (values: string[]) =>
    values.map((value, index) => value.padEnd(widths[index])).join("  ").trimEnd();
  return [line(columns), line(widths.map((width) => "-".repeat(width))), ...text.map(line)].join("\n");
}

async function main(): Promise<void> {
  const [file, ...assignments] = process.argv.slice(2);
  if (!file) throw new Error(USAGE);
  const { instance, connection } = await openDucknngConnection(
    await resolveDucknngExtension(PACKAGE_ROOT),
  );
  try {
    for (const assignment of assignments) {
      const split = assignment.indexOf("=");
      const name = assignment.slice(0, split);
      if (split < 1 || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new Error(USAGE);
      await connection.run(`SET VARIABLE ${name} = $value`, { value: assignment.slice(split + 1) });
    }
    const statements = await connection.extractStatements(await readFile(file, "utf8"));
    const printed: string[] = [];
    for (let index = 0; index < statements.count; index += 1) {
      const prepared = await statements.prepare(index);
      try {
        const select = prepared.statementType === StatementType.SELECT;
        const reader = await prepared.runAndReadAll();
        if (select) printed.push(table(reader.columnNames(), reader.getRows()));
      } finally {
        prepared.destroySync();
      }
    }
    console.log(printed.join("\n\n"));
  } finally {
    closeDucknngConnection(instance, connection);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
