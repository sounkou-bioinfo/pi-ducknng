#!/usr/bin/env python3
"""Render the docs/wasm.md transport capability matrix from the extension.

The capability contract lives in src/ducknng_net_backend.c as data for every
build target; this script loads the freshly built native extension, reads
ducknng_list_transport_capabilities(), and splices a markdown table between
the BEGIN/END GENERATED TRANSPORT MATRIX markers in docs/wasm.md. Run through
`make wasm_matrix` so the extension is built first.
"""

import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
DOC = REPO / "docs" / "wasm.md"
EXTENSION = REPO / "build" / "release" / "ducknng.duckdb_extension"
BEGIN = "<!-- BEGIN GENERATED TRANSPORT MATRIX (make wasm_matrix) -->"
END = "<!-- END GENERATED TRANSPORT MATRIX -->"


def render_table() -> str:
    if not EXTENSION.exists():
        raise SystemExit(
            "generate_wasm_matrix: build/release/ducknng.duckdb_extension is missing; "
            "run through `make wasm_matrix` so a native release is built first"
        )
    caps_source = REPO / "src" / "ducknng_net_backend.c"
    if caps_source.exists() and EXTENSION.stat().st_mtime < caps_source.stat().st_mtime:
        raise SystemExit(
            "generate_wasm_matrix: the native extension is older than "
            "src/ducknng_net_backend.c; rebuild before rendering the matrix"
        )
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    rows = con.execute(
        """
        SELECT target, http, https, http_response_stream, inproc, tcp, ipc, tls_tcp, websocket,
               async_is_real, honors_timeout, honors_cancel, tls_owner
        FROM ducknng_list_transport_capabilities()
        ORDER BY CASE target WHEN 'native' THEN 0 WHEN 'wasm_eh' THEN 1 ELSE 2 END
        """
    ).fetchall()
    header = (
        "| target | http | https | response stream | inproc | tcp | ipc | tls+tcp | ws/wss | "
        "real async | timeout | cancel | TLS owner |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    )
    body = []
    for row in rows:
        target, *caps = row
        caps_cells = caps[:8]
        flags = ["yes" if flag else "no" for flag in caps[8:11]]
        tls_owner = caps[11]
        body.append(
            "| `" + target + "` | " + " | ".join(list(caps_cells) + flags + [tls_owner]) + " |"
        )
    return header + "\n" + "\n".join(body)


def main() -> int:
    text = DOC.read_text(encoding="utf-8")
    begin = text.index(BEGIN)
    end = text.index(END)
    if begin < 0 or end < 0 or end < begin:
        print("generate_wasm_matrix: markers not found in docs/wasm.md", file=sys.stderr)
        return 1
    table = render_table()
    updated = text[: begin + len(BEGIN)] + "\n" + table + "\n" + text[end:]
    if updated != text:
        DOC.write_text(updated, encoding="utf-8")
        print("generate_wasm_matrix: docs/wasm.md updated")
    else:
        print("generate_wasm_matrix: docs/wasm.md already current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
