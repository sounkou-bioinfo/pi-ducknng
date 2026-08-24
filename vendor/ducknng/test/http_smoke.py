#!/usr/bin/env python3
from __future__ import annotations

import http.server
import pathlib
import subprocess
import sys
import tempfile
import threading


class SmokeHandler(http.server.BaseHTTPRequestHandler):
    retry_count = 0
    retry_lock = threading.Lock()

    def do_GET(self) -> None:
        if self.path.startswith("/retry"):
            with self.retry_lock:
                type(self).retry_count += 1
                call = type(self).retry_count
            status = 503 if call <= 2 else 200
            body = ('[{"call":%d}]' % call).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path != "/hello":
            self.send_response(404)
            self.end_headers()
            return
        body = b"hello from ducknng http"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("X-Test", "hello")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path != "/echo":
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        self.send_response(200)
        self.send_header("Content-Type", self.headers.get("Content-Type", "application/octet-stream"))
        self.send_header("X-Test", "echo")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def sql_quote(path: str) -> str:
    return path.replace("'", "''")


def read_tsv_line(path: pathlib.Path) -> list[str]:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise AssertionError(f"no output captured in {path}")
    return text.split("\t")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: test/http_smoke.py <extension_path>")

    ext_path = pathlib.Path(sys.argv[1]).resolve()
    if not ext_path.exists():
        raise SystemExit(f"extension not found: {ext_path}")

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), SmokeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        port = server.server_address[1]
        with tempfile.TemporaryDirectory(prefix="ducknng-http-smoke-") as tmpdir:
            tmpdir_path = pathlib.Path(tmpdir)
            out1 = tmpdir_path / "hello.tsv"
            out2 = tmpdir_path / "echo.tsv"
            out3 = tmpdir_path / "retry.tsv"
            sql = f"""
LOAD '{sql_quote(str(ext_path))}';
COPY (
  SELECT ok, status, body_text, position('X-Test' IN headers_json) > 0 AS has_xtest
  FROM ducknng_ncurl('http://127.0.0.1:{port}/hello', NULL, NULL, NULL, 2000, 0::UBIGINT)
) TO '{sql_quote(str(out1))}' (DELIMITER '\t', HEADER FALSE);
COPY (
  SELECT ok, status, hex(body), position('X-Test' IN headers_json) > 0 AS has_xtest, body_text IS NULL AS text_is_null
  FROM ducknng_ncurl(
    'http://127.0.0.1:{port}/echo',
    'POST',
    '[{{"name":"Content-Type","value":"application/octet-stream"}},{{"name":"X-Sent","value":"duck"}}]',
    from_hex('01020304'),
    2000,
    0::UBIGINT
  )
) TO '{sql_quote(str(out2))}' (DELIMITER '\t', HEADER FALSE);
COPY (
  WITH RECURSIVE attempts(attempt, status, body_text) AS (
    SELECT 1, status, body_text
    FROM ducknng_ncurl('http://127.0.0.1:{port}/retry?attempt=1', 'GET', NULL, NULL, 2000, 0::UBIGINT)
    UNION ALL
    SELECT attempt + 1, r.status, r.body_text
    FROM (SELECT * FROM attempts WHERE status <> 200 AND attempt < 5) a,
         ducknng_ncurl(
           'http://127.0.0.1:{port}/retry?attempt=' || (a.attempt + 1)::VARCHAR,
           'GET', NULL, NULL, 2000, 0::UBIGINT
         ) r
  )
  SELECT string_agg(status::VARCHAR || ':' || regexp_extract(body_text, '[0-9]+'), '|' ORDER BY attempt) AS trace,
         max(attempt)::INTEGER AS max_attempt
  FROM attempts
) TO '{sql_quote(str(out3))}' (DELIMITER '\t', HEADER FALSE);
"""
            try:
                import duckdb
            except ModuleNotFoundError:
                proc = subprocess.run(
                    ["duckdb", "-unsigned"],
                    input=sql,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if proc.returncode != 0:
                    sys.stderr.write(proc.stdout)
                    sys.stderr.write(proc.stderr)
                    raise SystemExit(proc.returncode)
            else:
                con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
                con.execute(sql)
                con.close()

            hello = read_tsv_line(out1)
            echo = read_tsv_line(out2)
            retry = read_tsv_line(out3)

            assert hello == ["true", "200", "hello from ducknng http", "true"], hello
            assert echo == ["true", "200", "01020304", "true", "true"], echo
            assert retry == ["503:1|503:2|200:3", "3"], retry
            assert SmokeHandler.retry_count == 3, SmokeHandler.retry_count
            print("ducknng http smoke: ok")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
