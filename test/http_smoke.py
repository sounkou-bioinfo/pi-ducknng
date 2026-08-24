#!/usr/bin/env python3
from __future__ import annotations

import http.server
import pathlib
import subprocess
import sys
import tempfile
import threading
import time


class SmokeHandler(http.server.BaseHTTPRequestHandler):
    retry_count = 0
    retry_lock = threading.Lock()

    def do_GET(self) -> None:
        if self.path == "/stream":
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for body in (b'{"event":1}\n', b'{"event":2}\n'):
                self.wfile.write(f"{len(body):x}\r\n".encode("ascii"))
                self.wfile.write(body + b"\r\n")
                self.wfile.flush()
                time.sleep(0.2)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            return
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
            out4 = tmpdir_path / "stream.tsv"
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
CREATE TEMP TABLE stream_open_aio AS
SELECT ducknng_ncurl_stream_open_aio(
  'http://127.0.0.1:{port}/stream', 'GET', NULL, NULL, 2000, 0::UBIGINT
) AS aio_id;
CREATE TEMP TABLE stream_open AS
SELECT * FROM ducknng_ncurl_stream_open_aio_collect(
  (SELECT list_value(aio_id) FROM stream_open_aio), 2000
);
CREATE TEMP TABLE stream_results(seq INTEGER, body BLOB, end_of_stream BOOLEAN);
CREATE TEMP TABLE stream_recv_aio AS
SELECT ducknng_ncurl_stream_recv_aio(
  (SELECT stream_id FROM stream_open), 65536::UBIGINT, 2000
) AS aio_id;
INSERT INTO stream_results
SELECT 1, body, end_of_stream
FROM ducknng_ncurl_stream_recv_aio_collect(
  (SELECT list_value(aio_id) FROM stream_recv_aio), 2000
);
SELECT ducknng_aio_drop(aio_id) FROM stream_recv_aio;
DELETE FROM stream_recv_aio;
INSERT INTO stream_recv_aio
SELECT ducknng_ncurl_stream_recv_aio(
  (SELECT stream_id FROM stream_open), 65536::UBIGINT, 2000
);
INSERT INTO stream_results
SELECT 2, body, end_of_stream
FROM ducknng_ncurl_stream_recv_aio_collect(
  (SELECT list_value(aio_id) FROM stream_recv_aio), 2000
);
SELECT ducknng_aio_drop(aio_id) FROM stream_recv_aio;
DELETE FROM stream_recv_aio;
INSERT INTO stream_recv_aio
SELECT ducknng_ncurl_stream_recv_aio(
  (SELECT stream_id FROM stream_open), 65536::UBIGINT, 2000
);
INSERT INTO stream_results
SELECT 3, body, end_of_stream
FROM ducknng_ncurl_stream_recv_aio_collect(
  (SELECT list_value(aio_id) FROM stream_recv_aio), 2000
);
COPY (
  SELECT seq, coalesce(hex(body), ''), end_of_stream
  FROM stream_results ORDER BY seq
) TO '{sql_quote(str(out4))}' (DELIMITER '\t', HEADER FALSE);
SELECT ducknng_aio_drop(aio_id) FROM stream_recv_aio;
SELECT ducknng_ncurl_stream_close(stream_id) FROM stream_open;
SELECT ducknng_aio_drop(aio_id) FROM stream_open_aio;
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
            stream = out4.read_text(encoding="utf-8").strip().splitlines()

            assert hello == ["true", "200", "hello from ducknng http", "true"], hello
            assert echo == ["true", "200", "01020304", "true", "true"], echo
            assert retry == ["503:1|503:2|200:3", "3"], retry
            assert stream == [
                "1\t7B226576656E74223A317D0A\tfalse",
                "2\t7B226576656E74223A327D0A\tfalse",
                '3\t""\ttrue',
            ], stream
            assert SmokeHandler.retry_count == 3, SmokeHandler.retry_count
            print("ducknng http smoke: ok")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
