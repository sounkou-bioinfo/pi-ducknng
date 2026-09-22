#!/usr/bin/env python3
"""Follow an event route with an ordinary HTTP client.

Python's http.client stands in for a browser EventSource: it reads the chunked
text/event-stream body incrementally while SQL publishes on the PUB socket the
route subscribes to, then sees the stream end when the server stops.

usage: test/event_route_smoke.py <extension_path>
"""
from __future__ import annotations

import http.client
import pathlib
import sys
import urllib.parse

import duckdb


def read_event(response: http.client.HTTPResponse) -> bytes:
    """Read one SSE block: lines up to and including the blank line."""
    lines = []
    while True:
        line = response.readline()
        if not line:
            raise AssertionError(f"stream ended inside an event: {b''.join(lines)!r}")
        lines.append(line)
        if line == b"\n":
            return b"".join(lines)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__.strip().splitlines()[-1])
    ext_path = pathlib.Path(sys.argv[1]).resolve()
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext_path}'")

    def one(sql: str, *params):
        return con.execute(sql, list(params)).fetchone()[0]

    assert one("SELECT ducknng_start_server('smoke_events', 'http://127.0.0.1:0/_ducknng', 1, "
               "134217728, 300000, 0::UBIGINT)")
    listen = urllib.parse.urlparse(
        one("SELECT listen::VARCHAR FROM ducknng_list_servers() WHERE name = 'smoke_events'"))
    pub = one("SELECT (ducknng_open_socket('pub')).socket_id")
    assert one(f"SELECT (ducknng_listen_socket({pub}, 'inproc://event_route_smoke', 1048576, 0::UBIGINT)).ok")
    assert one("SELECT ducknng_add_event_route('smoke_events', '/events', "
               "'SELECT ''inproc://event_route_smoke'' AS url, "
               "ducknng_http_query_param(''topic'') AS topic, ''hint'' AS event', 60000)")

    def publish(message: bytes) -> None:
        assert one(f"SELECT (ducknng_send_socket_raw({pub}, $1::BLOB, 1000)).ok", message)

    client = http.client.HTTPConnection(listen.hostname, listen.port, timeout=10)
    client.request("GET", "/events?topic=alpha", headers={"Accept": "text/event-stream"})
    response = client.getresponse()
    assert response.status == 200, response.status
    assert response.getheader("Content-Type").startswith("text/event-stream")
    assert read_event(response) == b": ready\n\n"

    publish(b"beta:filtered")
    publish(b"alpha:one")
    assert read_event(response) == b"event: hint\ndata: alpha:one\n\n"
    publish(b"alpha\r\ntwo\rthree")
    assert read_event(response) == b"event: hint\ndata: alpha\ndata: two\ndata: three\n\n"

    assert one("SELECT ducknng_stop_server('smoke_events')")
    assert response.read() == b"", "stream did not end when the server stopped"
    client.close()
    assert one(f"SELECT (ducknng_close_socket({pub})).ok")
    print("event_route_smoke: relayed filtered, multi-line events and ended on stop")


if __name__ == "__main__":
    main()
