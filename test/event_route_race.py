#!/usr/bin/env python3
"""Stop an HTTP server while event-route relays are still starting.

Each round starts a burst of HTTP clients on an event route and stops the
server without waiting for them, retrying while ducknng_stop_server() reports
an inflight request. The stop must never hang, every client must see its
response end, and no relay may be freed while its start path still uses it;
run this against an AddressSanitizer build to check the last.

usage: test/event_route_race.py <extension_path> [rounds]
"""
from __future__ import annotations

import http.client
import pathlib
import sys
import threading
import time
import urllib.parse

import duckdb

CLIENTS = 16


def follow(host: str, port: int, ended: list[bool], index: int) -> None:
    """Read one stream to its end; a refusal or reset also ends it, but a
    client left waiting until its own timeout does not."""
    try:
        client = http.client.HTTPConnection(host, port, timeout=10)
        client.request("GET", "/events")
        client.getresponse().read()
        client.close()
    except TimeoutError:
        return
    except (ConnectionError, http.client.HTTPException, OSError):
        pass
    ended[index] = True


def main() -> None:
    if len(sys.argv) not in (2, 3):
        raise SystemExit(__doc__.strip().splitlines()[-1])
    ext_path = pathlib.Path(sys.argv[1]).resolve()
    rounds = int(sys.argv[2]) if len(sys.argv) == 3 else 40
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext_path}'")

    def one(sql: str):
        return con.execute(sql).fetchone()[0]

    pub = one("SELECT (ducknng_open_socket('pub')).socket_id")
    assert one(f"SELECT (ducknng_listen_socket({pub}, 'inproc://event_route_race', 1048576, 0::UBIGINT)).ok")
    slowest = 0.0
    for round_number in range(rounds):
        name = f"race_{round_number}"
        assert one(f"SELECT ducknng_start_server('{name}', 'http://127.0.0.1:0/_ducknng', 1, "
                   "134217728, 300000, 0::UBIGINT)")
        listen = urllib.parse.urlparse(
            one(f"SELECT listen::VARCHAR FROM ducknng_list_servers() WHERE name = '{name}'"))
        assert one(f"SELECT ducknng_add_event_route('{name}', '/events', "
                   "'SELECT ''inproc://event_route_race'' AS url', 60000)")
        ended = [False] * CLIENTS
        clients = [threading.Thread(target=follow, args=(listen.hostname, listen.port, ended, i))
                   for i in range(CLIENTS)]
        for client in clients:
            client.start()
        # Vary the delay so stops land before, during, and after relay starts.
        time.sleep((round_number % 8) * 0.002)
        started = time.monotonic()
        while not one(f"SELECT ducknng_stop_server('{name}')"):
            if time.monotonic() - started > 10:
                raise SystemExit(f"round {round_number}: stop did not succeed within 10 s")
            time.sleep(0.001)
        slowest = max(slowest, time.monotonic() - started)
        for client in clients:
            client.join(15)
        if not all(ended):
            raise SystemExit(f"round {round_number}: {ended.count(False)} streams did not end")
    assert one(f"SELECT (ducknng_close_socket({pub})).ok")
    print(f"event_route_race: {rounds} rounds of {CLIENTS} clients, slowest stop {slowest * 1000:.0f} ms")


if __name__ == "__main__":
    main()
