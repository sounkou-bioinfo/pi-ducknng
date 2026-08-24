#!/usr/bin/env python3
"""Native round-trip smoke test for the server ducknng-frame-over-WebSocket
endpoint (issue #11).

Starts an HTTP RPC server (which also brings up the sibling raw-WebSocket frame
endpoint), then acts as a browser would: opens a raw WS, sends ducknng frames as
binary messages, and reads the reply frames back -- no browser required. The WS
client here is the exact wire a browser JS WebSocket drives.

Usage: test/ws_smoke.py <extension_path>
"""
from __future__ import annotations

import struct
import sys
import pathlib

import websocket  # websocket-client

WIRE_VERSION = 1
RPC_MANIFEST = 0
RPC_ERROR = 3
HEADER_LEN = 22


def build_frame(rtype: int, flags: int = 0, name: bytes = b"", payload: bytes = b"") -> bytes:
    """Encode a ducknng wire frame: 22-byte header then name/error/payload."""
    hdr = bytearray(HEADER_LEN)
    hdr[0] = WIRE_VERSION
    hdr[1] = rtype
    struct.pack_into("<I", hdr, 2, flags)
    struct.pack_into("<I", hdr, 6, len(name))
    struct.pack_into("<I", hdr, 10, 0)  # error_len
    struct.pack_into("<Q", hdr, 14, len(payload))
    return bytes(hdr) + name + payload


def parse_frame(buf: bytes) -> dict:
    if len(buf) < HEADER_LEN:
        raise AssertionError(f"reply shorter than a frame header: {len(buf)} bytes")
    version = buf[0]
    rtype = buf[1]
    flags = struct.unpack_from("<I", buf, 2)[0]
    name_len = struct.unpack_from("<I", buf, 6)[0]
    error_len = struct.unpack_from("<I", buf, 10)[0]
    payload_len = struct.unpack_from("<Q", buf, 14)[0]
    total = HEADER_LEN + name_len + error_len + payload_len
    if len(buf) != total:
        raise AssertionError(f"frame length mismatch: got {len(buf)}, header implies {total}")
    off = HEADER_LEN
    name = buf[off:off + name_len]; off += name_len
    error = buf[off:off + error_len]; off += error_len
    payload = buf[off:off + payload_len]
    return {
        "version": version, "type": rtype, "flags": flags,
        "name": name, "error": error, "payload": payload,
    }


def http_to_ws(listen: str) -> str:
    """http://h:p/path -> ws://h:p/path/ws (matches the server's derivation)."""
    if listen.startswith("https://"):
        base = "wss://" + listen[len("https://"):]
    elif listen.startswith("http://"):
        base = "ws://" + listen[len("http://"):]
    else:
        raise AssertionError(f"unexpected listen url: {listen}")
    return base + ("ws" if base.endswith("/") else "/ws")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: test/ws_smoke.py <extension_path>")
    ext_path = pathlib.Path(sys.argv[1]).resolve()
    if not ext_path.exists():
        raise SystemExit(f"extension not found: {ext_path}")

    import duckdb
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{str(ext_path).replace(chr(39), chr(39)*2)}';")
    con.execute(
        "SELECT ducknng_start_server('ws_smoke', 'http://127.0.0.1:0/_ducknng', "
        "1, 4194304, 300000, 0::UBIGINT);"
    )
    try:
        listen = con.execute(
            "SELECT listen FROM ducknng_list_servers() WHERE name = 'ws_smoke'"
        ).fetchone()[0]
        ws_url = http_to_ws(listen)

        ws = websocket.create_connection(ws_url, timeout=5)
        try:
            # 1) MANIFEST round trip, twice, over one persistent connection.
            for attempt in (1, 2):
                ws.send_binary(build_frame(RPC_MANIFEST))
                reply = parse_frame(ws.recv())
                assert reply["version"] == WIRE_VERSION, reply
                assert reply["error"] == b"", (attempt, reply["error"])
                assert reply["type"] != RPC_ERROR, (attempt, reply)
                assert len(reply["payload"]) > 0, (attempt, "empty manifest payload")

            # 2) A malformed frame is answered with an error frame, and the
            #    connection remains usable afterwards.
            ws.send_binary(b"\x99\x99\x99\x99\x99")
            err = parse_frame(ws.recv())
            assert err["type"] == RPC_ERROR, err
            assert len(err["error"]) > 0, "error frame had no message"

            # 3) Prove the connection survived the rejected frame.
            ws.send_binary(build_frame(RPC_MANIFEST))
            again = parse_frame(ws.recv())
            assert again["type"] != RPC_ERROR and again["error"] == b"", again
        finally:
            ws.close()
        print("ducknng ws smoke: ok")
    finally:
        con.execute("SELECT ducknng_stop_server('ws_smoke');")
        con.close()


if __name__ == "__main__":
    main()
