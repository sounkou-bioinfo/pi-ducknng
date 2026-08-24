#!/usr/bin/env python3
"""Require complete measured MC/DC for the declared ducknng pure-C core."""

import json
import os
import sys

EXPECTED = {
    "ducknng_checked.c",
    "ducknng_quack_core.c",
    "ducknng_wire_core.c",
}


def main() -> int:
    report = json.load(sys.stdin)
    data = report.get("data", [])
    if not data:
        print("MC/DC report contains no data", file=sys.stderr)
        return 1
    files = data[0].get("files", [])
    summaries = {
        os.path.basename(item.get("filename", "")): item.get("summary", {})
        for item in files
    }
    missing = EXPECTED.difference(summaries)
    if missing:
        print(f"MC/DC report is missing: {', '.join(sorted(missing))}", file=sys.stderr)
        return 1

    failed = False
    for name in sorted(EXPECTED):
        mcdc = summaries[name].get("mcdc", {})
        covered = int(mcdc.get("covered", 0))
        count = int(mcdc.get("count", 0))
        percent = float(mcdc.get("percent", 0.0))
        print(f"{name}: {covered}/{count} MC/DC conditions ({percent:.2f}%)")
        if count == 0 or covered != count or percent != 100.0:
            failed = True

    if failed:
        print("pure-core MC/DC gate failed", file=sys.stderr)
        return 1
    print("pure-core MC/DC gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
