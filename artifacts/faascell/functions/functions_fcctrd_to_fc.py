#!/usr/bin/env python3

"""Transform FaaSRail ShrinkRay's function entries into `rt-fc`-compatible entries.

Reads a JSON array from stdin and writes a JSON array to stdout.
"""

import json
import sys
from typing import Any, Iterable


def _entrypoint(fn_entry: dict[str, Any]) -> str:
    for key in ("process_args", "entrypoint"):
        val = fn_entry.get(key)
        if isinstance(val, str) and val:
            return val
    raise KeyError('function entry has no field compatible with `rt-fc` "entrypoint"')


def transform(fn_entries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    ret = []
    for fn_entry in fn_entries:
        ret.append(
            {
                "id": fn_entry["id"],
                "memory": fn_entry["memory"],
                "vcpu_count": fn_entry.get("vcpu_count", 1),
            }
        )
        # Populate "entrypoint" on a best-effort basis:
        try:
            ret[-1]["entrypoint"] = _entrypoint(fn_entry)
        except KeyError:
            pass
    return ret


def main() -> int:
    data = json.load(sys.stdin)
    if not isinstance(data, list):
        raise SystemExit("ERROR: expected a JSON array on stdin")

    json.dump(transform(data), sys.stdout, indent=4)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
