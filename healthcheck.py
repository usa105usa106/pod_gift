"""Fast Docker healthcheck for Gift Hunter v0029.

The main process writes /app/data/heartbeat.json every ten seconds. This probe
uses only the Python standard library and never starts a second bot instance.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


APP_VERSION = "v0029"


def main() -> int:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    heartbeat_path = data_dir / "heartbeat.json"

    try:
        max_age = float(os.getenv("HEALTHCHECK_MAX_AGE_SECONDS", "45"))
    except (TypeError, ValueError):
        max_age = 45.0
    if max_age <= 0:
        max_age = 45.0

    try:
        payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        timestamp = float(payload["timestamp"])
        version = str(payload.get("version", "unknown"))
    except FileNotFoundError:
        print(f"heartbeat not found: {heartbeat_path}", file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        print(f"invalid heartbeat: {type(exc).__name__}", file=sys.stderr)
        return 1

    age = time.time() - timestamp
    if age < -5:
        print("heartbeat timestamp is in the future", file=sys.stderr)
        return 1
    if age > max_age:
        print(f"heartbeat stale: {age:.1f}s > {max_age:.1f}s", file=sys.stderr)
        return 1

    print(f"ok: {version}, heartbeat age {max(0.0, age):.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
