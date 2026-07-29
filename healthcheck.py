"""Lightweight Docker healthcheck for Gift Hunter.

The bot writes /app/data/heartbeat.json every 10 seconds.  This probe uses only
Python's standard library, so it finishes quickly and does not import aiogram or
Telethon on every Docker healthcheck run.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def main() -> int:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    heartbeat_path = data_dir / "heartbeat.json"

    try:
        max_age_seconds = float(os.getenv("HEALTHCHECK_MAX_AGE_SECONDS", "45"))
    except ValueError:
        print("HEALTHCHECK_MAX_AGE_SECONDS must be a number", file=sys.stderr)
        return 1

    if max_age_seconds <= 0:
        print("HEALTHCHECK_MAX_AGE_SECONDS must be greater than zero", file=sys.stderr)
        return 1

    try:
        payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        timestamp = float(payload["timestamp"])
    except FileNotFoundError:
        print(f"heartbeat not found: {heartbeat_path}", file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        print(f"invalid heartbeat: {type(exc).__name__}", file=sys.stderr)
        return 1

    age_seconds = time.time() - timestamp
    if age_seconds < -5:
        print("heartbeat timestamp is in the future", file=sys.stderr)
        return 1
    if age_seconds > max_age_seconds:
        print(
            f"heartbeat is stale: {age_seconds:.1f}s > {max_age_seconds:.1f}s",
            file=sys.stderr,
        )
        return 1

    print(f"ok: heartbeat age {max(0.0, age_seconds):.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
