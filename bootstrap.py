from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from cluster import MAX_SHOOTERS, ProvisionStore, StableConfigGate, resolve_fire_secret


APP_VERSION = "v0030"


def append_bootstrap_log(path: Path, event: str, **fields: object) -> None:
    """Append a token-free bootstrap event to the participant's own data volume."""
    path.parent.mkdir(parents=True, exist_ok=True)
    parts = [time.strftime("%Y-%m-%d %H:%M:%S%z"), event]
    parts.extend(f"{key}={value}" for key, value in sorted(fields.items()))
    line = " ".join(parts) + "\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, line.encode("utf-8", errors="replace"))
    finally:
        os.close(fd)


def safe_append_bootstrap_log(path: Path, event: str, **fields: object) -> None:
    try:
        append_bootstrap_log(path, event, **fields)
    except Exception as exc:
        print(f"bootstrap local log failed: {type(exc).__name__}: {exc}", flush=True)


def safe_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)).strip())
    except (AttributeError, TypeError, ValueError):
        value = int(default)
    return max(minimum, value)


def safe_float_env(name: str, default: float, *, minimum: float = 0.5) -> float:
    try:
        value = float(os.getenv(name, str(default)).strip())
    except (AttributeError, TypeError, ValueError):
        value = float(default)
    return max(minimum, value)


def append_shared_event(store: ProvisionStore, event: str, **fields: object) -> None:
    try:
        store.append_event(event, **fields)
    except Exception as exc:
        print(f"bootstrap shared log failed: {type(exc).__name__}: {exc}", flush=True)


def main() -> None:
    shooter_id = min(MAX_SHOOTERS, safe_int_env("SHOOTER_ID", 1))
    if shooter_id == 1:
        os.execvpe(sys.executable, [sys.executable, "/app/main.py", "bot"], os.environ.copy())

    provision_dir = Path(os.getenv("PROVISION_DIR", "/provision"))
    store = ProvisionStore(provision_dir)
    token_path = Path(
        os.getenv("BOT_TOKEN_FILE", f"/app/provision-token/bot.token")
    )
    poll_seconds = safe_float_env("BOOTSTRAP_POLL_SECONDS", 2.0)
    stable_reads = safe_int_env("CLUSTER_CONFIG_STABLE_READS", 2, minimum=2)
    gate = StableConfigGate(
        current=None,
        required_reads=stable_reads,
        require_new_generation=True,
    )

    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    heartbeat_path = data_dir / "heartbeat.json"
    bootstrap_log_path = data_dir / f"gift-hunter-{APP_VERSION}-bootstrap.log"
    last_heartbeat = 0.0
    last_state: tuple[str, int, int, str] | None = None
    last_invalid_error = ""

    def publish_state(state: str, generation: int, active_shooters: int, reason: str = "") -> None:
        nonlocal last_state
        marker = (state, int(generation), int(active_shooters), reason)
        if marker == last_state:
            return
        last_state = marker
        try:
            store.save_lifecycle(
                shooter_id,
                state=state,
                generation=generation,
                active_shooters=active_shooters,
                reason=reason,
                version=APP_VERSION,
                pid=os.getpid(),
            )
        except Exception as exc:
            print(f"bootstrap lifecycle write failed: {type(exc).__name__}: {exc}", flush=True)
        safe_append_bootstrap_log(
            bootstrap_log_path,
            f"bootstrap_{state}",
            version=APP_VERSION,
            shooter_id=shooter_id,
            generation=generation,
            active_shooters=active_shooters,
            reason=reason or "none",
        )
        append_shared_event(
            store,
            f"bootstrap_{state}",
            version=APP_VERSION,
            shooter_id=shooter_id,
            generation=generation,
            active_shooters=active_shooters,
            reason=reason or "none",
        )

    publish_state("waiting", 0, 1, "waiting_for_stable_config")
    print(f"hunter-{shooter_id} bootstrap sleeping; waiting for activation", flush=True)

    while True:
        now = time.time()
        if now - last_heartbeat >= 10.0:
            current = gate.current
            payload = {
                "timestamp": now,
                "version": APP_VERSION,
                "scanner_active": False,
                "bootstrap_sleeping": True,
                "shooter_id": shooter_id,
                "generation": current.generation if current else 0,
                "active_shooters": current.active_shooters if current else 1,
            }
            try:
                tmp = heartbeat_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(payload), encoding="utf-8")
                tmp.replace(heartbeat_path)
            except OSError as exc:
                print(f"bootstrap heartbeat write failed: {type(exc).__name__}: {exc}", flush=True)
            last_heartbeat = now

        result = store.read_config()
        gate.observe(result)
        if not result.valid:
            error = result.error or "unknown"
            if error != last_invalid_error:
                last_invalid_error = error
                safe_append_bootstrap_log(
                    bootstrap_log_path,
                    "bootstrap_config_invalid",
                    version=APP_VERSION,
                    shooter_id=shooter_id,
                    error=error,
                )
                append_shared_event(
                    store,
                    "bootstrap_config_invalid",
                    version=APP_VERSION,
                    shooter_id=shooter_id,
                    error=error,
                )
        else:
            last_invalid_error = ""

        config = gate.current
        if config is None:
            publish_state("waiting", 0, 1, "waiting_for_stable_config")
            time.sleep(poll_seconds)
            continue

        if shooter_id > config.active_shooters:
            publish_state(
                "sleeping",
                config.generation,
                config.active_shooters,
                "not_selected",
            )
            time.sleep(poll_seconds)
            continue

        fire_secret, _fire_secret_source = resolve_fire_secret(
            provision_dir,
            shooter_id,
            os.getenv("FIRE_SECRET", ""),
        )
        if not fire_secret:
            publish_state(
                "waiting",
                config.generation,
                config.active_shooters,
                "fire_secret_missing",
            )
            time.sleep(poll_seconds)
            continue

        try:
            token = token_path.read_text(encoding="utf-8").strip()
        except (FileNotFoundError, OSError):
            token = ""
        if not token:
            publish_state(
                "waiting",
                config.generation,
                config.active_shooters,
                "token_missing",
            )
            time.sleep(poll_seconds)
            continue

        publish_state(
            "starting",
            config.generation,
            config.active_shooters,
            "selected_and_token_present",
        )
        env = os.environ.copy()
        env["BOT_TOKEN"] = token
        print(f"hunter-{shooter_id} activated", flush=True)
        os.execvpe(sys.executable, [sys.executable, "/app/main.py", "bot"], env)


if __name__ == "__main__":
    main()
