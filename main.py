from __future__ import annotations

import argparse
import asyncio
import contextlib
import gc
import faulthandler
import html
import inspect
import json
import logging
import os
import re
import resource
import shutil
import signal
import statistics
import socket
import sys
import threading
import time
import tempfile
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    FSInputFile,
)
from telethon import TelegramClient, errors, functions, types, utils

from cluster import (
    ClusterBus,
    ClusterConfig,
    MAX_SHOOTERS,
    ProvisionStore,
    ShooterStatus,
    StableConfigGate,
    campaign_id_for,
    resolve_fire_secret,
)

from logic import (
    AdaptiveRateController,
    base_slug_from_unique,
    evaluate_target,
    find_object_by_class_name,
    next_target,
    parse_bool,
    parse_target_numbers,
    nearest_rank_percentile,
    slug_candidates,
    sum_invoice_amount,
    stress_test_interval_ms,
)


def env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    """Read an integer environment variable without crashing on a typo."""
    try:
        value = int(os.getenv(name, str(default)).strip())
    except (AttributeError, TypeError, ValueError):
        value = int(default)
    return max(minimum, value) if minimum is not None else value


def env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    """Read a float environment variable without crashing on a typo."""
    try:
        value = float(os.getenv(name, str(default)).strip())
    except (AttributeError, TypeError, ValueError):
        value = float(default)
    return max(minimum, value) if minimum is not None else value


APP_VERSION = "v0039"
APP_NAME = "Gift Hunter"
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
SETTINGS_PATH = DATA_DIR / "settings.json"
HEARTBEAT_PATH = DATA_DIR / "heartbeat.json"
LOG_PATH = DATA_DIR / f"gift-hunter-{APP_VERSION}.log"
PAYMENT_AUDIT_PATH = DATA_DIR / f"gift-hunter-{APP_VERSION}-payment-audit.jsonl"
DIAGNOSTICS_PATH = DATA_DIR / "diagnostics.json"
STRESS_REPORT_PATH = DATA_DIR / "stress-test-latest.json"
STRESS_HISTORY_PATH = DATA_DIR / "stress-tests.jsonl"
CATALOG_REPORT_PATH = DATA_DIR / "catalog-numbers-latest.json"
RATE_LIMIT_PATH = DATA_DIR / "rate-limit.json"
PAYMENT_GUARD_PATH = DATA_DIR / "payment-submit-guard.json"
SCANNER_RESUME_PATH = DATA_DIR / "scanner-resume.json"
PENDING_PAYMENT_HOLD_MESSAGE = (
    "Есть платёж с неподтверждённым результатом. Повторная оплата и изменение "
    "связанных настроек заблокированы до сверки с Telegram."
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MT_SESSION = os.getenv("MT_SESSION", "").strip()
SETUP_PIN = os.getenv("SETUP_PIN", "").strip()
ADAPTIVE_SCAN = parse_bool(os.getenv("ADAPTIVE_SCAN", "true"), True)
SCAN_START_INTERVAL_MS = env_int("SCAN_START_INTERVAL_MS", 120, minimum=0)
SCAN_MIN_INTERVAL_MS = env_int("SCAN_MIN_INTERVAL_MS", 0, minimum=0)
SCAN_MAX_INTERVAL_MS = max(SCAN_MIN_INTERVAL_MS, env_int("SCAN_MAX_INTERVAL_MS", 2000, minimum=0))
SCAN_ACCELERATE_EVERY = env_int("SCAN_ACCELERATE_EVERY", 4, minimum=1)
SCAN_ACCELERATE_FACTOR = min(0.99, max(0.10, env_float("SCAN_ACCELERATE_FACTOR", 0.75)))
SCAN_BACKOFF_FACTOR = max(1.10, env_float("SCAN_BACKOFF_FACTOR", 2.0))
SCAN_BACKOFF_FLOOR_MS = max(SCAN_MIN_INTERVAL_MS, env_int("SCAN_BACKOFF_FLOOR_MS", 100, minimum=0))
FLOOD_WAIT_EXTRA_MS = env_int("FLOOD_WAIT_EXTRA_MS", 150, minimum=0)
NEAR_TARGET_DISTANCE = env_int("NEAR_TARGET_DISTANCE", 25, minimum=1)
PREPARE_AHEAD = env_int("PREPARE_AHEAD", 100, minimum=1)
# Telegram Stars payment forms expire after 10 minutes. Keep a generous safety
# margin and continuously refresh prepared forms while the scanner is active.
#
# Refresh policy is intentionally built in so a Coolify redeploy picks it up
# without requiring an environment-variable rename or manual migration:
#   * farther than 50 numbers from the next target -> refresh after 5 minutes;
#   * within 50 numbers -> refresh after 2 minutes.
# The 1-second worker tick only checks ages; it does not call Telegram every second.
TELEGRAM_PAYMENT_FORM_TTL_SECONDS = 600
PAYMENT_FORM_MAX_AGE_SECONDS = min(
    540, env_int("PAYMENT_FORM_MAX_AGE_SECONDS", 480, minimum=120)
)
PAYMENT_FORM_NEAR_TARGET_DISTANCE = 50
PAYMENT_FORM_FAR_REFRESH_SECONDS = min(PAYMENT_FORM_MAX_AGE_SECONDS - 60, 300)
PAYMENT_FORM_NEAR_REFRESH_SECONDS = min(PAYMENT_FORM_FAR_REFRESH_SECONDS, 120)
# Internal compatibility name used by plan preparation/tests: this is the far-zone
# age, while the background worker switches to the near-zone age dynamically.
PREPARE_REFRESH_SECONDS = PAYMENT_FORM_FAR_REFRESH_SECONDS
FORM_REFRESH_TICK_SECONDS = env_float("FORM_REFRESH_TICK_SECONDS", 1.0, minimum=0.25)
FORM_REFRESH_BATCH_SIZE = min(50, env_int("FORM_REFRESH_BATCH_SIZE", 1, minimum=1))
FORM_REFRESH_MAX_BATCH_SIZE = max(
    FORM_REFRESH_BATCH_SIZE,
    min(50, env_int("FORM_REFRESH_MAX_BATCH_SIZE", 50, minimum=1)),
)
FORM_REFRESH_LATENCY_INITIAL_SECONDS = env_float(
    "FORM_REFRESH_LATENCY_INITIAL_SECONDS", 0.5, minimum=0.05
)
FORM_REFRESH_LATENCY_EWMA_ALPHA = min(
    1.0,
    max(0.05, env_float("FORM_REFRESH_LATENCY_EWMA_ALPHA", 0.25, minimum=0.0)),
)
DEFAULT_MAX_UPGRADE_STARS = 3000
MAX_UPGRADE_STARS = env_int("MAX_UPGRADE_STARS", DEFAULT_MAX_UPGRADE_STARS, minimum=0)
DIAGNOSTICS_INTERVAL_SECONDS = env_float("DIAGNOSTICS_INTERVAL_SECONDS", 1.0, minimum=0.5)
VERIFY_DELAYS_SECONDS = (0.10, 0.25, 0.50, 1.0, 2.0, 3.0)
STOP_AFTER_SUCCESS = parse_bool(os.getenv("STOP_AFTER_SUCCESS", "false"), False)
KEEP_ORIGINAL_DETAILS = parse_bool(os.getenv("KEEP_ORIGINAL_DETAILS", "true"), True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = env_int("LOG_MAX_BYTES", 25_000_000, minimum=1_000_000)
LOG_BACKUP_COUNT = env_int("LOG_BACKUP_COUNT", 12, minimum=1)
PAYMENT_AUDIT_MAX_BYTES = env_int("PAYMENT_AUDIT_MAX_BYTES", 25_000_000, minimum=1_000_000)
PAYMENT_AUDIT_BACKUP_COUNT = env_int("PAYMENT_AUDIT_BACKUP_COUNT", 12, minimum=1)
PAYMENT_AUDIT_HEALTHCHECK_SECONDS = env_float(
    "PAYMENT_AUDIT_HEALTHCHECK_SECONDS", 60.0, minimum=10.0
)
LOG_FULL_WINDOW_SECONDS = 24 * 60 * 60
# Keep a little room below Telegram's nominal 50 MB document ceiling.
LOG_FULL_TELEGRAM_LIMIT_BYTES = 49_000_000
LOG_FULL_PART_TARGET_BYTES = 45_000_000
LOG_FULL_FRAGMENT_BYTES = 20_000_000

STRESS_TEST_DURATION_SECONDS = 300.0
STRESS_FIRST_PHASE_SECONDS = 60.0
STRESS_MAX_PHASE_START_SECONDS = 120.0
STRESS_FIRST_INTERVAL_MS = 300.0
STRESS_SECOND_INTERVAL_MS = 120.0
STRESS_MAX_INTERVAL_MS = 0.0
# Long-running scanners must not edit one Telegram message every few seconds:
# Telegram eventually applies very long EditMessageText RetryAfter penalties.
# The card now gets a low-frequency heartbeat and is refreshed immediately when
# the observed collectible number changes.
LIVE_STATUS_INTERVAL_SECONDS = env_float("LIVE_STATUS_INTERVAL_SECONDS", 60.0, minimum=60.0)
STATUS_URGENT_MIN_INTERVAL_SECONDS = env_float("STATUS_URGENT_MIN_INTERVAL_SECONDS", 1.0, minimum=0.5)
STRESS_STATUS_INTERVAL_SECONDS = env_float("STRESS_STATUS_INTERVAL_SECONDS", 3.0, minimum=1.0)
STATUS_BAD_REQUEST_COOLDOWN_SECONDS = env_float("STATUS_BAD_REQUEST_COOLDOWN_SECONDS", 900.0, minimum=60.0)
STATUS_TRANSIENT_FAILURE_COOLDOWN_SECONDS = env_float("STATUS_TRANSIENT_FAILURE_COOLDOWN_SECONDS", 60.0, minimum=10.0)
STATUS_MANUAL_REFRESH_COOLDOWN_SECONDS = env_float("STATUS_MANUAL_REFRESH_COOLDOWN_SECONDS", 60.0, minimum=60.0)
TASK_STOP_TIMEOUT_SECONDS = env_float("TASK_STOP_TIMEOUT_SECONDS", 4.0, minimum=1.0)
CATALOG_CONCURRENCY = env_int("CATALOG_CONCURRENCY", 6, minimum=1)
SPECIAL_NUMBER_MAX_DISTANCE = 100
SPECIAL_NUMBER_LENGTHS = (4, 5, 6)
EXACT_PROBE_DISTANCE = env_int("EXACT_PROBE_DISTANCE", 100, minimum=2)
EXACT_COUNTER_REFRESH_SECONDS = env_float("EXACT_COUNTER_REFRESH_SECONDS", 1.0, minimum=0.2)
MAX_PRIMARY_VOLLEY_SIZE = 50
MAX_SECONDARY_VOLLEY_SIZE = 50
DEFAULT_FAST_QUIET_DISTANCE = 10
FAST_QUIET_DISTANCE = env_int("FAST_QUIET_DISTANCE", DEFAULT_FAST_QUIET_DISTANCE, minimum=1)
# Gap between the queue-start of adjacent Stars submits in a FAST volley.
# 10 ms is the default selected for the next live test: simultaneous submits
# have collided with Telegram's duplicate-payment protection, while a small
# stagger avoids the old ~0.5 s ordered/dependency penalty. This is user-configurable
# in settings via /stagger and persisted in settings.json.
DEFAULT_FAST_VOLLEY_STAGGER_MS = 10
MAX_FAST_VOLLEY_STAGGER_MS = 1000
# Force-refresh the full FAST payment set once when the frontier enters the same
# 50-number near-target zone used by the faster form-refresh policy. This value
# is built in deliberately: old Coolify FAST_FORM_FREEZE_DISTANCE settings are
# ignored and no environment-variable migration is required on redeploy.
FAST_FORM_ARM_DISTANCE = max(FAST_QUIET_DISTANCE + 1, PAYMENT_FORM_NEAR_TARGET_DISTANCE)
FAST_DISABLE_GC = parse_bool(os.getenv("FAST_DISABLE_GC", "true"), True)
FAST_CPU_AFFINITY = os.getenv("FAST_CPU_AFFINITY", "").strip()
SHOOTER_ID = min(MAX_SHOOTERS, max(1, env_int("SHOOTER_ID", 1, minimum=1)))
PRIMARY_SHOOTER = SHOOTER_ID == 1
PROVISION_DIR = Path(os.getenv("PROVISION_DIR", "/provision"))
FIRE_PORT = env_int("FIRE_PORT", 45444, minimum=1024)
FIRE_SECRET, FIRE_SECRET_SOURCE = resolve_fire_secret(
    PROVISION_DIR,
    SHOOTER_ID,
    os.getenv("FIRE_SECRET", ""),
)
CLUSTER_STATUS_INTERVAL_SECONDS = env_float("CLUSTER_STATUS_INTERVAL_SECONDS", 60.0, minimum=60.0)
CLUSTER_STALE_SECONDS = env_float("CLUSTER_STALE_SECONDS", 180.0, minimum=120.0)
CLUSTER_PING_TIMEOUT_MS = env_int("CLUSTER_PING_TIMEOUT_MS", 500, minimum=100)
CLUSTER_CONFIG_POLL_SECONDS = env_float("CLUSTER_CONFIG_POLL_SECONDS", 2.0, minimum=0.5)
CLUSTER_CONFIG_STABLE_READS = env_int("CLUSTER_CONFIG_STABLE_READS", 2, minimum=2)
CLUSTER_RECONFIG_CONFIRM_SECONDS = env_float("CLUSTER_RECONFIG_CONFIRM_SECONDS", 10.0, minimum=3.0)
WATCHDOG_STALL_SECONDS = env_float("WATCHDOG_STALL_SECONDS", 90.0, minimum=30.0)
WATCHDOG_CHECK_SECONDS = env_float("WATCHDOG_CHECK_SECONDS", 5.0, minimum=1.0)
PROCESS_STARTED_MONOTONIC = time.monotonic()
_EVENT_LOOP_LAST_TICK = time.monotonic()
_WATCHDOG_STOP = threading.Event()


MSK_TIMEZONE = timezone(timedelta(hours=3), name="MSK")

def msk_now() -> datetime:
    """Return wall-clock time for display only; scanner timing stays monotonic."""
    return datetime.now(MSK_TIMEZONE)

def msk_time_str() -> str:
    return msk_now().strftime("%H:%M:%S")

def format_duration(seconds: float) -> str:
    total = max(0, int(float(seconds) + 0.999))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}ч {minutes:02d}м {secs:02d}с"
    if minutes:
        return f"{minutes}м {secs:02d}с"
    return f"{secs}с"


def format_process_uptime(seconds: float) -> str:
    total = max(0, int(seconds))
    days, remainder = divmod(total, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    if days:
        return f"{days}д {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _read_positive_int(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        if not raw or raw == "max":
            return None
        value = int(raw)
        return value if value > 0 else None
    except (FileNotFoundError, OSError, TypeError, ValueError):
        return None


def _host_memory_total_bytes() -> int | None:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, OSError, IndexError, TypeError, ValueError):
        return None
    return None


def current_memory_bytes() -> int:
    """Return current container memory when available, otherwise process RSS."""
    for path in (
        Path("/sys/fs/cgroup/memory.current"),
        Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    ):
        value = _read_positive_int(path)
        if value is not None:
            return value
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return max(0, int(line.split()[1]) * 1024)
    except (FileNotFoundError, OSError, IndexError, TypeError, ValueError):
        pass
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    multiplier = 1 if sys.platform == "darwin" else 1024
    return max(0, int(max_rss) * multiplier)


def memory_limit_bytes() -> int | None:
    """Return the effective cgroup memory limit, or None when it is unlimited."""
    for path in (
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ):
        value = _read_positive_int(path)
        if value is None:
            continue
        host_total = _host_memory_total_bytes()
        if value >= (1 << 60):
            return None
        if host_total is not None and value > host_total * 4:
            return None
        return value
    return None


def format_memory_mb(value_bytes: int) -> str:
    value_mb = max(0, int(round(int(value_bytes) / (1024 * 1024))))
    return f"{value_mb:,}".replace(",", " ") + " МБ"


def nearest_special_number(current: int) -> tuple[int | None, int | None]:
    """Return the next 4-6 digit repdigit and its forward distance.

    Examples: 4344 -> (4444, 100), 444350 -> (444444, 94).
    The current number itself is considered near with distance zero.
    """
    value = max(0, int(current))
    candidates = sorted(
        int(str(digit) * length)
        for length in SPECIAL_NUMBER_LENGTHS
        for digit in range(1, 10)
    )
    for target in candidates:
        if target >= value:
            return target, target - value
    return None, None


def catalog_number_is_near_special(
    current: int,
    total: int | None = None,
) -> tuple[bool, int | None, int | None]:
    target, distance = nearest_special_number(current)
    target_is_reachable = total is None or target is None or target <= int(total)
    highlighted = (
        target_is_reachable
        and distance is not None
        and 0 <= distance <= SPECIAL_NUMBER_MAX_DISTANCE
    )
    return highlighted, target, distance


def apply_fast_cpu_affinity() -> None:
    """Optionally pin the process to one allowed CPU core.

    Leave FAST_CPU_AFFINITY empty by default.  On a dedicated-vCPU service it
    can be set to an integer core index after checking the container's cpuset.
    """
    if not FAST_CPU_AFFINITY:
        return
    try:
        core = int(FAST_CPU_AFFINITY)
        allowed = set(os.sched_getaffinity(0))
        if core not in allowed:
            raise ValueError(f"core {core} not in allowed cpuset {sorted(allowed)}")
        os.sched_setaffinity(0, {core})
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        logger.warning("fast_cpu_affinity_not_applied value=%s error=%s", FAST_CPU_AFFINITY, exc)
    else:
        logger.info("fast_cpu_affinity_applied core=%s", core)


DATA_DIR.mkdir(parents=True, exist_ok=True)
PROVISION_DIR.mkdir(parents=True, exist_ok=True)
_PROVISION_TOKEN_DIR_RAW = os.getenv("PROVISION_TOKEN_DIR", "").strip()
PROVISION_TOKEN_DIR = Path(_PROVISION_TOKEN_DIR_RAW) if _PROVISION_TOKEN_DIR_RAW else None
provision_store = ProvisionStore(PROVISION_DIR, token_root=PROVISION_TOKEN_DIR)


def cluster_config() -> ClusterConfig:
    return provision_store.load_config()


def active_shooter_count() -> int:
    # Once ClusterRuntime exists, use its accepted config from RAM.  This keeps
    # status/heartbeat code away from the shared volume and cannot affect the
    # scanner/payment loop.
    cluster = globals().get("cluster_runtime")
    applied = getattr(cluster, "applied_config", None) if cluster is not None else None
    if applied is not None:
        return int(applied.active_shooters)
    return cluster_config().active_shooters


def max_volley_size_for_shooter(shooter_id: int) -> int:
    """Return the local account limit without reading shared cluster state.

    This function is safe to call in the payment hot path: it depends only on
    the immutable process role and performs no filesystem or network work.
    """
    return MAX_PRIMARY_VOLLEY_SIZE if int(shooter_id) == 1 else MAX_SECONDARY_VOLLEY_SIZE


def effective_max_volley_size() -> int:
    return max_volley_size_for_shooter(SHOOTER_ID)



LOG_ROTATION_COORD_LOCK = threading.RLock()


class CoordinatedRotatingFileHandler(RotatingFileHandler):
    """Rotating handler whose namespace changes are coordinated with /log_full.

    Normal emits do not take the coordination lock. Only rollover renames do,
    so background pruning can detach numbered backups without ever replacing a
    file that a concurrent rollover has just created.
    """

    def doRollover(self) -> None:  # noqa: N802 - logging API
        with LOG_ROTATION_COORD_LOCK:
            super().doRollover()


def configure_logging() -> logging.Logger:
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    if not any(isinstance(handler, RotatingFileHandler) for handler in root.handlers):
        file_handler = CoordinatedRotatingFileHandler(
            LOG_PATH,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    if not any(isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler) for handler in root.handlers):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

    return logging.getLogger("gift_hunter")


logger = configure_logging()


@dataclass
class PaymentAuditHealth:
    """Thread-safe health state for the dedicated payment audit file."""

    writable: bool = False
    last_check_at: str | None = None
    last_success_at: str | None = None
    last_error: str | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def mark_success(self) -> None:
        stamp = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.writable = True
            self.last_check_at = stamp
            self.last_success_at = stamp
            self.last_error = None

    def mark_failure(self, error: BaseException | str) -> None:
        stamp = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.writable = False
            self.last_check_at = stamp
            self.last_error = str(error)[:500]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "writable": self.writable,
                "last_check_at": self.last_check_at,
                "last_success_at": self.last_success_at,
                "last_error": self.last_error,
            }


payment_audit_health = PaymentAuditHealth()


class PaymentAuditFileHandler(CoordinatedRotatingFileHandler):
    """Rotating handler that exposes write failures swallowed by logging.

    ``logging`` normally routes file I/O failures through ``handleError`` and
    does not re-raise them to the caller.  That makes a try/except around
    ``logger.info`` insufficient for a financial audit trail.  This handler
    records both successful writes and hidden handler failures explicitly.
    """

    def __init__(self, *args: Any, health: PaymentAuditHealth, **kwargs: Any) -> None:
        self.health = health
        self._emit_failed = False
        super().__init__(*args, **kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        self._emit_failed = False
        super().emit(record)
        if not self._emit_failed:
            self.health.mark_success()

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802 - logging API
        self._emit_failed = True
        error = sys.exc_info()[1] or RuntimeError("unknown payment audit handler error")
        self.health.mark_failure(error)
        # Preserve the standard debug-time stderr report without recursively
        # logging through the same potentially broken filesystem.
        if logging.raiseExceptions:
            super().handleError(record)

    def verify_writable(self) -> bool:
        """Flush + fsync the live audit stream and update the health indicator."""
        self.acquire()
        try:
            if self.stream is None:
                self.stream = self._open()
            self.stream.flush()
            os.fsync(self.stream.fileno())
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self.health.mark_failure(exc)
            return False
        finally:
            self.release()
        self.health.mark_success()
        return True


def configure_payment_audit_logging() -> logging.Logger:
    """Create a dedicated structured payment/form audit log.

    It deliberately excludes tokens/session data and is separate from the main
    log so payment evidence survives long-running scanner sessions and rotation.
    """
    audit = logging.getLogger("gift_hunter.payment_audit")
    audit.setLevel(logging.INFO)
    audit.propagate = False
    if not any(isinstance(handler, PaymentAuditFileHandler) for handler in audit.handlers):
        handler = PaymentAuditFileHandler(
            PAYMENT_AUDIT_PATH,
            maxBytes=PAYMENT_AUDIT_MAX_BYTES,
            backupCount=PAYMENT_AUDIT_BACKUP_COUNT,
            encoding="utf-8",
            delay=True,
            health=payment_audit_health,
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        audit.addHandler(handler)
    return audit


payment_audit_logger = configure_payment_audit_logging()
log_full_lock = asyncio.Lock()


def record_payment_event(event: str, **fields: Any) -> bool:
    """Persist one structured, non-secret payment diagnostic event."""
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": APP_VERSION,
        "shooter_id": SHOOTER_ID,
        "event": event,
        **fields,
    }
    try:
        payment_audit_logger.info(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
        )
    except Exception as exc:
        payment_audit_health.mark_failure(exc)
        logger.warning("payment_audit_write_failed event=%s error=%s", event, exc)
        return False
    return bool(payment_audit_health.snapshot()["writable"])


def check_payment_audit_writable() -> bool:
    """Actively verify that payment audit writes reach the filesystem."""
    record_payment_event("payment_audit_healthcheck")
    handlers = [
        handler
        for handler in payment_audit_logger.handlers
        if isinstance(handler, PaymentAuditFileHandler)
    ]
    if not handlers:
        payment_audit_health.mark_failure("payment audit file handler is missing")
        return False
    ok = all(handler.verify_writable() for handler in handlers)
    if not ok:
        logger.error(
            "payment_audit_not_writable error=%s",
            payment_audit_health.snapshot().get("last_error"),
        )
    return ok


async def payment_audit_health_loop() -> None:
    """Periodically verify/flush the audit file without blocking asyncio."""
    while True:
        try:
            await asyncio.to_thread(check_payment_audit_writable)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            payment_audit_health.mark_failure(exc)
            logger.error("payment_audit_healthcheck_failed error=%s", exc)
        await asyncio.sleep(PAYMENT_AUDIT_HEALTHCHECK_SECONDS)


def record_cluster_event(event: str, **fields: Any) -> None:
    """Write one safe cross-container event outside the payment hot path."""
    try:
        provision_store.append_event(event, version=APP_VERSION, shooter_id=SHOOTER_ID, **fields)
    except Exception as exc:
        logger.warning("cluster_event_write_failed event=%s error=%s", event, exc)


def safe_save_lifecycle(shooter_id: int, **fields: Any) -> bool:
    """Persist lifecycle diagnostics without taking the bot down on disk errors."""
    try:
        provision_store.save_lifecycle(shooter_id, **fields)
        return True
    except Exception as exc:
        logger.error(
            "cluster_lifecycle_write_failed shooter_id=%s state=%s error=%s",
            shooter_id,
            fields.get("state", "unknown"),
            exc,
        )
        return False


@dataclass
class Settings:
    version: str = APP_VERSION
    owner_user_id: int | None = None
    api_id: int | None = None
    api_hash: str | None = None
    phone: str | None = None
    channel_id: int | None = None
    channel_title: str | None = None
    channel_username: str | None = None
    selected_saved_ids: list[int] = field(default_factory=list)
    legacy_selected_gift_ids: list[int] = field(default_factory=list)
    target_numbers: list[int] = field(default_factory=list)
    live_upgrades: bool = False
    volley_size: int = 1
    fast_volley_stagger_ms: int = DEFAULT_FAST_VOLLEY_STAGGER_MS
    slug_map: dict[str, str] = field(default_factory=dict)
    payment_hold_saved_ids: list[int] = field(default_factory=list)
    payment_hold_targets: dict[str, int] = field(default_factory=dict)
    payment_hold_reason: str | None = None
    payment_verification_url: str | None = None
    payment_guard_token: str | None = None


class SettingsStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()
        self.settings = self._load()

    def _load_raw(self) -> dict[str, Any]:
        candidates = [self.path, DATA_DIR / "config.json", DATA_DIR / "state.json"]
        for candidate in candidates:
            try:
                data = json.loads(candidate.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except FileNotFoundError:
                continue
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("settings_read_failed path=%s error=%s", candidate, exc)
        return {}

    def _load(self) -> Settings:
        raw = self._load_raw()
        nested = raw.get("settings") if isinstance(raw.get("settings"), dict) else raw

        api_id = nested.get("api_id", nested.get("tg_api_id", nested.get("TG_API_ID")))
        try:
            api_id = int(api_id) if api_id else None
        except (TypeError, ValueError):
            api_id = None

        old_selected = nested.get("selected_gift_ids", []) or []
        selected_saved = nested.get("selected_saved_ids", []) or []

        settings = Settings(
            version=APP_VERSION,
            owner_user_id=_int_or_none(nested.get("owner_user_id")),
            api_id=api_id,
            api_hash=_str_or_none(nested.get("api_hash", nested.get("tg_api_hash", nested.get("TG_API_HASH")))),
            phone=_str_or_none(nested.get("phone", nested.get("tg_phone", nested.get("TG_PHONE")))),
            channel_id=_int_or_none(nested.get("channel_id")),
            channel_title=_str_or_none(nested.get("channel_title")),
            channel_username=_str_or_none(nested.get("channel_username")),
            selected_saved_ids=_unique_ints(selected_saved),
            legacy_selected_gift_ids=_unique_ints(nested.get("legacy_selected_gift_ids", old_selected)),
            target_numbers=_unique_ints(nested.get("target_numbers", [])),
            live_upgrades=parse_bool(nested.get("live_upgrades", False), False),
            volley_size=min(max_volley_size_for_shooter(SHOOTER_ID), max(1, _int_or_none(nested.get("volley_size")) or 1)),
            fast_volley_stagger_ms=min(
                MAX_FAST_VOLLEY_STAGGER_MS,
                max(0, _int_or_none(nested.get("fast_volley_stagger_ms"))
                    if _int_or_none(nested.get("fast_volley_stagger_ms")) is not None
                    else DEFAULT_FAST_VOLLEY_STAGGER_MS),
            ),
            slug_map={str(k): str(v) for k, v in (nested.get("slug_map", {}) or {}).items() if v},
            payment_hold_saved_ids=_unique_ints(nested.get("payment_hold_saved_ids", [])),
            payment_hold_targets={
                str(saved_id): target
                for key, value in (nested.get("payment_hold_targets", {}) or {}).items()
                if (saved_id := _positive_int_or_none(key)) is not None
                and (target := _positive_int_or_none(value)) is not None
            },
            payment_hold_reason=_str_or_none(nested.get("payment_hold_reason")),
            payment_verification_url=_str_or_none(nested.get("payment_verification_url")),
            payment_guard_token=_str_or_none(nested.get("payment_guard_token")),
        )
        return settings

    async def save(self) -> None:
        async with self._lock:
            payload = asdict(self.settings)
            payload["version"] = APP_VERSION
            temp = self.path.with_suffix(".tmp")
            with temp.open("w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temp, 0o600)
            temp.replace(self.path)
            os.chmod(self.path, 0o600)
            _fsync_parent(self.path)

    async def reset_operational(self) -> None:
        """Reset every user-facing setting while preserving authorization.

        Authorization is deliberately split into two layers and both survive:
        * owner_user_id keeps the Bot API control binding;
        * api_id/api_hash/phone plus the SQLite ``*.session`` file keep MTProto.

        Session files are never opened, renamed or deleted here.  Replacing the
        Settings object with a small allow-list also makes newly added operational
        fields reset by default instead of being accidentally retained.
        """
        current = self.settings

        replacement = Settings(
            version=APP_VERSION,
            owner_user_id=current.owner_user_id,
            api_id=current.api_id,
            api_hash=current.api_hash,
            phone=current.phone,
        )
        self.settings = replacement
        try:
            await self.save()
        except BaseException:
            # A failed disk write must not leave the process with a half-reset
            # in-memory configuration that differs from settings.json.
            self.settings = current
            raise


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _positive_int_or_none(value: Any) -> int | None:
    parsed = _int_or_none(value)
    return parsed if parsed is not None and parsed > 0 else None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _is_stargift_slug_invalid(exc: BaseException) -> bool:
    text = str(exc).upper()
    class_name = exc.__class__.__name__.upper()
    return (
        "STARGIFT_SLUG_INVALID" in text
        or "STARGIFTSLUGINVALID" in class_name
    )


def _unique_ints(values: Iterable[Any]) -> list[int]:
    output: list[int] = []
    seen: set[int] = set()
    for item in values or []:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in seen:
            output.append(value)
            seen.add(value)
    return output


class RateLimitActiveError(RuntimeError):
    def __init__(self, remaining_seconds: float, source: str | None = None):
        self.remaining_seconds = max(0.0, float(remaining_seconds))
        self.source = source
        seconds = max(1, int(self.remaining_seconds + 0.999))
        suffix = f" ({source})" if source else ""
        super().__init__(f"Telegram ограничил запросы: подожди ещё {seconds}с{suffix}")


class RateLimitStore:
    """Persistent account-wide cooldown after Telegram FLOOD_WAIT."""

    def __init__(self, path: Path):
        self.path = path
        self.blocked_until = 0.0
        self.source: str | None = None
        self.wait_seconds = 0.0
        self.updated_at = 0.0
        self._load()

    def _load(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            self.blocked_until = float(payload.get("blocked_until", 0.0) or 0.0)
            self.source = _str_or_none(payload.get("source"))
            self.wait_seconds = float(payload.get("wait_seconds", 0.0) or 0.0)
            self.updated_at = float(payload.get("updated_at", 0.0) or 0.0)
        except FileNotFoundError:
            return
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            logger.warning("rate_limit_state_read_failed error=%s", exc)

    def remaining_seconds(self) -> float:
        return max(0.0, self.blocked_until - time.time())

    def assert_available(self) -> None:
        remaining = self.remaining_seconds()
        if remaining > 0:
            raise RateLimitActiveError(remaining, self.source)

    def register(self, wait_seconds: float, source: str) -> float:
        wait = max(0.0, float(wait_seconds)) + FLOOD_WAIT_EXTRA_MS / 1000.0
        now = time.time()
        candidate = now + wait
        if candidate >= self.blocked_until:
            self.blocked_until = candidate
            self.source = source
            self.wait_seconds = wait
            self.updated_at = now
            self._save()
        return self.remaining_seconds()

    def clear_if_expired(self) -> None:
        if self.blocked_until and self.remaining_seconds() <= 0:
            self.blocked_until = 0.0
            self.source = None
            self.wait_seconds = 0.0
            self.updated_at = time.time()
            self._save()

    def reset(self) -> None:
        """Forget every persisted FloodWait/cooldown during an explicit full reset."""
        self.blocked_until = 0.0
        self.source = None
        self.wait_seconds = 0.0
        self.updated_at = time.time()
        try:
            self.path.unlink(missing_ok=True)
            _fsync_parent(self.path)
        except OSError as exc:
            logger.warning("rate_limit_state_reset_failed error=%s", exc)

    def _save(self) -> None:
        payload = {
            "blocked_until": self.blocked_until,
            "source": self.source,
            "wait_seconds": self.wait_seconds,
            "updated_at": self.updated_at,
        }
        try:
            temp = self.path.with_suffix(".tmp")
            temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.chmod(temp, 0o600)
            temp.replace(self.path)
            os.chmod(self.path, 0o600)
        except OSError as exc:
            logger.warning("rate_limit_state_write_failed error=%s", exc)


PAYMENT_GUARD_LOCK = threading.RLock()


class PaymentGuardStateError(OSError):
    """Persistent payment guard exists but cannot be trusted or parsed."""


def _fsync_parent(path: Path) -> None:
    """Best-effort directory fsync for durable atomic state replacement."""
    flags = getattr(os, "O_DIRECTORY", 0) | os.O_RDONLY
    try:
        fd = os.open(str(path.parent), flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _load_payment_submission_guard(path: Path = PAYMENT_GUARD_PATH) -> dict[str, Any] | None:
    """Load the durable pre-submit guard, failing closed on corruption.

    A missing file means there is no armed submission.  An existing file that
    cannot be read or validated is different: silently treating it as absent
    could permit a second Stars payment after an interrupted first submission.
    """
    with PAYMENT_GUARD_LOCK:
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise PaymentGuardStateError(f"cannot read payment guard {path}: {exc}") from exc
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PaymentGuardStateError(f"invalid payment guard JSON {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise PaymentGuardStateError(f"invalid payment guard payload {path}")
        guard_id = _str_or_none(payload.get("guard_id"))
        entries = payload.get("entries")
        if guard_id is None or not isinstance(entries, list):
            raise PaymentGuardStateError(f"incomplete payment guard payload {path}")
        for entry in entries:
            if not isinstance(entry, dict):
                raise PaymentGuardStateError(f"invalid payment guard entry {path}")
            if not str(entry.get("slug", "")).strip():
                raise PaymentGuardStateError(f"payment guard entry has no slug {path}")
            if _positive_int_or_none(entry.get("target")) is None:
                raise PaymentGuardStateError(f"payment guard entry has invalid target {path}")
            if not _unique_ints(entry.get("saved_ids", [])):
                raise PaymentGuardStateError(f"payment guard entry has no saved_ids {path}")
            state = _str_or_none(entry.get("state"))
            # v0038 had no state field. Treat that legacy shape as unknown on
            # recovery (fail closed), but accept it here so the Reset button can
            # still clear it after an upgrade. New v0039 entries are explicit.
            if state is not None and state not in {"armed", "submitted"}:
                raise PaymentGuardStateError(f"payment guard entry has invalid state {path}")
        return payload


def add_payment_submission_guard_entry(
    *,
    slug: str,
    target: int,
    saved_ids: Iterable[int],
    campaign_id: str | None = None,
    path: Path = PAYMENT_GUARD_PATH,
) -> str:
    """Durably arm a pre-submit guard before any Stars request may be sent.

    The guard intentionally lives outside settings.json. If the process dies or
    the post-volley settings save fails, startup converts every guarded saved_id
    into a payment hold instead of risking a duplicate payment after restart.
    """
    with PAYMENT_GUARD_LOCK:
        payload = _load_payment_submission_guard(path) or {}
        # Every new ARMED generation gets a fresh token.  If clearing an
        # already-confirmed guard failed and a later run arms another payment,
        # the old token stored in settings.json must never make startup mistake
        # the newer submission for stale cleanup residue.
        guard_id = os.urandom(16).hex()
        entries = [item for item in payload.get("entries", []) if isinstance(item, dict)]
        key_slug = str(slug).strip()
        key_target = int(target)
        entries = [
            item for item in entries
            if not (str(item.get("slug", "")).strip() == key_slug and _int_or_none(item.get("target")) == key_target)
        ]
        entries.append(
            {
                "slug": key_slug,
                "target": key_target,
                "saved_ids": _unique_ints(saved_ids),
                "campaign_id": _str_or_none(campaign_id),
                "state": "armed",
                "armed_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        body = {
            "version": APP_VERSION,
            "guard_id": guard_id,
            "entries": entries,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        temp = path.with_name(path.name + ".tmp")
        with temp.open("w", encoding="utf-8") as stream:
            json.dump(body, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp, 0o600)
        temp.replace(path)
        os.chmod(path, 0o600)
        _fsync_parent(path)
        return guard_id


def mark_payment_submission_guard_submitted(
    *,
    slug: str,
    target: int,
    saved_ids: Iterable[int],
    path: Path = PAYMENT_GUARD_PATH,
) -> str:
    """Durably switch one pre-armed volley to SUBMITTED immediately before send.

    ARMED means payment forms exist but no Stars RPC has been dispatched. A
    watchdog/container restart while only ARMED is therefore safe and must not
    create a payment hold. SUBMITTED is written as the last durable preflight
    operation before the first sender call; a restart after that point stays
    fail-closed because a payment may have reached Telegram.
    """
    with PAYMENT_GUARD_LOCK:
        payload = _load_payment_submission_guard(path)
        if payload is None:
            raise PaymentGuardStateError("payment guard disappeared before submit")
        key_slug = str(slug).strip()
        key_target = int(target)
        wanted = set(_unique_ints(saved_ids))
        if not wanted:
            raise PaymentGuardStateError("payment guard submit has no saved_ids")
        match: dict[str, Any] | None = None
        for entry in payload.get("entries", []):
            if not isinstance(entry, dict):
                continue
            if (
                str(entry.get("slug", "")).strip() == key_slug
                and _int_or_none(entry.get("target")) == key_target
                and set(_unique_ints(entry.get("saved_ids", []))) == wanted
            ):
                match = entry
                break
        if match is None:
            raise PaymentGuardStateError(
                f"payment guard entry missing before submit: slug={key_slug} target={key_target}"
            )
        match["state"] = "submitted"
        match["submitted_at"] = datetime.now(timezone.utc).isoformat()
        payload["version"] = APP_VERSION
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        temp = path.with_name(path.name + ".tmp")
        with temp.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp, 0o600)
        temp.replace(path)
        os.chmod(path, 0o600)
        _fsync_parent(path)
        guard_id = _str_or_none(payload.get("guard_id"))
        if guard_id is None:
            raise PaymentGuardStateError("payment guard lost guard_id before submit")
        return guard_id


def remove_payment_submission_guard_entries(
    keys: Iterable[tuple[str, int]],
    *,
    path: Path = PAYMENT_GUARD_PATH,
) -> None:
    with PAYMENT_GUARD_LOCK:
        payload = _load_payment_submission_guard(path)
        if payload is None:
            return
        remove = {(str(slug).strip(), int(target)) for slug, target in keys}
        entries = [
            item for item in payload.get("entries", [])
            if isinstance(item, dict)
            and (str(item.get("slug", "")).strip(), _int_or_none(item.get("target"))) not in remove
        ]
        if not entries:
            path.unlink(missing_ok=True)
            _fsync_parent(path)
            return
        payload["entries"] = entries
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        temp = path.with_name(path.name + ".tmp")
        with temp.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp, 0o600)
        temp.replace(path)
        os.chmod(path, 0o600)
        _fsync_parent(path)


def clear_payment_submission_guard(path: Path = PAYMENT_GUARD_PATH) -> None:
    with PAYMENT_GUARD_LOCK:
        path.unlink(missing_ok=True)
        _fsync_parent(path)


def _recover_settings_from_payment_guard(settings: Settings) -> None:
    try:
        payload = _load_payment_submission_guard()
    except PaymentGuardStateError as exc:
        # Keep the bot controllable for diagnostics, but fail closed for LIVE.
        settings.live_upgrades = False
        settings.payment_hold_reason = (
            "Persistent payment guard повреждён или недоступен; LIVE заблокирован "
            "до полного сброса/исправления payment-submit-guard.json"
        )
        logger.critical("payment_submission_guard_invalid error=%s", exc)
        return
    if payload is None:
        return
    guard_id = _str_or_none(payload.get("guard_id"))
    # A matching token means the exact post-volley state already reached
    # settings.json; a leftover guard is only an unlink failure.
    if guard_id is not None and guard_id == settings.payment_guard_token:
        with contextlib.suppress(OSError):
            clear_payment_submission_guard()
        return

    guarded: list[int] = []
    targets = dict(settings.payment_hold_targets)
    armed_keys: set[tuple[str, int]] = set()
    legacy_unknown = False
    for entry in payload.get("entries", []):
        if not isinstance(entry, dict):
            continue
        slug = str(entry.get("slug", "")).strip()
        target = _positive_int_or_none(entry.get("target"))
        ids = _unique_ints(entry.get("saved_ids", []))
        state = _str_or_none(entry.get("state"))
        if state == "armed":
            # No Stars RPC was sent. A watchdog restart while waiting near the
            # target must never turn clean ammunition into a false payment hold.
            if slug and target is not None:
                armed_keys.add((slug, target))
            logger.warning(
                "payment_submission_guard_armed_recovered_safe slug=%s target=%s saved_ids=%s",
                slug, target, ids,
            )
            continue
        # state == submitted is a real ambiguous payment window. A v0038 guard
        # has no state at all, so it remains fail-closed as legacy/unknown.
        if state is None:
            legacy_unknown = True
        guarded.extend(ids)
        if target is not None:
            for saved_id in ids:
                targets[str(saved_id)] = target

    if armed_keys:
        try:
            remove_payment_submission_guard_entries(armed_keys)
        except OSError as exc:
            logger.error("payment_submission_guard_armed_cleanup_failed error=%s", exc)

    guarded = _unique_ints(guarded)
    if not guarded:
        # All surviving entries were ARMED only; they are safe to discard and
        # LIVE remains exactly as persisted in settings.json for auto-resume.
        return
    settings.payment_hold_saved_ids = _unique_ints([*settings.payment_hold_saved_ids, *guarded])
    settings.payment_hold_targets = targets
    settings.payment_hold_reason = (
        "Обнаружен SUBMITTED payment guard после аварийного перезапуска; повторная "
        "оплата заблокирована до сверки с Telegram"
        if not legacy_unknown
        else
        "Обнаружен guard старой версии с неизвестным фактом submit; повторная оплата "
        "заблокирована до сверки или явного полного сброса"
    )
    settings.payment_verification_url = None
    settings.live_upgrades = False
    logger.error(
        "payment_submission_guard_recovered guard_id=%s saved_ids=%s legacy_unknown=%s",
        guard_id, guarded, legacy_unknown,
    )


def _write_scanner_resume_marker(path: Path = SCANNER_RESUME_PATH) -> None:
    payload = {
        "version": APP_VERSION,
        "shooter_id": SHOOTER_ID,
        "selected_saved_ids": list(store.settings.selected_saved_ids),
        "target_numbers": list(store.settings.target_numbers),
        "live_upgrades": bool(store.settings.live_upgrades),
        "volley_size": int(store.settings.volley_size),
        "fast_volley_stagger_ms": int(store.settings.fast_volley_stagger_ms),
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temp, 0o600)
    temp.replace(path)
    os.chmod(path, 0o600)
    _fsync_parent(path)


def _load_scanner_resume_marker(path: Path = SCANNER_RESUME_PATH) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("scanner_resume_marker_read_failed error=%s", exc)
        return None
    return payload if isinstance(payload, dict) else None


def _clear_scanner_resume_marker(path: Path = SCANNER_RESUME_PATH) -> None:
    try:
        path.unlink(missing_ok=True)
        _fsync_parent(path)
    except OSError as exc:
        logger.error("scanner_resume_marker_clear_failed error=%s", exc)


def clear_full_operational_files() -> None:
    """Clear every local operational/safety artifact while preserving auth/session/logs."""
    # Historical logs/payment audit are deliberately retained: they do not affect
    # runtime behaviour and are useful after a bug. Authorization *.session files
    # are never touched.
    for path in (
        PAYMENT_GUARD_PATH,
        SCANNER_RESUME_PATH,
        RATE_LIMIT_PATH,
        DIAGNOSTICS_PATH,
        HEARTBEAT_PATH,
        STRESS_REPORT_PATH,
        STRESS_HISTORY_PATH,
        CATALOG_REPORT_PATH,
    ):
        try:
            path.unlink(missing_ok=True)
            path.with_name(path.name + ".tmp").unlink(missing_ok=True)
            path.with_suffix(".tmp").unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("full_reset_file_cleanup_failed path=%s error=%s", path, exc)
    _fsync_parent(DATA_DIR / ".")


def current_rss_mb() -> float:
    """Current resident memory on Linux; fallback to process lifetime peak."""
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return float(line.split()[1]) / 1024.0
    except (OSError, ValueError, IndexError):
        pass
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / 1024.0


store = SettingsStore(SETTINGS_PATH)
_recover_settings_from_payment_guard(store.settings)


def effective_volley_size() -> int:
    """Return this Hunter's configured volley clamped to its role limit."""
    return min(effective_max_volley_size(), max(1, int(store.settings.volley_size)))


def effective_fast_volley_stagger_ms() -> int:
    """Return the persisted FAST inter-submit gap, clamped to a safe range."""
    return min(
        MAX_FAST_VOLLEY_STAGGER_MS,
        max(0, int(store.settings.fast_volley_stagger_ms)),
    )


rate_limit = RateLimitStore(RATE_LIMIT_PATH)


@dataclass
class RuntimeState:
    active: bool = False
    checks: int = 0
    last_cycle_ms: float | None = None
    last_error: str | None = None
    current_by_slug: dict[str, int] = field(default_factory=dict)
    title_by_slug: dict[str, str] = field(default_factory=dict)
    last_success: str | None = None
    started_at: float | None = None
    adaptive_interval_ms: float | None = None
    sleep_ms: float | None = None
    poll_gap_ms: float | None = None
    flood_count: int = 0
    last_flood_wait_s: int | None = None
    rate_cooldown_cycles: int = 0
    pending_verification_url: str | None = None
    stress_active: bool = False
    stress_started_at: float | None = None
    stress_phase: str | None = None
    stress_elapsed_s: float = 0.0
    stress_interval_ms: float | None = None
    stress_checks: int = 0
    stress_successes: int = 0
    stress_errors: int = 0
    stress_flood_count: int = 0
    stress_flood_seconds: float = 0.0
    stress_last_error: str | None = None
    stress_avg_latency_ms: float | None = None
    stress_p95_latency_ms: float | None = None
    stress_current_rate_per_s: float = 0.0
    stress_max_rate_per_s: float = 0.0
    stress_result: str | None = None
    fast_quiet: bool = False
    fast_fired: bool = False
    fast_volley_size: int = 1
    fast_trigger_to_submit_ms: float | None = None
    fast_task_launch_ms: float | None = None
    fast_first_send_start_ms: float | None = None
    fast_send_start_offsets_ms: list[float | None] = field(default_factory=list)
    fast_fire_source: str | None = None
    fast_udp_peers_sent: int = 0
    fast_campaign_id: str | None = None
    payment_form_refresh_count: int = 0
    payment_form_refresh_failures: int = 0
    payment_form_last_refresh_at: str | None = None
    payment_form_last_refresh_error: str | None = None
    payment_form_oldest_age_s: float | None = None


runtime = RuntimeState(pending_verification_url=store.settings.payment_verification_url)


@dataclass(frozen=True)
class ChannelChoice:
    channel_id: int
    title: str
    username: str | None
    upgradable_count: int


@dataclass
class SavedGiftInfo:
    saved_id: int
    base_gift_id: int
    title: str
    slug: str | None
    can_upgrade: bool
    prepaid: bool
    upgrade_cost: int
    gift_num: int | None
    raw: Any


@dataclass
class GiftCounter:
    slug: str
    title: str
    current: int
    total: int | None
    base_gift_id: int | None


@dataclass(frozen=True)
class ExactGiftProbe:
    slug: str
    title: str
    num: int
    issued: int | None
    total: int | None
    base_gift_id: int | None


@dataclass(frozen=True)
class CatalogNumber:
    gift_id: int
    title: str
    issued: int | None
    total: int | None
    slug: str | None
    error: str | None = None


@dataclass
class PreparedUpgrade:
    saved_id: int
    input_saved: Any
    invoice: Any | None
    form_id: int | None
    cost: int
    prepaid: bool
    created_at: float
    request: Any | None = None
    fast_send_started_ns: int | None = None


@dataclass
class UpgradeOutcome:
    status: str
    actual_num: int | None = None
    actual_slug: str | None = None
    verification_url: str | None = None
    detail: str | None = None


def invoice_saved_id(invoice: Any | None) -> int | None:
    """Return the saved gift ID carried by a Star Gift upgrade invoice."""
    if invoice is None:
        return None
    stargift = getattr(invoice, "stargift", None)
    return _int_or_none(getattr(stargift, "saved_id", None))


def prepared_payment_debug(plan: PreparedUpgrade) -> dict[str, Any]:
    """Non-secret fields that prove which gift/form a FAST request is bound to."""
    request = plan.request
    request_invoice = getattr(request, "invoice", None) if request is not None else None
    return {
        "saved_id": plan.saved_id,
        "prepaid": plan.prepaid,
        "form_id": plan.form_id,
        "invoice_saved_id": invoice_saved_id(plan.invoice),
        "request_form_id": _int_or_none(getattr(request, "form_id", None)) if request is not None else None,
        "request_invoice_saved_id": invoice_saved_id(request_invoice),
        "request_object_id": id(request) if request is not None else None,
        "age_ms": round(max(0.0, time.monotonic() - plan.created_at) * 1000.0, 3),
    }


class MTProtoService:
    def __init__(self, settings_store: SettingsStore):
        self.store = settings_store
        self.client: TelegramClient | None = None
        self._client_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._slug_cache: dict[str, GiftCounter] = {}
        self._slug_cache_at: dict[str, float] = {}
        self._slug_probe: dict[str, int] = {}
        self._gift_catalog: dict[int, Any] = {}
        self._gift_catalog_at: float = 0.0
        self._authorized: bool | None = None
        # Monotonic diagnostics only.  These counters never participate in
        # authorization or reconnect logic; they simply let /log_full prove
        # whether the scanner reused the same client/connection around a shot.
        self._client_epoch: int = 0
        self._connect_epoch: int = 0
        self._client_created_monotonic: float | None = None
        self._connected_monotonic: float | None = None

    @staticmethod
    def session_base() -> str:
        if MT_SESSION:
            explicit = Path(MT_SESSION)
            if explicit.suffix == ".session":
                explicit = explicit.with_suffix("")
            explicit.parent.mkdir(parents=True, exist_ok=True)
            return str(explicit)
        preferred = DATA_DIR / "user"
        candidates = [
            DATA_DIR / "user.session",
            DATA_DIR / "telegram.session",
            DATA_DIR / "telegram_user.session",
            DATA_DIR / "gift_hunter.session",
            DATA_DIR / "mtproto.session",
        ]
        candidates.extend(sorted(DATA_DIR.glob("*.session"), key=lambda p: p.stat().st_mtime, reverse=True))
        for candidate in candidates:
            if candidate.exists():
                return str(candidate.with_suffix(""))
        return str(preferred)

    def clear_operational_cache(self) -> None:
        """Drop gift/channel lookup caches without touching authorization."""
        self._slug_cache.clear()
        self._slug_cache_at.clear()
        self._slug_probe.clear()
        self._gift_catalog.clear()
        self._gift_catalog_at = 0.0

    def configured(self) -> bool:
        s = self.store.settings
        return bool(s.api_id and s.api_hash and s.phone)

    async def get_client(self, *, reload: bool = False) -> TelegramClient:
        if not self.configured():
            raise RuntimeError("MTProto не настроен: нужны TG_API_ID, TG_API_HASH и номер телефона")
        async with self._client_lock:
            if reload and self.client is not None:
                logger.info(
                    "mtproto_client_reload client_epoch=%s connect_epoch=%s connected=%s",
                    self._client_epoch,
                    self._connect_epoch,
                    bool(self.client.is_connected()),
                )
                with contextlib.suppress(Exception):
                    await self.client.disconnect()
                self.client = None
                self._authorized = None
            if self.client is None:
                s = self.store.settings
                self.client = TelegramClient(
                    self.session_base(),
                    int(s.api_id),
                    str(s.api_hash),
                    device_model=f"Gift Hunter {SHOOTER_ID} VPS",
                    system_version="Linux",
                    app_version=APP_VERSION,
                    lang_code="ru",
                    system_lang_code="ru-RU",
                    auto_reconnect=True,
                    request_retries=1,
                    connection_retries=5,
                    flood_sleep_threshold=0,
                )
                self._client_epoch += 1
                self._client_created_monotonic = time.monotonic()
                logger.info(
                    "mtproto_client_created client_epoch=%s session=%s app_version=%s",
                    self._client_epoch,
                    Path(self.session_base()).name,
                    APP_VERSION,
                )
            if not self.client.is_connected():
                logger.info(
                    "mtproto_connect_started client_epoch=%s next_connect_epoch=%s",
                    self._client_epoch,
                    self._connect_epoch + 1,
                )
                await self.client.connect()
                self._connect_epoch += 1
                self._connected_monotonic = time.monotonic()
                logger.info(
                    "mtproto_connect_ready client_epoch=%s connect_epoch=%s",
                    self._client_epoch,
                    self._connect_epoch,
                )
            return self.client

    def connection_snapshot(self, client: TelegramClient | None = None) -> dict[str, Any]:
        """Return non-secret transport diagnostics for payment audit logs."""
        current = client or self.client
        connected = bool(current is not None and current.is_connected())
        session = getattr(current, "session", None) if current is not None else None
        sender = getattr(current, "_sender", None) if current is not None else None
        now = time.monotonic()
        return {
            "client_epoch": self._client_epoch,
            "connect_epoch": self._connect_epoch,
            "connected": connected,
            "dc_id": _int_or_none(getattr(session, "dc_id", None)),
            "client_age_s": (
                round(max(0.0, now - self._client_created_monotonic), 3)
                if self._client_created_monotonic is not None
                else None
            ),
            "connection_age_s": (
                round(max(0.0, now - self._connected_monotonic), 3)
                if self._connected_monotonic is not None
                else None
            ),
            "sender_reconnecting": bool(getattr(sender, "_reconnecting", False)),
        }

    async def is_authorized(self, *, reload: bool = False) -> bool:
        if not self.configured():
            return False
        try:
            client = await self.get_client(reload=reload)
            async with self._request_lock:
                self._authorized = bool(await client.is_user_authorized())
            return bool(self._authorized)
        except Exception as exc:
            logger.warning("authorization_check_failed error=%s", exc)
            self._authorized = False
            return False

    async def require_authorized(self) -> TelegramClient:
        client = await self.get_client()
        if self._authorized is not True:
            async with self._request_lock:
                self._authorized = bool(await client.is_user_authorized())
        if not self._authorized:
            raise RuntimeError("Telegram-аккаунт не авторизован. Выполни python main.py auth")
        return client

    async def call(self, request: Any) -> Any:
        rate_limit.clear_if_expired()
        rate_limit.assert_available()
        client = await self.require_authorized()
        async with self._request_lock:
            try:
                return await client(request)
            except errors.FloodWaitError as exc:
                rate_limit.register(float(exc.seconds), request.__class__.__name__)
                raise

    async def disconnect(self) -> None:
        async with self._client_lock:
            if self.client is not None:
                logger.info(
                    "mtproto_disconnect_requested client_epoch=%s connect_epoch=%s connected=%s",
                    self._client_epoch,
                    self._connect_epoch,
                    bool(self.client.is_connected()),
                )
                with contextlib.suppress(Exception):
                    await self.client.disconnect()
                self.client = None
                self._authorized = None

    async def resolve_channel(self) -> Any:
        rate_limit.clear_if_expired()
        rate_limit.assert_available()
        s = self.store.settings
        if not s.channel_id and not s.channel_username:
            raise RuntimeError("Канал не выбран. Нажми «📣 Канал» и выбери канал явно.")
        client = await self.require_authorized()

        if s.channel_username:
            try:
                entity = await client.get_entity(s.channel_username)
                if isinstance(entity, types.Channel) and (
                    s.channel_id is None or int(entity.id) == int(s.channel_id)
                ):
                    if not getattr(entity, "creator", False):
                        raise RuntimeError("Выбранный канал больше не принадлежит аккаунту")
                    s.channel_id = int(entity.id)
                    s.channel_title = getattr(entity, "title", s.channel_title)
                    await self.store.save()
                    return await client.get_input_entity(entity)
            except errors.FloodWaitError as exc:
                rate_limit.register(float(exc.seconds), "resolve_channel")
                raise
            except Exception:
                pass

        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, types.Channel) and s.channel_id and int(entity.id) == int(s.channel_id):
                    if not getattr(entity, "creator", False):
                        raise RuntimeError("Выбранный канал больше не принадлежит аккаунту")
                    s.channel_title = getattr(entity, "title", s.channel_title)
                    s.channel_username = getattr(entity, "username", None)
                    await self.store.save()
                    return await client.get_input_entity(entity)
        except errors.FloodWaitError as exc:
            rate_limit.register(float(exc.seconds), "resolve_channel")
            raise

        raise RuntimeError("Выбранный канал не найден. Нажми «📣 Канал» и выбери его заново.")

    async def list_channel_choices(self) -> list[ChannelChoice]:
        rate_limit.clear_if_expired()
        rate_limit.assert_available()
        client = await self.require_authorized()
        choices: list[ChannelChoice] = []
        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if not isinstance(entity, types.Channel) or not getattr(entity, "creator", False):
                    continue
                input_peer = await client.get_input_entity(entity)
                try:
                    gifts = await self.fetch_saved_gifts(input_peer, only_upgradable=True, limit_total=100)
                except RateLimitActiveError:
                    raise
                except errors.FloodWaitError:
                    raise
                except Exception as exc:
                    logger.debug("channel_gift_scan_failed channel_id=%s error=%s", entity.id, exc)
                    continue
                if gifts:
                    choices.append(
                        ChannelChoice(
                            channel_id=int(entity.id),
                            title=str(getattr(entity, "title", entity.id)),
                            username=_str_or_none(getattr(entity, "username", None)),
                            upgradable_count=len(gifts),
                        )
                    )
        except errors.FloodWaitError as exc:
            rate_limit.register(float(exc.seconds), "list_channels")
            raise
        choices.sort(key=lambda item: (-item.upgradable_count, item.title.casefold()))
        return choices

    async def select_channel(self, channel_id: int) -> ChannelChoice:
        choices = await self.list_channel_choices()
        choice = next((item for item in choices if item.channel_id == int(channel_id)), None)
        if choice is None:
            raise RuntimeError("Канал не найден или в нём нет подарков для улучшения")
        s = self.store.settings
        changed = s.channel_id != choice.channel_id
        if changed and s.payment_hold_saved_ids:
            raise RuntimeError(PENDING_PAYMENT_HOLD_MESSAGE + " Сначала дождись успешной сверки в текущем канале.")
        s.channel_id = choice.channel_id
        s.channel_title = choice.title
        s.channel_username = choice.username
        if changed:
            s.selected_saved_ids = []
            s.legacy_selected_gift_ids = []
            s.live_upgrades = False
            s.payment_hold_targets = {}
            s.payment_hold_reason = None
            s.payment_verification_url = None
        await self.store.save()
        logger.info(
            "channel_selected channel_id=%s title=%s gifts=%s",
            choice.channel_id,
            choice.title,
            choice.upgradable_count,
        )
        return choice

    async def reconcile_payment_holds(self, peer: Any) -> tuple[list[tuple[int, int, str | None]], list[int]]:
        """Reconcile ambiguous payment results without resubmitting a payment.

        Returns confirmed ``(saved_id, number, slug)`` items and still-pending IDs.
        The attempted target is persisted per saved gift so a restart can clean up
        the correct goal even when a race assigned a different number.
        """
        held = _unique_ints(self.store.settings.payment_hold_saved_ids)
        if not held:
            stale = bool(
                self.store.settings.payment_hold_targets
                or self.store.settings.payment_hold_reason
                or self.store.settings.payment_verification_url
            )
            self.store.settings.payment_hold_targets = {}
            self.store.settings.payment_hold_reason = None
            self.store.settings.payment_verification_url = None
            runtime.pending_verification_url = None
            if stale:
                await self.store.save()
            return [], []
        mapping = await self.fetch_saved_by_ids(peer, held)
        confirmed: list[tuple[int, int, str | None]] = []
        pending: list[int] = []
        changed = False
        targets = dict(self.store.settings.payment_hold_targets)
        for saved_id in held:
            item = mapping.get(saved_id)
            gift = getattr(item, "gift", None) if item is not None else None
            if gift is not None and gift.__class__.__name__ == "StarGiftUnique":
                number = _int_or_none(getattr(gift, "num", None))
                if number is not None:
                    confirmed.append((saved_id, number, _str_or_none(getattr(gift, "slug", None))))
                    attempted_target = _int_or_none(targets.pop(str(saved_id), None))
                    with contextlib.suppress(ValueError):
                        self.store.settings.selected_saved_ids.remove(saved_id)
                    self.store.settings.target_numbers = [
                        value
                        for value in self.store.settings.target_numbers
                        if value != attempted_target and value > number
                    ]
                    changed = True
                    continue
            pending.append(saved_id)

        pending_targets = {str(saved_id): targets[str(saved_id)] for saved_id in pending if str(saved_id) in targets}
        if pending != held or pending_targets != self.store.settings.payment_hold_targets:
            self.store.settings.payment_hold_saved_ids = pending
            self.store.settings.payment_hold_targets = pending_targets
            if not pending:
                self.store.settings.payment_hold_reason = None
                self.store.settings.payment_verification_url = None
                runtime.pending_verification_url = None
            changed = True
        if changed:
            await self.store.save()
        return confirmed, pending

    async def fetch_saved_gifts(self, peer: Any, *, only_upgradable: bool, limit_total: int = 500) -> list[Any]:
        client = await self.require_authorized()
        output: list[Any] = []
        offset = ""
        while len(output) < limit_total:
            kwargs: dict[str, Any] = {
                "peer": peer,
                "offset": offset,
                "limit": min(100, limit_total - len(output)),
                "exclude_unique": True,
            }
            if only_upgradable:
                kwargs["exclude_unupgradable"] = True
            request = construct(functions.payments.GetSavedStarGiftsRequest, **kwargs)
            result = await self.call(request)
            gifts = list(getattr(result, "gifts", []) or [])
            output.extend(gifts)
            next_offset = getattr(result, "next_offset", None)
            if not next_offset or not gifts:
                break
            offset = str(next_offset)
        if only_upgradable:
            output = [gift for gift in output if bool(getattr(gift, "can_upgrade", False))]
        return output

    def input_saved(self, peer: Any, saved_id: int) -> Any:
        return construct(types.InputSavedStarGiftChat, peer=peer, saved_id=int(saved_id))

    async def fetch_saved_by_ids(self, peer: Any, saved_ids: list[int]) -> dict[int, Any]:
        if not saved_ids:
            return {}
        client = await self.require_authorized()
        inputs = [self.input_saved(peer, saved_id) for saved_id in saved_ids]
        request = construct(functions.payments.GetSavedStarGiftRequest, stargift=inputs)
        result = await self.call(request)
        mapping: dict[int, Any] = {}
        for item in getattr(result, "gifts", []) or []:
            saved_id = _int_or_none(getattr(item, "saved_id", None))
            if saved_id:
                mapping[saved_id] = item
        return mapping

    async def get_gift_catalog(self, *, cache_seconds: float = 300.0) -> dict[int, Any]:
        """Return regular Telegram gifts keyed by base gift ID.

        Saved gift objects do not always include the optional title.  The
        catalog is therefore used as a name source, not as the number source.
        """
        now = time.monotonic()
        if self._gift_catalog and now - self._gift_catalog_at <= cache_seconds:
            return dict(self._gift_catalog)
        request_cls = getattr(functions.payments, "GetStarGiftsRequest", None)
        if request_cls is None:
            return dict(self._gift_catalog)
        try:
            result = await self.call(construct(request_cls, hash=0))
            gifts = list(getattr(result, "gifts", []) or [])
            catalog: dict[int, Any] = {}
            for gift in gifts:
                gift_id = _int_or_none(getattr(gift, "id", None))
                if gift_id is not None:
                    catalog[gift_id] = gift
            if catalog:
                self._gift_catalog = catalog
                self._gift_catalog_at = now
        except errors.FloodWaitError:
            raise
        except Exception as exc:
            logger.debug("gift_catalog_failed error=%s", exc)
        return dict(self._gift_catalog)

    async def fetch_global_catalog_numbers(
        self,
        *,
        concurrency: int = CATALOG_CONCURRENCY,
    ) -> tuple[list[CatalogNumber], float]:
        """Fetch the global Telegram collectible catalog and latest issued numbers.

        This intentionally does not resolve or inspect the selected channel.  It
        mirrors the v0002 behaviour: ``payments.getStarGifts`` supplies every
        regular gift type that supports collectible upgrades, and one
        ``payments.getUniqueStarGift`` lookup per type supplies
        ``availability_issued``.  Lookups are bounded-concurrent so a full
        catalog remains fast without creating unbounded request bursts.
        """
        rate_limit.clear_if_expired()
        rate_limit.assert_available()
        started = time.perf_counter()
        client = await self.require_authorized()
        request_cls = getattr(functions.payments, "GetStarGiftsRequest", None)
        unique_cls = getattr(functions.payments, "GetUniqueStarGiftRequest", None)
        if request_cls is None or unique_cls is None:
            raise RuntimeError("Установленная версия Telethon не поддерживает каталог подарков")

        catalog_result = await self.call(construct(request_cls, hash=0))
        gifts = [
            gift
            for gift in list(getattr(catalog_result, "gifts", []) or [])
            if gift.__class__.__name__ == "StarGift"
            and getattr(gift, "upgrade_stars", None) is not None
            and _int_or_none(getattr(gift, "id", None)) is not None
        ]
        semaphore = asyncio.Semaphore(max(1, int(concurrency)))

        async def resolve(gift: Any) -> CatalogNumber:
            gift_id = int(getattr(gift, "id"))
            title = str(getattr(gift, "title", None) or f"Gift {gift_id}")
            stored = self.store.settings.slug_map.get(str(gift_id))
            candidates: list[str] = []
            for candidate in ([stored] if stored else []) + slug_candidates(title):
                if candidate and candidate not in candidates:
                    candidates.append(candidate)
            if not candidates:
                return CatalogNumber(gift_id, title, None, None, None, "slug unavailable")

            last_error: str | None = None
            for base in candidates:
                for probe in (1, 2, 3, 10):
                    try:
                        async with semaphore:
                            rate_limit.clear_if_expired()
                            rate_limit.assert_available()
                            response = await client(construct(unique_cls, slug=f"{base}-{probe}"))
                        unique = getattr(response, "gift", None)
                        if unique is None or unique.__class__.__name__ != "StarGiftUnique":
                            continue
                        returned_id = _int_or_none(getattr(unique, "gift_id", None))
                        if returned_id != gift_id:
                            continue
                        issued = _int_or_none(getattr(unique, "availability_issued", None))
                        if issued is None:
                            continue
                        number = _int_or_none(getattr(unique, "num", None))
                        full_slug = _str_or_none(getattr(unique, "slug", None))
                        resolved_slug = base_slug_from_unique(full_slug or "", number) or base
                        return CatalogNumber(
                            gift_id=gift_id,
                            title=str(getattr(unique, "title", None) or title),
                            issued=issued,
                            total=_int_or_none(getattr(unique, "availability_total", None)),
                            slug=resolved_slug,
                        )
                    except errors.FloodWaitError as exc:
                        rate_limit.register(float(exc.seconds), "global_catalog_numbers")
                        return CatalogNumber(
                            gift_id, title, None, None, base, f"FLOOD_WAIT_{int(exc.seconds)}"
                        )
                    except RateLimitActiveError as exc:
                        return CatalogNumber(gift_id, title, None, None, base, str(exc))
                    except errors.RPCError as exc:
                        last_error = exc.__class__.__name__
                        if "STARGIFT_SLUG_INVALID" in str(exc).upper():
                            continue
                        logger.debug(
                            "global_catalog_item_rpc_error gift_id=%s slug=%s error=%s",
                            gift_id,
                            base,
                            exc,
                        )
                    except Exception as exc:
                        last_error = exc.__class__.__name__
                        logger.debug(
                            "global_catalog_item_error gift_id=%s slug=%s error=%s",
                            gift_id,
                            base,
                            exc,
                        )
            return CatalogNumber(gift_id, title, None, None, candidates[0], last_error or "not found")

        results = await asyncio.gather(*(resolve(gift) for gift in gifts))
        results.sort(key=lambda item: item.title.casefold())

        changed = False
        for item in results:
            if item.slug and item.issued is not None and self.store.settings.slug_map.get(str(item.gift_id)) != item.slug:
                self.store.settings.slug_map[str(item.gift_id)] = item.slug
                changed = True
        if changed:
            await self.store.save()

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        logger.info(
            "global_catalog_numbers_complete collections=%s resolved=%s errors=%s elapsed_ms=%.1f concurrency=%s",
            len(results),
            sum(1 for item in results if item.issued is not None),
            sum(1 for item in results if item.issued is None),
            elapsed_ms,
            max(1, int(concurrency)),
        )
        return results, elapsed_ms

    async def _remember_counter(self, counter: GiftCounter, *, probe: int | None = None) -> GiftCounter:
        self._slug_cache[counter.slug] = counter
        self._slug_cache_at[counter.slug] = time.monotonic()
        if probe is not None and probe > 0:
            self._slug_probe[counter.slug] = int(probe)
        if counter.base_gift_id and self.store.settings.slug_map.get(str(counter.base_gift_id)) != counter.slug:
            self.store.settings.slug_map[str(counter.base_gift_id)] = counter.slug
            await self.store.save()
        return counter

    async def _counter_from_unique(self, gift: Any, *, expected_gift_id: int) -> GiftCounter | None:
        if gift is None or gift.__class__.__name__ != "StarGiftUnique":
            return None
        gift_id = _int_or_none(getattr(gift, "gift_id", None))
        if gift_id != expected_gift_id:
            return None
        full_slug = _str_or_none(getattr(gift, "slug", None))
        number = _int_or_none(getattr(gift, "num", None))
        base_slug = base_slug_from_unique(full_slug or "", number)
        current = _int_or_none(getattr(gift, "availability_issued", None))
        if not base_slug or current is None:
            return None
        return await self._remember_counter(
            GiftCounter(
                slug=base_slug,
                title=str(getattr(gift, "title", None) or base_slug),
                current=current,
                total=_int_or_none(getattr(gift, "availability_total", None)),
                base_gift_id=gift_id,
            ),
            probe=number,
        )

    async def discover_counter_by_gift_id(self, gift_id: int, *, peer: Any | None = None) -> GiftCounter | None:
        """Resolve a collectible type from the exact regular gift ID.

        This avoids asking the user to type a slug.  The primary lookup uses
        ``payments.getResaleStarGifts(gift_id=...)`` because every returned
        collectible already contains its title, full slug and current issued
        counter.  If no item is currently on resale, owned unique gifts and
        the regular gift catalog are used as fallbacks.
        """
        gift_id = int(gift_id)

        resale_cls = getattr(functions.payments, "GetResaleStarGiftsRequest", None)
        if resale_cls is not None:
            try:
                request = construct(
                    resale_cls,
                    gift_id=gift_id,
                    offset="",
                    limit=1,
                    sort_by_num=True,
                )
                result = await self.call(request)
                for gift in list(getattr(result, "gifts", []) or []):
                    counter = await self._counter_from_unique(gift, expected_gift_id=gift_id)
                    if counter is not None:
                        logger.info("slug_auto_resolved source=resale gift_id=%s slug=%s", gift_id, counter.slug)
                        return counter
            except errors.FloodWaitError:
                raise
            except errors.RPCError as exc:
                logger.debug("slug_resale_lookup_rpc_error gift_id=%s error=%s", gift_id, exc)
            except Exception as exc:
                logger.debug("slug_resale_lookup_error gift_id=%s error=%s", gift_id, exc)

        if peer is not None:
            request_cls = getattr(functions.payments, "GetSavedStarGiftsRequest", None)
            if request_cls is not None:
                offset = ""
                try:
                    for _page in range(5):
                        result = await self.call(construct(request_cls, peer=peer, offset=offset, limit=100))
                        gifts = list(getattr(result, "gifts", []) or [])
                        for item in gifts:
                            counter = await self._counter_from_unique(
                                getattr(item, "gift", None), expected_gift_id=gift_id
                            )
                            if counter is not None:
                                logger.info(
                                    "slug_auto_resolved source=owned_unique gift_id=%s slug=%s",
                                    gift_id,
                                    counter.slug,
                                )
                                return counter
                        next_offset = getattr(result, "next_offset", None)
                        if not next_offset or not gifts:
                            break
                        offset = str(next_offset)
                except errors.FloodWaitError:
                    raise
                except Exception as exc:
                    logger.debug("slug_owned_lookup_error gift_id=%s error=%s", gift_id, exc)

        catalog = await self.get_gift_catalog(cache_seconds=0.0)
        regular = catalog.get(gift_id)
        title = _str_or_none(getattr(regular, "title", None)) if regular is not None else None
        if title:
            counter = await self.resolve_slug(title, expected_gift_id=gift_id, cache_seconds=0.0)
            if counter is not None:
                logger.info("slug_auto_resolved source=catalog_title gift_id=%s slug=%s", gift_id, counter.slug)
                return counter
        return None

    async def list_upgradable_infos(self, peer: Any) -> list[SavedGiftInfo]:
        gifts = await self.fetch_saved_gifts(peer, only_upgradable=True)
        catalog = await self.get_gift_catalog()
        infos: list[SavedGiftInfo] = []
        for item in gifts:
            saved_id = _int_or_none(getattr(item, "saved_id", None))
            gift = getattr(item, "gift", None)
            base_id = _int_or_none(getattr(gift, "id", None))
            if not saved_id or not base_id:
                continue
            catalog_gift = catalog.get(base_id)
            title = str(
                getattr(gift, "title", None)
                or (getattr(catalog_gift, "title", None) if catalog_gift is not None else None)
                or f"Gift {base_id}"
            )
            cached_slug = self.store.settings.slug_map.get(str(base_id))
            cost = _int_or_none(getattr(gift, "upgrade_stars", None)) or 0
            prepaid = bool(getattr(item, "upgrade_separate", False) or getattr(item, "upgrade_stars", None))
            infos.append(
                SavedGiftInfo(
                    saved_id=saved_id,
                    base_gift_id=base_id,
                    title=title,
                    slug=cached_slug,
                    can_upgrade=bool(getattr(item, "can_upgrade", False)),
                    prepaid=prepaid,
                    upgrade_cost=cost,
                    gift_num=_int_or_none(getattr(item, "gift_num", None)),
                    raw=item,
                )
            )
        return infos

    async def get_selected_infos(self, peer: Any) -> list[SavedGiftInfo]:
        selected = list(self.store.settings.selected_saved_ids)
        mapping = await self.fetch_saved_by_ids(peer, selected)
        catalog = await self.get_gift_catalog()
        infos: list[SavedGiftInfo] = []
        for saved_id in selected:
            item = mapping.get(saved_id)
            if item is None:
                continue
            gift = getattr(item, "gift", None)
            base_id = _int_or_none(getattr(gift, "id", None))
            if not base_id:
                # Already unique gifts expose gift_id rather than id; they are no longer candidates.
                continue
            catalog_gift = catalog.get(base_id)
            title = str(
                getattr(gift, "title", None)
                or (getattr(catalog_gift, "title", None) if catalog_gift is not None else None)
                or f"Gift {base_id}"
            )
            slug = self.store.settings.slug_map.get(str(base_id))
            if not slug and not title.startswith("Gift "):
                resolved = await self.resolve_slug(title, expected_gift_id=base_id)
                slug = resolved.slug if resolved else None
            infos.append(
                SavedGiftInfo(
                    saved_id=saved_id,
                    base_gift_id=base_id,
                    title=title,
                    slug=slug,
                    can_upgrade=bool(getattr(item, "can_upgrade", False)),
                    prepaid=bool(getattr(item, "upgrade_separate", False) or getattr(item, "upgrade_stars", None)),
                    upgrade_cost=_int_or_none(getattr(gift, "upgrade_stars", None)) or 0,
                    gift_num=_int_or_none(getattr(item, "gift_num", None)),
                    raw=item,
                )
            )
        return infos

    async def resolve_slug(self, query: str, *, expected_gift_id: int | None = None, cache_seconds: float = 2.0) -> GiftCounter | None:
        client = await self.require_authorized()
        last_error: Exception | None = None
        for base in slug_candidates(query):
            cached = self._slug_cache.get(base)
            cached_at = self._slug_cache_at.get(base, 0)
            if cached and time.monotonic() - cached_at <= cache_seconds:
                if expected_gift_id is None or cached.base_gift_id == expected_gift_id:
                    return cached
            for probe in (1, 2, 3, 10):
                slug = f"{base}-{probe}"
                try:
                    request = construct(functions.payments.GetUniqueStarGiftRequest, slug=slug)
                    result = await self.call(request)
                    gift = getattr(result, "gift", None)
                    current = _int_or_none(getattr(gift, "availability_issued", None))
                    if gift is None or current is None:
                        continue
                    base_gift_id = _int_or_none(getattr(gift, "gift_id", None))
                    if expected_gift_id is not None and base_gift_id != expected_gift_id:
                        continue
                    counter = GiftCounter(
                        slug=base,
                        title=str(getattr(gift, "title", None) or base),
                        current=current,
                        total=_int_or_none(getattr(gift, "availability_total", None)),
                        base_gift_id=base_gift_id,
                    )
                    self._slug_cache[base] = counter
                    self._slug_cache_at[base] = time.monotonic()
                    self._slug_probe[base] = probe
                    if base_gift_id and self.store.settings.slug_map.get(str(base_gift_id)) != base:
                        self.store.settings.slug_map[str(base_gift_id)] = base
                        await self.store.save()
                    return counter
                except errors.RPCError as exc:
                    last_error = exc
                    if "STARGIFT_SLUG_INVALID" not in str(exc).upper():
                        logger.debug("slug_lookup_rpc_error slug=%s error=%s", slug, exc)
                except Exception as exc:
                    last_error = exc
                    logger.debug("slug_lookup_error slug=%s error=%s", slug, exc)
        if last_error:
            logger.debug("slug_not_resolved query=%s last_error=%s", query, last_error)
        return None

    async def fetch_counter_fast(self, slug: str, *, expected_gift_id: int | None = None) -> GiftCounter:
        """Fetch one collectible counter with one MTProto request in the normal path."""
        client = await self.require_authorized()
        probes = [self._slug_probe.get(slug, 1), 1, 2, 3, 10]
        seen: set[int] = set()
        last_error: Exception | None = None
        for probe in probes:
            if probe in seen:
                continue
            seen.add(probe)
            try:
                request = construct(functions.payments.GetUniqueStarGiftRequest, slug=f"{slug}-{probe}")
                result = await self.call(request)
                gift = getattr(result, "gift", None)
                current = _int_or_none(getattr(gift, "availability_issued", None))
                if gift is None or current is None:
                    continue
                base_gift_id = _int_or_none(getattr(gift, "gift_id", None))
                if expected_gift_id is not None and base_gift_id != expected_gift_id:
                    raise RuntimeError(f"Slug {slug} относится к другому типу подарка")
                counter = GiftCounter(
                    slug=slug,
                    title=str(getattr(gift, "title", None) or slug),
                    current=current,
                    total=_int_or_none(getattr(gift, "availability_total", None)),
                    base_gift_id=base_gift_id,
                )
                self._slug_probe[slug] = probe
                self._slug_cache[slug] = counter
                self._slug_cache_at[slug] = time.monotonic()
                return counter
            except errors.RPCError as exc:
                last_error = exc
                if "STARGIFT_SLUG_INVALID" not in str(exc).upper():
                    raise
        raise RuntimeError(f"Не удалось получить текущий номер {slug}: {last_error or 'нет данных'}")

    async def fetch_exact_unique(
        self,
        slug: str,
        number: int,
        *,
        expected_gift_id: int | None = None,
    ) -> ExactGiftProbe | None:
        """Resolve one exact collectible number.

        ``None`` means Telegram explicitly reported ``STARGIFT_SLUG_INVALID``
        for this exact slug. Other RPC/network failures are raised so LIVE never
        treats an uncertain answer as proof that the target is still free.
        """
        await self.require_authorized()
        exact_number = int(number)
        if exact_number <= 0:
            return None
        full_slug = f"{slug}-{exact_number}"
        try:
            request = construct(functions.payments.GetUniqueStarGiftRequest, slug=full_slug)
            result = await self.call(request)
        except errors.RPCError as exc:
            if _is_stargift_slug_invalid(exc):
                return None
            raise

        gift = getattr(result, "gift", None)
        if gift is None or gift.__class__.__name__ != "StarGiftUnique":
            raise RuntimeError(f"Telegram вернул неожиданный ответ для {full_slug}")
        returned_num = _int_or_none(getattr(gift, "num", None))
        if returned_num != exact_number:
            raise RuntimeError(
                f"Telegram вернул номер {returned_num} вместо запрошенного {exact_number}"
            )
        base_gift_id = _int_or_none(getattr(gift, "gift_id", None))
        if expected_gift_id is not None and base_gift_id != expected_gift_id:
            raise RuntimeError(f"Slug {slug} относится к другому типу подарка")
        return ExactGiftProbe(
            slug=slug,
            title=str(getattr(gift, "title", None) or slug),
            num=returned_num,
            issued=_int_or_none(getattr(gift, "availability_issued", None)),
            total=_int_or_none(getattr(gift, "availability_total", None)),
            base_gift_id=base_gift_id,
        )


    async def counter_for_info(
        self,
        info: SavedGiftInfo,
        *,
        peer: Any | None = None,
        cache_seconds: float = 0.0,
    ) -> GiftCounter:
        slug = info.slug or self.store.settings.slug_map.get(str(info.base_gift_id))
        if slug:
            counter = await self.resolve_slug(
                slug,
                expected_gift_id=info.base_gift_id,
                cache_seconds=cache_seconds,
            )
            if counter:
                info.slug = counter.slug
                return counter

        # Exact gift-ID discovery is the normal path when Telegram omits the
        # optional regular-gift title from a saved gift object.
        counter = await self.discover_counter_by_gift_id(info.base_gift_id, peer=peer)
        if counter:
            info.slug = counter.slug
            info.title = counter.title
            return counter

        if info.title and not info.title.startswith("Gift "):
            counter = await self.resolve_slug(
                info.title,
                expected_gift_id=info.base_gift_id,
                cache_seconds=cache_seconds,
            )
            if counter:
                info.slug = counter.slug
                return counter
        raise RuntimeError(
            f"Не удалось автоматически определить тип подарка ID {info.base_gift_id}. "
            "Telegram не вернул ни одного коллекционного экземпляра или названия для этого типа. "
            "Можно прислать slug вручную, но сканер останется остановлен до успешной привязки."
        )

    async def prepare_upgrade(self, peer: Any, info: SavedGiftInfo) -> PreparedUpgrade:
        """Prepare one gift-specific upgrade request and capture its payment binding.

        Paid gifts receive a fresh getPaymentForm result. The returned form_id is
        bound to this exact InputInvoiceStarGiftUpgrade and is never shared or
        substituted between saved gifts.
        """
        await self.require_authorized()
        started = time.perf_counter()
        input_saved = self.input_saved(peer, info.saved_id)
        record_payment_event(
            "upgrade_prepare_started",
            saved_id=info.saved_id,
            base_gift_id=info.base_gift_id,
            prepaid=bool(info.prepaid),
        )
        if info.prepaid:
            request = construct(
                functions.payments.UpgradeStarGiftRequest,
                stargift=input_saved,
                keep_original_details=KEEP_ORIGINAL_DETAILS,
            )
            prepared = PreparedUpgrade(
                info.saved_id, input_saved, None, None, 0, True, time.monotonic(), request
            )
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            logger.info(
                "upgrade_prepaid_prepared saved_id=%s request_type=%s elapsed_ms=%.3f",
                info.saved_id,
                request.__class__.__name__,
                elapsed_ms,
            )
            record_payment_event(
                "upgrade_prepaid_prepared",
                saved_id=info.saved_id,
                request_type=request.__class__.__name__,
                elapsed_ms=round(elapsed_ms, 3),
            )
            return prepared

        invoice = construct(
            types.InputInvoiceStarGiftUpgrade,
            stargift=input_saved,
            keep_original_details=KEEP_ORIGINAL_DETAILS,
        )
        try:
            form_request = construct(
                functions.payments.GetPaymentFormRequest, invoice=invoice, theme_params=None
            )
            form = await self.call(form_request)
        except errors.RPCError as exc:
            code = self._rpc_code(exc)
            if "NO_PAYMENT_NEEDED" in str(exc).upper():
                request = construct(
                    functions.payments.UpgradeStarGiftRequest,
                    stargift=input_saved,
                    keep_original_details=KEEP_ORIGINAL_DETAILS,
                )
                prepared = PreparedUpgrade(
                    info.saved_id, input_saved, None, None, 0, True, time.monotonic(), request
                )
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                logger.info(
                    "upgrade_no_payment_needed saved_id=%s elapsed_ms=%.3f",
                    info.saved_id,
                    elapsed_ms,
                )
                record_payment_event(
                    "upgrade_no_payment_needed",
                    saved_id=info.saved_id,
                    elapsed_ms=round(elapsed_ms, 3),
                )
                return prepared
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            logger.warning(
                "payment_form_prepare_failed saved_id=%s code=%s error_type=%s elapsed_ms=%.3f error=%s",
                info.saved_id,
                code,
                type(exc).__name__,
                elapsed_ms,
                exc,
            )
            record_payment_event(
                "payment_form_prepare_failed",
                saved_id=info.saved_id,
                code=code,
                error_type=type(exc).__name__,
                error=str(exc)[:500],
                elapsed_ms=round(elapsed_ms, 3),
            )
            raise
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            logger.warning(
                "payment_form_prepare_failed saved_id=%s error_type=%s elapsed_ms=%.3f error=%s",
                info.saved_id,
                type(exc).__name__,
                elapsed_ms,
                exc,
            )
            record_payment_event(
                "payment_form_prepare_failed",
                saved_id=info.saved_id,
                error_type=type(exc).__name__,
                error=str(exc)[:500],
                elapsed_ms=round(elapsed_ms, 3),
            )
            raise

        cost = sum_invoice_amount(getattr(form, "invoice", None)) or info.upgrade_cost
        if cost <= 0:
            raise RuntimeError("Telegram не вернул положительную стоимость улучшения")
        if MAX_UPGRADE_STARS and cost > MAX_UPGRADE_STARS:
            raise RuntimeError(f"Цена улучшения {cost} ⭐ превышает лимит {MAX_UPGRADE_STARS} ⭐")
        form_id = _int_or_none(getattr(form, "form_id", None))
        if not form_id:
            raise RuntimeError("Telegram не вернул form_id для оплаты улучшения")
        request = construct(
            functions.payments.SendStarsFormRequest,
            form_id=int(form_id),
            invoice=invoice,
        )
        prepared = PreparedUpgrade(
            info.saved_id, input_saved, invoice, form_id, cost, False, time.monotonic(), request
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        debug = prepared_payment_debug(prepared)
        logger.info(
            "payment_form_prepared saved_id=%s form_id=%s invoice_saved_id=%s request_form_id=%s "
            "request_invoice_saved_id=%s cost=%s form_type=%s elapsed_ms=%.3f",
            info.saved_id,
            form_id,
            debug["invoice_saved_id"],
            debug["request_form_id"],
            debug["request_invoice_saved_id"],
            cost,
            form.__class__.__name__,
            elapsed_ms,
        )
        record_payment_event(
            "payment_form_prepared",
            saved_id=info.saved_id,
            form_id=form_id,
            invoice_saved_id=debug["invoice_saved_id"],
            request_form_id=debug["request_form_id"],
            request_invoice_saved_id=debug["request_invoice_saved_id"],
            cost=cost,
            form_type=form.__class__.__name__,
            elapsed_ms=round(elapsed_ms, 3),
        )
        return prepared

    async def _verify_unique(self, peer: Any, saved_id: int) -> tuple[int | None, str | None]:
        for delay in VERIFY_DELAYS_SECONDS:
            await asyncio.sleep(delay)
            try:
                mapping = await self.fetch_saved_by_ids(peer, [saved_id])
            except errors.FloodWaitError as exc:
                logger.warning("upgrade_verify_flood_wait saved_id=%s seconds=%s", saved_id, exc.seconds)
                return None, None
            except Exception as exc:
                logger.debug("upgrade_verify_failed saved_id=%s error=%s", saved_id, exc)
                continue
            saved = mapping.get(saved_id)
            gift = getattr(saved, "gift", None) if saved else None
            if gift is not None and gift.__class__.__name__ == "StarGiftUnique":
                return _int_or_none(getattr(gift, "num", None)), _str_or_none(getattr(gift, "slug", None))
        return None, None

    @staticmethod
    def _rpc_code(exc: BaseException) -> str:
        text = str(exc).upper()
        codes = (
            "FORM_EXPIRED", "STARS_FORM_AMOUNT_MISMATCH", "FORM_SUBMIT_DUPLICATE",
            "MSG_WAIT_FAILED", "MSG_WAIT_TIMEOUT",
            "BALANCE_TOO_LOW", "BOT_INVOICE_INVALID", "FORM_ID_EMPTY", "GIFT_STARS_INVALID",
            "INVOICE_INVALID", "SAVED_ID_EMPTY", "STARGIFT_ALREADY_CONVERTED",
            "STARGIFT_ALREADY_UPGRADED", "STARGIFT_NOT_FOUND", "STARGIFT_OWNER_INVALID",
            "STARGIFT_PEER_INVALID", "STARGIFT_UPGRADE_UNAVAILABLE", "PAYMENT_REQUIRED",
            "STARGIFT_USAGE_LIMITED", "STARGIFT_USER_USAGE_LIMITED", "FORM_UNSUPPORTED",
            "PRECHECKOUT_FAILED",
        )
        for code in codes:
            if code in text:
                return code
        class_map = {
            "FORMEXPIREDERROR": "FORM_EXPIRED",
            "STARSFORMAMOUNTMISMATCHERROR": "STARS_FORM_AMOUNT_MISMATCH",
            "FORMSUBMITDUPLICATEERROR": "FORM_SUBMIT_DUPLICATE",
            "MSGWAITFAILEDERROR": "MSG_WAIT_FAILED",
            "MSGWAITTIMEOUTERROR": "MSG_WAIT_TIMEOUT",
            "BALANCETOOLOWERROR": "BALANCE_TOO_LOW",
            "BOTINVOICEINVALIDERROR": "BOT_INVOICE_INVALID",
            "FORMIDEMPTYERROR": "FORM_ID_EMPTY",
            "GIFTSTARSINVALIDERROR": "GIFT_STARS_INVALID",
            "INVOICEINVALIDERROR": "INVOICE_INVALID",
            "SAVEDIDEMPTYERROR": "SAVED_ID_EMPTY",
            "STARGIFTALREADYCONVERTEDERROR": "STARGIFT_ALREADY_CONVERTED",
            "STARGIFTALREADYUPGRADEDERROR": "STARGIFT_ALREADY_UPGRADED",
            "STARGIFTNOTFOUNDERROR": "STARGIFT_NOT_FOUND",
            "STARGIFTOWNERINVALIDERROR": "STARGIFT_OWNER_INVALID",
            "STARGIFTPEERINVALIDERROR": "STARGIFT_PEER_INVALID",
            "STARGIFTUPGRADEUNAVAILABLEERROR": "STARGIFT_UPGRADE_UNAVAILABLE",
            "PAYMENTREQUIREDERROR": "PAYMENT_REQUIRED",
            "STARGIFTUSAGELIMITEDERROR": "STARGIFT_USAGE_LIMITED",
            "STARGIFTUSERUSAGELIMITEDERROR": "STARGIFT_USER_USAGE_LIMITED",
            "FORMUNSUPPORTEDERROR": "FORM_UNSUPPORTED",
            "PRECHECKOUTFAILEDERROR": "PRECHECKOUT_FAILED",
        }
        return class_map.get(exc.__class__.__name__.upper(), exc.__class__.__name__.upper())

    @staticmethod
    def _is_definitive_upgrade_error(code: str) -> bool:
        return code in {
            "BALANCE_TOO_LOW", "BOT_INVOICE_INVALID", "FORM_ID_EMPTY", "GIFT_STARS_INVALID",
            "INVOICE_INVALID", "SAVED_ID_EMPTY", "STARGIFT_ALREADY_CONVERTED",
            "STARGIFT_NOT_FOUND", "STARGIFT_OWNER_INVALID", "STARGIFT_PEER_INVALID",
            "STARGIFT_UPGRADE_UNAVAILABLE", "PAYMENT_REQUIRED", "STARGIFT_USAGE_LIMITED",
            "STARGIFT_USER_USAGE_LIMITED", "FORM_UNSUPPORTED", "PRECHECKOUT_FAILED",
        }

    async def _interpret_upgrade_result(self, peer: Any, saved_id: int, result: Any) -> UpgradeOutcome:
        verification = find_object_by_class_name(result, "PaymentVerificationNeeded")
        if verification is not None:
            return UpgradeOutcome(
                status="verification",
                verification_url=_str_or_none(getattr(verification, "url", None)),
                detail="Telegram запросил дополнительное подтверждение платежа",
            )

        unique = find_object_by_class_name(result, "StarGiftUnique")
        if unique is not None:
            unique_num = _int_or_none(getattr(unique, "num", None))
            if unique_num is not None:
                return UpgradeOutcome(
                    status="confirmed",
                    actual_num=unique_num,
                    actual_slug=_str_or_none(getattr(unique, "slug", None)),
                )

        actual_num, actual_slug = await self._verify_unique(peer, saved_id)
        if actual_num is not None:
            return UpgradeOutcome("confirmed", actual_num, actual_slug)
        return UpgradeOutcome(
            status="unknown",
            detail="Запрос принят, но Telegram не подтвердил результат. Повторная оплата не отправлялась.",
        )

    @staticmethod
    def _multi_error_parts(exc: BaseException, expected: int) -> tuple[list[Any], list[BaseException | None]] | None:
        """Duck-type Telethon MultiError without coupling tests to its class."""
        results = getattr(exc, "results", None)
        exceptions = getattr(exc, "exceptions", None)
        if not isinstance(results, (list, tuple)) or not isinstance(exceptions, (list, tuple)):
            return None
        if len(results) != expected or len(exceptions) != expected:
            return None
        normalized: list[BaseException | None] = []
        for item in exceptions:
            normalized.append(item if isinstance(item, BaseException) else None)
        return list(results), normalized

    async def _fast_outcome_from_exception(
        self,
        peer: Any,
        info: SavedGiftInfo,
        request: Any,
        exc: BaseException,
    ) -> UpgradeOutcome:
        if isinstance(exc, errors.FloodWaitError):
            source = f"FAST:{request.__class__.__name__}"
            remaining = rate_limit.register(float(exc.seconds), source)
            record_payment_event(
                "fast_payment_flood_wait",
                saved_id=info.saved_id,
                wait_seconds=int(exc.seconds),
                persistent_remaining_seconds=round(remaining, 3),
                source=source,
            )
            return UpgradeOutcome("failed", detail=f"FLOOD_WAIT_{int(exc.seconds)}: платёж не повторялся")

        if isinstance(exc, errors.RPCError):
            code = self._rpc_code(exc)
            if code in {"FORM_SUBMIT_DUPLICATE", "STARGIFT_ALREADY_UPGRADED"}:
                actual_num, actual_slug = await self._verify_unique(peer, info.saved_id)
                if actual_num is not None:
                    return UpgradeOutcome("confirmed", actual_num, actual_slug)
                return UpgradeOutcome("unknown", detail=f"{code}: результат не удалось подтвердить")
            if self._is_definitive_upgrade_error(code) or code in {
                "FORM_EXPIRED",
                "STARS_FORM_AMOUNT_MISMATCH",
            }:
                return UpgradeOutcome("failed", detail=f"{code}: {exc}")
            actual_num, actual_slug = await self._verify_unique(peer, info.saved_id)
            if actual_num is not None:
                return UpgradeOutcome("confirmed", actual_num, actual_slug)
            return UpgradeOutcome("unknown", detail=f"{code}: {type(exc).__name__}: {exc}")

        actual_num, actual_slug = await self._verify_unique(peer, info.saved_id)
        if actual_num is not None:
            return UpgradeOutcome("confirmed", actual_num, actual_slug)
        return UpgradeOutcome(
            "unknown",
            detail=f"{type(exc).__name__}: {exc}; FAST-повтор не отправлялся",
        )

    async def execute_upgrade_fast_batch(
        self,
        peer: Any,
        items: list[tuple[SavedGiftInfo, PreparedUpgrade]],
        *,
        client: TelegramClient,
        stagger_ms: int | None = None,
    ) -> list[UpgradeOutcome]:
        """Submit prebuilt FAST payments with a tiny configurable stagger.

        Live simultaneous-burst testing showed that truly simultaneous ``sendStarsForm``
        requests can collide with Telegram's duplicate-payment protection: one
        request succeeds while another may return ``FORM_SUBMIT_DUPLICATE``.
        v0039 keeps every form prebuilt and keeps ``ordered=False`` (so there is
        no ~0.5 s invokeAfterMsg/dependency delay), but queues adjacent payment
        requests a few milliseconds apart. The default is 10 ms and is persisted
        in settings via ``/stagger``.

        Production still submits directly to the already-connected MTProto
        sender, bypassing the TelegramClient request-retry loop. No failed or
        ambiguous financial request is automatically submitted a second time.
        """
        if not items:
            return []

        if stagger_ms is None:
            stagger_ms = self.store.settings.fast_volley_stagger_ms
        stagger_ms = min(MAX_FAST_VOLLEY_STAGGER_MS, max(0, int(stagger_ms)))
        stagger_ns = int(stagger_ms * 1_000_000)

        now = time.monotonic()
        for info, prepared in items:
            if prepared.saved_id != info.saved_id or prepared.request is None:
                raise RuntimeError(
                    f"FAST-план отсутствует или относится к другому подарку: saved_id={info.saved_id}"
                )
            if now - prepared.created_at > PAYMENT_FORM_MAX_AGE_SECONDS:
                raise RuntimeError(
                    f"FAST-платёжная форма устарела: saved_id={info.saved_id}; оплата не отправлена"
                )

        snapshot_before = self.connection_snapshot(client)
        record_payment_event(
            "fast_payment_batch_started",
            count=len(items),
            ordered=False,
            stagger_ms=stagger_ms,
            transport="telethon_sender_unordered_staggered",
            connection=snapshot_before,
            entries=[prepared_payment_debug(plan) for _info, plan in items],
        )

        raw_results: list[Any] = [None] * len(items)
        raw_errors: list[BaseException | None] = [None] * len(items)
        transport = "telethon_sender_unordered_staggered"
        queue_offsets_ms: list[float | None] = [None] * len(items)
        first_queue_ns: int | None = None
        last_queue_ns: int | None = None

        sender = getattr(client, "_sender", None)
        sender_send = getattr(sender, "send", None)
        if callable(sender_send):
            queued: list[tuple[int, Any]] = []
            for index, (info, prepared) in enumerate(items):
                if last_queue_ns is not None and stagger_ns > 0:
                    remaining_ns = stagger_ns - (time.perf_counter_ns() - last_queue_ns)
                    if remaining_ns > 0:
                        await asyncio.sleep(remaining_ns / 1_000_000_000.0)

                queue_ns = time.perf_counter_ns()
                prepared.fast_send_started_ns = queue_ns
                if first_queue_ns is None:
                    first_queue_ns = queue_ns
                queue_offsets_ms[index] = (queue_ns - first_queue_ns) / 1_000_000.0
                last_queue_ns = queue_ns

                try:
                    future = sender_send(prepared.request, ordered=False)
                    if isinstance(future, (list, tuple)):
                        futures = list(future)
                        if len(futures) != 1:
                            raise RuntimeError(
                                f"FAST sender вернул {len(futures)} future для одного запроса"
                            )
                        future = futures[0]
                    queued.append((index, future))
                except BaseException as exc:
                    if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                        raise
                    # This specific request may already be ambiguous; never retry.
                    raw_errors[index] = exc

            if queued:
                settled = await asyncio.gather(
                    *(future for _index, future in queued),
                    return_exceptions=True,
                )
                for (index, _future), value in zip(queued, settled):
                    if isinstance(value, BaseException):
                        raw_errors[index] = value
                    else:
                        raw_results[index] = value
        else:
            # Test/custom-client fallback only. Production TelegramClient always
            # exposes _sender; the direct-sender path above is what avoids the
            # normal client-level retry loop for Stars payments.
            transport = "telethon_client_unordered_staggered_fallback"
            tasks: list[tuple[int, asyncio.Task[Any]]] = []
            for index, (_info, prepared) in enumerate(items):
                if last_queue_ns is not None and stagger_ns > 0:
                    remaining_ns = stagger_ns - (time.perf_counter_ns() - last_queue_ns)
                    if remaining_ns > 0:
                        await asyncio.sleep(remaining_ns / 1_000_000_000.0)
                queue_ns = time.perf_counter_ns()
                prepared.fast_send_started_ns = queue_ns
                if first_queue_ns is None:
                    first_queue_ns = queue_ns
                queue_offsets_ms[index] = (queue_ns - first_queue_ns) / 1_000_000.0
                last_queue_ns = queue_ns
                tasks.append(
                    (index, asyncio.create_task(client(prepared.request, ordered=False)))
                )
            if tasks:
                settled = await asyncio.gather(
                    *(task for _index, task in tasks),
                    return_exceptions=True,
                )
                for (index, _task), value in zip(tasks, settled):
                    if isinstance(value, BaseException):
                        raw_errors[index] = value
                    else:
                        raw_results[index] = value

        record_payment_event(
            "fast_payment_batch_dispatched",
            count=len(items),
            ordered=False,
            stagger_ms=stagger_ms,
            queue_offsets_ms=[round(value, 3) if value is not None else None for value in queue_offsets_ms],
            transport=transport,
            submitted_saved_ids=[info.saved_id for info, _prepared in items],
            connection=self.connection_snapshot(client),
        )

        interpretation_jobs: list[tuple[int, asyncio.Task[UpgradeOutcome]]] = []
        outcomes: list[UpgradeOutcome | None] = [None] * len(items)
        for index, (info, prepared) in enumerate(items):
            error = raw_errors[index]
            if error is not None:
                interpretation_jobs.append(
                    (
                        index,
                        asyncio.create_task(
                            self._fast_outcome_from_exception(
                                peer, info, prepared.request, error
                            )
                        ),
                    )
                )
            else:
                interpretation_jobs.append(
                    (
                        index,
                        asyncio.create_task(
                            self._interpret_upgrade_result(peer, info.saved_id, raw_results[index])
                        ),
                    )
                )

        if interpretation_jobs:
            interpreted = await asyncio.gather(
                *(task for _index, task in interpretation_jobs),
                return_exceptions=True,
            )
            for (index, _task), value in zip(interpretation_jobs, interpreted):
                if isinstance(value, UpgradeOutcome):
                    outcomes[index] = value
                elif isinstance(value, BaseException):
                    outcomes[index] = UpgradeOutcome(
                        "unknown",
                        detail=f"{type(value).__name__}: {value}; результат проверки неясен",
                    )
                else:
                    outcomes[index] = UpgradeOutcome(
                        "unknown", detail="Неожиданный результат проверки FAST-платежа"
                    )

        snapshot_after = self.connection_snapshot(client)
        final = [
            outcome
            if isinstance(outcome, UpgradeOutcome)
            else UpgradeOutcome("unknown", detail="FAST batch завершился без результата")
            for outcome in outcomes
        ]
        record_payment_event(
            "fast_payment_batch_finished",
            count=len(items),
            phases=1,
            ordered=False,
            stagger_ms=stagger_ms,
            queue_offsets_ms=[round(value, 3) if value is not None else None for value in queue_offsets_ms],
            transport=transport,
            connection_before=snapshot_before,
            connection_after=snapshot_after,
            statuses=[outcome.status for outcome in final],
        )
        return final

    async def execute_upgrade_fast(
        self,
        peer: Any,
        info: SavedGiftInfo,
        prepared: PreparedUpgrade,
        *,
        client: TelegramClient,
    ) -> UpgradeOutcome:
        """Submit exactly one prebuilt request with no refresh and no retry.

        The caller launches one or more of these coroutines together.  No global
        request lock is used: the exact-number probe has already completed and
        Telethon can multiplex the independent payment RPCs on the hot session.
        All verification and state writes happen only after the first submission.
        """
        if prepared.saved_id != info.saved_id or prepared.request is None:
            return UpgradeOutcome("failed", detail="FAST-план отсутствует или относится к другому подарку")
        if time.monotonic() - prepared.created_at > PAYMENT_FORM_MAX_AGE_SECONDS:
            return UpgradeOutcome("failed", detail="FAST-платёжная форма устарела; повторный запрос запрещён")

        try:
            prepared.fast_send_started_ns = time.perf_counter_ns()
            result = await client(prepared.request)
            return await self._interpret_upgrade_result(peer, info.saved_id, result)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            return await self._fast_outcome_from_exception(peer, info, prepared.request, exc)



def construct(cls: Any, **kwargs: Any) -> Any:
    """Instantiate generated Telethon classes while tolerating layer-specific optional fields."""
    try:
        signature = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        return cls(**kwargs)
    parameters = signature.parameters
    accepted = {key: value for key, value in kwargs.items() if key in parameters}
    return cls(**accepted)


class StatusMessageUpdater:
    """Edit one plain status card without ever coupling UI failures to scanning.

    Telegram RetryAfter values are obeyed as hard pauses. The exact permanent
    ``message can't be edited`` error replaces the stale card once with a plain
    message; other failures never start a SendMessage retry loop. A manual
    refresh can also publish one full RAM snapshot without touching MTProto.
    """

    def __init__(
        self,
        bot_getter: Any,
        *,
        name: str,
        min_interval_seconds: float,
        urgent_min_interval_seconds: float | None = None,
    ) -> None:
        self.bot_getter = bot_getter
        self.name = name
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        if urgent_min_interval_seconds is None:
            urgent_min_interval_seconds = min(
                self.min_interval_seconds,
                STATUS_URGENT_MIN_INTERVAL_SECONDS,
            )
        self.urgent_min_interval_seconds = max(0.0, float(urgent_min_interval_seconds))
        self.chat_id: int | None = None
        self.message_id: int | None = None
        self._last_edit_at = 0.0
        self._last_text: str | None = None
        self._retry_after_until = 0.0
        self._pause_reason: str | None = None
        self._lock = asyncio.Lock()

    def attach(self, chat_id: int, message_id: int) -> None:
        self.chat_id = int(chat_id)
        self.message_id = int(message_id)
        self._last_edit_at = 0.0
        self._last_text = None
        # Keep an existing Telegram pause when a fresh card is attached in the
        # same process. A redeploy starts with zero and learns the remaining
        # RetryAfter on the first rejected edit.

    def clear(self) -> None:
        self.chat_id = None
        self.message_id = None
        self._last_edit_at = 0.0
        self._last_text = None
        self._retry_after_until = 0.0
        self._pause_reason = None

    @property
    def retry_after_remaining(self) -> float:
        return max(0.0, self._retry_after_until - time.monotonic())

    @property
    def pause_reason(self) -> str | None:
        return self._pause_reason if self.retry_after_remaining > 0 else None

    def _apply_pause(
        self,
        delay_seconds: float,
        *,
        reason: str,
        error: BaseException,
    ) -> None:
        delay = max(1.0, float(delay_seconds)) + 0.25
        now = time.monotonic()
        self._retry_after_until = max(self._retry_after_until, now + delay)
        self._pause_reason = reason
        self._last_edit_at = now
        logger.warning(
            "%s_status_rate_limited retry_after_s=%.2f blocked_until_monotonic=%.3f reason=%s error=%s",
            self.name,
            delay,
            self._retry_after_until,
            reason,
            error,
        )

    def _apply_retry_after(self, retry_after: float, *, error: BaseException) -> None:
        self._apply_pause(
            retry_after,
            reason="Telegram RetryAfter",
            error=error,
        )

    async def replace_with_new_message(
        self,
        text: str,
        *,
        chat_id: int | None = None,
        reply_markup: Any = None,
    ) -> int:
        """Send one fresh RAM snapshot and switch future edits to it.

        The existing edit pause is deliberately preserved. Thus the button can
        show current state through a new plain card, but it cannot restart an
        edit loop or force edits before Telegram's RetryAfter expires.
        """
        async with self._lock:
            target_chat_id = int(chat_id) if chat_id is not None else self.chat_id
            if target_chat_id is None:
                raise RuntimeError("Чат статусной карточки не задан")
            bot = self.bot_getter()
            if bot is None:
                raise RuntimeError("Bot API ещё не готов")

            try:
                replacement = await bot.send_message(
                    chat_id=target_chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
            except TelegramRetryAfter as exc:
                self._apply_retry_after(getattr(exc, "retry_after", 1.0), error=exc)
                raise
            except Exception as exc:
                retry_after = getattr(exc, "retry_after", None)
                if retry_after is not None:
                    self._apply_retry_after(retry_after, error=exc)
                raise

            new_message_id = int(getattr(replacement, "message_id"))
            old_message_id = self.message_id
            now = time.monotonic()
            self.chat_id = target_chat_id
            self.message_id = new_message_id
            self._last_text = text
            self._last_edit_at = now
            logger.warning(
                "%s_status_message_manually_replaced chat_id=%s old_message_id=%s new_message_id=%s pause_remaining_s=%.2f",
                self.name,
                target_chat_id,
                old_message_id,
                new_message_id,
                self.retry_after_remaining,
            )
            return new_message_id

    async def update(self, text: str, *, force: bool = False) -> bool:
        if self.chat_id is None or self.message_id is None:
            return False
        now = time.monotonic()
        if now < self._retry_after_until:
            return False
        required_interval = self.urgent_min_interval_seconds if force else self.min_interval_seconds
        if now - self._last_edit_at < required_interval:
            return False
        if not force and text == self._last_text:
            self._last_edit_at = now
            return False

        async with self._lock:
            if self.chat_id is None or self.message_id is None:
                return False
            now = time.monotonic()
            if now < self._retry_after_until:
                return False
            required_interval = self.urgent_min_interval_seconds if force else self.min_interval_seconds
            if now - self._last_edit_at < required_interval:
                return False
            if not force and text == self._last_text:
                self._last_edit_at = now
                return False

            bot = self.bot_getter()
            if bot is None:
                return False
            try:
                await bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self.message_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                )
                first_success = self._last_text is None
                self._last_text = text
                self._last_edit_at = now
                self._pause_reason = None
                if first_success:
                    logger.info(
                        "%s_status_edit_started chat_id=%s message_id=%s",
                        self.name,
                        self.chat_id,
                        self.message_id,
                    )
                return True
            except TelegramRetryAfter as exc:
                self._apply_retry_after(getattr(exc, "retry_after", 1.0), error=exc)
                return False
            except TelegramBadRequest as exc:
                lowered = str(exc).lower()
                if "message is not modified" in lowered:
                    self._last_text = text
                    self._last_edit_at = now
                    return True
                if "message can't be edited" in lowered or "message can not be edited" in lowered:
                    # The old card is no longer editable. Replace it once with a
                    # plain status message; never attach the control keyboard to
                    # the card and never turn this exact error into a 15-minute
                    # local cooldown. The scanner does not await this UI path.
                    try:
                        replacement = await bot.send_message(
                            chat_id=self.chat_id,
                            text=text,
                            parse_mode=ParseMode.HTML,
                        )
                    except TelegramRetryAfter as send_exc:
                        self._apply_retry_after(
                            getattr(send_exc, "retry_after", 1.0), error=send_exc
                        )
                        return False
                    except Exception as send_exc:
                        retry_after = getattr(send_exc, "retry_after", None)
                        if retry_after is not None:
                            self._apply_retry_after(retry_after, error=send_exc)
                        else:
                            self._apply_pause(
                                STATUS_TRANSIENT_FAILURE_COOLDOWN_SECONDS,
                                reason="не удалось заменить карточку",
                                error=send_exc,
                            )
                        return False
                    old_message_id = self.message_id
                    self.message_id = int(getattr(replacement, "message_id"))
                    self._last_text = text
                    self._last_edit_at = time.monotonic()
                    self._pause_reason = None
                    logger.warning(
                        "%s_status_message_replaced_after_uneditable chat_id=%s old_message_id=%s new_message_id=%s",
                        self.name, self.chat_id, old_message_id, self.message_id,
                    )
                    return True
                self._apply_pause(
                    STATUS_TRANSIENT_FAILURE_COOLDOWN_SECONDS,
                    reason="EditMessageText отклонён",
                    error=exc,
                )
                return False
            except Exception as exc:
                retry_after = getattr(exc, "retry_after", None)
                if retry_after is not None:
                    self._apply_retry_after(retry_after, error=exc)
                    return False
                self._apply_pause(
                    STATUS_TRANSIENT_FAILURE_COOLDOWN_SECONDS,
                    reason="ошибка интерфейса",
                    error=exc,
                )
                return False


mtproto = MTProtoService(store)


class Scanner:
    def __init__(self, service: MTProtoService, bot_getter: Any):
        self.service = service
        self.bot_getter = bot_getter
        self.task: asyncio.Task[None] | None = None
        self.monitor_task: asyncio.Task[None] | None = None
        self.form_refresh_task: asyncio.Task[None] | None = None
        self.stop_event = asyncio.Event()
        self.prepared: dict[int, PreparedUpgrade] = {}
        self._form_refresh_retry_after: dict[int, float] = {}
        self._form_refresh_lock = asyncio.Lock()
        self._form_refresh_latency_ewma_s = FORM_REFRESH_LATENCY_INITIAL_SECONDS
        self._critical_form_ready: set[tuple[str, int]] = set()
        self._critical_form_unarmed: dict[tuple[str, int], str] = {}
        self._critical_form_retry_after: dict[tuple[str, int], float] = {}
        self._payment_guard_keys: set[tuple[str, int]] = set()
        self._payment_guard_id: str | None = None
        self.triggered: set[tuple[str, int]] = set()
        self.notified_missed: set[tuple[str, int]] = set()
        self.status_chat_id: int | None = None
        self.status_message_id: int | None = None
        self.status_updater = StatusMessageUpdater(
            bot_getter,
            name="scanner",
            min_interval_seconds=LIVE_STATUS_INTERVAL_SECONDS,
        )
        self._last_diagnostics_write = 0.0
        self._last_poll_started: float | None = None
        self._status_wakeup = asyncio.Event()
        self._manual_status_refresh_lock = asyncio.Lock()
        self._last_manual_status_refresh_at = 0.0
        self._plan_dirty = True
        self._groups: dict[str, list[SavedGiftInfo]] = {}
        self._counter_meta: dict[str, GiftCounter] = {}
        self._campaign_ids_by_slug: dict[str, str] = {}
        self._slug_by_campaign_id: dict[str, str] = {}
        self._exact_probe_cursor: dict[str, int] = {}
        self._last_exact_regular_refresh: dict[str, float] = {}
        self._fast_fired = False
        self._fast_client: TelegramClient | None = None
        self._fast_peer: Any | None = None
        self._fast_trigger_detected_at: float | None = None
        self._gc_disabled_by_scanner = False
        self._quiet_mode = False
        self._quiet_final_task: asyncio.Task[None] | None = None
        self.rate = AdaptiveRateController(
            min_interval_ms=SCAN_MIN_INTERVAL_MS,
            max_interval_ms=SCAN_MAX_INTERVAL_MS,
            start_interval_ms=SCAN_START_INTERVAL_MS,
            accelerate_every=SCAN_ACCELERATE_EVERY,
            accelerate_factor=SCAN_ACCELERATE_FACTOR,
            backoff_factor=SCAN_BACKOFF_FACTOR,
            backoff_floor_ms=SCAN_BACKOFF_FLOOR_MS,
        )

    def _enter_fast_quiet(self) -> None:
        if self._quiet_mode:
            return
        self._quiet_mode = True
        runtime.fast_quiet = True
        monitor = self.monitor_task
        if monitor and monitor is not asyncio.current_task():
            monitor.cancel()
        self.monitor_task = None
        # Publish one final full card with the quiet-mode line. This task is
        # detached from the scanner; after it runs, automatic edits stay off.
        if self.status_chat_id is not None:
            self._quiet_final_task = asyncio.create_task(
                self._publish_quiet_entry_card(),
                name="gift-scanner-quiet-card",
            )
        if FAST_DISABLE_GC and runtime.active and gc.isenabled():
            gc.disable()
            self._gc_disabled_by_scanner = True

    async def _publish_quiet_entry_card(self) -> None:
        try:
            await asyncio.sleep(self.status_updater.urgent_min_interval_seconds)
            if self._quiet_mode and runtime.active and not self.stop_event.is_set():
                await self.refresh_status_message(force=True)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("quiet_entry_card_failed error=%s", exc)

    def _leave_fast_quiet(self) -> None:
        runtime.fast_quiet = False
        self._quiet_mode = False
        quiet_task = self._quiet_final_task
        if quiet_task and not quiet_task.done():
            quiet_task.cancel()
        self._quiet_final_task = None
        if self._gc_disabled_by_scanner:
            gc.enable()
            self._gc_disabled_by_scanner = False

    def attach_status_message(self, chat_id: int, message_id: int) -> None:
        self.status_chat_id = int(chat_id)
        self.status_message_id = int(message_id)
        self.status_updater.attach(chat_id, message_id)
        self._status_wakeup.set()
        if runtime.active and not runtime.fast_quiet and (self.monitor_task is None or self.monitor_task.done()):
            self.monitor_task = asyncio.create_task(self._monitor_loop(), name="gift-scanner-monitor")

    async def refresh_status_message(self, *, force: bool = False) -> bool:
        text = await status_text()
        updated = await self.status_updater.update(text, force=force)
        self.status_chat_id = self.status_updater.chat_id
        self.status_message_id = self.status_updater.message_id
        return updated

    async def manually_replace_status_message(
        self,
        chat_id: int,
        *,
        reply_markup: Any = None,
    ) -> int:
        """Send a new full card from RAM without probing MTProto or adding a keyboard."""
        async with self._manual_status_refresh_lock:
            now = time.monotonic()
            remaining = (
                STATUS_MANUAL_REFRESH_COOLDOWN_SECONDS
                - (now - self._last_manual_status_refresh_at)
            )
            if remaining > 0:
                raise RuntimeError(
                    f"Подожди ещё {int(remaining + 0.999)}с перед следующим перевыпуском карточки"
                )

            text = await status_text()
            new_message_id = await self.status_updater.replace_with_new_message(
                text,
                chat_id=chat_id,
                reply_markup=reply_markup,
            )
            self.status_chat_id = self.status_updater.chat_id
            self.status_message_id = new_message_id
            self._last_manual_status_refresh_at = time.monotonic()
            self._status_wakeup.clear()
            if runtime.active and not runtime.fast_quiet and (self.monitor_task is None or self.monitor_task.done()):
                self.monitor_task = asyncio.create_task(
                    self._monitor_loop(),
                    name="gift-scanner-monitor",
                )
            return new_message_id

    async def _maybe_write_diagnostics(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if force or now - self._last_diagnostics_write >= DIAGNOSTICS_INTERVAL_SECONDS:
            await write_diagnostics()
            self._last_diagnostics_write = now

    async def _wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=max(0.0, seconds))
        except asyncio.TimeoutError:
            pass

    async def _monitor_loop(self) -> None:
        """Refresh once per minute and immediately for meaningful changes.

        UI updates stay isolated from the MTProto scanner. Telegram pauses are
        obeyed without automatic SendMessage retries; the scanner never waits
        for this loop.
        """
        next_heartbeat = time.monotonic() + LIVE_STATUS_INTERVAL_SECONDS
        try:
            while runtime.active and not self.stop_event.is_set():
                await self._maybe_write_diagnostics()
                now = time.monotonic()
                timeout = min(0.5, max(0.0, next_heartbeat - now))
                woke_for_change = False
                try:
                    await asyncio.wait_for(self._status_wakeup.wait(), timeout=timeout)
                    self._status_wakeup.clear()
                    woke_for_change = True
                except asyncio.TimeoutError:
                    pass

                if not runtime.active or self.stop_event.is_set():
                    break

                now = time.monotonic()
                if woke_for_change:
                    updated = await self.refresh_status_message(force=True)
                    # A change that lands inside the one-second urgent throttle
                    # must not be lost until the next minute heartbeat. Retry once
                    # after the short local throttle, but never during Telegram's
                    # explicit UI pause.
                    if (
                        not updated
                        and self.status_updater.retry_after_remaining <= 0
                        and runtime.active
                        and not self.stop_event.is_set()
                    ):
                        await self._wait(self.status_updater.urgent_min_interval_seconds)
                        if runtime.active and not self.stop_event.is_set():
                            await self.refresh_status_message(force=True)
                    next_heartbeat = time.monotonic() + LIVE_STATUS_INTERVAL_SECONDS
                elif now >= next_heartbeat:
                    await self.refresh_status_message()
                    next_heartbeat = now + LIVE_STATUS_INTERVAL_SECONDS
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("scanner_monitor_failed error=%s", exc)

    async def _load_plan(self, peer: Any) -> None:
        confirmed_holds, pending_holds = await self.service.reconcile_payment_holds(peer)
        if confirmed_holds:
            for _saved_id, number, slug in confirmed_holds:
                logger.warning("payment_hold_reconciled number=%s slug=%s", number, slug)
        if pending_holds and store.settings.live_upgrades:
            raise RuntimeError(
                PENDING_PAYMENT_HOLD_MESSAGE
                + " Открой «🎁 Подарки» и нажми «Обновить» для повторной сверки."
            )

        infos = await self.service.get_selected_infos(peer)
        if not infos:
            raise RuntimeError("Выбранные подарки не найдены или уже улучшены")

        runtime.current_by_slug.clear()
        runtime.title_by_slug.clear()
        groups: dict[str, list[SavedGiftInfo]] = {}
        counters: dict[str, GiftCounter] = {}
        counter_by_base: dict[int, GiftCounter] = {}
        for info in infos:
            counter = counter_by_base.get(info.base_gift_id)
            if counter is None:
                counter = await self.service.counter_for_info(info, peer=peer, cache_seconds=0)
                counter_by_base[info.base_gift_id] = counter
            info.slug = counter.slug
            groups.setdefault(counter.slug, []).append(info)
            counters[counter.slug] = counter

        if len(groups) > 1:
            raise RuntimeError("Для максимальной скорости выбери подарки только одного типа")

        current = next(iter(counters.values())).current
        future_targets = sorted({value for value in store.settings.target_numbers if value > current})
        passed_targets = [value for value in store.settings.target_numbers if value <= current]
        if passed_targets:
            store.settings.target_numbers = future_targets
            await store.save()
        if not future_targets:
            raise RuntimeError(f"Все номера выстрела уже прошли. Текущий номер: {current}")

        for slug, counter in counters.items():
            runtime.current_by_slug[slug] = counter.current
            runtime.title_by_slug[slug] = counter.title

        self._groups = groups
        self._counter_meta = counters
        self._campaign_ids_by_slug = {
            slug: campaign_id_for(slug, counter.base_gift_id)
            for slug, counter in counters.items()
        }
        self._slug_by_campaign_id = {
            campaign_id: slug for slug, campaign_id in self._campaign_ids_by_slug.items()
        }
        self._plan_dirty = False

        # Keep only plans that still belong to selected gifts. For consecutive
        # targets, prepare every required instance ahead of time so the second
        # upgrade does not pause to fetch a payment form after the first success.
        selected_ids = {item.saved_id for item in infos}
        self.prepared = {saved_id: plan for saved_id, plan in self.prepared.items() if saved_id in selected_ids}
        if store.settings.live_upgrades:
            for slug, group in groups.items():
                current = counters[slug].current
                future_targets = sorted({target for target in store.settings.target_numbers if target > current})
                required = min(len(group), effective_volley_size())
                if len(group) < effective_volley_size():
                    raise RuntimeError(
                        f"Для залпа {effective_volley_size()} выбрано только {len(group)} подарков"
                    )
                for candidate in group[:required]:
                    existing = self.prepared.get(candidate.saved_id)
                    if existing is not None and time.monotonic() - existing.created_at <= PREPARE_REFRESH_SECONDS:
                        continue
                    self.prepared[candidate.saved_id] = await self.service.prepare_upgrade(peer, candidate)
                    logger.info(
                        "upgrade_prepared_on_plan slug=%s saved_id=%s cost=%s",
                        slug,
                        candidate.saved_id,
                        self.prepared[candidate.saved_id].cost,
                    )

    def _fast_candidates_from_ram(self) -> list[SavedGiftInfo]:
        """Return the currently armed FAST candidates without network or disk I/O."""
        required = effective_volley_size()
        output: list[SavedGiftInfo] = []
        selected = set(store.settings.selected_saved_ids)
        for group in self._groups.values():
            output.extend(
                item for item in group if item.saved_id in selected and item.can_upgrade
            )
        return output[:required]

    @staticmethod
    def _selected_fast_candidates(group: list[SavedGiftInfo]) -> list[SavedGiftInfo]:
        required = effective_volley_size()
        selected = set(store.settings.selected_saved_ids)
        return [
            item for item in group
            if item.saved_id in selected and item.can_upgrade
        ][:required]

    def _fast_ammo_error(self, candidates: list[SavedGiftInfo]) -> str | None:
        """Return why the in-RAM FAST volley is not fully armed, without I/O."""
        required = effective_volley_size()
        if len(candidates) != required:
            return f"нужно {required} готовых подарков, доступно {len(candidates)}"

        paid_form_ids: list[int] = []
        now = time.monotonic()
        for candidate in candidates:
            plan = self.prepared.get(candidate.saved_id)
            if plan is None or plan.request is None or plan.saved_id != candidate.saved_id:
                return f"saved_id={candidate.saved_id}: платёжный план отсутствует"
            if not plan.prepaid and now - plan.created_at > PAYMENT_FORM_MAX_AGE_SECONDS:
                return f"saved_id={candidate.saved_id}: платёжная форма устарела"
            try:
                self._validate_prepared_binding(candidate, plan)
            except Exception as exc:
                return f"saved_id={candidate.saved_id}: {exc}"
            if not plan.prepaid and plan.form_id is not None:
                paid_form_ids.append(int(plan.form_id))

        if len(paid_form_ids) != len(set(paid_form_ids)):
            return "обнаружены повторяющиеся form_id"
        return None

    def _reset_critical_form_state(self) -> None:
        self._critical_form_ready.clear()
        self._critical_form_unarmed.clear()
        self._critical_form_retry_after.clear()

    async def _clear_prearmed_payment_guards(self) -> None:
        """Remove guards that this live process armed but never submitted."""
        if not self._payment_guard_keys:
            return
        keys = set(self._payment_guard_keys)
        try:
            await asyncio.to_thread(remove_payment_submission_guard_entries, keys)
        except OSError as exc:
            logger.error("payment_submission_guard_cleanup_failed keys=%s error=%s", sorted(keys), exc)
            return
        self._payment_guard_keys.difference_update(keys)
        if not self._payment_guard_keys:
            self._payment_guard_id = None

    async def _prepare_fast_forms(
        self,
        peer: Any,
        slug: str,
        target: int,
        group: list[SavedGiftInfo],
    ) -> bool:
        """Force-refresh every FAST form once before the hot zone.

        The normal background refresher deliberately keeps running afterwards.
        The frontier can remain inside the hot zone for hours; permanently
        freezing forms here would let otherwise valid FAST ammunition expire.
        """
        key = (slug, int(target))
        candidates = self._selected_fast_candidates(group)

        # A previously armed key is cheap to validate from RAM on every pass.
        # If a worker died or Telegram could not refresh for long enough, do not
        # keep trusting a stale "ready" bit: drop it and rebuild the forms.
        if key in self._critical_form_ready:
            reason = self._fast_ammo_error(candidates)
            if reason is None:
                return True
            self._critical_form_ready.discard(key)
            self._critical_form_unarmed[key] = reason

        retry_after = self._critical_form_retry_after.get(key, 0.0)
        if retry_after > time.monotonic():
            return False
        self._critical_form_unarmed.pop(key, None)

        required = effective_volley_size()
        if len(candidates) != required:
            reason = f"нужно {required} подарков, доступно {len(candidates)}"
            self._critical_form_unarmed[key] = reason
            self._critical_form_retry_after[key] = time.monotonic() + 15.0
            runtime.last_error = f"FAST-залп не вооружён: {reason}"
            return False

        required_refresh_ids = {
            candidate.saved_id
            for candidate in candidates
            if not (
                (existing := self.prepared.get(candidate.saved_id)) is not None
                and existing.prepaid
            )
        }
        refreshed_ids: set[int] = set()
        await self._refresh_due_payment_forms(
            peer,
            force=True,
            max_items=len(candidates),
            candidates=candidates,
            refreshed_ids=refreshed_ids,
        )
        missing_refreshes = sorted(required_refresh_ids - refreshed_ids)
        reason = (
            "forced refresh не завершён для saved_id=" + ",".join(map(str, missing_refreshes))
            if missing_refreshes
            else self._fast_ammo_error(candidates)
        )
        if reason is not None:
            self._critical_form_unarmed[key] = reason
            self._critical_form_retry_after[key] = time.monotonic() + 15.0
            runtime.last_error = f"FAST-залп не вооружён: {reason}"
            logger.error(
                "fast_forms_unarmed slug=%s target=%s candidates=%s reason=%s",
                slug,
                target,
                [item.saved_id for item in candidates],
                reason,
            )
            record_payment_event(
                "fast_forms_unarmed",
                slug=slug,
                target=target,
                saved_ids=[item.saved_id for item in candidates],
                reason=reason[:500],
            )
            return False

        try:
            guard_id = await asyncio.to_thread(
                add_payment_submission_guard_entry,
                slug=slug,
                target=target,
                saved_ids=[item.saved_id for item in candidates],
                campaign_id=self._campaign_ids_by_slug.get(slug),
            )
        except OSError as exc:
            reason = f"persistent payment guard не записан: {exc}"
            self._critical_form_unarmed[key] = reason
            self._critical_form_retry_after[key] = time.monotonic() + 15.0
            runtime.last_error = f"FAST-залп не вооружён: {reason}"
            logger.exception(
                "fast_payment_guard_arm_failed slug=%s target=%s saved_ids=%s",
                slug,
                target,
                [item.saved_id for item in candidates],
            )
            return False
        self._payment_guard_id = guard_id
        self._payment_guard_keys.add(key)
        self._critical_form_unarmed.pop(key, None)
        self._critical_form_retry_after.pop(key, None)
        self._critical_form_ready.add(key)
        runtime.last_error = None
        logger.info(
            "fast_forms_refreshed_and_armed slug=%s target=%s candidates=%s arm_distance=%s",
            slug,
            target,
            [item.saved_id for item in candidates],
            FAST_FORM_ARM_DISTANCE,
        )
        record_payment_event(
            "fast_forms_refreshed_and_armed",
            slug=slug,
            target=target,
            saved_ids=[item.saved_id for item in candidates],
            arm_distance=FAST_FORM_ARM_DISTANCE,
        )
        return True

    def _update_payment_form_age_metric(self) -> None:
        now = time.monotonic()
        ages = [
            max(0.0, now - plan.created_at)
            for plan in self.prepared.values()
            if not plan.prepaid
        ]
        runtime.payment_form_oldest_age_s = round(max(ages), 3) if ages else None

    @staticmethod
    def _validate_prepared_binding(candidate: SavedGiftInfo, plan: PreparedUpgrade) -> None:
        """Fail closed unless a prepared request is bound to exactly one saved gift."""
        if plan.saved_id != candidate.saved_id or plan.request is None:
            raise RuntimeError(
                f"prepared binding mismatch: candidate={candidate.saved_id}, plan_saved_id={plan.saved_id}"
            )
        if plan.prepaid:
            return
        plan_invoice_saved_id = invoice_saved_id(plan.invoice)
        request_form_id = _int_or_none(getattr(plan.request, "form_id", None))
        request_invoice_saved_id = invoice_saved_id(getattr(plan.request, "invoice", None))
        if plan.form_id is None or request_form_id is None:
            raise RuntimeError(
                f"prepared form_id missing: candidate={candidate.saved_id}, "
                f"plan_form_id={plan.form_id}, request_form_id={request_form_id}"
            )
        if plan_invoice_saved_id != candidate.saved_id:
            raise RuntimeError(
                f"prepared invoice mismatch: candidate={candidate.saved_id}, "
                f"invoice_saved_id={plan_invoice_saved_id}, form_id={plan.form_id}"
            )
        if request_invoice_saved_id != candidate.saved_id:
            raise RuntimeError(
                f"prepared request invoice mismatch: candidate={candidate.saved_id}, "
                f"request_invoice_saved_id={request_invoice_saved_id}, form_id={plan.form_id}"
            )
        if request_form_id != plan.form_id:
            raise RuntimeError(
                f"prepared request form mismatch: candidate={candidate.saved_id}, "
                f"plan_form_id={plan.form_id}, request_form_id={request_form_id}"
            )

    def _observe_form_refresh_latency(self, elapsed_s: float) -> None:
        sample = max(0.001, float(elapsed_s))
        alpha = FORM_REFRESH_LATENCY_EWMA_ALPHA
        self._form_refresh_latency_ewma_s = (
            alpha * sample + (1.0 - alpha) * self._form_refresh_latency_ewma_s
        )

    def _payment_form_refresh_policy(self) -> tuple[int, int | None]:
        """Return (refresh_age_seconds, distance_to_next_target).

        Farther than 50 numbers we keep the low-noise five-minute cadence. Once
        the observed frontier is within 50 numbers of the next target, every
        prepared form is maintained on a two-minute cadence. Because each form's
        created_at is updated after its own refresh, a large volley naturally
        stays staggered instead of creating one synchronized burst.
        """
        distances: list[int] = []
        for slug, meta in self._counter_meta.items():
            current = int(runtime.current_by_slug.get(slug, meta.current))
            target = next_target(store.settings.target_numbers, current)
            if target is None:
                continue
            distance = int(target) - current
            if distance > 0:
                distances.append(distance)

        nearest = min(distances) if distances else None
        if nearest is not None and nearest <= PAYMENT_FORM_NEAR_TARGET_DISTANCE:
            return PAYMENT_FORM_NEAR_REFRESH_SECONDS, nearest
        return PAYMENT_FORM_FAR_REFRESH_SECONDS, nearest

    def _adaptive_form_refresh_batch_size(
        self,
        due: list[tuple[float, SavedGiftInfo, PreparedUpgrade | None]],
    ) -> tuple[int, float | None]:
        """Choose the smallest batch that can plausibly clear the oldest backlog.

        The base batch remains conservative on a healthy connection.  As measured
        getPaymentForm latency grows or the oldest form approaches the local safe
        age, the worker removes idle tick gaps by refreshing more oldest-first
        forms in the same pass.  Requests are still sequential and capped at 50.
        """
        if not due:
            return FORM_REFRESH_BATCH_SIZE, None

        due_count = min(len(due), FORM_REFRESH_MAX_BATCH_SIZE)
        oldest_age = due[0][0]
        if oldest_age == float("inf"):
            return max(FORM_REFRESH_BATCH_SIZE, due_count), 0.0

        reserve_s = max(0.0, PAYMENT_FORM_MAX_AGE_SECONDS - max(0.0, oldest_age))
        request_time_s = due_count * max(0.001, self._form_refresh_latency_ewma_s)

        if reserve_s <= request_time_s:
            required = due_count
        else:
            idle_budget_s = reserve_s - request_time_s
            max_batches = max(1, int(idle_budget_s / FORM_REFRESH_TICK_SECONDS))
            required = max(1, (due_count + max_batches - 1) // max_batches)

        return (
            min(
                due_count,
                FORM_REFRESH_MAX_BATCH_SIZE,
                max(FORM_REFRESH_BATCH_SIZE, required),
            ),
            reserve_s,
        )

    async def _refresh_due_payment_forms(
        self,
        peer: Any,
        *,
        force: bool = False,
        max_items: int | None = None,
        candidates: list[SavedGiftInfo] | None = None,
        refreshed_ids: set[int] | None = None,
    ) -> int:
        """Refresh a small batch of paid forms so a month-long run stays armed.

        Telegram forms are valid for 10 minutes. The worker refreshes them well
        before that limit and staggers refreshes to avoid a 50-form burst in the
        scanner hot path. No payment is submitted here.
        """
        if (
            not store.settings.live_upgrades
            or self._fast_fired
            or self.stop_event.is_set()
        ):
            self._update_payment_form_age_metric()
            return 0

        now = time.monotonic()
        refresh_after_s, target_distance = self._payment_form_refresh_policy()
        refresh_candidates = list(candidates) if candidates is not None else self._fast_candidates_from_ram()
        due: list[tuple[float, SavedGiftInfo, PreparedUpgrade | None]] = []
        for candidate in refresh_candidates:
            plan = self.prepared.get(candidate.saved_id)
            if plan is not None and plan.prepaid:
                continue
            retry_after = self._form_refresh_retry_after.get(candidate.saved_id, 0.0)
            if not force and retry_after > now:
                continue
            age = float("inf") if plan is None else max(0.0, now - plan.created_at)
            if force or plan is None or age >= refresh_after_s:
                due.append((age, candidate, plan))

        # Oldest first: the form with the least remaining safe lifetime always
        # gets the next network slot.
        due.sort(key=lambda item: item[0], reverse=True)
        if max_items is not None:
            limit = max(1, int(max_items))
            reserve_s: float | None = None
        else:
            limit, reserve_s = self._adaptive_form_refresh_batch_size(due)
            if limit > FORM_REFRESH_BATCH_SIZE:
                logger.info(
                    "payment_form_refresh_batch_scaled due=%s batch=%s base_batch=%s "
                    "oldest_reserve_s=%s latency_ewma_s=%.3f",
                    len(due),
                    limit,
                    FORM_REFRESH_BATCH_SIZE,
                    None if reserve_s is None else round(reserve_s, 3),
                    self._form_refresh_latency_ewma_s,
                )
                record_payment_event(
                    "payment_form_refresh_batch_scaled",
                    due_count=len(due),
                    batch_size=limit,
                    base_batch_size=FORM_REFRESH_BATCH_SIZE,
                    max_batch_size=FORM_REFRESH_MAX_BATCH_SIZE,
                    oldest_reserve_s=None if reserve_s is None else round(reserve_s, 3),
                    latency_ewma_s=round(self._form_refresh_latency_ewma_s, 3),
                )
        refreshed = 0
        for old_age, candidate, old_plan in due[:limit]:
            if self._fast_fired or self.stop_event.is_set():
                break
            old_form_id = old_plan.form_id if old_plan is not None else None
            try:
                # Serialize form maintenance itself, one candidate at a time.
                # The worker stays enabled even while the target is inside the
                # hot zone, so a long wait cannot age the forms past the limit.
                async with self._form_refresh_lock:
                    request_started = time.perf_counter()
                    try:
                        new_plan = await self.service.prepare_upgrade(peer, candidate)
                    finally:
                        self._observe_form_refresh_latency(
                            time.perf_counter() - request_started
                        )
                self._validate_prepared_binding(candidate, new_plan)
                if not new_plan.prepaid and new_plan.form_id is not None:
                    duplicate_saved_ids = [
                        saved_id
                        for saved_id, existing in self.prepared.items()
                        if saved_id != candidate.saved_id
                        and not existing.prepaid
                        and existing.form_id == new_plan.form_id
                    ]
                    if duplicate_saved_ids:
                        raise RuntimeError(
                            f"duplicate refreshed form_id={new_plan.form_id} for saved_id={candidate.saved_id}; "
                            f"already_used_by={duplicate_saved_ids}"
                        )
                if self._fast_fired or self.stop_event.is_set():
                    break
                self.prepared[candidate.saved_id] = new_plan
                self._form_refresh_retry_after.pop(candidate.saved_id, None)
                refreshed += 1
                if refreshed_ids is not None:
                    refreshed_ids.add(candidate.saved_id)
                runtime.payment_form_refresh_count += 1
                runtime.payment_form_last_refresh_at = datetime.now(timezone.utc).isoformat()
                runtime.payment_form_last_refresh_error = None
                logger.info(
                    "payment_form_refreshed saved_id=%s old_form_id=%s new_form_id=%s old_age_s=%.3f "
                    "refresh_after_s=%s target_distance=%s max_age_s=%s",
                    candidate.saved_id,
                    old_form_id,
                    new_plan.form_id,
                    0.0 if old_age == float("inf") else old_age,
                    refresh_after_s,
                    target_distance,
                    PAYMENT_FORM_MAX_AGE_SECONDS,
                )
                record_payment_event(
                    "payment_form_refreshed",
                    saved_id=candidate.saved_id,
                    old_form_id=old_form_id,
                    new_form_id=new_plan.form_id,
                    old_age_s=None if old_age == float("inf") else round(old_age, 3),
                    invoice_saved_id=invoice_saved_id(new_plan.invoice),
                    request_form_id=_int_or_none(getattr(new_plan.request, "form_id", None)),
                    request_invoice_saved_id=invoice_saved_id(getattr(new_plan.request, "invoice", None)),
                    refresh_after_s=refresh_after_s,
                    target_distance=target_distance,
                    max_age_s=PAYMENT_FORM_MAX_AGE_SECONDS,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                runtime.payment_form_refresh_failures += 1
                runtime.payment_form_last_refresh_error = f"{type(exc).__name__}: {exc}"[:500]
                # A failed getPaymentForm is non-financial; retry later without
                # replacing the last known plan. This also keeps duplicate forms
                # visible to the audit instead of silently arming them.
                retry_delay = 15.0
                if isinstance(exc, RateLimitActiveError):
                    retry_delay = max(retry_delay, exc.remaining_seconds)
                elif isinstance(exc, errors.FloodWaitError):
                    retry_delay = max(retry_delay, float(exc.seconds) + FLOOD_WAIT_EXTRA_MS / 1000.0)
                self._form_refresh_retry_after[candidate.saved_id] = time.monotonic() + retry_delay
                logger.warning(
                    "payment_form_refresh_failed saved_id=%s old_form_id=%s old_age_s=%s "
                    "retry_in_s=%.3f error_type=%s error=%s",
                    candidate.saved_id,
                    old_form_id,
                    None if old_age == float("inf") else round(old_age, 3),
                    retry_delay,
                    type(exc).__name__,
                    exc,
                )
                record_payment_event(
                    "payment_form_refresh_failed",
                    saved_id=candidate.saved_id,
                    old_form_id=old_form_id,
                    old_age_s=None if old_age == float("inf") else round(old_age, 3),
                    retry_in_s=round(retry_delay, 3),
                    error_type=type(exc).__name__,
                    error=str(exc)[:500],
                )

        self._update_payment_form_age_metric()
        return refreshed

    async def _payment_form_refresh_loop(self, peer: Any) -> None:
        logger.info(
            "payment_form_refresh_worker_started far_refresh_s=%s near_refresh_s=%s near_distance=%s "
            "max_age_s=%s tick_s=%s batch=%s",
            PAYMENT_FORM_FAR_REFRESH_SECONDS,
            PAYMENT_FORM_NEAR_REFRESH_SECONDS,
            PAYMENT_FORM_NEAR_TARGET_DISTANCE,
            PAYMENT_FORM_MAX_AGE_SECONDS,
            FORM_REFRESH_TICK_SECONDS,
            FORM_REFRESH_BATCH_SIZE,
        )
        record_payment_event(
            "payment_form_refresh_worker_started",
            far_refresh_s=PAYMENT_FORM_FAR_REFRESH_SECONDS,
            near_refresh_s=PAYMENT_FORM_NEAR_REFRESH_SECONDS,
            near_distance=PAYMENT_FORM_NEAR_TARGET_DISTANCE,
            max_age_s=PAYMENT_FORM_MAX_AGE_SECONDS,
            telegram_ttl_s=TELEGRAM_PAYMENT_FORM_TTL_SECONDS,
            tick_s=FORM_REFRESH_TICK_SECONDS,
            batch_size=FORM_REFRESH_BATCH_SIZE,
        )
        try:
            while (
                runtime.active
                and store.settings.live_upgrades
                and not self.stop_event.is_set()
                and not self._fast_fired
            ):
                try:
                    await self._refresh_due_payment_forms(peer)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # An unexpected bookkeeping/worker error must not kill form
                    # maintenance for the rest of a long-running scan. Record it,
                    # wait one normal tick, and try again. Candidate-level network
                    # failures are already handled inside _refresh_due_payment_forms.
                    runtime.payment_form_last_refresh_error = (
                        f"worker: {type(exc).__name__}: {exc}"[:500]
                    )
                    logger.exception("payment_form_refresh_worker_iteration_failed")
                    record_payment_event(
                        "payment_form_refresh_worker_iteration_failed",
                        error_type=type(exc).__name__,
                        error=str(exc)[:500],
                    )
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(), timeout=FORM_REFRESH_TICK_SECONDS
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        finally:
            self._update_payment_form_age_metric()
            logger.info("payment_form_refresh_worker_stopped")
            record_payment_event(
                "payment_form_refresh_worker_stopped",
                refresh_count=runtime.payment_form_refresh_count,
                failures=runtime.payment_form_refresh_failures,
                oldest_age_s=runtime.payment_form_oldest_age_s,
            )

    async def _stop_payment_form_refresh_worker(self) -> None:
        task = self.form_refresh_task
        if task and task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(task, timeout=TASK_STOP_TIMEOUT_SECONDS)
        self.form_refresh_task = None

    async def start(self) -> None:
        if self.task and not self.task.done():
            return
        await self._stop_payment_form_refresh_worker()
        if not store.settings.selected_saved_ids:
            raise RuntimeError("Подарки не выбраны")
        if not store.settings.target_numbers:
            raise RuntimeError("Номера выстрела не заданы")
        rate_limit.clear_if_expired()
        rate_limit.assert_available()
        if store.settings.live_upgrades:
            try:
                _load_payment_submission_guard()
            except PaymentGuardStateError as exc:
                raise RuntimeError(
                    "Повреждён persistent payment guard; LIVE-запуск заблокирован до "
                    "исправления payment-submit-guard.json"
                ) from exc
        if not await self.service.is_authorized():
            raise RuntimeError("Telegram-аккаунт не авторизован")
        if active_shooter_count() > 1:
            await cluster_runtime.verify_active_peers()

        # Reset the visible state first, but do not mark the scanner active until
        # channel, gifts, slug/counter and (for LIVE) payment forms have all been
        # validated synchronously.  This prevents a failed start from briefly
        # showing a green "active" status.
        self.stop_event = asyncio.Event()
        self.triggered.clear()
        self.notified_missed.clear()
        self._form_refresh_retry_after.clear()
        self._form_refresh_latency_ewma_s = FORM_REFRESH_LATENCY_INITIAL_SECONDS
        await self._clear_prearmed_payment_guards()
        self._reset_critical_form_state()
        self._plan_dirty = True
        self._groups.clear()
        self._counter_meta.clear()
        self._campaign_ids_by_slug.clear()
        self._slug_by_campaign_id.clear()
        self._exact_probe_cursor.clear()
        self._last_exact_regular_refresh.clear()
        self._last_poll_started = None
        self._fast_fired = False
        self._fast_trigger_detected_at = None
        self._fast_client = None
        self._fast_peer = None
        self._leave_fast_quiet()
        self.rate.reset()
        runtime.active = False
        runtime.started_at = None
        runtime.checks = 0
        runtime.last_cycle_ms = None
        runtime.last_error = None
        runtime.last_success = None
        runtime.current_by_slug.clear()
        runtime.title_by_slug.clear()
        runtime.pending_verification_url = (
            store.settings.payment_verification_url if store.settings.payment_hold_saved_ids else None
        )
        runtime.adaptive_interval_ms = self.rate.current_interval_ms
        runtime.sleep_ms = None
        runtime.poll_gap_ms = None
        runtime.flood_count = 0
        runtime.last_flood_wait_s = None
        runtime.rate_cooldown_cycles = 0
        runtime.fast_quiet = False
        runtime.fast_fired = False
        runtime.fast_volley_size = effective_volley_size()
        runtime.fast_trigger_to_submit_ms = None
        runtime.fast_task_launch_ms = None
        runtime.fast_first_send_start_ms = None
        runtime.fast_send_start_offsets_ms.clear()
        runtime.fast_fire_source = None
        runtime.fast_udp_peers_sent = 0
        runtime.fast_campaign_id = None
        runtime.payment_form_refresh_count = 0
        runtime.payment_form_refresh_failures = 0
        runtime.payment_form_last_refresh_at = None
        runtime.payment_form_last_refresh_error = None
        runtime.payment_form_oldest_age_s = None

        try:
            peer = await self.service.resolve_channel()
            await self._load_plan(peer)
            self._fast_client = await self.service.require_authorized()
            self._fast_peer = peer
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            runtime.active = False
            runtime.started_at = None
            runtime.current_by_slug.clear()
            runtime.title_by_slug.clear()
            runtime.last_error = f"{type(exc).__name__}: {exc}"
            await self._maybe_write_diagnostics(force=True)
            raise

        try:
            await asyncio.to_thread(_write_scanner_resume_marker)
        except OSError as exc:
            runtime.last_error = f"resume marker: {type(exc).__name__}: {exc}"
            raise RuntimeError(
                f"Не удалось записать marker восстановления сканера: {exc}"
            ) from exc

        runtime.active = True
        runtime.started_at = time.monotonic()
        runtime.last_error = None
        self._plan_dirty = False
        self.task = asyncio.create_task(self._run(peer), name="gift-scanner")
        self.monitor_task = None
        self.form_refresh_task = (
            asyncio.create_task(
                self._payment_form_refresh_loop(peer),
                name="gift-payment-form-refresh",
            )
            if store.settings.live_upgrades
            else None
        )
        self._update_payment_form_age_metric()
        first_target = min(store.settings.target_numbers)
        cluster_runtime.disarm()
        for campaign_id in self._campaign_ids_by_slug.values():
            cluster_runtime.arm(campaign_id, first_target)
        cluster_runtime.notify_state_changed()
        logger.info(
            "scanner_started version=%s shooter_id=%s mode=%s active_shooters=%s saved_ids=%s targets=%s live=%s volley=%s volley_limit=%s stagger_ms=%s adaptive=%s start_ms=%s min_ms=%s",
            APP_VERSION,
            SHOOTER_ID,
            "sniper" if effective_volley_size() == 1 else "volley",
            active_shooter_count(),
            store.settings.selected_saved_ids,
            store.settings.target_numbers,
            store.settings.live_upgrades,
            effective_volley_size(),
            effective_max_volley_size(),
            effective_fast_volley_stagger_ms(),
            ADAPTIVE_SCAN,
            SCAN_START_INTERVAL_MS,
            SCAN_MIN_INTERVAL_MS,
        )
        record_cluster_event(
            "scanner_started",
            mode="sniper" if effective_volley_size() == 1 else "volley",
            active_shooters=active_shooter_count(),
            saved_ids=list(store.settings.selected_saved_ids),
            targets=list(store.settings.target_numbers),
            live=bool(store.settings.live_upgrades),
            volley=effective_volley_size(),
            volley_limit=effective_max_volley_size(),
            stagger_ms=effective_fast_volley_stagger_ms(),
        )

    async def stop(self, reason: str = "manual") -> None:
        was_active = bool(runtime.active)
        preserve_resume = (
            reason == "shutdown"
            and was_active
            and bool(store.settings.selected_saved_ids)
            and bool(store.settings.target_numbers)
        )
        self.stop_event.set()
        task = self.task
        monitor = self.monitor_task
        refresh_task = self.form_refresh_task
        if task and task is not asyncio.current_task():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=TASK_STOP_TIMEOUT_SECONDS)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        if monitor and monitor is not asyncio.current_task():
            monitor.cancel()
            try:
                await asyncio.wait_for(monitor, timeout=TASK_STOP_TIMEOUT_SECONDS)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        if refresh_task and refresh_task is not asyncio.current_task():
            refresh_task.cancel()
            try:
                await asyncio.wait_for(refresh_task, timeout=TASK_STOP_TIMEOUT_SECONDS)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        self.task = None
        self.monitor_task = None
        self.form_refresh_task = None
        if not self._fast_fired:
            await self._clear_prearmed_payment_guards()
        runtime.active = False
        runtime.started_at = None
        if not preserve_resume:
            await asyncio.to_thread(_clear_scanner_resume_marker)
        else:
            logger.warning("scanner_resume_marker_preserved reason=shutdown")
        self._leave_fast_quiet()
        cluster_runtime.disarm()
        cluster_runtime.notify_state_changed()
        await self._maybe_write_diagnostics(force=True)
        await self.refresh_status_message(force=True)
        logger.info("scanner_stopped reason=%s", reason)

    @staticmethod
    def _counter_with_current(counter: GiftCounter, current: int) -> GiftCounter:
        return GiftCounter(
            slug=counter.slug,
            title=counter.title,
            current=int(current),
            total=counter.total,
            base_gift_id=counter.base_gift_id,
        )

    async def _poll_counter_for_target(
        self,
        slug: str,
        meta: GiftCounter,
    ) -> tuple[GiftCounter, bool, int | None]:
        """Poll one type and return ``(counter, predecessor_exact, existing_target)``.

        Far from the goal the inexpensive aggregate counter is used. Inside the
        exact window the scanner follows concrete slugs one-by-one (for example
        ``DurovsGlasses-3332``), so a stale ``availability_issued`` value cannot
        delay the trigger. The displayed/current value is monotonic.
        """
        previous = int(runtime.current_by_slug.get(slug, meta.current))
        target = next_target(store.settings.target_numbers, previous)
        if target is None:
            return self._counter_with_current(meta, previous), False, None

        distance = target - previous
        if distance > EXACT_PROBE_DISTANCE:
            raw = await self.service.fetch_counter_fast(
                slug, expected_gift_id=meta.base_gift_id
            )
            current = max(previous, raw.current)
            if raw.current < previous:
                logger.info(
                    "counter_regression_ignored slug=%s previous=%s response=%s target=%s",
                    slug, previous, raw.current, target,
                )
            return self._counter_with_current(raw, current), False, None

        if store.settings.live_upgrades and distance <= FAST_QUIET_DISTANCE:
            self._enter_fast_quiet()

        now = time.monotonic()
        cursor = self._exact_probe_cursor.get(slug)
        if cursor is None:
            # Confirm the aggregate value once before trusting it as an exact
            # predecessor. This costs one cycle and prevents a stale aggregate
            # response from triggering LIVE by itself.
            cursor = max(0, previous - 1)
            self._exact_probe_cursor[slug] = cursor
            logger.info(
                "exact_probe_mode_entered slug=%s current=%s target=%s distance=%s",
                slug, previous, target, distance,
            )

        last_refresh = self._last_exact_regular_refresh.get(slug, 0.0)
        display_counter = meta
        if not self._quiet_mode and now - last_refresh >= EXACT_COUNTER_REFRESH_SECONDS:
            raw = await self.service.fetch_counter_fast(
                slug, expected_gift_id=meta.base_gift_id
            )
            self._last_exact_regular_refresh[slug] = now
            display_counter = raw
            if raw.current < previous:
                logger.info(
                    "counter_regression_ignored slug=%s previous=%s response=%s target=%s",
                    slug, previous, raw.current, target,
                )
            previous = max(previous, raw.current)
            # Use aggregate data only to jump close to the frontier. Never skip
            # confirmation of target-1 itself.
            jump_to = min(max(0, raw.current - 1), max(0, target - 2))
            if jump_to > cursor:
                cursor = jump_to
                self._exact_probe_cursor[slug] = cursor
                logger.info(
                    "exact_probe_cursor_advanced slug=%s cursor=%s aggregate=%s target=%s",
                    slug, cursor, raw.current, target,
                )

        probe_number = cursor + 1
        exact = await self.service.fetch_exact_unique(
            slug, probe_number, expected_gift_id=meta.base_gift_id
        )
        predecessor_exact = False
        existing_target: int | None = None
        if exact is not None:
            self._exact_probe_cursor[slug] = exact.num
            previous = max(previous, exact.num)
            display_counter = GiftCounter(
                slug=slug,
                title=exact.title,
                current=previous,
                total=exact.total if exact.total is not None else display_counter.total,
                base_gift_id=exact.base_gift_id or display_counter.base_gift_id,
            )
            if exact.num == target:
                existing_target = target
                logger.info(
                    "exact_number_confirmed slug=%s number=%s target=%s issued_hint=%s",
                    slug, exact.num, target, exact.issued,
                )
            elif exact.num == target - 1:
                # FAST deliberately does not spend another network round-trip on
                # checking target itself. The financial request is launched from
                # the caller immediately after this exact predecessor response.
                predecessor_exact = True
                self._fast_trigger_detected_at = time.perf_counter()
            else:
                logger.info(
                    "exact_number_confirmed slug=%s number=%s target=%s issued_hint=%s",
                    slug, exact.num, target, exact.issued,
                )

        return self._counter_with_current(display_counter, previous), predecessor_exact, existing_target

    async def _run(self, peer: Any) -> None:
        try:
            while not self.stop_event.is_set():
                poll_started = time.monotonic()
                if self._last_poll_started is not None:
                    runtime.poll_gap_ms = (poll_started - self._last_poll_started) * 1000.0
                self._last_poll_started = poll_started
                started_perf = time.perf_counter()
                critical = False

                try:
                    if self._plan_dirty:
                        await self._load_plan(peer)

                    counters: dict[str, GiftCounter] = {}
                    exact_predecessors: dict[str, bool] = {}
                    exact_existing_targets: dict[str, int | None] = {}
                    # Far from the goal one aggregate request is used. Near the
                    # goal the hot path follows exact numbered slugs, with only a
                    # periodic aggregate refresh for display/catch-up.
                    for slug, meta in self._counter_meta.items():
                        previous = int(runtime.current_by_slug.get(slug, meta.current))
                        upcoming_target = next_target(store.settings.target_numbers, previous)
                        if (
                            store.settings.live_upgrades
                            and upcoming_target is not None
                            and 0 < upcoming_target - previous <= FAST_FORM_ARM_DISTANCE
                        ):
                            await self._prepare_fast_forms(
                                peer,
                                slug,
                                upcoming_target,
                                self._groups.get(slug, []),
                            )

                        counter, predecessor_exact, existing_target = await self._poll_counter_for_target(
                            slug, meta
                        )
                        if (
                            predecessor_exact
                            and store.settings.live_upgrades
                            and not self._fast_fired
                        ):
                            hot_target = next_target(store.settings.target_numbers, counter.current)
                            if hot_target is not None and counter.current == hot_target - 1:
                                critical_key = (slug, hot_target)
                                if critical_key in self._critical_form_ready:
                                    await self._fast_volley(
                                        peer,
                                        slug,
                                        counter,
                                        hot_target,
                                        self._groups.get(slug, []),
                                    )
                                else:
                                    reason = self._critical_form_unarmed.get(
                                        critical_key,
                                        "формы не были полностью обновлены до критической зоны",
                                    )
                                    runtime.last_error = f"FAST-залп не вооружён: {reason}"
                                    logger.error(
                                        "fast_trigger_blocked_unarmed slug=%s target=%s reason=%s",
                                        slug,
                                        hot_target,
                                        reason,
                                    )
                                    # Keep scanning without doing any payment
                                    # preparation in the hot loop.  The exact
                                    # predecessor must not fall through to the
                                    # generic trigger block below.
                                    predecessor_exact = False
                                if self._fast_fired:
                                    break
                        counters[slug] = counter
                        exact_predecessors[slug] = predecessor_exact
                        exact_existing_targets[slug] = existing_target
                        previous_current = runtime.current_by_slug.get(slug)
                        runtime.current_by_slug[slug] = max(
                            int(previous_current or 0), counter.current
                        )
                        counter = self._counter_with_current(
                            counter, runtime.current_by_slug[slug]
                        )
                        counters[slug] = counter
                        runtime.title_by_slug[slug] = counter.title
                        if previous_current is not None and previous_current != counter.current:
                            target = next_target(store.settings.target_numbers, counter.current)
                            logger.info(
                                "gift_counter_changed slug=%s from=%s to=%s target=%s",
                                slug, previous_current, counter.current, target,
                            )
                            self._status_wakeup.set()

                    if self.stop_event.is_set():
                        break

                    exact_missed_changed = False
                    for slug, existing_target in exact_existing_targets.items():
                        if existing_target is None:
                            continue
                        key = (slug, existing_target)
                        if key not in self.notified_missed:
                            self.notified_missed.add(key)
                            await self.notify(
                                f"⚠️ Выстрел <b>{existing_target}</b> для "
                                f"<b>{html.escape(counters[slug].title)}</b> уже существует. "
                                "Оплата не отправлена."
                            )
                        remaining = [
                            value for value in store.settings.target_numbers
                            if value > existing_target
                        ]
                        if remaining != store.settings.target_numbers:
                            store.settings.target_numbers = remaining
                            exact_missed_changed = True
                    if exact_missed_changed:
                        await store.save()
                        if not store.settings.target_numbers:
                            runtime.last_error = "Выстрел уже существует; оплата не отправлена"
                            self.stop_event.set()
                            break
                        self._reset_critical_form_state()

                    current_max = max(counter.current for counter in counters.values())
                    future_targets = [value for value in store.settings.target_numbers if value > current_max]
                    if not future_targets:
                        store.settings.target_numbers = []
                        await store.save()
                        runtime.last_error = f"Все номера выстрела уже прошли. Текущий номер: {current_max}"
                        await self.notify(
                            f"⚠️ Все номера выстрела уже прошли. Текущий номер: <b>{current_max}</b>. Сканер остановлен."
                        )
                        self.stop_event.set()
                        break

                    runtime.checks += 1

                    for slug, group in list(self._groups.items()):
                        counter = counters[slug]
                        target = next_target(store.settings.target_numbers, counter.current)
                        if target is None:
                            continue
                        state = evaluate_target(counter.current, target)
                        critical = critical or bool(
                            state.distance is not None and state.distance <= NEAR_TARGET_DISTANCE
                        )

                        # LIVE may trigger only after the concrete target-1 slug
                        # was resolved. FAST intentionally skips a second target
                        # probe, so aggregate availability alone is still never
                        # sufficient for payment.
                        if state.should_trigger and exact_predecessors.get(slug, False):
                            key = (slug, target)
                            if key in self.triggered:
                                continue
                            if not store.settings.live_upgrades:
                                self.triggered.add(key)
                                await self.notify(
                                    f"🧪 DRY-RUN: <b>{html.escape(counter.title)}</b> сейчас #{counter.current}, "
                                    f"следующий номер — выстрел <b>#{target}</b>. Оплата выключена, улучшение не отправлено."
                                )
                            else:
                                await self._fast_volley(peer, slug, counter, target, group)
                                self.triggered.add(key)
                            continue

                        for missed_target in sorted(set(store.settings.target_numbers)):
                            if missed_target > counter.current:
                                break
                            key = (slug, missed_target)
                            if key not in self.notified_missed:
                                self.notified_missed.add(key)
                                await self.notify(
                                    f"⚠️ Выстрел <b>{missed_target}</b> для <b>{html.escape(counter.title)}</b> уже прошёл. "
                                    f"Текущий номер: <b>{counter.current}</b>."
                                )

                    if self.stop_event.is_set():
                        break

                    if not self._critical_form_unarmed:
                        runtime.last_error = None
                    elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                    runtime.last_cycle_ms = elapsed_ms
                    if ADAPTIVE_SCAN:
                        self.rate.on_success(critical=critical)
                    runtime.adaptive_interval_ms = self.rate.current_interval_ms
                    runtime.rate_cooldown_cycles = self.rate.cooldown_remaining
                    sleep_ms = self.rate.sleep_after_cycle_ms(elapsed_ms)
                    runtime.sleep_ms = sleep_ms

                    if self.stop_event.is_set():
                        break
                    await self._wait(sleep_ms / 1000.0)

                except errors.FloodWaitError as exc:
                    rate_limit.register(float(exc.seconds), "scanner")
                    elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                    runtime.last_cycle_ms = elapsed_ms
                    runtime.last_error = f"FloodWait {exc.seconds}s"
                    runtime.last_flood_wait_s = int(exc.seconds)
                    self.rate.on_flood(float(exc.seconds))
                    runtime.flood_count = self.rate.flood_count
                    runtime.adaptive_interval_ms = self.rate.current_interval_ms
                    runtime.rate_cooldown_cycles = self.rate.cooldown_remaining
                    runtime.sleep_ms = float(exc.seconds) * 1000.0 + FLOOD_WAIT_EXTRA_MS
                    logger.warning(
                        "scanner_flood_wait seconds=%s new_interval_ms=%.1f count=%s",
                        exc.seconds,
                        self.rate.current_interval_ms,
                        self.rate.flood_count,
                    )
                    await self._maybe_write_diagnostics(force=True)
                    if not self._quiet_mode:
                        await self.refresh_status_message(force=True)
                    await self._wait(max(0.0, float(exc.seconds)) + FLOOD_WAIT_EXTRA_MS / 1000.0)

                except RateLimitActiveError as exc:
                    runtime.last_error = str(exc)
                    runtime.sleep_ms = exc.remaining_seconds * 1000.0
                    await self._wait(exc.remaining_seconds)

                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                    runtime.last_cycle_ms = elapsed_ms
                    runtime.last_error = f"{type(exc).__name__}: {exc}"
                    if ADAPTIVE_SCAN:
                        self.rate.on_transient_error()
                    runtime.adaptive_interval_ms = self.rate.current_interval_ms
                    runtime.rate_cooldown_cycles = self.rate.cooldown_remaining
                    runtime.sleep_ms = max(250.0, self.rate.current_interval_ms)
                    logger.exception("scanner_cycle_failed")
                    await self._maybe_write_diagnostics(force=True)
                    if not self._quiet_mode:
                        await self.refresh_status_message(force=True)
                    await self._wait(runtime.sleep_ms / 1000.0)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            runtime.last_error = f"{type(exc).__name__}: {exc}"
            logger.exception("scanner_failed")
            await self.notify(f"❌ Сканер остановлен: {html.escape(str(exc))}")
        finally:
            runtime.active = False
            runtime.started_at = None
            await self._stop_payment_form_refresh_worker()
            if not self._fast_fired:
                await self._clear_prearmed_payment_guards()
            self._leave_fast_quiet()
            cluster_runtime.disarm()
            cluster_runtime.notify_state_changed()
            self.task = None
            await self._maybe_write_diagnostics(force=True)
            await self.refresh_status_message(force=True)
            monitor = self.monitor_task
            if monitor and monitor is not asyncio.current_task():
                monitor.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await monitor
            self.monitor_task = None

    def launch_external_fire(self, *, campaign_id: str, shot: int, trigger: int) -> None:
        """Schedule one follower shot from a validated collection-bound UDP packet.

        This method runs inside the UDP callback and intentionally performs only
        RAM checks plus task scheduling.  Logging and disk work happen in the
        scheduled coroutine after the payment launch decision.
        """
        if self._fast_fired or not runtime.active or not store.settings.live_upgrades:
            return
        if int(shot) not in store.settings.target_numbers or int(trigger) != int(shot) - 1:
            return
        campaign_id = str(campaign_id).strip().lower()
        slug = self._slug_by_campaign_id.get(campaign_id)
        if slug is None:
            return
        peer = self._fast_peer
        group = self._groups.get(slug)
        meta = self._counter_meta.get(slug)
        if peer is None or not group or meta is None:
            return
        if self._campaign_ids_by_slug.get(slug) != campaign_id:
            return
        counter = GiftCounter(
            slug=slug,
            title=meta.title,
            current=int(trigger),
            total=meta.total,
            base_gift_id=meta.base_gift_id,
        )
        if self._fast_trigger_detected_at is None:
            self._fast_trigger_detected_at = time.perf_counter()
        asyncio.get_running_loop().create_task(
            self._run_external_fire(peer, campaign_id, slug, counter, int(shot), group),
            name=f"udp-fire-{SHOOTER_ID}-{campaign_id[:8]}-{shot}",
        )

    async def _run_external_fire(
        self,
        peer: Any,
        campaign_id: str,
        slug: str,
        counter: GiftCounter,
        target: int,
        group: list[SavedGiftInfo],
    ) -> None:
        try:
            await self._fast_volley(
                peer,
                slug,
                counter,
                target,
                group,
                campaign_id=campaign_id,
                broadcast=False,
                source="udp",
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime.last_error = f"UDP fire: {type(exc).__name__}: {exc}"
            logger.exception(
                "udp_fire_launch_failed shooter_id=%s campaign_id=%s slug=%s trigger=%s target=%s",
                SHOOTER_ID,
                campaign_id,
                slug,
                counter.current,
                target,
            )
            record_cluster_event(
                "udp_fire_launch_failed",
                campaign_id=campaign_id,
                slug=slug,
                trigger=counter.current,
                target=target,
                error=f"{type(exc).__name__}: {exc}"[:300],
            )
            await self._maybe_write_diagnostics(force=True)

    async def _fast_volley(
        self,
        peer: Any,
        slug: str,
        counter: GiftCounter,
        target: int,
        group: list[SavedGiftInfo],
        *,
        campaign_id: str | None = None,
        broadcast: bool = True,
        source: str = "local",
    ) -> None:
        """Launch prebuilt requests and relay FIRE without control-plane work.

        A confirmed exact predecessor claims the one-shot latch immediately.  A
        healthy detector creates its own payment tasks first and then broadcasts
        the prebuilt HMAC packet with no ``await`` between those operations.  If
        local RAM preflight fails, the detector still broadcasts so healthy peers
        are not forced to wait for their own slower Telegram observation.
        """
        if self._fast_fired:
            return

        campaign_id = (campaign_id or self._campaign_ids_by_slug.get(slug) or "").strip().lower()
        if not campaign_id or self._slug_by_campaign_id.get(campaign_id) != slug:
            raise RuntimeError("FAST campaign_id не подготовлен для выбранной коллекции")

        # Claim before any branch so duplicate local/UDP detections cannot create
        # a second volley.  Everything below up to FIRE broadcast is RAM-only.
        self._fast_fired = True
        runtime.fast_fired = True
        runtime.fast_fire_source = source
        runtime.fast_campaign_id = campaign_id
        self.triggered.add((slug, target))
        self.stop_event.set()

        volley_size = effective_volley_size()
        runtime.fast_volley_size = volley_size
        candidates: list[SavedGiftInfo] = []
        plans: list[PreparedUpgrade] = []
        batch_task: asyncio.Task[list[UpgradeOutcome]] | None = None
        local_error: BaseException | None = None
        client = self._fast_client

        try:
            candidates = [
                item
                for item in group
                if item.saved_id in store.settings.selected_saved_ids and item.can_upgrade
            ][:volley_size]
            if len(candidates) != volley_size:
                raise RuntimeError(
                    f"FAST-залп {volley_size} невозможен: доступно {len(candidates)} подарков"
                )

            for candidate in candidates:
                plan = self.prepared.get(candidate.saved_id)
                if (
                    plan is None
                    or plan.request is None
                    or plan.saved_id != candidate.saved_id
                    or time.monotonic() - plan.created_at > PAYMENT_FORM_MAX_AGE_SECONDS
                ):
                    raise RuntimeError(
                        f"FAST-форма для saved_id={candidate.saved_id} не готова или устарела; "
                        "оплата не отправлена"
                    )

                if not plan.prepaid:
                    plan_invoice_saved_id = invoice_saved_id(plan.invoice)
                    request_form_id = _int_or_none(getattr(plan.request, "form_id", None))
                    request_invoice_saved_id = invoice_saved_id(getattr(plan.request, "invoice", None))
                    if plan.form_id is None or request_form_id is None:
                        raise RuntimeError(
                            f"FAST form_id missing: candidate={candidate.saved_id}, "
                            f"plan_form_id={plan.form_id}, request_form_id={request_form_id}"
                        )
                    if plan_invoice_saved_id != candidate.saved_id:
                        raise RuntimeError(
                            f"FAST invoice mismatch: candidate={candidate.saved_id}, "
                            f"invoice_saved_id={plan_invoice_saved_id}, form_id={plan.form_id}"
                        )
                    if request_invoice_saved_id != candidate.saved_id:
                        raise RuntimeError(
                            f"FAST request invoice mismatch: candidate={candidate.saved_id}, "
                            f"request_invoice_saved_id={request_invoice_saved_id}, form_id={plan.form_id}"
                        )
                    if request_form_id != plan.form_id:
                        raise RuntimeError(
                            f"FAST request form mismatch: candidate={candidate.saved_id}, "
                            f"plan_form_id={plan.form_id}, request_form_id={request_form_id}"
                        )
                plan.fast_send_started_ns = None
                plans.append(plan)

            paid_form_ids = [int(plan.form_id) for plan in plans if not plan.prepaid and plan.form_id is not None]
            if len(paid_form_ids) != len(set(paid_form_ids)):
                duplicates = sorted({value for value in paid_form_ids if paid_form_ids.count(value) > 1})
                raise RuntimeError(
                    "FAST duplicate payment form_id detected before submit: "
                    + ",".join(str(value) for value in duplicates)
                    + "; оплата не отправлена"
                )

            guard_payload = _load_payment_submission_guard()
            guard_matches = False
            if guard_payload is not None:
                wanted = {item.saved_id for item in candidates}
                for entry in guard_payload.get("entries", []):
                    if not isinstance(entry, dict):
                        continue
                    if (
                        str(entry.get("slug", "")).strip() == slug
                        and _int_or_none(entry.get("target")) == int(target)
                        and set(_unique_ints(entry.get("saved_ids", []))) == wanted
                    ):
                        guard_matches = True
                        self._payment_guard_id = _str_or_none(guard_payload.get("guard_id"))
                        self._payment_guard_keys.add((slug, int(target)))
                        break
            if not guard_matches:
                # Normally armed 25 numbers earlier. This synchronous fallback is
                # intentionally fail-closed for a lagging UDP follower: no Stars
                # request may leave the process without a durable restart guard.
                self._payment_guard_id = add_payment_submission_guard_entry(
                    slug=slug,
                    target=target,
                    saved_ids=[item.saved_id for item in candidates],
                    campaign_id=campaign_id,
                )
                self._payment_guard_keys.add((slug, int(target)))

            if client is None or not client.is_connected():
                raise RuntimeError("FAST MTProto-соединение не готово; оплата не отправлена")
            rate_limit.assert_available()
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            local_error = exc

        # ARMED guards are safe while waiting. Flip to SUBMITTED only at the
        # last possible pre-send point, after every other preflight check passed.
        # This fixes the v0038 false payment-hold after watchdog restarts near a
        # target. The tiny durable write happens once per volley, not per shot.
        if local_error is None and client is not None:
            guard_mark_started = time.perf_counter()
            try:
                self._payment_guard_id = mark_payment_submission_guard_submitted(
                    slug=slug,
                    target=target,
                    saved_ids=[item.saved_id for item in candidates],
                )
                record_payment_event(
                    "fast_payment_guard_submitted",
                    slug=slug,
                    target=target,
                    saved_ids=[item.saved_id for item in candidates],
                    durable_mark_ms=round((time.perf_counter() - guard_mark_started) * 1000.0, 3),
                )
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                local_error = exc

        launch_started = time.perf_counter()
        if local_error is None and client is not None:
            # Fallback timestamp for adapters/tests. The production
            # MTProtoService overwrites it at each actual staggered sender queue call.
            scheduled_ns = time.perf_counter_ns()
            for plan in plans:
                plan.fast_send_started_ns = scheduled_ns
            batch_task = asyncio.create_task(
                self.service.execute_upgrade_fast_batch(
                    peer,
                    list(zip(candidates, plans)),
                    client=client,
                ),
                name=f"fast-volley-batch-{SHOOTER_ID}-{campaign_id[:8]}-{target}",
            )
        launch_finished = time.perf_counter()
        peers_sent = 0
        if broadcast:
            peers_sent = cluster_runtime.broadcast_fire_nowait(
                campaign_id=campaign_id, shot=target, trigger=counter.current
            )
        runtime.fast_udp_peers_sent = peers_sent
        runtime.fast_task_launch_ms = (launch_finished - launch_started) * 1000.0

        if local_error is not None:
            runtime.fast_trigger_to_submit_ms = None
            runtime.fast_first_send_start_ms = None
            runtime.fast_send_start_offsets_ms.clear()
            runtime.last_error = f"{type(local_error).__name__}: {local_error}"
            logger.error(
                "fast_local_preflight_failed_relayed shooter_id=%s source=%s campaign_id=%s "
                "slug=%s predecessor=%s target=%s volley=%s udp_peers_sent=%s error=%s",
                SHOOTER_ID,
                source,
                campaign_id,
                slug,
                counter.current,
                target,
                volley_size,
                peers_sent,
                runtime.last_error,
            )
            record_payment_event(
                "fast_local_preflight_failed",
                source=source,
                campaign_id=campaign_id,
                slug=slug,
                predecessor=counter.current,
                target=target,
                volley=volley_size,
                error=runtime.last_error[:500],
                plans=[prepared_payment_debug(plan) for plan in plans],
            )
            record_cluster_event(
                "fast_local_preflight_failed_relayed",
                source=source,
                campaign_id=campaign_id,
                slug=slug,
                predecessor=counter.current,
                target=target,
                volley=volley_size,
                udp_peers_sent=peers_sent,
                error=runtime.last_error[:300],
            )
            store.settings.live_upgrades = False
            preflight_saved = False
            if self._payment_guard_id:
                store.settings.payment_guard_token = self._payment_guard_id
            try:
                await store.save()
                preflight_saved = True
            except Exception:
                logger.exception("fast_preflight_state_save_failed")
            if preflight_saved and self._payment_guard_keys:
                with contextlib.suppress(OSError):
                    await asyncio.to_thread(clear_payment_submission_guard)
                    self._payment_guard_keys.clear()
                    self._payment_guard_id = None
            with contextlib.suppress(Exception):
                await self.notify(
                    "⚠️ Локальный FAST-залп не отправлен, но сигнал другим стрелкам передан. "
                    + html.escape(runtime.last_error[:300])
                )
            return

        # Yield directly into the staggered FAST task: no logging, disk write or
        # UI work occurs before the payment pipeline gets its first event-loop
        # turn. Once a financial request is launched, a manual Stop must not
        # cancel it midway.
        if batch_task is None:
            raw_results: list[Any] = [
                UpgradeOutcome("unknown", detail="FAST batch task отсутствует")
                for _candidate in candidates
            ]
        else:
            try:
                raw_results = await asyncio.shield(batch_task)
            except asyncio.CancelledError:
                raw_results = await batch_task
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                raw_results = [
                    UpgradeOutcome(
                        "unknown",
                        detail=f"{type(exc).__name__}: {exc}; FAST batch завершился аварийно",
                    )
                    for _candidate in candidates
                ]

        trigger_at = self._fast_trigger_detected_at
        offsets: list[float | None] = []
        for plan in plans:
            if plan.fast_send_started_ns is None or trigger_at is None:
                offsets.append(None)
            else:
                offsets.append(max(0.0, plan.fast_send_started_ns / 1_000_000.0 - trigger_at * 1000.0))
        runtime.fast_send_start_offsets_ms = offsets
        valid_offsets = [value for value in offsets if value is not None]
        runtime.fast_first_send_start_ms = min(valid_offsets) if valid_offsets else None
        runtime.fast_trigger_to_submit_ms = runtime.fast_first_send_start_ms

        outcomes: list[UpgradeOutcome] = []
        for result in raw_results:
            if isinstance(result, UpgradeOutcome):
                outcomes.append(result)
            elif isinstance(result, BaseException):
                outcomes.append(
                    UpgradeOutcome(
                        "unknown",
                        detail=f"{type(result).__name__}: {result}; FAST-повтор не отправлялся",
                    )
                )
            else:
                outcomes.append(
                    UpgradeOutcome("unknown", detail="Неожиданный результат FAST-залпа")
                )
        outcome_log = [
            {
                "saved_id": candidate.saved_id,
                "form_id": plan.form_id,
                "invoice_saved_id": invoice_saved_id(plan.invoice),
                "request_form_id": _int_or_none(getattr(plan.request, "form_id", None)),
                "request_invoice_saved_id": invoice_saved_id(getattr(plan.request, "invoice", None)),
                "form_age_ms": round(max(0.0, time.monotonic() - plan.created_at) * 1000.0, 3),
                "status": outcome.status,
                "actual_num": outcome.actual_num,
                "send_start_ms": offsets[index],
                "detail": (outcome.detail or "")[:180],
            }
            for index, (candidate, plan, outcome) in enumerate(zip(candidates, plans, outcomes))
        ]
        logger.warning(
            "fast_volley_completed shooter_id=%s source=%s campaign_id=%s slug=%s predecessor=%s target=%s "
            "volley=%s volley_limit=%s stagger_ms=%s udp_peers_sent=%s first_send_start_ms=%.3f task_launch_ms=%.3f outcomes=%s",
            SHOOTER_ID,
            source,
            campaign_id,
            slug,
            counter.current,
            target,
            volley_size,
            effective_max_volley_size(),
            effective_fast_volley_stagger_ms(),
            peers_sent,
            runtime.fast_first_send_start_ms or 0.0,
            runtime.fast_task_launch_ms or 0.0,
            json.dumps(outcome_log, ensure_ascii=False, separators=(",", ":")),
        )
        record_payment_event(
            "fast_volley_completed",
            source=source,
            campaign_id=campaign_id,
            slug=slug,
            predecessor=counter.current,
            target=target,
            volley=volley_size,
            volley_limit=effective_max_volley_size(),
            stagger_ms=effective_fast_volley_stagger_ms(),
            first_send_start_ms=runtime.fast_first_send_start_ms,
            task_launch_ms=runtime.fast_task_launch_ms,
            send_start_offsets_ms=offsets,
            outcomes=outcome_log,
        )
        record_cluster_event(
            "fast_volley_completed",
            source=source,
            campaign_id=campaign_id,
            slug=slug,
            predecessor=counter.current,
            target=target,
            volley=volley_size,
            volley_limit=effective_max_volley_size(),
            stagger_ms=effective_fast_volley_stagger_ms(),
            udp_peers_sent=peers_sent,
            first_send_start_ms=runtime.fast_first_send_start_ms,
            task_launch_ms=runtime.fast_task_launch_ms,
            send_start_offsets_ms=offsets,
            outcomes=outcome_log,
        )
        await self._finish_fast_volley(candidates, counter, target, outcomes)

    async def _finish_fast_volley(
        self,
        candidates: list[SavedGiftInfo],
        counter: GiftCounter,
        target: int,
        outcomes: list[UpgradeOutcome],
    ) -> None:
        """Persist and report the volley only after every first submission ends."""
        confirmed: list[tuple[SavedGiftInfo, UpgradeOutcome]] = []
        pending: list[tuple[SavedGiftInfo, UpgradeOutcome]] = []
        failed: list[tuple[SavedGiftInfo, UpgradeOutcome]] = []

        for candidate, outcome in zip(candidates, outcomes):
            if outcome.status == "confirmed" and outcome.actual_num is not None:
                confirmed.append((candidate, outcome))
                with contextlib.suppress(ValueError):
                    store.settings.selected_saved_ids.remove(candidate.saved_id)
                self.prepared.pop(candidate.saved_id, None)
            elif outcome.status in {"verification", "unknown"}:
                pending.append((candidate, outcome))
            else:
                failed.append((candidate, outcome))

        if confirmed:
            highest = max(int(outcome.actual_num or 0) for _candidate, outcome in confirmed)
            store.settings.target_numbers = [
                value for value in store.settings.target_numbers if value > highest
            ]
            got = sorted(int(outcome.actual_num or 0) for _candidate, outcome in confirmed)
            runtime.last_success = (
                f"{counter.title}: выстрел {target}, FAST-залп получил "
                + ", ".join(f"#{number}" for number in got)
            )
            runtime.last_error = None

        holds = set(store.settings.payment_hold_saved_ids)
        verification_url: str | None = None
        for candidate, outcome in pending:
            holds.add(candidate.saved_id)
            store.settings.payment_hold_targets[str(candidate.saved_id)] = int(target)
            if outcome.verification_url and verification_url is None:
                verification_url = outcome.verification_url
        store.settings.payment_hold_saved_ids = sorted(holds)
        if pending:
            store.settings.payment_hold_reason = (
                f"FAST-залп отправлен, но результат {len(pending)} платежей не подтверждён; "
                "автоматического повтора не было"
            )
            store.settings.payment_verification_url = verification_url
            runtime.pending_verification_url = verification_url
        elif not store.settings.payment_hold_saved_ids:
            store.settings.payment_hold_reason = None
            store.settings.payment_verification_url = None
            runtime.pending_verification_url = None

        store.settings.live_upgrades = False
        if not confirmed:
            details = [outcome.detail or outcome.status for _candidate, outcome in pending + failed]
            runtime.last_error = "; ".join(details)[:500] or "FAST-залп не подтверждён"

        save_error: Exception | None = None
        if self._payment_guard_id:
            store.settings.payment_guard_token = self._payment_guard_id
        try:
            await store.save()
        except Exception as exc:
            save_error = exc
            logger.exception("fast_volley_state_save_failed")
        else:
            if self._payment_guard_keys:
                try:
                    await asyncio.to_thread(clear_payment_submission_guard)
                except OSError:
                    logger.exception("payment_submission_guard_clear_failed")
                else:
                    self._payment_guard_keys.clear()
                    self._payment_guard_id = None

        await asyncio.to_thread(_clear_scanner_resume_marker)

        lines = [
            f"⚡ <b>{APP_NAME} {APP_VERSION}: залп завершён</b>",
            f"Подарок: <b>{html.escape(counter.title)}</b>",
            f"Выстрел: <b>#{target}</b>",
            f"Выстрелов: <b>{len(candidates)}</b>",
            (
                f"Реакция после точного #{target - 1}: "
                f"<b>{runtime.fast_trigger_to_submit_ms:.3f} мс</b>"
                if runtime.fast_trigger_to_submit_ms is not None
                else "Реакция: —"
            ),
        ]
        for candidate, outcome in zip(candidates, outcomes):
            if outcome.status == "confirmed" and outcome.actual_num is not None:
                mark = "✅" if outcome.actual_num == target else "⚠️"
                lines.append(
                    f"{mark} <code>{candidate.saved_id}</code> → <b>#{outcome.actual_num}</b>"
                )
            elif outcome.status == "verification":
                lines.append(f"🔐 <code>{candidate.saved_id}</code> → требуется подтверждение")
            elif outcome.status == "unknown":
                lines.append(f"❔ <code>{candidate.saved_id}</code> → результат не подтверждён")
            else:
                lines.append(
                    f"❌ <code>{candidate.saved_id}</code> → "
                    f"{html.escape((outcome.detail or 'ошибка')[:180])}"
                )
        if pending:
            lines.append(
                "⚠️ Неподтверждённые экземпляры поставлены на сверку после залпа; "
                "повторная оплата не отправляется."
            )
        if save_error is not None:
            lines.append("⚠️ Не удалось сохранить итог на диск; проверь подарки вручную перед перезапуском.")
        await self.notify("\n".join(lines))



    async def notify(self, text: str) -> None:
        owner = store.settings.owner_user_id
        bot = self.bot_getter()
        if not owner or bot is None:
            return
        with contextlib.suppress(Exception):
            await bot.send_message(owner, text)



class StressTester:
    """Five-minute read-only load test using the scanner's real hot-path call."""

    def __init__(self, service: MTProtoService, bot_getter: Any):
        self.service = service
        self.bot_getter = bot_getter
        self.task: asyncio.Task[None] | None = None
        self.monitor_task: asyncio.Task[None] | None = None
        self.stop_event = asyncio.Event()
        self.status_chat_id: int | None = None
        self.status_message_id: int | None = None
        self.status_updater = StatusMessageUpdater(
            bot_getter,
            name="stress",
            min_interval_seconds=STRESS_STATUS_INTERVAL_SECONDS,
        )

    async def start(self, chat_id: int) -> None:
        if runtime.active:
            raise RuntimeError("Сначала останови основной сканер")
        if self.task and not self.task.done():
            raise RuntimeError("Стресс-тест уже запущен")
        rate_limit.clear_if_expired()
        rate_limit.assert_available()
        if not await self.service.is_authorized():
            raise RuntimeError("Telegram-аккаунт не авторизован")

        peer = await self.service.resolve_channel()
        infos = await self.service.get_selected_infos(peer)
        if not infos:
            raise RuntimeError("Сначала выбери подарок")

        counters: dict[int, GiftCounter] = {}
        for info in infos:
            if info.base_gift_id not in counters:
                counters[info.base_gift_id] = await self.service.counter_for_info(info, peer=peer, cache_seconds=0)
        if len(counters) != 1:
            raise RuntimeError("Для теста выбери подарки только одного типа")

        counter = next(iter(counters.values()))
        self.stop_event = asyncio.Event()
        self.status_chat_id = int(chat_id)
        self.status_message_id = None
        self.status_updater.clear()
        self._reset_runtime(counter)
        self.task = asyncio.create_task(
            self._run(counter.slug, counter.base_gift_id, counter.title),
            name="gift-stress-test",
        )
        logger.info(
            "stress_test_started version=%s slug=%s duration_s=300 phase1_ms=300 phase2_ms=120 max_ms=0",
            APP_VERSION,
            counter.slug,
        )

    def attach_status_message(self, chat_id: int, message_id: int) -> None:
        self.status_chat_id = int(chat_id)
        self.status_message_id = int(message_id)
        self.status_updater.attach(chat_id, message_id)
        if runtime.stress_active and (self.monitor_task is None or self.monitor_task.done()):
            self.monitor_task = asyncio.create_task(self._monitor_loop(), name="gift-stress-status-monitor")

    def _reset_runtime(self, counter: GiftCounter) -> None:
        runtime.stress_active = True
        runtime.stress_started_at = time.monotonic()
        runtime.stress_phase = "1/3 · 300 мс"
        runtime.stress_elapsed_s = 0.0
        runtime.stress_interval_ms = STRESS_FIRST_INTERVAL_MS
        runtime.stress_checks = 0
        runtime.stress_successes = 0
        runtime.stress_errors = 0
        runtime.stress_flood_count = 0
        runtime.stress_flood_seconds = 0.0
        runtime.stress_last_error = None
        runtime.stress_avg_latency_ms = None
        runtime.stress_p95_latency_ms = None
        runtime.stress_current_rate_per_s = 0.0
        runtime.stress_max_rate_per_s = 0.0
        runtime.stress_result = None
        runtime.current_by_slug = {counter.slug: counter.current}
        runtime.title_by_slug = {counter.slug: counter.title}

    async def stop(self, reason: str = "manual") -> None:
        task = self.task
        if not task or task.done():
            runtime.stress_active = False
            self.task = None
            monitor = self.monitor_task
            if monitor and monitor is not asyncio.current_task():
                monitor.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await monitor
            self.monitor_task = None
            return
        self.stop_event.set()
        if task is not asyncio.current_task():
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=TASK_STOP_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                logger.warning("stress_test_stop_timeout reason=%s", reason)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        monitor = self.monitor_task
        if monitor and monitor is not asyncio.current_task():
            monitor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await monitor
        self.monitor_task = None
        logger.info("stress_test_stop_requested reason=%s", reason)

    @staticmethod
    def _phase(elapsed: float) -> tuple[str, float]:
        interval = stress_test_interval_ms(
            elapsed,
            first_phase_seconds=STRESS_FIRST_PHASE_SECONDS,
            max_phase_starts_seconds=STRESS_MAX_PHASE_START_SECONDS,
            first_interval_ms=STRESS_FIRST_INTERVAL_MS,
            second_interval_ms=STRESS_SECOND_INTERVAL_MS,
            maximum_interval_ms=STRESS_MAX_INTERVAL_MS,
        )
        if elapsed < STRESS_FIRST_PHASE_SECONDS:
            return "1/3 · 300 мс", interval
        if elapsed < STRESS_MAX_PHASE_START_SECONDS:
            return "2/3 · 120 мс", interval
        return "3/3 · максимум", interval

    @staticmethod
    def _percentile95(values: list[float]) -> float | None:
        return nearest_rank_percentile(values, 0.95)

    @staticmethod
    def _new_stats() -> dict[str, Any]:
        return {
            "checks": 0,
            "successes": 0,
            "errors": 0,
            "flood_count": 0,
            "flood_seconds": 0.0,
            "latencies_ms": [],
            "max_rate_per_second": 0.0,
        }

    @classmethod
    def _serialize_stats(cls, stats: dict[str, Any]) -> dict[str, Any]:
        values = list(stats.get("latencies_ms", []))
        return {
            "checks": int(stats.get("checks", 0)),
            "successes": int(stats.get("successes", 0)),
            "errors": int(stats.get("errors", 0)),
            "flood_count": int(stats.get("flood_count", 0)),
            "flood_seconds": float(stats.get("flood_seconds", 0.0)),
            "avg_latency_ms": statistics.fmean(values) if values else None,
            "p95_latency_ms": cls._percentile95(values),
            "max_rate_per_second": float(stats.get("max_rate_per_second", 0.0)),
        }

    @classmethod
    def _log_minute(cls, minute: int, stats: dict[str, Any]) -> None:
        data = cls._serialize_stats(stats)
        avg = data["avg_latency_ms"]
        logger.info(
            "stress_test_minute minute=%s checks=%s successes=%s errors=%s floods=%s flood_seconds=%.1f avg_latency_ms=%s p95_latency_ms=%s max_rate_s=%.2f",
            minute,
            data["checks"],
            data["successes"],
            data["errors"],
            data["flood_count"],
            data["flood_seconds"],
            f"{avg:.1f}" if avg is not None else "-",
            f"{data['p95_latency_ms']:.1f}" if data["p95_latency_ms"] is not None else "-",
            data["max_rate_per_second"],
        )

    @staticmethod
    def _log_telemetry_second(*, elapsed: float, phase: str, interval_ms: float, slug: str) -> None:
        """Write one compact telemetry line per second for the complete test trace."""
        current = runtime.current_by_slug.get(slug)
        logger.info(
            "stress_test_tick elapsed_s=%s phase=%s interval_ms=%.1f checks=%s successes=%s errors=%s floods=%s "
            "avg_latency_ms=%s p95_latency_ms=%s max_rate_s=%.2f current=%s rss_mb=%.1f",
            int(elapsed),
            phase,
            interval_ms,
            runtime.stress_checks,
            runtime.stress_successes,
            runtime.stress_errors,
            runtime.stress_flood_count,
            f"{runtime.stress_avg_latency_ms:.1f}" if runtime.stress_avg_latency_ms is not None else "-",
            f"{runtime.stress_p95_latency_ms:.1f}" if runtime.stress_p95_latency_ms is not None else "-",
            runtime.stress_max_rate_per_s,
            current if current is not None else "-",
            current_rss_mb(),
        )

    async def _refresh_status(self, *, force: bool = False) -> None:
        await self.status_updater.update(await stress_status_text(), force=force)
        self.status_chat_id = self.status_updater.chat_id
        self.status_message_id = self.status_updater.message_id

    async def _monitor_loop(self) -> None:
        try:
            while runtime.stress_active and not self.stop_event.is_set():
                await self._refresh_status()
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("stress_status_monitor_failed error=%s", exc)

    async def _interruptible_wait(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _run(self, slug: str, base_gift_id: int | None, title: str) -> None:
        started = time.monotonic()
        cpu_started = time.process_time()
        rss_start_mb = current_rss_mb()
        rss_peak_mb = rss_start_mb
        latencies: list[float] = []
        success_timestamps: list[float] = []
        phase_stats: dict[str, dict[str, Any]] = {}
        minute_stats = {minute: self._new_stats() for minute in range(1, 6)}
        logged_minutes: set[int] = set()
        last_phase: str | None = None
        last_telemetry_second = -1
        aborted = False

        try:
            while not self.stop_event.is_set():
                elapsed = time.monotonic() - started
                if elapsed >= STRESS_TEST_DURATION_SECONDS:
                    break

                phase, interval_ms = self._phase(elapsed)
                minute = min(5, int(elapsed // 60) + 1)
                runtime.stress_elapsed_s = elapsed
                runtime.stress_phase = phase
                runtime.stress_interval_ms = interval_ms
                stats = phase_stats.setdefault(phase, self._new_stats())
                minute_bucket = minute_stats[minute]

                if phase != last_phase:
                    logger.info("stress_test_phase phase=%s elapsed_s=%.1f interval_ms=%.1f", phase, elapsed, interval_ms)
                    last_phase = phase

                for completed_minute in range(1, minute):
                    if completed_minute not in logged_minutes:
                        self._log_minute(completed_minute, minute_stats[completed_minute])
                        logged_minutes.add(completed_minute)

                request_started = time.perf_counter()
                runtime.stress_checks += 1
                stats["checks"] += 1
                minute_bucket["checks"] += 1
                try:
                    counter = await self.service.fetch_counter_fast(slug, expected_gift_id=base_gift_id)
                    latency_ms = (time.perf_counter() - request_started) * 1000.0
                    latencies.append(latency_ms)
                    stats["latencies_ms"].append(latency_ms)
                    minute_bucket["latencies_ms"].append(latency_ms)
                    runtime.stress_successes += 1
                    stats["successes"] += 1
                    minute_bucket["successes"] += 1
                    runtime.stress_last_error = None
                    runtime.current_by_slug[slug] = counter.current
                    runtime.title_by_slug[slug] = counter.title
                    now = time.monotonic()
                    success_timestamps.append(now)
                    cutoff = now - 1.0
                    while success_timestamps and success_timestamps[0] < cutoff:
                        success_timestamps.pop(0)
                    current_rate = float(len(success_timestamps))
                    runtime.stress_current_rate_per_s = current_rate
                    runtime.stress_max_rate_per_s = max(runtime.stress_max_rate_per_s, current_rate)
                    stats["max_rate_per_second"] = max(stats["max_rate_per_second"], current_rate)
                    minute_bucket["max_rate_per_second"] = max(minute_bucket["max_rate_per_second"], current_rate)
                    runtime.stress_avg_latency_ms = statistics.fmean(latencies)
                    runtime.stress_p95_latency_ms = self._percentile95(latencies)
                    rss_peak_mb = max(rss_peak_mb, current_rss_mb())

                    sleep_ms = max(0.0, interval_ms - latency_ms)
                    await self._interruptible_wait(sleep_ms / 1000.0)

                except errors.FloodWaitError as exc:
                    rate_limit.register(float(exc.seconds), "stress_test")
                    wait_s = max(0.0, float(exc.seconds))
                    runtime.stress_flood_count += 1
                    runtime.stress_flood_seconds += wait_s
                    runtime.stress_last_error = f"FloodWait {int(wait_s)}с"
                    stats["flood_count"] += 1
                    stats["flood_seconds"] += wait_s
                    minute_bucket["flood_count"] += 1
                    minute_bucket["flood_seconds"] += wait_s
                    logger.warning(
                        "stress_test_flood_wait phase=%s minute=%s elapsed_s=%.1f seconds=%s checks=%s blocked_until=%.3f",
                        phase,
                        minute,
                        elapsed,
                        exc.seconds,
                        runtime.stress_checks,
                        rate_limit.blocked_until,
                    )
                    remaining_test = max(0.0, STRESS_TEST_DURATION_SECONDS - (time.monotonic() - started))
                    await self._interruptible_wait(min(remaining_test, rate_limit.remaining_seconds()))

                except RateLimitActiveError as exc:
                    runtime.stress_last_error = str(exc)
                    remaining_test = max(0.0, STRESS_TEST_DURATION_SECONDS - (time.monotonic() - started))
                    await self._interruptible_wait(min(remaining_test, exc.remaining_seconds))

                except asyncio.CancelledError:
                    aborted = True
                    raise
                except Exception as exc:
                    runtime.stress_errors += 1
                    stats["errors"] += 1
                    minute_bucket["errors"] += 1
                    runtime.stress_last_error = f"{type(exc).__name__}: {exc}"
                    logger.exception("stress_test_request_failed phase=%s minute=%s elapsed_s=%.1f", phase, minute, elapsed)
                    await self._interruptible_wait(0.5)

                rss_peak_mb = max(rss_peak_mb, current_rss_mb())
                telemetry_second = int(min(time.monotonic() - started, STRESS_TEST_DURATION_SECONDS))
                if telemetry_second != last_telemetry_second:
                    self._log_telemetry_second(
                        elapsed=time.monotonic() - started,
                        phase=phase,
                        interval_ms=interval_ms,
                        slug=slug,
                    )
                    last_telemetry_second = telemetry_second
                # Live Telegram UI updates run in a separate monitor task so
                # Bot API latency is not counted as MTProto stress-test latency.

            aborted = self.stop_event.is_set() and (time.monotonic() - started) < STRESS_TEST_DURATION_SECONDS

        except asyncio.CancelledError:
            aborted = True
        finally:
            duration = min(time.monotonic() - started, STRESS_TEST_DURATION_SECONDS)
            cpu_seconds = max(0.0, time.process_time() - cpu_started)
            cpu_percent = (cpu_seconds / duration * 100.0) if duration > 0 else 0.0
            rss_peak_mb = max(rss_peak_mb, current_rss_mb())
            error_rate = runtime.stress_errors / max(1, runtime.stress_checks)
            cooldown_remaining = rate_limit.remaining_seconds()

            if aborted:
                result = "ОСТАНОВЛЕН"
            elif error_rate >= 0.05:
                result = "НЕСТАБИЛЬНО"
            elif runtime.stress_flood_count > 0 or cooldown_remaining > 0:
                result = "ЕСТЬ ОГРАНИЧЕНИЯ"
            elif runtime.stress_successes == 0:
                result = "НЕСТАБИЛЬНО"
            else:
                result = "СТАБИЛЬНО"

            runtime.stress_elapsed_s = duration
            runtime.stress_result = result
            runtime.stress_active = False
            runtime.stress_started_at = None
            runtime.stress_avg_latency_ms = statistics.fmean(latencies) if latencies else None
            runtime.stress_p95_latency_ms = self._percentile95(latencies)

            for minute in range(1, 6):
                if minute not in logged_minutes:
                    self._log_minute(minute, minute_stats[minute])
                    logged_minutes.add(minute)

            report = {
                "version": APP_VERSION,
                "timestamp": time.time(),
                "result": result,
                "duration_seconds": round(duration, 3),
                "profile": [
                    {"from_second": 0, "to_second": 60, "interval_ms": 300},
                    {"from_second": 60, "to_second": 120, "interval_ms": 120},
                    {"from_second": 120, "to_second": 300, "interval_ms": 0},
                ],
                "slug": slug,
                "title": title,
                "checks": runtime.stress_checks,
                "successes": runtime.stress_successes,
                "errors": runtime.stress_errors,
                "error_rate": error_rate,
                "flood_count": runtime.stress_flood_count,
                "flood_seconds": runtime.stress_flood_seconds,
                "cooldown_remaining_seconds": cooldown_remaining,
                "blocked_until": rate_limit.blocked_until if cooldown_remaining > 0 else None,
                "avg_latency_ms": runtime.stress_avg_latency_ms,
                "p95_latency_ms": runtime.stress_p95_latency_ms,
                "max_rate_per_second": runtime.stress_max_rate_per_s,
                "cpu_seconds_during_test": cpu_seconds,
                "average_cpu_percent_one_core_during_test": cpu_percent,
                "rss_start_mb": rss_start_mb,
                "rss_peak_observed_mb": rss_peak_mb,
                "rss_peak_delta_mb": max(0.0, rss_peak_mb - rss_start_mb),
                "last_error": runtime.stress_last_error,
                "phases": {phase: self._serialize_stats(stats) for phase, stats in phase_stats.items()},
                "minutes": [
                    {"minute": minute, **self._serialize_stats(minute_stats[minute])}
                    for minute in range(1, 6)
                ],
            }
            write_status = await self._write_report(report)
            logger.info("stress_test_finished %s", json.dumps(report, ensure_ascii=False, separators=(",", ":")))
            self.task = None
            await write_diagnostics()
            await self._refresh_status(force=True)
            monitor = self.monitor_task
            if monitor and monitor is not asyncio.current_task():
                monitor.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await monitor
            self.monitor_task = None
            await self._notify_report(report, write_status=write_status)

    async def _write_report(self, report: dict[str, Any]) -> dict[str, bool]:
        status = {"latest": False, "history": False}
        try:
            temp = STRESS_REPORT_PATH.with_suffix(".tmp")
            temp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            temp.replace(STRESS_REPORT_PATH)
            status["latest"] = True
        except OSError as exc:
            logger.warning("stress_report_latest_write_failed error=%s", exc)
        try:
            with STRESS_HISTORY_PATH.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(report, ensure_ascii=False, separators=(",", ":")) + "\n")
            status["history"] = True
        except OSError as exc:
            logger.warning("stress_report_history_write_failed error=%s", exc)
        return status

    async def _notify_report(self, report: dict[str, Any], *, write_status: dict[str, bool]) -> None:
        bot = self.bot_getter()
        owner = store.settings.owner_user_id
        if bot is None or owner is None:
            return
        avg = report.get("avg_latency_ms")
        p95 = report.get("p95_latency_ms")
        cooldown = float(report.get("cooldown_remaining_seconds") or 0.0)
        lines = [
            "🧪 <b>Стресс-тест завершён · тумблер ВЫКЛ</b>",
            f"Результат: <b>{html.escape(str(report['result']))}</b>",
            f"Длительность: <b>{report['duration_seconds']:.0f}с</b>",
            f"Проверок: <b>{report['checks']}</b> · успешных: <b>{report['successes']}</b>",
            f"Ошибок: <b>{report['errors']}</b> · FloodWait: <b>{report['flood_count']}</b>",
            f"Средний ответ: <b>{avg:.1f} мс</b>" if avg is not None else "Средний ответ: —",
            f"P95: <b>{p95:.1f} мс</b>" if p95 is not None else "P95: —",
            f"Максимум: <b>{report['max_rate_per_second']:.1f} проверок/с</b>",
            f"CPU процесса во время теста: <b>{report['average_cpu_percent_one_core_during_test']:.1f}% ядра</b>",
            f"RAM: старт <b>{report['rss_start_mb']:.1f} МБ</b> · пик теста <b>{report['rss_peak_observed_mb']:.1f} МБ</b>",
        ]
        if cooldown > 0:
            lines.append(f"⏳ До следующего MTProto-запроса: <b>{int(cooldown + 0.999)}с</b>")
        if write_status.get("latest") and write_status.get("history"):
            lines.append("Весь тест записан в Log; полный отчёт сохранён в stress-test-latest.json и истории.")
        elif write_status.get("latest"):
            lines.append("⚠️ Последний JSON-отчёт записан, но историю дополнить не удалось.")
        else:
            lines.append("⚠️ Не удалось записать JSON-отчёт на диск; подробности есть в системном логе.")
        with contextlib.suppress(Exception):
            await bot.send_message(owner, "\n".join(lines), reply_markup=main_keyboard())


_bot_instance: Bot | None = None
_dispatcher_instance: Dispatcher | None = None
scanner = Scanner(mtproto, lambda: _bot_instance)
stress_tester = StressTester(mtproto, lambda: _bot_instance)


class ClusterRuntime:
    """Control-plane status plus local UDP fire transport.

    Status work is deliberately outside the scanner hot path. The only hot-path
    call is ``broadcast_fire_nowait`` after local payment tasks are created.
    """

    def __init__(self) -> None:
        self.bus: ClusterBus | None = None
        self.task: asyncio.Task[None] | None = None
        self.stop_event = asyncio.Event()
        self.wakeup = asyncio.Event()
        self.aggregate_wakeup = asyncio.Event()
        self.remote_statuses: dict[int, ShooterStatus] = {}
        self.last_local_status: ShooterStatus | None = None
        self.last_ready = False
        initial_read = provision_store.read_config()
        self.applied_config: ClusterConfig | None = initial_read.config if initial_read.valid else None
        self.config_gate = StableConfigGate(
            current=self.applied_config,
            required_reads=CLUSTER_CONFIG_STABLE_READS,
            require_new_generation=True,
        )
        self.config_check_requested = False
        self.last_config_error = ""
        self.deactivation_in_progress = False
        self.deactivation_quiesce_task: asyncio.Task[None] | None = None
        self.last_quiesced_generation = 0
        self.aggregate_updater = StatusMessageUpdater(
            lambda: _bot_instance,
            name="cluster",
            min_interval_seconds=CLUSTER_STATUS_INTERVAL_SECONDS,
            urgent_min_interval_seconds=CLUSTER_STATUS_INTERVAL_SECONDS,
        )

    async def start(self) -> None:
        self.stop_event.clear()
        if FIRE_SECRET:
            self.bus = ClusterBus(
                shooter_id=SHOOTER_ID,
                secret=FIRE_SECRET,
                port=FIRE_PORT,
                provision=provision_store,
                on_fire=self._on_fire,
                on_status=self._on_status,
                on_config_notice=self._on_config_notice,
            )
            await self.bus.start()
            if self.applied_config is not None:
                await self.bus.refresh_config_and_peers(self.applied_config)
            logger.info(
                "cluster_udp_started shooter_id=%s port=%s secret_source=%s active_shooters=%s resolved_peers=%s expected_peers=%s fully_connected=%s",
                SHOOTER_ID,
                FIRE_PORT,
                FIRE_SECRET_SOURCE,
                self.bus.config.active_shooters,
                sorted(self.bus.resolved_peer_ids),
                sorted(self.bus.expected_peer_ids),
                self.bus.fully_connected,
            )
        else:
            logger.warning(
                "cluster_udp_disabled fire_secret_unavailable shooter_id=%s secret_source=%s",
                SHOOTER_ID,
                FIRE_SECRET_SOURCE,
            )
        self.task = asyncio.create_task(self._loop(), name=f"cluster-runtime-{SHOOTER_ID}")

    def _on_config_notice(self, generation: int, active_shooters: int) -> None:
        """RAM-only UDP callback; durable audit is deferred off the event loop."""
        asyncio.get_running_loop().create_task(
            asyncio.to_thread(
                self._log_config_notice_received, generation, active_shooters
            ),
            name=f"config-notice-log-{SHOOTER_ID}",
        )
        current_generation = self.applied_config.generation if self.applied_config else 0
        if (
            not PRIMARY_SHOOTER
            and generation > current_generation
            and generation > self.last_quiesced_generation
            and (self.deactivation_quiesce_task is None or self.deactivation_quiesce_task.done())
        ):
            self.deactivation_quiesce_task = asyncio.create_task(
                self._quiesce_for_config_notice(generation, active_shooters),
                name=f"cluster-quiesce-{SHOOTER_ID}",
            )
        self.config_check_requested = True
        self.wakeup.set()


    @staticmethod
    def _log_config_notice_received(generation: int, active_shooters: int) -> None:
        logger.info(
            "cluster_config_notice_received shooter_id=%s generation=%s active_shooters=%s",
            SHOOTER_ID,
            generation,
            active_shooters,
        )
        record_cluster_event(
            "cluster_config_notice_received",
            generation=generation,
            active_shooters=active_shooters,
        )

    async def _quiesce_for_config_notice(
        self,
        generation: int,
        active_shooters: int,
    ) -> None:
        """Disarm a participant immediately, but never sleep on UDP alone."""
        if generation <= self.last_quiesced_generation:
            return
        self.last_quiesced_generation = generation
        removed = active_shooters < SHOOTER_ID
        logger.warning(
            "cluster_config_notice_quiesce shooter_id=%s generation=%s active_shooters=%s removed=%s",
            SHOOTER_ID,
            generation,
            active_shooters,
            removed,
        )
        record_cluster_event(
            "cluster_config_notice_quiesce",
            generation=generation,
            active_shooters=active_shooters,
            removed=removed,
        )
        with contextlib.suppress(Exception):
            if runtime.active:
                await scanner.stop("cluster_config_notice")
        with contextlib.suppress(Exception):
            if runtime.stress_active:
                await stress_tester.stop("cluster_config_notice")
        store.settings.live_upgrades = False
        scanner.prepared.clear()
        self.disarm()
        with contextlib.suppress(Exception):
            await store.save()
        safe_save_lifecycle(
            SHOOTER_ID,
            state="deactivating" if removed else "reconfiguring",
            generation=generation,
            active_shooters=active_shooters,
            reason="awaiting_stable_config",
            version=APP_VERSION,
            pid=os.getpid(),
        )

    async def _apply_config(
        self,
        config: ClusterConfig,
        *,
        trusted: bool = False,
        broadcast_notice: bool = False,
    ) -> None:
        previous = self.applied_config or (self.bus.config if self.bus is not None else ClusterConfig())
        notice_sent = 0
        if broadcast_notice and self.bus is not None:
            notice_sent = self.bus.broadcast_config_notice_nowait(config)
            logger.info(
                "cluster_config_notice_sent generation=%s active_shooters=%s peers=%s",
                config.generation,
                config.active_shooters,
                notice_sent,
            )
            record_cluster_event(
                "cluster_config_notice_sent",
                generation=config.generation,
                active_shooters=config.active_shooters,
                peers=notice_sent,
            )
        self.applied_config = config
        self.config_gate.current = config
        if self.bus is not None:
            await self.bus.refresh_config_and_peers(config)
        if PRIMARY_SHOOTER:
            self.remote_statuses = {
                shooter_id: status
                for shooter_id, status in self.remote_statuses.items()
                if shooter_id <= config.active_shooters
            }
        if SHOOTER_ID <= config.active_shooters:
            safe_save_lifecycle(
                SHOOTER_ID,
                state="active",
                generation=config.generation,
                active_shooters=config.active_shooters,
                reason="config_applied",
                version=APP_VERSION,
                pid=os.getpid(),
            )
        logger.info(
            "cluster_config_applied shooter_id=%s previous_generation=%s generation=%s previous_active_shooters=%s active_shooters=%s trusted=%s",
            SHOOTER_ID,
            previous.generation,
            config.generation,
            previous.active_shooters,
            config.active_shooters,
            trusted,
        )
        record_cluster_event(
            "cluster_config_applied",
            previous_generation=previous.generation,
            generation=config.generation,
            previous_active_shooters=previous.active_shooters,
            active_shooters=config.active_shooters,
            trusted=trusted,
        )
        self.notify_state_changed()

    async def refresh_config(
        self,
        config: ClusterConfig | None = None,
        *,
        broadcast_notice: bool = False,
    ) -> None:
        if config is None:
            result = provision_store.read_config()
            if not result.valid:
                logger.warning(
                    "cluster_config_refresh_rejected shooter_id=%s error=%s",
                    SHOOTER_ID,
                    result.error,
                )
                return
            config = result.config
        await self._apply_config(
            config,
            trusted=True,
            broadcast_notice=broadcast_notice,
        )

    async def _graceful_deactivate(self, config: ClusterConfig) -> None:
        global _dispatcher_instance
        if self.deactivation_in_progress:
            return
        self.deactivation_in_progress = True
        logger.warning(
            "cluster_participant_deactivation_started shooter_id=%s active_shooters=%s generation=%s",
            SHOOTER_ID,
            config.active_shooters,
            config.generation,
        )
        record_cluster_event(
            "cluster_participant_deactivation_started",
            active_shooters=config.active_shooters,
            generation=config.generation,
        )
        safe_save_lifecycle(
            SHOOTER_ID,
            state="deactivating",
            generation=config.generation,
            active_shooters=config.active_shooters,
            reason="removed_from_active_set",
            version=APP_VERSION,
            pid=os.getpid(),
        )
        with contextlib.suppress(Exception):
            if runtime.active:
                await scanner.stop("cluster_deactivated")
        with contextlib.suppress(Exception):
            if runtime.stress_active:
                await stress_tester.stop("cluster_deactivated")
        store.settings.live_upgrades = False
        scanner.prepared.clear()
        self.disarm()
        with contextlib.suppress(Exception):
            await store.save()
        with contextlib.suppress(Exception):
            await self._publish_local_status()
        safe_save_lifecycle(
            SHOOTER_ID,
            state="sleeping",
            generation=config.generation,
            active_shooters=config.active_shooters,
            reason="deactivation_ack",
            version=APP_VERSION,
            pid=os.getpid(),
        )
        logger.warning(
            "cluster_participant_deactivated shooter_id=%s active_shooters=%s generation=%s",
            SHOOTER_ID,
            config.active_shooters,
            config.generation,
        )
        record_cluster_event(
            "cluster_participant_deactivated",
            active_shooters=config.active_shooters,
            generation=config.generation,
        )
        dispatcher = _dispatcher_instance
        if dispatcher is not None:
            try:
                await asyncio.wait_for(dispatcher.stop_polling(), timeout=3.0)
                return
            except Exception as exc:
                logger.error("cluster_deactivation_stop_polling_failed error=%s", exc)
        os._exit(75)

    async def stop(self) -> None:
        self.stop_event.set()
        self.wakeup.set()
        self.aggregate_wakeup.set()
        task = self.task
        if task and task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.task = None
        quiesce_task = self.deactivation_quiesce_task
        if quiesce_task and quiesce_task is not asyncio.current_task() and not quiesce_task.done():
            quiesce_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await quiesce_task
        self.deactivation_quiesce_task = None
        if self.bus is not None:
            self.bus.close()
        self.bus = None

    def notify_state_changed(self) -> None:
        self.wakeup.set()

    def arm(self, campaign_id: str, shot: int) -> None:
        if self.bus is not None:
            self.bus.arm(campaign_id, int(shot))

    def disarm(self) -> None:
        if self.bus is not None:
            self.bus.disarm()

    def broadcast_fire_nowait(
        self, *, campaign_id: str, shot: int, trigger: int
    ) -> int:
        if self.bus is None:
            return 0
        return self.bus.broadcast_fire_nowait(
            campaign_id=campaign_id, shot=shot, trigger=trigger
        )

    def _on_fire(self, campaign_id: str, shot: int, trigger: int) -> None:
        scanner.launch_external_fire(
            campaign_id=campaign_id, shot=shot, trigger=trigger
        )

    async def verify_active_peers(
        self, *, timeout_seconds: float | None = None
    ) -> dict[int, float | None]:
        """One-shot authenticated liveness check used before LIVE/Start.

        No background heartbeat is introduced.  Results stay in ClusterBus RAM
        and are reused by readiness cards until the cluster generation changes.
        """
        config = self.applied_config or (self.bus.config if self.bus is not None else ClusterConfig())
        if self.bus is None or not self.bus.ready:
            raise RuntimeError("UDP-сокет между стрелками не готов")
        if not self.bus.fully_connected:
            missing = sorted(self.bus.expected_peer_ids - self.bus.resolved_peer_ids)
            raise RuntimeError(
                "Не найдены UDP-адреса стрелков: " + ", ".join(f"Hunter {value}" for value in missing)
            )
        timeout = (
            max(0.05, CLUSTER_PING_TIMEOUT_MS / 1000.0)
            if timeout_seconds is None
            else max(0.05, float(timeout_seconds))
        )
        results = await self.bus.ping_active_shooters(timeout_seconds=timeout)
        missing = [
            shooter_id
            for shooter_id in range(1, config.active_shooters + 1)
            if results.get(shooter_id) is None
        ]
        logger.info(
            "cluster_preflight_ping generation=%s active_shooters=%s results=%s",
            config.generation,
            config.active_shooters,
            json.dumps(results, ensure_ascii=False, separators=(",", ":")),
        )
        record_cluster_event(
            "cluster_preflight_ping",
            generation=config.generation,
            active_shooters=config.active_shooters,
            results=results,
            missing=missing,
        )
        if missing:
            raise RuntimeError(
                "Не ответили по UDP: " + ", ".join(f"Hunter {value}" for value in missing)
            )
        return results

    def _on_status(self, status: ShooterStatus) -> None:
        """RAM-only UDP callback; file logging is deferred to a worker thread."""
        if not PRIMARY_SHOOTER:
            return
        if status.shooter_id == 1:
            # Local Hunter 1 state is tracked by ``last_local_status`` and is
            # never a remote participant. Ignore a looped-back status silently.
            return
        config = self.applied_config
        expected_campaign = next(iter(scanner._campaign_ids_by_slug.values()), None)
        reject_reason: str | None = None
        if config is None:
            reject_reason = "config_missing"
        elif status.shooter_id > config.active_shooters:
            reject_reason = "shooter_outside_active_set"
        elif status.generation != config.generation:
            reject_reason = "generation_mismatch"
        elif expected_campaign and status.campaign_id != expected_campaign:
            reject_reason = "campaign_mismatch"
        if reject_reason is not None:
            asyncio.get_running_loop().create_task(
                asyncio.to_thread(
                    self._log_status_rejected,
                    status,
                    reject_reason,
                    config,
                    expected_campaign,
                ),
                name=f"status-reject-log-{status.shooter_id}",
            )
            return
        previous = self.remote_statuses.get(status.shooter_id)
        changed = previous is None or previous.payload() != status.payload()
        was_ready = bool(previous and previous.ready)
        self.remote_statuses[status.shooter_id] = status
        if changed:
            asyncio.get_running_loop().create_task(
                asyncio.to_thread(
                    self._log_remote_status_change, status, was_ready
                ),
                name=f"status-change-log-{status.shooter_id}",
            )
        self.aggregate_wakeup.set()

    @staticmethod
    def _log_status_rejected(
        status: ShooterStatus,
        reason: str,
        config: ClusterConfig | None,
        expected_campaign: str | None,
    ) -> None:
        logger.warning(
            "cluster_status_rejected shooter_id=%s reason=%s status_generation=%s "
            "active_generation=%s status_campaign=%s expected_campaign=%s",
            status.shooter_id,
            reason,
            status.generation,
            config.generation if config else 0,
            status.campaign_id,
            expected_campaign,
        )
        record_cluster_event(
            "cluster_status_rejected",
            participant_id=status.shooter_id,
            reason=reason,
            status_generation=status.generation,
            active_generation=config.generation if config else 0,
            status_campaign=status.campaign_id,
            expected_campaign=expected_campaign,
        )

    @staticmethod
    def _log_remote_status_change(status: ShooterStatus, was_ready: bool) -> None:
        logger.info(
            "cluster_remote_status_changed shooter_id=%s generation=%s campaign_id=%s "
            "bot=%s mtproto=%s gift=%s shot=%s plan=%s signal=%s live=%s active=%s ready=%s",
            status.shooter_id,
            status.generation,
            status.campaign_id,
            status.bot,
            status.mtproto,
            status.gift,
            status.shot,
            status.plan,
            status.signal,
            status.live,
            status.active,
            status.ready,
        )
        record_cluster_event(
            "cluster_remote_status_changed",
            participant_id=status.shooter_id,
            generation=status.generation,
            campaign_id=status.campaign_id,
            status=status.payload(),
            ready=status.ready,
        )
        if status.ready and not was_ready:
            logger.info("cluster_remote_ready shooter_id=%s shot=%s", status.shooter_id, status.shot)
            record_cluster_event(
                "cluster_remote_ready",
                participant_id=status.shooter_id,
                shot=status.shot,
                generation=status.generation,
                campaign_id=status.campaign_id,
            )
        elif was_ready and not status.ready:
            logger.warning("cluster_remote_not_ready shooter_id=%s", status.shooter_id)
            record_cluster_event(
                "cluster_remote_not_ready", participant_id=status.shooter_id
            )

    async def local_status(self) -> ShooterStatus:
        if runtime.active:
            authorized = True
        elif mtproto._authorized is not None:
            authorized = bool(mtproto._authorized)
        else:
            authorized = await mtproto.is_authorized()
        shot = min(store.settings.target_numbers) if store.settings.target_numbers else None
        required = effective_volley_size()
        plan_ready = required > 0 and len(scanner.prepared) >= required
        config = self.applied_config or getattr(self.bus, "config", None) or ClusterConfig(
            active_shooters=active_shooter_count(), configured=True, generation=1
        )
        campaign_id = next(iter(scanner._campaign_ids_by_slug.values()), None)
        return ShooterStatus(
            shooter_id=SHOOTER_ID,
            bot=_bot_instance is not None,
            mtproto=authorized,
            gift=bool(store.settings.selected_saved_ids),
            shot=shot,
            plan=plan_ready,
            signal=(
                bool(getattr(self.bus, "peers_ready", getattr(self.bus, "fully_connected", False)))
                if self.bus is not None else config.active_shooters == 1
            ),
            live=bool(store.settings.live_upgrades),
            active=bool(runtime.active),
            generation=config.generation,
            campaign_id=campaign_id,
            updated_monotonic=time.monotonic(),
        )

    @staticmethod
    def _mark(value: bool) -> str:
        return "✅" if value else "❌"

    def shooter_card(self, status: ShooterStatus, *, title: str | None = None) -> str:
        shot = f"✅ #{status.shot}" if status.shot is not None else "❌"
        lines = [
            title or f"Hunter {status.shooter_id}",
            f"BOT:       {self._mark(status.bot)}",
            f"MTProto:   {self._mark(status.mtproto)}",
            f"Подарок:   {self._mark(status.gift)}",
            f"Выстрел:   {shot}",
            f"План:      {self._mark(status.plan)}",
            f"Сигнал:    {self._mark(status.signal)} UDP",
            f"LIVE:      {self._mark(status.live and status.active)}",
        ]
        return "\n".join(lines)

    async def aggregate_text(self) -> str:
        config = self.applied_config or ClusterConfig()
        local = await self.local_status()
        statuses: dict[int, ShooterStatus] = {1: local} if PRIMARY_SHOOTER else {}
        statuses.update(self.remote_statuses)
        now = time.monotonic()
        lines = [
            f"🎯 <b>Стрелки {APP_VERSION}</b>",
            f"Выбрано стрелков: <b>{config.active_shooters}</b>",
            "",
        ]
        ready_count = 0
        for shooter_id in range(1, config.active_shooters + 1):
            status = statuses.get(shooter_id)
            if status is None or (shooter_id != 1 and now - status.updated_monotonic > CLUSTER_STALE_SECONDS):
                lines.extend([
                    f"Hunter {shooter_id}",
                    "BOT:       ❌",
                    "MTProto:   ❌",
                    "Подарок:   ❌",
                    "Выстрел:   ❌",
                    "План:      ❌",
                    "Сигнал:    ❌ UDP",
                    "LIVE:      ❌",
                    "",
                ])
                continue
            if status.ready:
                ready_count += 1
            lines.append(self.shooter_card(status))
            lines.append("")
        if ready_count == config.active_shooters:
            lines.append(f"🟢 <b>ПОЛНОСТЬЮ ГОТОВО: {ready_count}/{config.active_shooters}</b>")
        else:
            lines.append(f"🟡 <b>ГОТОВО: {ready_count}/{config.active_shooters}</b>")
        return "\n".join(lines).rstrip()

    async def _send_ready_transition(self, status: ShooterStatus) -> None:
        if status.ready and not self.last_ready:
            logger.info("cluster_local_ready shooter_id=%s shot=%s", SHOOTER_ID, status.shot)
            record_cluster_event("cluster_local_ready", shot=status.shot)
            bot = _bot_instance
            owner = store.settings.owner_user_id
            if bot is not None and owner is not None:
                with contextlib.suppress(Exception):
                    await bot.send_message(
                        owner,
                        self.shooter_card(
                            status, title=f"🟢 HUNTER {SHOOTER_ID} ПОЛНОСТЬЮ ГОТОВ"
                        ),
                    )
        elif self.last_ready and not status.ready:
            logger.warning("cluster_local_not_ready shooter_id=%s", SHOOTER_ID)
            record_cluster_event("cluster_local_not_ready")
        self.last_ready = status.ready

    async def _publish_local_status(self) -> None:
        status = await self.local_status()
        changed = self.last_local_status is None or status.payload() != self.last_local_status.payload()
        self.last_local_status = status
        if self.bus is not None:
            self.bus.send_status_nowait(status)
        elif PRIMARY_SHOOTER:
            self._on_status(status)
        if changed:
            logger.info(
                "cluster_local_status_changed shooter_id=%s bot=%s mtproto=%s gift=%s shot=%s plan=%s signal=%s live=%s active=%s ready=%s",
                status.shooter_id,
                status.bot,
                status.mtproto,
                status.gift,
                status.shot,
                status.plan,
                status.signal,
                status.live,
                status.active,
                status.ready,
            )
            record_cluster_event(
                "cluster_local_status_changed",
                status=status.payload(),
                ready=status.ready,
            )
            await self._send_ready_transition(status)
            if PRIMARY_SHOOTER:
                self.aggregate_wakeup.set()

    async def _refresh_aggregate_card(self) -> None:
        if not PRIMARY_SHOOTER:
            return
        config = provision_store.load_config()
        if not config.configured:
            return
        owner = store.settings.owner_user_id
        bot = _bot_instance
        if owner is None or bot is None:
            return
        text = await self.aggregate_text()
        if self.aggregate_updater.message_id is None:
            replacement = await bot.send_message(owner, text)
            self.aggregate_updater.attach(owner, int(replacement.message_id))
            self.aggregate_updater._last_text = text
            self.aggregate_updater._last_edit_at = time.monotonic()
            return
        await self.aggregate_updater.update(text, force=True)

    async def _loop(self) -> None:
        next_status = 0.0
        next_peer_refresh = 0.0
        next_config_poll = 0.0
        next_aggregate = 0.0
        try:
            while not self.stop_event.is_set():
                now = time.monotonic()
                timeout = max(0.1, min(1.0, next_status - now if next_status > now else 0.0))
                try:
                    await asyncio.wait_for(self.wakeup.wait(), timeout=timeout)
                    self.wakeup.clear()
                    next_status = 0.0
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                if self.config_check_requested:
                    self.config_check_requested = False
                    next_config_poll = 0.0
                if now >= next_config_poll:
                    # Shared-volume reads are control-plane work. Keep them off the
                    # asyncio loop so a slow filesystem cannot add jitter to the
                    # scanner/payment path.
                    result = await asyncio.to_thread(provision_store.read_config)
                    if not result.valid:
                        error = result.error or "unknown"
                        if error != self.last_config_error:
                            self.last_config_error = error
                            logger.error(
                                "cluster_config_invalid_ignored shooter_id=%s error=%s",
                                SHOOTER_ID,
                                error,
                            )
                            record_cluster_event(
                                "cluster_config_invalid_ignored",
                                error=error,
                                retained_generation=(self.applied_config.generation if self.applied_config else 0),
                                retained_active_shooters=(self.applied_config.active_shooters if self.applied_config else 1),
                            )
                    else:
                        self.last_config_error = ""
                    accepted = self.config_gate.observe(result)
                    if accepted is not None:
                        if not PRIMARY_SHOOTER and accepted.generation > self.last_quiesced_generation:
                            await self._quiesce_for_config_notice(
                                accepted.generation,
                                accepted.active_shooters,
                            )
                        await self._apply_config(accepted)
                        if not PRIMARY_SHOOTER and accepted.active_shooters < SHOOTER_ID:
                            await self._graceful_deactivate(accepted)
                            return
                    next_config_poll = now + CLUSTER_CONFIG_POLL_SECONDS

                if self.bus is not None and now >= next_peer_refresh:
                    before = (
                        self.bus.config.active_shooters,
                        self.bus.config.generation,
                        tuple(sorted(self.bus.resolved_peer_ids)),
                    )
                    await self.bus.refresh_config_and_peers(self.applied_config or self.bus.config)
                    after = (
                        self.bus.config.active_shooters,
                        self.bus.config.generation,
                        tuple(sorted(self.bus.resolved_peer_ids)),
                    )
                    if after != before:
                        logger.info(
                            "cluster_peers_changed shooter_id=%s active_shooters=%s generation=%s resolved_peers=%s expected_peers=%s fully_connected=%s",
                            SHOOTER_ID,
                            self.bus.config.active_shooters,
                            self.bus.config.generation,
                            sorted(self.bus.resolved_peer_ids),
                            sorted(self.bus.expected_peer_ids),
                            self.bus.fully_connected,
                        )
                    next_peer_refresh = now + CLUSTER_STATUS_INTERVAL_SECONDS

                if now >= next_status:
                    await self._publish_local_status()
                    next_status = now + CLUSTER_STATUS_INTERVAL_SECONDS

                if PRIMARY_SHOOTER and self.aggregate_wakeup.is_set():
                    self.aggregate_wakeup.clear()
                    next_aggregate = min(next_aggregate or now, now)
                if PRIMARY_SHOOTER and now >= next_aggregate:
                    await self._refresh_aggregate_card()
                    next_aggregate = now + CLUSTER_STATUS_INTERVAL_SECONDS
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.exception("cluster_runtime_failed error=%s", exc)
            if not self.stop_event.is_set():
                config = self.applied_config or provision_store.load_config()
                safe_save_lifecycle(
                    SHOOTER_ID,
                    state="error",
                    generation=config.generation,
                    active_shooters=config.active_shooters,
                    reason="cluster_runtime_failed",
                    version=APP_VERSION,
                    pid=os.getpid(),
                    error_type=type(exc).__name__,
                )
                record_cluster_event(
                    "cluster_runtime_failed",
                    generation=config.generation,
                    active_shooters=config.active_shooters,
                    error_type=type(exc).__name__,
                )
                os._exit(71)


cluster_runtime = ClusterRuntime()


class SetupStates(StatesGroup):
    pin = State()
    shooter_count = State()
    shooter_token = State()
    api_id = State()
    api_hash = State()
    phone = State()
    targets = State()


router = Router()


BTN_CHANNEL = "📣 Канал"
BTN_GIFTS = "🎁 Подарки"
BTN_CHECK = "🔢 Проверить номера"
BTN_TARGETS = "🎯 Задать выстрел"
BTN_START = "▶️ Запустить"
BTN_STOP = "⛔ Остановить"
BTN_PAYMENT_OFF = "🛡 Оплата: ВЫКЛ"
BTN_PAYMENT_ON = "💳 Оплата: ВКЛ"
BTN_VOLLEY_PREFIX = "💥 Залп:"
BTN_PING = "📡 Ping"
BTN_SHOOTER_COUNT_PREFIX = "👥 Количество стрелков:"
BTN_REFRESH_CARD = "♻️ Обновить карточку"
BTN_STRESS_OFF = "🧪 Стресс-тест: ВЫКЛ"
BTN_STRESS_ON = "🧪 Стресс-тест: ВКЛ"
BTN_LOG = "📄 Log"
BTN_SETTINGS = "⚙️ Настройки"
BTN_RESET = "🗑 Сброс"


def volley_button_text() -> str:
    size = effective_volley_size()
    return f"{BTN_VOLLEY_PREFIX} {size}"


def shooter_count_button_text() -> str:
    return f"{BTN_SHOOTER_COUNT_PREFIX} {active_shooter_count()}"


def main_keyboard() -> ReplyKeyboardMarkup:
    payment = BTN_PAYMENT_ON if store.settings.live_upgrades else BTN_PAYMENT_OFF
    start_stop = BTN_STOP if runtime.active else BTN_START
    stress_button = BTN_STRESS_ON if runtime.stress_active else BTN_STRESS_OFF
    keyboard = [
        [KeyboardButton(text=BTN_CHANNEL), KeyboardButton(text=BTN_GIFTS)],
        [KeyboardButton(text=BTN_CHECK), KeyboardButton(text=BTN_TARGETS)],
        [KeyboardButton(text=start_stop), KeyboardButton(text=payment)],
        [KeyboardButton(text=volley_button_text()), KeyboardButton(text=BTN_PING)],
        [KeyboardButton(text=BTN_REFRESH_CARD), KeyboardButton(text=BTN_SETTINGS)],
        [KeyboardButton(text=BTN_LOG), KeyboardButton(text=stress_button)],
        [KeyboardButton(text=BTN_RESET)],
    ]
    if PRIMARY_SHOOTER:
        keyboard.insert(4, [KeyboardButton(text=shooter_count_button_text())])
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder=f"{APP_NAME} {APP_VERSION}",
    )


def auth_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✅ Проверить авторизацию", callback_data="auth:check")]]
    )


def payment_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Включить автооплату", callback_data="payment:on")],
            [InlineKeyboardButton(text="Отмена", callback_data="payment:cancel")],
        ]
    )


def reset_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Сбросить все настройки", callback_data="reset:yes")],
            [InlineKeyboardButton(text="Отмена", callback_data="reset:no")],
        ]
    )


def channels_keyboard(choices: list[ChannelChoice]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    selected_id = store.settings.channel_id
    for choice in choices[:50]:
        marker = "✅" if selected_id == choice.channel_id else "▫️"
        label = f"{marker} {choice.title} · {choice.upgradable_count} шт."
        rows.append([InlineKeyboardButton(text=label[:60], callback_data=f"channel:{choice.channel_id}")])
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="channel:refresh")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def gifts_keyboard(infos: list[SavedGiftInfo], selected: set[int]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    duplicate_index: dict[int, int] = {}
    for info in infos[:80]:
        duplicate_index[info.base_gift_id] = duplicate_index.get(info.base_gift_id, 0) + 1
        held = info.saved_id in store.settings.payment_hold_saved_ids
        marker = "⚠️" if held else ("✅" if info.saved_id in selected else "▫️")
        label = f"{marker} {info.title} · экз. {duplicate_index[info.base_gift_id]}"
        if held:
            label += " · платёж?"
        if info.upgrade_cost:
            label += f" · {info.upgrade_cost}⭐"
        rows.append([InlineKeyboardButton(text=label[:60], callback_data=f"gift:{info.saved_id}")])
    rows.append(
        [
            InlineKeyboardButton(text="✅ Готово", callback_data="gift:done"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="gift:refresh"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def owner_guard_message(message: Message) -> bool:
    owner = store.settings.owner_user_id
    user_id = message.from_user.id if message.from_user else None
    if owner is None:
        return True
    if user_id != owner:
        await message.answer("Доступ запрещён.")
        return False
    return True


async def owner_guard_callback(callback: CallbackQuery) -> bool:
    owner = store.settings.owner_user_id
    user_id = callback.from_user.id if callback.from_user else None
    if owner is not None and user_id != owner:
        await callback.answer("Доступ запрещён", show_alert=True)
        return False
    return True


async def safe_delete(message: Message) -> bool:
    try:
        await message.delete()
        return True
    except Exception:
        return False


async def safe_edit_markup(message: Message, markup: InlineKeyboardMarkup) -> None:
    try:
        await message.edit_reply_markup(reply_markup=markup)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


async def bind_owner(user_id: int) -> None:
    store.settings.owner_user_id = user_id
    await store.save()
    logger.info("owner_bound user_id=%s", user_id)


async def continue_setup(message: Message, state: FSMContext) -> None:
    s = store.settings
    if not s.api_id:
        await state.set_state(SetupStates.api_id)
        await message.answer("Отправь <b>TG_API_ID</b> с my.telegram.org. Сообщение будет удалено.")
        return
    if not s.api_hash:
        await state.set_state(SetupStates.api_hash)
        await message.answer("Отправь <b>TG_API_HASH</b>. Сообщение будет удалено.")
        return
    if not s.phone:
        await state.set_state(SetupStates.phone)
        await message.answer("Отправь номер телефона в формате <code>+79991234567</code>. Сообщение будет удалено.")
        return
    await state.clear()
    if await mtproto.is_authorized(reload=True):
        await ensure_channel_and_show(message)
    else:
        # Release the SQLite session before the separate `python main.py auth`
        # process opens it, otherwise Telethon may report "database is locked".
        await mtproto.disconnect()
        hostname = socket.gethostname()
        await message.answer(
            "Данные сохранены. Заверши авторизацию в терминале VPS:\n\n"
            f"<code>docker exec -it {html.escape(hostname)} python main.py auth</code>\n\n"
            "Код входа и пароль 2FA вводи только в терминале. Затем нажми кнопку ниже.",
            reply_markup=auth_keyboard(),
        )


async def show_channel_picker(message: Message) -> None:
    choices = await mtproto.list_channel_choices()
    if not choices:
        await message.answer(
            "Не найдено каналов, которыми владеет аккаунт и где есть подарки для улучшения.",
            reply_markup=main_keyboard(),
        )
        return
    await message.answer(
        "📣 <b>Выбери канал явно</b>\n"
        "LIVE будет работать только с выбранным каналом. При смене канала выбор подарков и LIVE сбрасываются.",
        reply_markup=channels_keyboard(choices),
    )


async def ensure_channel_and_show(message: Message) -> None:
    try:
        if not store.settings.channel_id and not store.settings.channel_username:
            await message.answer("✅ Аккаунт подключён.", reply_markup=main_keyboard())
            await show_channel_picker(message)
            return
        peer = await mtproto.resolve_channel()
        infos = await mtproto.list_upgradable_infos(peer)
        await migrate_legacy_selection(infos)
        if infos:
            await message.answer(
                f"✅ Аккаунт подключён\n"
                f"📣 Канал: <b>{html.escape(store.settings.channel_title or 'выбран')}</b>\n"
                f"🎁 Доступно для улучшения: <b>{len(infos)}</b>\n\n"
                "Открой «🎁 Подарки», выбери конкретный экземпляр и задай выстрел.",
                reply_markup=main_keyboard(),
            )
        else:
            await message.answer(
                "✅ Аккаунт подключён, но в выбранном канале нет подарков, доступных для улучшения.",
                reply_markup=main_keyboard(),
            )
    except RateLimitActiveError as exc:
        await message.answer(f"⏳ {html.escape(str(exc))}", reply_markup=main_keyboard())
    except Exception as exc:
        logger.warning("ensure_channel_failed error=%s", exc)
        await message.answer(f"⚠️ {html.escape(str(exc))}", reply_markup=main_keyboard())
        with contextlib.suppress(Exception):
            await show_channel_picker(message)


async def migrate_legacy_selection(infos: list[SavedGiftInfo]) -> None:
    legacy = set(store.settings.legacy_selected_gift_ids)
    if not legacy or store.settings.selected_saved_ids:
        return
    selected: list[int] = []
    for info in infos:
        if info.base_gift_id in legacy:
            selected.append(info.saved_id)
    if selected:
        store.settings.selected_saved_ids = selected
    store.settings.legacy_selected_gift_ids = []
    await store.save()


async def confirm_cluster_reconfigure(
    message: Message,
    config: ClusterConfig,
    previous_count: int,
) -> None:
    """One-time post-change check; no background Ping is started."""
    deadline = time.monotonic() + CLUSTER_RECONFIG_CONFIRM_SECONDS
    old_max = max(int(previous_count), config.active_shooters)
    snapshots: dict[int, dict[str, Any] | None] = {}

    def lifecycle_ready(shooter_id: int, expected_state: str) -> bool:
        life = snapshots.get(shooter_id)
        if not life:
            return False
        try:
            generation = int(life.get("generation", 0))
        except (TypeError, ValueError):
            return False
        state = str(life.get("state", ""))
        return generation >= config.generation and state == expected_state

    while True:
        snapshots = {
            shooter_id: provision_store.load_lifecycle(shooter_id)
            for shooter_id in range(1, old_max + 1)
        }
        active_ok = all(
            lifecycle_ready(shooter_id, "active")
            for shooter_id in range(1, config.active_shooters + 1)
        )
        sleeping_ok = all(
            lifecycle_ready(shooter_id, "sleeping")
            for shooter_id in range(config.active_shooters + 1, old_max + 1)
        )
        if active_ok and sleeping_ok:
            break
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(0.5)

    ping_results: dict[int, float | None]
    if cluster_runtime.bus is not None:
        with contextlib.suppress(Exception):
            await cluster_runtime.bus.refresh_config_and_peers(config)
        try:
            ping_results = await cluster_runtime.bus.ping_active_shooters(
                timeout_seconds=CLUSTER_PING_TIMEOUT_MS / 1000.0
            )
        except Exception as exc:
            logger.warning("cluster_confirmation_ping_failed error=%s", exc)
            ping_results = {
                shooter_id: None
                for shooter_id in range(1, config.active_shooters + 1)
            }
    else:
        ping_results = {
            shooter_id: None
            for shooter_id in range(1, config.active_shooters + 1)
        }

    lines = [
        f"🔄 <b>Проверка состава {config.active_shooters}/{MAX_SHOOTERS}</b>",
        f"Поколение: <b>{config.generation}</b>",
        "",
    ]
    confirmed_active = 0
    confirmed_sleeping = 0
    for shooter_id in range(1, config.active_shooters + 1):
        life = snapshots.get(shooter_id) or provision_store.load_lifecycle(shooter_id)
        token_missing = shooter_id > 1 and not provision_store.load_token(shooter_id)
        rtt = ping_results.get(shooter_id)
        state = str((life or {}).get("state", ""))
        reason = str((life or {}).get("reason", ""))
        try:
            generation = int((life or {}).get("generation", 0))
        except (TypeError, ValueError):
            generation = 0
        if token_missing:
            lines.append(f"Hunter {shooter_id} — ❌ токен отсутствует")
        elif generation >= config.generation and state == "error":
            lines.append(
                f"Hunter {shooter_id} — ❌ ошибка запуска: {html.escape(reason or 'неизвестно')}"
            )
        elif generation >= config.generation and state == "active" and rtt is not None:
            local = " (локальный)" if shooter_id == 1 else ""
            lines.append(f"Hunter {shooter_id} — ✅ UDP {rtt:.2f} мс{local}")
            confirmed_active += 1
        elif generation >= config.generation and state in {"starting", "waiting"}:
            detail = reason or state
            lines.append(f"Hunter {shooter_id} — ⚠️ {html.escape(detail)}")
        elif generation >= config.generation and state == "active":
            lines.append(f"Hunter {shooter_id} — ❌ UDP не ответил")
        else:
            lines.append(f"Hunter {shooter_id} — ❌ контейнер не подтвердил запуск")

    for shooter_id in range(config.active_shooters + 1, old_max + 1):
        life = snapshots.get(shooter_id) or provision_store.load_lifecycle(shooter_id)
        state = str((life or {}).get("state", ""))
        try:
            generation = int((life or {}).get("generation", 0))
        except (TypeError, ValueError):
            generation = 0
        if generation >= config.generation and state == "sleeping":
            lines.append(f"Hunter {shooter_id} — 💤 отключён")
            confirmed_sleeping += 1
        elif generation >= config.generation and state == "deactivating":
            lines.append(f"Hunter {shooter_id} — 🟡 завершает работу")
        else:
            lines.append(f"Hunter {shooter_id} — ⚠️ нет подтверждения сна")

    lines.extend([
        "",
        f"Активны и отвечают: <b>{confirmed_active}/{config.active_shooters}</b>",
    ])
    if old_max > config.active_shooters:
        lines.append(
            f"Отключены: <b>{confirmed_sleeping}/{old_max - config.active_shooters}</b>"
        )
    logger.info(
        "cluster_reconfigure_confirmed generation=%s active_shooters=%s active_confirmed=%s sleeping_confirmed=%s",
        config.generation,
        config.active_shooters,
        confirmed_active,
        confirmed_sleeping,
    )
    record_cluster_event(
        "cluster_reconfigure_confirmed",
        generation=config.generation,
        active_shooters=config.active_shooters,
        active_confirmed=confirmed_active,
        sleeping_confirmed=confirmed_sleeping,
    )
    await message.answer("\n".join(lines), reply_markup=main_keyboard())


async def continue_initial_setup(message: Message, state: FSMContext) -> None:
    if PRIMARY_SHOOTER and not provision_store.load_config().configured:
        await state.set_state(SetupStates.shooter_count)
        await message.answer(
            "Сколько стрелков будем использовать? Отправь число от <b>1</b> до <b>6</b>."
        )
        return
    await continue_setup(message, state)


async def validate_bot_token(token: str) -> tuple[bool, str]:
    if not re.fullmatch(r"\d{5,}:[A-Za-z0-9_-]{20,}", token):
        return False, "Токен выглядит неверно"
    probe = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    try:
        me = await probe.get_me()
        username = getattr(me, "username", None) or str(getattr(me, "id", "бот"))
        return True, f"@{username}" if not str(username).startswith("@") else str(username)
    except Exception:
        return False, "Telegram не принял токен"
    finally:
        await probe.session.close()


async def ask_next_shooter_token(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    total = min(MAX_SHOOTERS, max(1, int(data.get("shooter_total", 1))))
    current = max(2, int(data.get("shooter_current", 2)))
    while current <= total and provision_store.load_token(current):
        current += 1
    if current > total:
        previous_count = max(1, int(data.get("shooter_previous_count", total)))
        generation = max(1, int(data.get("shooter_generation", provision_store.load_config().generation)))
        config = provision_store.load_config()
        if config.generation != generation or config.active_shooters != total:
            logger.warning(
                "cluster_confirmation_config_changed expected_generation=%s actual_generation=%s expected_active=%s actual_active=%s",
                generation,
                config.generation,
                total,
                config.active_shooters,
            )
        await state.clear()
        await cluster_runtime.refresh_config(config)
        logger.info(
            "cluster_token_setup_complete active_shooters=%s configured_token_ids=%s",
            total,
            provision_store.configured_token_ids(),
        )
        record_cluster_event(
            "cluster_token_setup_complete",
            active_shooters=total,
            configured_token_ids=provision_store.configured_token_ids(),
        )
        await message.answer(
            f"✅ Токены стрелков настроены: <b>{total}</b>. "
            "Открой каждого нового бота, нажми /start и настрой его отдельно."
        )
        await confirm_cluster_reconfigure(message, config, previous_count)
        await continue_setup(message, state)
        return
    await state.update_data(shooter_current=current, shooter_total=total)
    await state.set_state(SetupStates.shooter_token)
    await message.answer(
        f"Отправь Telegram Bot API token для <b>Hunter {current}</b>. "
        "Сообщение с токеном будет сразу удалено."
    )


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    owner = store.settings.owner_user_id
    if owner is None:
        if SETUP_PIN:
            await state.set_state(SetupStates.pin)
            await message.answer("Введи SETUP_PIN для назначения владельца.")
            return
        await bind_owner(message.from_user.id)
    elif owner != message.from_user.id:
        await message.answer("Доступ запрещён.")
        return
    await continue_initial_setup(message, state)


@router.message(SetupStates.pin)
async def pin_handler(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    value = (message.text or "").strip()
    await safe_delete(message)
    if value != SETUP_PIN:
        await message.answer("Неверный PIN.")
        return
    await bind_owner(message.from_user.id)
    await continue_initial_setup(message, state)


@router.message(F.text.startswith(BTN_SHOOTER_COUNT_PREFIX))
async def shooter_count_button_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    if not PRIMARY_SHOOTER:
        await message.answer("Количество стрелков меняется только в Hunter 1.", reply_markup=main_keyboard())
        return

    previous_count = active_shooter_count()
    scanner_was_active = bool(runtime.active)
    stress_was_active = bool(runtime.stress_active)
    live_was_enabled = bool(store.settings.live_upgrades)
    if scanner_was_active:
        await scanner.stop("cluster_reconfigure")
    if stress_was_active:
        await stress_tester.stop("cluster_reconfigure")
    store.settings.live_upgrades = False
    scanner.prepared.clear()
    cluster_runtime.disarm()
    cluster_runtime.notify_state_changed()
    await store.save()
    await state.set_state(SetupStates.shooter_count)
    logger.info(
        "cluster_reconfigure_requested previous_active_shooters=%s scanner_stopped=%s stress_stopped=%s live_disabled=%s",
        previous_count,
        scanner_was_active,
        stress_was_active,
        live_was_enabled,
    )
    record_cluster_event(
        "cluster_reconfigure_requested",
        previous_active_shooters=previous_count,
        scanner_stopped=scanner_was_active,
        stress_stopped=stress_was_active,
        live_disabled=live_was_enabled,
    )
    await message.answer(
        f"Сейчас выбрано стрелков: <b>{previous_count}</b>. "
        "Отправь новое число от <b>1</b> до <b>6</b>. "
        "В Hunter 1 сканер и стресс-тест остановлены, LIVE выключен.",
        reply_markup=main_keyboard(),
    )


@router.message(SetupStates.shooter_count)
async def shooter_count_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    value = (message.text or "").strip()
    try:
        count = int(value)
    except ValueError:
        count = 0
    if not 1 <= count <= MAX_SHOOTERS:
        await message.answer("Нужно отправить число от 1 до 6.")
        return
    previous_count = active_shooter_count()
    config = provision_store.configure(count)
    settings_changed = False
    # Volley size belongs to each Hunter and is never copied or flattened when
    # the cluster size changes. Hunter 1-6 each keep an independent 1-50 local volley.
    if store.settings.live_upgrades:
        store.settings.live_upgrades = False
        settings_changed = True
    if scanner.prepared:
        scanner.prepared.clear()
    if settings_changed:
        await store.save()
    logger.info(
        "cluster_configured previous_active_shooters=%s active_shooters=%s generation=%s local_volley=%s local_volley_limit=%s",
        previous_count, config.active_shooters, config.generation,
        effective_volley_size(), effective_max_volley_size(),
    )
    record_cluster_event(
        "cluster_configured",
        previous_active_shooters=previous_count,
        active_shooters=config.active_shooters,
        generation=config.generation,
        local_volley=effective_volley_size(),
        local_volley_limit=effective_max_volley_size(),
    )
    await state.update_data(
        shooter_previous_count=previous_count,
        shooter_generation=config.generation,
    )
    await cluster_runtime.refresh_config(config, broadcast_notice=True)
    if count == 1:
        await state.clear()
        await message.answer(
            "✅ Выбран <b>1 стрелок</b>. Залп одного аккаунта доступен от 1 до 50."
        )
        await confirm_cluster_reconfigure(message, config, previous_count)
        await continue_setup(message, state)
        return
    await state.update_data(shooter_total=count, shooter_current=2)
    await ask_next_shooter_token(message, state)


@router.message(SetupStates.shooter_token)
async def shooter_token_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    token = (message.text or "").strip()
    message_deleted = await safe_delete(message)
    data = await state.get_data()
    current = max(2, int(data.get("shooter_current", 2)))
    total = min(MAX_SHOOTERS, max(1, int(data.get("shooter_total", current))))
    logger.info(
        "shooter_token_received shooter_id=%s message_deleted=%s token_length=%s",
        current,
        message_deleted,
        len(token),
    )
    if not message_deleted:
        logger.warning("shooter_token_message_delete_failed shooter_id=%s", current)
    known_tokens = {
        value
        for value in (
            BOT_TOKEN,
            *(provision_store.load_token(i) for i in range(2, MAX_SHOOTERS + 1)),
        )
        if value
    }
    if token and token in known_tokens:
        logger.warning("shooter_token_duplicate_rejected shooter_id=%s", current)
        await message.answer(f"Этот токен уже используется. Отправь другой токен для Hunter {current}.")
        return
    valid, identity = await validate_bot_token(token)
    if not valid:
        logger.warning(
            "shooter_token_validation_failed shooter_id=%s reason=%s message_deleted=%s",
            current,
            identity,
            message_deleted,
        )
        await message.answer(f"❌ {html.escape(identity)}. Отправь токен Hunter {current} ещё раз.")
        return
    token_path = provision_store.save_token(current, token)
    logger.info(
        "shooter_token_provisioned shooter_id=%s bot=%s message_deleted=%s token_file=%s mode=%o",
        current,
        identity,
        message_deleted,
        token_path.name,
        token_path.stat().st_mode & 0o777,
    )
    record_cluster_event(
        "shooter_token_provisioned",
        participant_id=current,
        bot=identity,
        message_deleted=message_deleted,
        token_file=token_path.name,
    )
    await message.answer(f"✅ Hunter {current} подключён: <b>{html.escape(identity)}</b>")
    await state.update_data(shooter_current=current + 1, shooter_total=total)
    await ask_next_shooter_token(message, state)


@router.message(SetupStates.api_id)
async def api_id_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    value = (message.text or "").strip()
    await safe_delete(message)
    try:
        api_id = int(value)
        if api_id <= 0:
            raise ValueError
    except ValueError:
        await message.answer("TG_API_ID должен быть положительным числом.")
        return
    store.settings.api_id = api_id
    await store.save()
    await continue_setup(message, state)


@router.message(SetupStates.api_hash)
async def api_hash_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    value = (message.text or "").strip()
    await safe_delete(message)
    if not re.fullmatch(r"[A-Fa-f0-9]{16,64}", value):
        await message.answer("TG_API_HASH выглядит неверно. Скопируй строку целиком с my.telegram.org.")
        return
    store.settings.api_hash = value
    await store.save()
    await continue_setup(message, state)


@router.message(SetupStates.phone)
async def phone_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    value = re.sub(r"[^+\d]", "", message.text or "")
    await safe_delete(message)
    if not re.fullmatch(r"\+\d{8,15}", value):
        await message.answer("Номер должен быть в международном формате, например <code>+79991234567</code>.")
        return
    store.settings.phone = value
    await store.save()
    await continue_setup(message, state)


@router.callback_query(F.data == "auth:check")
async def auth_check_handler(callback: CallbackQuery) -> None:
    if not await owner_guard_callback(callback):
        return
    await callback.answer("Проверяю…")
    if await mtproto.is_authorized(reload=True):
        cluster_runtime.notify_state_changed()
        if callback.message:
            await ensure_channel_and_show(callback.message)
    else:
        await callback.answer("Авторизация ещё не завершена", show_alert=True)


async def reject_changes_while_running_message(message: Message) -> bool:
    if runtime.active:
        await message.answer("Сначала нажми «⛔ Остановить». Во время сканирования настройки заблокированы для максимальной скорости.")
        return True
    if runtime.stress_active:
        await message.answer("Сначала выключи тумблер «🧪 Стресс-тест: ВКЛ». Во время теста настройки заблокированы.")
        return True
    return False


async def reject_changes_while_running_callback(callback: CallbackQuery) -> bool:
    if runtime.active:
        await callback.answer("Сначала останови сканер", show_alert=True)
        return True
    if runtime.stress_active:
        await callback.answer("Сначала выключи стресс-тест", show_alert=True)
        return True
    return False


@router.message(F.text == BTN_CHANNEL)
async def channel_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    if await reject_changes_while_running_message(message):
        return
    try:
        await show_channel_picker(message)
    except Exception as exc:
        logger.exception("channel_picker_failed")
        await message.answer(f"❌ {html.escape(str(exc))}", reply_markup=main_keyboard())


@router.callback_query(F.data.startswith("channel:"))
async def channel_callback_handler(callback: CallbackQuery) -> None:
    if not await owner_guard_callback(callback):
        return
    if await reject_changes_while_running_callback(callback):
        return
    action = (callback.data or "").split(":", 1)[1]
    try:
        if action == "refresh":
            choices = await mtproto.list_channel_choices()
            await callback.answer("Обновлено")
            if callback.message:
                await safe_edit_markup(callback.message, channels_keyboard(choices))
            return
        choice = await mtproto.select_channel(int(action))
        scanner.prepared.clear()
        cluster_runtime.notify_state_changed()
        await callback.answer("Канал выбран", show_alert=True)
        if callback.message:
            await callback.message.answer(
                f"✅ Канал: <b>{html.escape(choice.title)}</b>\n"
                f"Подарков для улучшения: <b>{choice.upgradable_count}</b>.\n"
                "Теперь выбери конкретный подарок.",
                reply_markup=main_keyboard(),
            )
    except Exception as exc:
        logger.exception("channel_select_failed")
        await callback.answer(str(exc)[:180], show_alert=True)


@router.message(F.text == BTN_GIFTS)
async def gifts_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    if await reject_changes_while_running_message(message):
        return
    try:
        peer = await mtproto.resolve_channel()
        confirmed_holds, pending_holds = await mtproto.reconcile_payment_holds(peer)
        if confirmed_holds:
            obtained = ", ".join(f"#{number}" for _sid, number, _slug in confirmed_holds)
            await message.answer(
                f"✅ Ранее неподтверждённый платёж теперь подтверждён: <b>{obtained}</b>. "
                "Повторная оплата не отправлялась."
            )
        if pending_holds:
            pending_text = ", ".join(str(saved_id) for saved_id in pending_holds)
            await message.answer(
                "⚠️ Telegram пока не подтвердил результат платежа. "
                f"Экземпляры <code>{html.escape(pending_text)}</code> заблокированы от повторной оплаты. "
                "Нажми «Обновить» позже; вручную удалять их нельзя."
            )
        infos = await mtproto.list_upgradable_infos(peer)
        await migrate_legacy_selection(infos)
        selected = set(store.settings.selected_saved_ids)
        if not infos:
            await message.answer("В канале нет подарков, доступных для улучшения.")
            return
        await message.answer(
            build_channel_gifts_text(infos),
            reply_markup=gifts_keyboard(infos, selected),
        )
    except Exception as exc:
        logger.exception("gifts_handler_failed")
        await message.answer(f"❌ {html.escape(str(exc))}")


async def toggle_gift_selection(saved_id: int, valid_ids: set[int]) -> None:
    if store.settings.payment_hold_saved_ids:
        raise RuntimeError(
            PENDING_PAYMENT_HOLD_MESSAGE
            + " Нажми «Обновить» в списке подарков и дождись результата сверки."
        )
    selected = list(store.settings.selected_saved_ids)
    if saved_id in selected:
        selected.remove(saved_id)
    else:
        if saved_id not in valid_ids:
            raise RuntimeError("Экземпляр больше недоступен. Нажми «Обновить».")
        selected.append(saved_id)
    store.settings.selected_saved_ids = _unique_ints(selected)
    scanner.prepared.clear()
    store.settings.live_upgrades = False
    await store.save()
    cluster_runtime.notify_state_changed()


@router.callback_query(F.data.startswith("gift:"))
async def gift_callback_handler(callback: CallbackQuery) -> None:
    if not await owner_guard_callback(callback):
        return
    if await reject_changes_while_running_callback(callback):
        return
    action = (callback.data or "").split(":", 1)[1]
    if action == "done":
        await callback.answer("Сохранено")
        if callback.message:
            await callback.message.answer(
                f"✅ Выбрано экземпляров: <b>{len(store.settings.selected_saved_ids)}</b>.\n"
                "Теперь задай целевой номер и запусти сканер.",
                reply_markup=main_keyboard(),
            )
        return
    try:
        peer = await mtproto.resolve_channel()
        infos = await mtproto.list_upgradable_infos(peer)
        if action == "refresh":
            await callback.answer("Обновлено")
        else:
            saved_id = int(action)
            valid_ids = {item.saved_id for item in infos}
            await toggle_gift_selection(saved_id, valid_ids)
            await callback.answer("Выбор изменён")
        if callback.message:
            await safe_edit_markup(callback.message, gifts_keyboard(infos, set(store.settings.selected_saved_ids)))
    except Exception as exc:
        logger.exception("gift_callback_failed")
        await callback.answer(str(exc)[:180], show_alert=True)


@router.message(F.text == BTN_TARGETS)
async def targets_prompt_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    if await reject_changes_while_running_message(message):
        return
    if store.settings.payment_hold_saved_ids:
        await message.answer(
            PENDING_PAYMENT_HOLD_MESSAGE
            + " Сначала открой «🎁 Подарки» и дождись сверки."
        )
        return
    await state.set_state(SetupStates.targets)
    await message.answer(
        "Отправь номера выстрела через запятую, пробел или с новой строки.\n"
        "Пример: <code>12842, 13000</code>\n\n"
        "Новый ввод полностью заменяет старый список."
    )


@router.message(SetupStates.targets)
async def targets_value_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    if await reject_changes_while_running_message(message):
        await state.clear()
        return
    if store.settings.payment_hold_saved_ids:
        await state.clear()
        await message.answer(
            PENDING_PAYMENT_HOLD_MESSAGE
            + " Номера выстрела не изменены; сначала дождись сверки в «🎁 Подарки»."
        )
        return
    targets = parse_target_numbers(message.text or "")
    if not targets:
        await message.answer("Не нашёл положительных чисел. Попробуй ещё раз.")
        return
    store.settings.target_numbers = targets
    store.settings.live_upgrades = False
    scanner.prepared.clear()
    await store.save()
    cluster_runtime.notify_state_changed()
    await state.clear()
    await message.answer(
        "🎯 Номера выстрела: " + ", ".join(map(str, targets)) + "\n🛡 LIVE выключен — включи его заново после проверки выстрела.",
        reply_markup=main_keyboard(),
    )


def build_channel_gifts_text(infos: list[SavedGiftInfo]) -> str:
    """Build the selected-channel gift picker without checking collectible counters."""
    grouped: dict[int, list[SavedGiftInfo]] = {}
    for info in infos:
        grouped.setdefault(info.base_gift_id, []).append(info)
    selected = set(store.settings.selected_saved_ids)
    lines = [
        f"🎁 <b>Подарки канала · {html.escape(store.settings.channel_title or 'канал')}</b>",
        f"Типов: <b>{len(grouped)}</b> · экземпляров: <b>{len(infos)}</b>",
        "",
    ]
    for index, group in enumerate(
        sorted(grouped.values(), key=lambda items: items[0].title.casefold()), start=1
    ):
        representative = group[0]
        selected_count = sum(1 for item in group if item.saved_id in selected)
        selected_suffix = f" · выбрано {selected_count}" if selected_count else ""
        prices = [item.upgrade_cost for item in group if item.upgrade_cost > 0]
        price_suffix = f" · до {max(prices)} ⭐" if prices else ""
        lines.append(
            f"{index}. <b>{html.escape(representative.title)}</b> — "
            f"{len(group)} шт.{selected_suffix}{price_suffix}"
        )
    lines.extend(
        [
            "",
            "Каждая кнопка ниже — отдельный экземпляр. "
            "Последние номера всех коллекций открываются отдельной кнопкой «🔢 Проверить номера».",
        ]
    )
    return "\n".join(lines)


async def build_global_gift_numbers_text(service: MTProtoService) -> str:
    """Return the global collectible catalog with issued and maximum totals."""
    results, elapsed_ms = await service.fetch_global_catalog_numbers()
    results = sorted(results, key=lambda item: item.title.casefold())
    resolved = sum(1 for item in results if item.issued is not None)
    errors_count = len(results) - resolved
    highlighted_count = 0
    snapshot_items: list[dict[str, Any]] = []
    lines = [
        f"🔢 <b>Последние выданные номера · {APP_VERSION}</b>",
        f"Проверено коллекций: <b>{len(results)}</b> · {elapsed_ms:.0f} мс",
    ]
    if errors_count:
        lines.append(f"Получено номеров: <b>{resolved}</b> · ошибок: <b>{errors_count}</b>")
    lines.append("")
    for item in results:
        highlighted = False
        special_target: int | None = None
        special_distance: int | None = None
        if item.issued is None:
            issued_text = "не определён"
        else:
            highlighted, special_target, special_distance = catalog_number_is_near_special(item.issued, item.total)
            if highlighted:
                highlighted_count += 1
                issued_text = f"<b>{item.issued}</b>"
                logger.info(
                    "catalog_number_near_special gift_id=%s title=%s issued=%s special_target=%s distance=%s total=%s",
                    item.gift_id,
                    item.title,
                    item.issued,
                    special_target,
                    special_distance,
                    item.total,
                )
            else:
                issued_text = str(item.issued)
        total_text = str(item.total) if item.total is not None else "не определён"
        lines.append(f"{html.escape(item.title)} — {issued_text} — {total_text}")
        snapshot_items.append(
            {
                "gift_id": item.gift_id,
                "title": item.title,
                "issued": item.issued,
                "total": item.total,
                "slug": item.slug,
                "error": item.error,
                "highlighted": highlighted,
                "special_target": special_target,
                "special_distance": special_distance,
            }
        )

    snapshot = {
        "version": APP_VERSION,
        "generated_at": msk_now().isoformat(),
        "elapsed_ms": round(float(elapsed_ms), 3),
        "collections": len(results),
        "resolved": resolved,
        "errors": errors_count,
        "highlighted": highlighted_count,
        "special_number_max_distance": SPECIAL_NUMBER_MAX_DISTANCE,
        "items": snapshot_items,
    }
    try:
        temp = CATALOG_REPORT_PATH.with_suffix(".tmp")
        temp.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        os.chmod(temp, 0o600)
        temp.replace(CATALOG_REPORT_PATH)
        os.chmod(CATALOG_REPORT_PATH, 0o600)
    except OSError as exc:
        logger.warning("catalog_numbers_snapshot_write_failed path=%s error=%s", CATALOG_REPORT_PATH, exc)
    else:
        logger.info(
            "catalog_numbers_render_complete collections=%s resolved=%s errors=%s with_total=%s highlighted=%s elapsed_ms=%.1f snapshot=%s",
            len(results),
            resolved,
            errors_count,
            sum(1 for item in results if item.total is not None),
            highlighted_count,
            elapsed_ms,
            CATALOG_REPORT_PATH,
        )
    return "\n".join(lines)


def split_message_text(text: str, *, limit: int = 3800) -> list[str]:
    """Split long line-oriented HTML messages without cutting a line."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in text.splitlines():
        addition = len(line) + (1 if current else 0)
        if current and current_len + addition > limit:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += addition
    if current:
        chunks.append("\n".join(current))
    return chunks


async def send_text_chunks(
    message: Message,
    text: str,
    *,
    reply_markup: Any | None = None,
) -> None:
    chunks = split_message_text(text)
    for index, chunk in enumerate(chunks):
        await message.answer(
            chunk,
            reply_markup=reply_markup if index == len(chunks) - 1 else None,
        )


def _parse_log_timestamp(value: Any) -> float | None:
    """Best-effort conversion of a log timestamp to Unix seconds."""
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    normalized = raw.replace("Z", "+00:00")
    with contextlib.suppress(ValueError):
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            # Normal logging timestamps are written in the container's local time.
            return time.mktime(dt.timetuple()) + dt.microsecond / 1_000_000
        return dt.timestamp()
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        with contextlib.suppress(ValueError):
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                return time.mktime(dt.timetuple())
            return dt.timestamp()
    return None


def _timestamp_from_log_line(line: str) -> float | None:
    stripped = line.lstrip()
    if stripped.startswith("{"):
        with contextlib.suppress(json.JSONDecodeError, TypeError, ValueError):
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                for key in ("timestamp", "generated_at", "finished_at", "started_at", "time"):
                    parsed = _parse_log_timestamp(payload.get(key))
                    if parsed is not None:
                        return parsed
    # Python logging: 2026-09-17 12:34:56,789 ...
    match = re.match(
        r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:[,.]\d+)?(?:([+-]\d{4}))?",
        line,
    )
    if match:
        return _parse_log_timestamp(match.group(1) + (match.group(2) or ""))
    return None


def _stream_filter_recent_log(
    source: Path,
    target: Path,
    cutoff_epoch: float,
) -> tuple[int, bool] | None:
    """Stream recent log entries into *target* without loading the file in RAM.

    Returns ``(bytes_written, changed)``. ``changed`` is true when at least one
    physical input line was excluded by the 24-hour filter. Ordinary Python
    log traceback continuation lines follow the timestamped line that precedes
    them; JSONL records are treated independently, matching the previous export
    semantics. Original bytes are preserved for every retained line.
    """
    try:
        source_stat = source.stat()
    except OSError:
        return None

    jsonl = ".jsonl" in source.name
    keep_continuation = source_stat.st_mtime >= cutoff_epoch
    saw_timestamp = False
    changed = False
    bytes_written = 0

    try:
        with source.open("rb") as input_stream, target.open("wb") as output_stream:
            for raw_line in input_stream:
                line = raw_line.decode("utf-8", errors="replace")
                timestamp = _timestamp_from_log_line(line)
                keep = False
                if timestamp is not None:
                    saw_timestamp = True
                    keep_continuation = timestamp >= cutoff_epoch
                    keep = keep_continuation
                elif jsonl:
                    keep = source_stat.st_mtime >= cutoff_epoch
                else:
                    keep = keep_continuation

                if keep:
                    output_stream.write(raw_line)
                    bytes_written += len(raw_line)
                else:
                    changed = True
    except OSError:
        target.unlink(missing_ok=True)
        return None

    if not saw_timestamp and source_stat.st_mtime < cutoff_epoch:
        changed = source_stat.st_size > 0
    with contextlib.suppress(OSError):
        os.chmod(target, 0o600)
    return bytes_written, changed


def _full_log_timeseries_paths() -> list[Path]:
    paths: list[Path] = []
    paths.extend(DATA_DIR.glob("gift-hunter-v*.log*"))
    paths.extend(DATA_DIR.glob("gift-hunter-v*-payment-audit.jsonl*"))
    paths.extend((STRESS_HISTORY_PATH, provision_store.event_path))
    unique: dict[str, Path] = {}
    for path in paths:
        if (
            path.exists()
            and path.is_file()
            and "-full-log" not in path.name
            and not path.name.endswith("-full.log")
        ):
            unique[str(path.resolve())] = path
    return sorted(unique.values(), key=lambda item: item.name)


def _full_log_snapshot_paths() -> list[Path]:
    candidates = [
        STRESS_REPORT_PATH,
        DIAGNOSTICS_PATH,
        CATALOG_REPORT_PATH,
        RATE_LIMIT_PATH,
        PAYMENT_GUARD_PATH,
        SCANNER_RESUME_PATH,
        provision_store.config_path,
        provision_store.generation_path,
        *sorted(provision_store.root.glob("hunter-*.lifecycle.json")),
    ]
    return [path for path in candidates if path.exists() and path.is_file()]


def _split_file_for_log_export(path: Path, staging_dir: Path) -> list[tuple[str, Path, int]]:
    """Split oversized staged files with a bounded streaming buffer."""
    size = path.stat().st_size
    if size <= LOG_FULL_FRAGMENT_BYTES:
        return [(path.name, path, size)]

    fragments: list[tuple[str, Path, int]] = []
    chunk_size = min(1024 * 1024, LOG_FULL_FRAGMENT_BYTES)
    with path.open("rb") as source:
        index = 1
        while True:
            fragment = staging_dir / f"{path.name}.chunk{index:03d}"
            written = 0
            with fragment.open("wb") as output:
                while written < LOG_FULL_FRAGMENT_BYTES:
                    payload = source.read(
                        min(chunk_size, LOG_FULL_FRAGMENT_BYTES - written)
                    )
                    if not payload:
                        break
                    output.write(payload)
                    written += len(payload)
            if written == 0:
                fragment.unlink(missing_ok=True)
                break
            os.chmod(fragment, 0o600)
            fragments.append((fragment.name, fragment, written))
            index += 1
    return fragments


def _write_log_zip(path: Path, entries: list[tuple[str, Path, int]], manifest: str) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("MANIFEST.txt", manifest)
        for arcname, source, _size in entries:
            archive.write(source, arcname=arcname)


def build_full_log_exports(*, now_epoch: float | None = None) -> list[Path]:
    """Build one or more ZIPs containing only the most recent 24 hours of logs."""
    now_epoch = time.time() if now_epoch is None else float(now_epoch)
    cutoff_epoch = now_epoch - LOG_FULL_WINDOW_SECONDS
    for old_export in DATA_DIR.glob(f"gift-hunter-{APP_VERSION}-full-log-24h*.zip"):
        old_export.unlink(missing_ok=True)

    with tempfile.TemporaryDirectory(prefix="log-full-24h-", dir=DATA_DIR) as tmp:
        staging = Path(tmp)
        base_entries: list[tuple[str, Path, int]] = []
        included_sources: list[str] = []

        for source in _full_log_timeseries_paths():
            target = staging / source.name
            filtered = _stream_filter_recent_log(source, target, cutoff_epoch)
            if filtered is None or filtered[0] == 0:
                target.unlink(missing_ok=True)
                continue
            included_sources.append(source.name)
            base_entries.append((target.name, target, filtered[0]))

        for source in _full_log_snapshot_paths():
            target = staging / source.name
            try:
                shutil.copyfile(source, target)
                os.chmod(target, 0o600)
                size = target.stat().st_size
            except OSError:
                target.unlink(missing_ok=True)
                continue
            included_sources.append(source.name)
            base_entries.append((target.name, target, size))

        generated = datetime.fromtimestamp(now_epoch, timezone.utc).isoformat()
        period_start = datetime.fromtimestamp(cutoff_epoch, timezone.utc).isoformat()
        manifest_base = (
            f"{APP_NAME} {APP_VERSION} full log export (last 24h)\n"
            f"generated_at={generated}\n"
            f"period_start_utc={period_start}\n"
            f"period_end_utc={generated}\n"
            f"window_hours=24\n"
            f"shooter_id={SHOOTER_ID}\n"
            f"active_shooters={active_shooter_count()}\n"
            f"source_files={len(included_sources)}\n"
            f"included={','.join(included_sources)}\n"
        )

        single = DATA_DIR / f"gift-hunter-{APP_VERSION}-full-log-24h.zip"
        _write_log_zip(single, base_entries, manifest_base + "part=1/1\n")
        if single.stat().st_size <= LOG_FULL_TELEGRAM_LIMIT_BYTES:
            return [single]
        single.unlink(missing_ok=True)

        entries: list[tuple[str, Path, int]] = []
        for _arcname, source, _size in base_entries:
            entries.extend(_split_file_for_log_export(source, staging))

        # Repack into independent ZIPs. Raw payload stays <=45 MB, which leaves
        # enough headroom below Telegram's 50 MB document limit even if DEFLATE
        # cannot compress a particular fragment.
        groups: list[list[tuple[str, Path, int]]] = []
        current: list[tuple[str, Path, int]] = []
        current_size = 0
        for entry in entries:
            entry_size = entry[2]
            if current and current_size + entry_size > LOG_FULL_PART_TARGET_BYTES:
                groups.append(current)
                current = []
                current_size = 0
            current.append(entry)
            current_size += entry_size
        if current or not groups:
            groups.append(current)

        result: list[Path] = []
        total = len(groups)
        for index, group in enumerate(groups, start=1):
            part = DATA_DIR / (
                f"gift-hunter-{APP_VERSION}-full-log-24h-part{index:02d}-of{total:02d}.zip"
            )
            _write_log_zip(part, group, manifest_base + f"part={index}/{total}\n")
            if part.stat().st_size > LOG_FULL_TELEGRAM_LIMIT_BYTES:
                raise RuntimeError(
                    f"log export part {index} is too large: {part.stat().st_size} bytes"
                )
            result.append(part)
        return result


def _replace_with_staged_log(target: Path, staged: Path) -> None:
    os.chmod(staged, 0o600)
    staged.replace(target)
    os.chmod(target, 0o600)


def _matching_rotating_handler(target: Path) -> RotatingFileHandler | None:
    target_resolved = str(target.resolve())
    for owner in (logging.getLogger(), payment_audit_logger):
        for handler in list(owner.handlers):
            if not isinstance(handler, RotatingFileHandler):
                continue
            with contextlib.suppress(OSError):
                if str(Path(handler.baseFilename).resolve()) == target_resolved:
                    return handler
    return None


def _detach_rotating_segments_for_prune(target: Path) -> list[Path]:
    """Quickly detach active+numbered log segments, then release logger locks.

    The only critical section is flush/rollover/rename. Heavy 24-hour filtering
    happens later on ``.history-*`` files that RotatingFileHandler will never
    rename, so asyncio log calls cannot be stalled by a multi-megabyte prune and
    a concurrent rollover cannot make us overwrite a fresh ``.1`` file.
    """
    handler = _matching_rotating_handler(target)
    if handler is None:
        return []

    detached: list[Path] = []
    stamp = f"{time.time_ns()}-{threading.get_ident()}"
    handler.acquire()
    try:
        handler.flush()
        # Force one quick rollover so the previously active file becomes a stable
        # numbered segment. The coordinated handler serializes namespace renames.
        handler.doRollover()
        with LOG_ROTATION_COORD_LOCK:
            backup_count = max(0, int(getattr(handler, "backupCount", 0)))
            for index in range(1, backup_count + 1):
                source = Path(f"{handler.baseFilename}.{index}")
                if not source.exists():
                    continue
                history = target.with_name(f"{target.name}.history-{stamp}-{index:02d}")
                history.unlink(missing_ok=True)
                source.replace(history)
                with contextlib.suppress(OSError):
                    os.chmod(history, 0o600)
                detached.append(history)
    finally:
        handler.release()
    return detached


def _prune_detached_log(path: Path, cutoff_epoch: float) -> tuple[bool, bool]:
    """Prune one stable segment. Returns ``(changed, deleted)``."""
    if not path.exists() or not path.is_file():
        return False, False
    temp = path.with_name(path.name + ".prune.tmp")
    temp.unlink(missing_ok=True)
    filtered = _stream_filter_recent_log(path, temp, cutoff_epoch)
    if filtered is None:
        temp.unlink(missing_ok=True)
        return False, False
    output_size, was_filtered = filtered
    if output_size == 0:
        temp.unlink(missing_ok=True)
        path.unlink(missing_ok=True)
        return False, True
    if not was_filtered:
        temp.unlink(missing_ok=True)
        return False, False
    _replace_with_staged_log(path, temp)
    return True, False


def prune_local_log_history_after_export(cutoff_epoch: float) -> tuple[int, int]:
    """Keep only recent history without holding a logging lock during filtering."""
    changed = 0
    deleted = 0

    detached_now: list[Path] = []
    for active in (LOG_PATH, PAYMENT_AUDIT_PATH):
        try:
            detached_now.extend(_detach_rotating_segments_for_prune(active))
        except OSError as exc:
            logger.error("log_segment_detach_failed path=%s error=%s", active, exc)

    # Numbered backups were detached under the handler lock and rotation lock.
    # Existing .history-* files are already outside RotatingFileHandler's namespace.
    candidates: list[Path] = list(detached_now)
    candidates.extend(DATA_DIR.glob("gift-hunter-v*.log.history-*"))
    candidates.extend(DATA_DIR.glob("gift-hunter-v*-payment-audit.jsonl.history-*"))
    candidates.append(STRESS_HISTORY_PATH)
    seen: set[str] = set()
    for path in candidates:
        try:
            resolved = str(path.resolve())
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        if not path.exists() or not path.is_file() or "-full-log" in path.name:
            continue
        try:
            was_changed, was_deleted = _prune_detached_log(path, cutoff_epoch)
        except OSError as exc:
            logger.error("log_history_prune_failed path=%s error=%s", path, exc)
            continue
        changed += int(was_changed)
        deleted += int(was_deleted)

    # cluster-events.jsonl is shared by all Hunter containers. ProvisionStore
    # serializes pruning with appenders so a /log_full on one bot cannot erase a
    # concurrent event written by another bot.
    with contextlib.suppress(OSError, ValueError):
        cluster_changed, cluster_deleted = provision_store.prune_events_before(cutoff_epoch)
        changed += int(cluster_changed)
        deleted += int(cluster_deleted)
    return changed, deleted


async def current_status_text() -> str:
    peer = await mtproto.resolve_channel()
    infos = await mtproto.get_selected_infos(peer)
    if not infos:
        return "Подарки не выбраны или выбранные экземпляры уже недоступны."
    lines = [f"🎯 <b>{APP_NAME} {APP_VERSION}</b>"]
    seen: set[str] = set()
    for info in infos:
        counter = await mtproto.counter_for_info(info, peer=peer, cache_seconds=0)
        if counter.slug in seen:
            continue
        seen.add(counter.slug)
        target = next_target(store.settings.target_numbers, counter.current)
        if target is None and store.settings.target_numbers:
            target = max(store.settings.target_numbers)
        state = evaluate_target(counter.current, target)
        lines.extend(
            [
                "",
                f"🎁 <b>{html.escape(counter.title)}</b>",
                f"Текущий номер: <b>{counter.current}</b>",
                f"Выстрел: <b>{target if target is not None else 'не задан'}</b>",
            ]
        )
        if state.distance is not None:
            if state.distance > 0:
                lines.append(f"До выстрела: <b>{state.distance}</b>")
            else:
                lines.append("Статус: <b>выстрел уже прошёл</b>")
    return "\n".join(lines)


@router.message(F.text == BTN_CHECK)
async def check_numbers_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    if runtime.stress_active:
        await message.answer(
            "Идёт стресс-тест. Общий список номеров временно недоступен, чтобы не искажать тест.",
            reply_markup=main_keyboard(),
        )
        return
    if runtime.active:
        await message.answer(
            "Сначала останови сканер: полная проверка каталога создаёт много запросов и не должна мешать ловле номера.",
            reply_markup=main_keyboard(),
        )
        return
    try:
        text = await build_global_gift_numbers_text(mtproto)
        chunks = split_message_text(text)
        logger.info(
            "check_numbers_ready collections_report=%s chunks=%s chars=%s",
            CATALOG_REPORT_PATH,
            len(chunks),
            len(text),
        )
        await send_text_chunks(message, text, reply_markup=main_keyboard())
        logger.info("check_numbers_sent chunks=%s chars=%s", len(chunks), len(text))
    except Exception as exc:
        logger.exception("check_numbers_failed")
        await message.answer(f"❌ {html.escape(str(exc))}", reply_markup=main_keyboard())


@router.message(F.text == BTN_START)
async def scanner_start_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    if runtime.stress_active:
        await message.answer("Сначала останови стресс-тест.", reply_markup=main_keyboard())
        return
    try:
        await scanner.start()
        # Wait for the first real MTProto cycle so the initial status already
        # contains both numbers instead of a temporary dash.
        for _ in range(40):
            if runtime.checks > 0 or runtime.last_error or not runtime.active:
                break
            await asyncio.sleep(0.1)
        sent = await message.answer(
            (await status_text())
            + ("\n\n💳 <b>LIVE:</b> бот отправит оплату при текущем номере = выстрел − 1."
               if store.settings.live_upgrades
               else "\n\n🧪 <b>DRY-RUN:</b> Stars не списываются."),
        )
        scanner.attach_status_message(sent.chat.id, sent.message_id)
    except Exception as exc:
        logger.exception("scanner_start_failed")
        await message.answer(f"Не удалось запустить: {html.escape(str(exc))}", reply_markup=main_keyboard())


@router.message(F.text == BTN_STOP)
async def scanner_stop_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    await scanner.stop("manual")
    await message.answer("⛔ Сканер остановлен.", reply_markup=main_keyboard())


async def live_preflight(*, prepare: bool) -> str:
    rate_limit.clear_if_expired()
    rate_limit.assert_available()
    peer = await mtproto.resolve_channel()

    confirmed_holds, pending_holds = await mtproto.reconcile_payment_holds(peer)
    if pending_holds:
        raise RuntimeError(
            PENDING_PAYMENT_HOLD_MESSAGE
            + " Открой «🎁 Подарки» и нажми «Обновить» для повторной сверки."
        )

    infos = await mtproto.get_selected_infos(peer)
    if not infos:
        if confirmed_holds:
            numbers = ", ".join(f"#{number}" for _sid, number, _slug in confirmed_holds)
            raise RuntimeError(f"Предыдущий платёж подтверждён ({numbers}), но новых подарков не выбрано")
        raise RuntimeError("Не выбран доступный подарок для улучшения")
    base_ids = {info.base_gift_id for info in infos}
    if len(base_ids) != 1:
        raise RuntimeError("Для LIVE выбери подарки только одного типа")
    if not store.settings.target_numbers:
        raise RuntimeError("Номера выстрела не заданы")
    if any(not info.can_upgrade for info in infos):
        raise RuntimeError("Один из выбранных подарков больше нельзя улучшить. Обнови список подарков.")

    counter = await mtproto.counter_for_info(infos[0], peer=peer, cache_seconds=0)
    future_targets = sorted({target for target in store.settings.target_numbers if target > counter.current})
    if not future_targets:
        raise RuntimeError(f"Все номера выстрела уже прошли. Текущий номер: {counter.current}")

    if active_shooter_count() > 1:
        await cluster_runtime.verify_active_peers()
    operation_count = effective_volley_size()
    if len(infos) < operation_count:
        raise RuntimeError(
            f"Для залпа {operation_count} выбрано только {len(infos)} подарков"
        )
    planned_infos = infos[:operation_count]
    planned_targets = [future_targets[0]] * operation_count
    plans: list[PreparedUpgrade | None] = []
    for candidate in planned_infos:
        plan = scanner.prepared.get(candidate.saved_id)
        if prepare and (plan is None or time.monotonic() - plan.created_at > PREPARE_REFRESH_SECONDS):
            plan = await mtproto.prepare_upgrade(peer, candidate)
            scanner.prepared[candidate.saved_id] = plan
        plans.append(plan)

    costs: list[int] = []
    prepaid_count = 0
    for candidate, plan in zip(planned_infos, plans):
        is_prepaid = candidate.prepaid or (plan is not None and plan.prepaid)
        if is_prepaid:
            prepaid_count += 1
            costs.append(0)
        else:
            cost = plan.cost if plan is not None else candidate.upgrade_cost
            if cost <= 0:
                raise RuntimeError(
                    f"Telegram не вернул стоимость улучшения для экземпляра {candidate.saved_id}"
                )
            if MAX_UPGRADE_STARS and cost > MAX_UPGRADE_STARS:
                raise RuntimeError(
                    f"Цена улучшения {cost} ⭐ превышает лимит {MAX_UPGRADE_STARS} ⭐"
                )
            costs.append(cost)

    paid_total = sum(costs)
    if prepaid_count == operation_count:
        payment_text = "все операции предоплачены"
    elif prepaid_count:
        payment_text = f"{paid_total} ⭐ максимум + {prepaid_count} предоплач."
    else:
        payment_text = f"до {paid_total} ⭐ суммарно"
    limit_text = f"{MAX_UPGRADE_STARS} ⭐" if MAX_UPGRADE_STARS else "без лимита"
    warning = ""
    if operation_count > 1:
        warning = (
            f"\n⚠️ Залп {operation_count}: запросы уйдут с шагом "
            f"{effective_fast_volley_stagger_ms()} мс по одному номеру выстрела. "
            f"Максимальное списание — {paid_total} ⭐; часть подарков может получить следующие номера."
        )

    return (
        "⚠️ <b>Подтверждение LIVE</b>\n"
        f"Канал: <b>{html.escape(store.settings.channel_title or '—')}</b>\n"
        f"Подарок: <b>{html.escape(counter.title)}</b>\n"
        f"Выбранных экземпляров: <b>{len(infos)}</b>\n"
        f"Текущий номер: <b>{counter.current}</b>\n"
        f"Выстрел FAST-залпа: <b>#{future_targets[0]}</b>\n"
        f"Размер залпа: <b>{operation_count}</b>\n"
        f"Разница отправки FAST: <b>{effective_fast_volley_stagger_ms()} мс</b>\n"
        f"Возможное списание: <b>{payment_text}</b>\n"
        f"Лимит одной операции: <b>{limit_text}</b>"
        f"{warning}\n\n"
        "Платёжные формы и сами MTProto-запросы подготовлены заранее. После точного появления "
        "номера выстрела минус один FAST отправит залп без дополнительной проверки выстрела, без записи на диск "
        "и без автоматического повтора. Точный номер не гарантируется."
    )


@router.message(F.text.startswith(BTN_VOLLEY_PREFIX))
async def volley_toggle_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    if await reject_changes_while_running_message(message):
        return
    if store.settings.payment_hold_saved_ids:
        await message.answer(PENDING_PAYMENT_HOLD_MESSAGE, reply_markup=main_keyboard())
        return

    maximum = effective_max_volley_size()
    current = min(maximum, max(1, int(store.settings.volley_size)))
    store.settings.volley_size = 1 if current >= maximum else current + 1
    live_was_enabled = store.settings.live_upgrades
    if live_was_enabled:
        store.settings.live_upgrades = False
        scanner.prepared.clear()
    await store.save()
    logger.info(
        "volley_size_changed shooter_id=%s volley=%s volley_limit=%s live_disabled=%s",
        SHOOTER_ID, store.settings.volley_size, maximum, live_was_enabled,
    )
    record_cluster_event(
        "volley_size_changed",
        volley=int(store.settings.volley_size),
        volley_limit=int(maximum),
        live_disabled=bool(live_was_enabled),
    )
    cluster_runtime.notify_state_changed()
    text = (
        f"💥 Размер FAST-залпа Hunter {SHOOTER_ID}: <b>{store.settings.volley_size}</b> из <b>{maximum}</b>. "
        f"Нужно выбрать минимум {store.settings.volley_size} одинаковых неулучшенных подарков."
    )
    if live_was_enabled:
        text += "\n🛡 LIVE выключен: включи оплату заново, чтобы подтвердить новый максимальный расход."
    if store.settings.volley_size > 1:
        text += (
            f"\n⚠️ При полной стоимости одного улучшения возможное списание — "
            f"до {store.settings.volley_size} × цена подарка."
        )
    await message.answer(text, reply_markup=main_keyboard())


@router.message(F.text.in_({BTN_PAYMENT_OFF, BTN_PAYMENT_ON}))
async def payment_toggle_handler(message: Message) -> None:
    """The payment switch and scanner mode are the same setting.

    Pressing the OFF label performs the full preflight and immediately enables
    LIVE. The button press itself is the explicit financial confirmation.
    """
    if not await owner_guard_message(message):
        return
    if await reject_changes_while_running_message(message):
        return
    if store.settings.live_upgrades:
        store.settings.live_upgrades = False
        scanner.prepared.clear()
        await store.save()
        cluster_runtime.notify_state_changed()
        logger.info("live_disabled_by_toggle")
        await message.answer("🛡 Оплата выключена. Режим DRY-RUN.", reply_markup=main_keyboard())
        return
    try:
        summary = await live_preflight(prepare=True)
        store.settings.live_upgrades = True
        await store.save()
        cluster_runtime.notify_state_changed()
        logger.info("live_enabled_by_toggle")
        await message.answer(
            "💳 <b>Оплата включена — режим LIVE активирован.</b>\n"
            + summary.replace("⚠️ <b>Подтверждение LIVE</b>\n", ""),
            reply_markup=main_keyboard(),
        )
    except Exception as exc:
        store.settings.live_upgrades = False
        await store.save()
        cluster_runtime.notify_state_changed()
        logger.exception("live_preflight_failed")
        await message.answer(f"LIVE не включён: {html.escape(str(exc))}", reply_markup=main_keyboard())


@router.callback_query(F.data.startswith("payment:"))
async def payment_confirm_handler(callback: CallbackQuery) -> None:
    """Reject confirmation buttons left in chat by older versions.

    Gift Hunter v0039 uses the reply-keyboard payment switch itself as confirmation, so a
    stale inline button must never change the current LIVE state.
    """
    if not await owner_guard_callback(callback):
        return
    await callback.answer(
        "Эта кнопка устарела. Используй тумблер «Оплата» в нижней клавиатуре.",
        show_alert=True,
    )


async def stress_status_text() -> str:
    elapsed = runtime.stress_elapsed_s
    remaining = max(0, int(STRESS_TEST_DURATION_SECONDS - elapsed))
    lines = [
        f"🧪 <b>{APP_NAME} {APP_VERSION} · стресс-тест</b>",
        f"Статус: {'🟢 идёт' if runtime.stress_active else '⚪ завершён'}",
        f"Этап: <b>{html.escape(runtime.stress_phase or '—')}</b>",
        f"Прошло: <b>{int(elapsed)}с</b> · осталось: <b>{remaining}с</b>",
        f"Период: <b>{runtime.stress_interval_ms:.0f} мс</b>" if runtime.stress_interval_ms is not None else "Период: —",
        f"Проверок: <b>{runtime.stress_checks}</b> · успешных: <b>{runtime.stress_successes}</b>",
        f"Ошибок: <b>{runtime.stress_errors}</b> · FloodWait: <b>{runtime.stress_flood_count}</b>",
        f"Средний ответ: <b>{runtime.stress_avg_latency_ms:.1f} мс</b>" if runtime.stress_avg_latency_ms is not None else "Средний ответ: —",
        f"P95: <b>{runtime.stress_p95_latency_ms:.1f} мс</b>" if runtime.stress_p95_latency_ms is not None else "P95: —",
        f"Скорость сейчас: <b>{runtime.stress_current_rate_per_s:.1f} проверок/с</b>",
        f"Максимум: <b>{runtime.stress_max_rate_per_s:.1f} проверок/с</b>",
        "Оплата: <b>принудительно не используется</b>",
        f"Обновлено: <b>{msk_time_str()}</b>",
    ]
    if runtime.current_by_slug:
        for slug, current in runtime.current_by_slug.items():
            title = runtime.title_by_slug.get(slug, slug)
            lines.append(f"Текущий номер {html.escape(title)}: <b>{current}</b>")
    if runtime.stress_last_error:
        lines.append(f"Последняя ошибка: <code>{html.escape(runtime.stress_last_error[:300])}</code>")
    cooldown = rate_limit.remaining_seconds()
    if cooldown > 0:
        lines.append(f"MTProto cooldown: <b>{int(cooldown + 0.999)}с</b>")
    if runtime.stress_result:
        lines.append(f"Итог: <b>{html.escape(runtime.stress_result)}</b>")
    return "\n".join(lines)


@router.message(F.text == BTN_STRESS_OFF)
@router.message(F.text == BTN_STRESS_ON)
async def stress_toggle_handler(message: Message) -> None:
    """Single ON/OFF control for the five-minute read-only stress test."""
    if not await owner_guard_message(message):
        return

    requested_state = (message.text or "").strip()

    # The ON label always means "turn the running test off". A stale ON
    # keyboard must never start a new test after automatic completion.
    if requested_state == BTN_STRESS_ON:
        if runtime.stress_active:
            await stress_tester.stop("manual_toggle_off")
            await message.answer(
                "🧪 <b>Стресс-тест: ВЫКЛ.</b> Тест остановлен, результат записан в Log.",
                reply_markup=main_keyboard(),
            )
        else:
            await message.answer("🧪 Стресс-тест уже выключен.", reply_markup=main_keyboard())
        return

    # The OFF label means "turn the test on". Ignore a stale OFF press while
    # the test is already running instead of launching a second task.
    if runtime.stress_active:
        await message.answer("🧪 Стресс-тест уже включён.", reply_markup=main_keyboard())
        return

    if runtime.active:
        await message.answer("Сначала останови основной сканер.", reply_markup=main_keyboard())
        return

    try:
        await stress_tester.start(message.chat.id)
        sent = await message.answer(await stress_status_text(), reply_markup=main_keyboard())
        stress_tester.attach_status_message(sent.chat.id, sent.message_id)
    except Exception as exc:
        logger.exception("stress_test_start_failed")
        await message.answer(f"Не удалось запустить тест: {html.escape(str(exc))}", reply_markup=main_keyboard())


async def status_text() -> str:
    if runtime.stress_active:
        return await stress_status_text()
    # Avoid an extra MTProto authorization request from the live status editor.
    authorized = True if (runtime.active or runtime.stress_active) else await mtproto.is_authorized()
    uptime = int(time.monotonic() - runtime.started_at) if runtime.started_at else 0
    lines = []
    if runtime.fast_quiet:
        lines.append("🔕 <b>Бот в тихом режиме</b>")
    lines.extend([
        f"🎯 <b>{APP_NAME} {APP_VERSION}</b>",
        f"Сканер: {'🟢 активен' if runtime.active else '⚪ остановлен'}",
        f"Режим: {'LIVE' if store.settings.live_upgrades else 'DRY-RUN'}",
        f"FAST-залп: {effective_volley_size()}",
        f"MTProto: {'подключён' if authorized else 'не подключён'}",
        f"Канал: {html.escape(store.settings.channel_title or 'не выбран')}",
        f"Проверок: {runtime.checks}",
        f"Последний цикл: {runtime.last_cycle_ms:.0f} мс" if runtime.last_cycle_ms is not None else "Последний цикл: —",
        f"Адаптивный период: {runtime.adaptive_interval_ms:.0f} мс" if runtime.adaptive_interval_ms is not None else "Адаптивный период: —",
        f"Фактический шаг: {runtime.poll_gap_ms:.0f} мс" if runtime.poll_gap_ms is not None else "Фактический шаг: —",
        f"FloodWait: {runtime.flood_count}" + (f" · последний {runtime.last_flood_wait_s}с" if runtime.last_flood_wait_s is not None else ""),
        f"Cooldown: {runtime.rate_cooldown_cycles} циклов" if runtime.rate_cooldown_cycles else "Cooldown: нет",
        (
            f"FAST-формы: старшая {runtime.payment_form_oldest_age_s:.0f}с · обновлений {runtime.payment_form_refresh_count}"
            if store.settings.live_upgrades and runtime.payment_form_oldest_age_s is not None
            else (f"FAST-формы: обновлений {runtime.payment_form_refresh_count}" if store.settings.live_upgrades else "FAST-формы: —")
        ),
        f"Uptime сканера: {uptime}с" if runtime.started_at else "Uptime сканера: —",
        f"Обновлено: <b>{msk_time_str()}</b>",
    ])
    if store.settings.live_upgrades and runtime.payment_form_last_refresh_error:
        lines.append(
            "⚠️ Refresh FAST-форм: <code>"
            + html.escape(runtime.payment_form_last_refresh_error[:300])
            + "</code>"
        )
    if runtime.fast_trigger_to_submit_ms is not None:
        lines.append(
            f"⚡ Последняя реакция: <b>{runtime.fast_trigger_to_submit_ms:.3f} мс</b> · "
            f"создание задач {runtime.fast_task_launch_ms or 0.0:.3f} мс"
        )
    cooldown = rate_limit.remaining_seconds()
    if cooldown > 0:
        lines.append(f"⏳ MTProto cooldown: <b>{int(cooldown + 0.999)}с</b>")
    if store.settings.payment_hold_saved_ids:
        hold_details = []
        for saved_id in store.settings.payment_hold_saved_ids:
            attempted = _positive_int_or_none(store.settings.payment_hold_targets.get(str(saved_id)))
            hold_details.append(f"{saved_id}→#{attempted}" if attempted else str(saved_id))
        lines.append(
            f"⚠️ Неподтверждённых платежей: <b>{len(store.settings.payment_hold_saved_ids)}</b> · "
            "повторная оплата и ручное удаление заблокированы"
        )
        lines.append("Ожидают сверки: <code>" + html.escape(", ".join(hold_details)) + "</code>")
        if store.settings.payment_hold_reason:
            lines.append(
                "Причина блокировки: <code>"
                + html.escape(store.settings.payment_hold_reason[:300])
                + "</code>"
            )
    if runtime.current_by_slug:
        lines.append("")
        for slug, current in runtime.current_by_slug.items():
            title = runtime.title_by_slug.get(slug, slug)
            target = next_target(store.settings.target_numbers, current)
            if target is None and store.settings.target_numbers:
                target = max(store.settings.target_numbers)
            lines.append(f"• {html.escape(title)}: текущий <b>{current}</b> · выстрел <b>{target or '—'}</b>")
    elif store.settings.target_numbers:
        lines.append("Выстрел: " + ", ".join(map(str, store.settings.target_numbers)))
    if runtime.last_error:
        lines.append(f"Ошибка: <code>{html.escape(runtime.last_error[:300])}</code>")
    if runtime.last_success:
        lines.append(f"Последний успех: {html.escape(runtime.last_success)}")
    verification_url = runtime.pending_verification_url or store.settings.payment_verification_url
    if verification_url:
        lines.append(f"Подтверждение: <code>{html.escape(verification_url)}</code>")
    return "\n".join(lines)


@router.message(F.text == BTN_REFRESH_CARD)
async def refresh_card_handler(message: Message) -> None:
    """Issue a fresh live card while leaving the scanner completely untouched."""
    if not await owner_guard_message(message):
        return
    if runtime.stress_active:
        await message.answer(
            "Сначала останови стресс-тест: кнопка перевыпуска относится к основной карточке сканера.",
            reply_markup=main_keyboard(),
        )
        return
    if not runtime.active:
        await message.answer(
            "Сканер остановлен. Новая живая карточка появится после запуска.",
            reply_markup=main_keyboard(),
        )
        return
    try:
        await scanner.manually_replace_status_message(message.chat.id)
    except TelegramRetryAfter as exc:
        # SendMessage itself is rate-limited, so do not answer with yet another
        # SendMessage and do not schedule retries. Ping will show the remainder.
        logger.warning(
            "manual_status_refresh_rate_limited retry_after_s=%s error=%s",
            getattr(exc, "retry_after", None),
            exc,
        )
    except Exception as exc:
        logger.warning("manual_status_refresh_failed error=%s", exc)
        if getattr(exc, "retry_after", None) is None:
            await message.answer(
                f"Не удалось обновить карточку: {html.escape(str(exc))}",
                reply_markup=main_keyboard(),
            )


@router.message(F.text == BTN_PING)
async def ping_handler(message: Message) -> None:
    """Return local health and, in Hunter 1, an on-demand UDP RTT check."""
    if not await owner_guard_message(message):
        return

    local_started = time.perf_counter()
    if runtime.active or runtime.stress_active:
        mtproto_note = "подключён"
    else:
        try:
            authorized = await mtproto.is_authorized()
            mtproto_note = "подключён" if authorized else "нет авторизации"
        except Exception as exc:
            mtproto_note = "ошибка подключения"
            logger.warning("ping_mtproto_check_failed shooter_id=%s error=%s", SHOOTER_ID, exc)
    memory_current = current_memory_bytes()
    memory_limit = memory_limit_bytes()
    process_uptime_seconds = max(0.0, time.monotonic() - PROCESS_STARTED_MONOTONIC)
    local_handler_ms = (time.perf_counter() - local_started) * 1000

    logger.info(
        "ping_started shooter_id=%s primary=%s memory_current_mb=%.1f memory_limit_mb=%s process_uptime_sec=%s scanner_checks=%s last_cycle_ms=%s",
        SHOOTER_ID,
        PRIMARY_SHOOTER,
        memory_current / (1024 * 1024),
        None if memory_limit is None else round(memory_limit / (1024 * 1024), 1),
        int(process_uptime_seconds),
        runtime.checks,
        runtime.last_cycle_ms,
    )

    lines = [
        "🏓 <b>PONG</b>",
        f"Версия: <b>{APP_NAME} {APP_VERSION}</b>",
        "Бот: <b>работает</b>",
        f"MTProto: <b>{mtproto_note}</b>",
        f"Сканер: <b>{'активен' if runtime.active else 'остановлен'}</b>",
        (
            f"RAM: <b>{format_memory_mb(memory_current)} / {format_memory_mb(memory_limit)}</b>"
            if memory_limit is not None
            else f"RAM: <b>{format_memory_mb(memory_current)} / без лимита</b>"
        ),
        f"Время работы: <b>{format_process_uptime(process_uptime_seconds)}</b>",
        f"Обработка Ping: <b>{local_handler_ms:.1f} мс</b>",
    ]

    ping_results: dict[int, float | None] = {}
    if PRIMARY_SHOOTER:
        config = cluster_runtime.applied_config or provision_store.load_config()
        logger.info(
            "ping_check_started active_shooters=%s timeout_ms=%s",
            config.active_shooters,
            CLUSTER_PING_TIMEOUT_MS,
        )
        record_cluster_event(
            "ping_check_started",
            active_shooters=config.active_shooters,
            timeout_ms=CLUSTER_PING_TIMEOUT_MS,
        )
        cluster_ping_started = time.perf_counter()
        if cluster_runtime.bus is not None:
            try:
                ping_results = await cluster_runtime.bus.ping_active_shooters(
                    timeout_seconds=CLUSTER_PING_TIMEOUT_MS / 1000.0
                )
            except Exception as exc:
                logger.exception("ping_check_failed error=%s", exc)
                ping_results = {peer_id: None for peer_id in range(1, config.active_shooters + 1)}
        else:
            ping_results = {peer_id: None for peer_id in range(1, config.active_shooters + 1)}

        cluster_ping_elapsed_ms = (time.perf_counter() - cluster_ping_started) * 1000
        lines.extend(["", "👥 <b>Стрелки:</b>"])
        answered = 0
        for peer_id in range(1, config.active_shooters + 1):
            rtt_ms = ping_results.get(peer_id)
            local_suffix = " (локальный)" if peer_id == SHOOTER_ID else ""
            if rtt_ms is None:
                lines.append(f"Hunter {peer_id} — 🔴 нет ответа{local_suffix}")
                logger.warning("peer_timeout shooter_id=%s timeout_ms=%s", peer_id, CLUSTER_PING_TIMEOUT_MS)
                record_cluster_event(
                    "peer_timeout",
                    participant_id=peer_id,
                    timeout_ms=CLUSTER_PING_TIMEOUT_MS,
                    local=peer_id == SHOOTER_ID,
                )
            else:
                answered += 1
                lines.append(f"Hunter {peer_id} — 🟢 UDP {rtt_ms:.2f} мс{local_suffix}")
                logger.info("peer_pong shooter_id=%s rtt_ms=%.3f local=%s", peer_id, rtt_ms, peer_id == SHOOTER_ID)
                record_cluster_event(
                    "peer_pong",
                    participant_id=peer_id,
                    rtt_ms=round(rtt_ms, 3),
                    local=peer_id == SHOOTER_ID,
                )
        lines.extend(["", f"Ответили: <b>{answered}/{config.active_shooters}</b>"])
        logger.info(
            "ping_check_completed active_shooters=%s answered=%s timed_out=%s elapsed_ms=%.3f",
            config.active_shooters,
            answered,
            config.active_shooters - answered,
            cluster_ping_elapsed_ms,
        )
        record_cluster_event(
            "ping_check_completed",
            active_shooters=config.active_shooters,
            answered=answered,
            timed_out=config.active_shooters - answered,
            elapsed_ms=round(cluster_ping_elapsed_ms, 3),
        )
        cluster_runtime.notify_state_changed()

    logger.info(
        "ping_completed shooter_id=%s local_processing_ms=%.3f cluster_checked=%s",
        SHOOTER_ID,
        local_handler_ms,
        PRIMARY_SHOOTER,
    )
    await message.answer("\n".join(lines), reply_markup=main_keyboard())


def settings_summary_text() -> str:
    return (
        f"⚙️ <b>Настройки {APP_NAME} {APP_VERSION}</b>\n"
        f"FAST-залп: <b>{effective_volley_size()}</b>\n"
        f"Разница отправки платежей: <b>{effective_fast_volley_stagger_ms()} мс</b>\n"
        f"Тихий режим: <b>{FAST_QUIET_DISTANCE} номеров</b> до цели\n"
        f"FAST-формы: <b>5 мин</b> далеко / <b>2 мин</b> в зоне ≤50\n\n"
        f"Изменить задержку: <code>/stagger 10</code>\n"
        f"Допустимо: <b>0–{MAX_FAST_VOLLEY_STAGGER_MS} мс</b>. "
        "0 мс = одновременная постановка запросов. Изменение доступно только при остановленном сканере."
    )


@router.message(F.text == BTN_SETTINGS)
@router.message(Command("settings"))
async def settings_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    await message.answer(settings_summary_text(), reply_markup=main_keyboard())


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    await message.answer(
        f"📖 <b>{APP_NAME} {APP_VERSION} — команды</b>\n"
        f"<code>/stagger 10</code> — разница между стартом соседних FAST-платежей в миллисекундах; "
        f"по умолчанию {DEFAULT_FAST_VOLLEY_STAGGER_MS} мс, диапазон 0–{MAX_FAST_VOLLEY_STAGGER_MS}.\n"
        "<code>/stagger</code> — показать текущее значение.\n"
        "<code>/settings</code> — показать рабочие настройки.\n"
        "<code>/log_full</code> — выгрузить полный лог за последние 24 часа.\n"
        "Кнопка <b>🗑 Сброс</b> — полный рабочий сброс: удаляет payment hold/guard, cooldown, цели, подарки, LIVE и временное состояние; авторизация и MTProto-session сохраняются.\n"
        "<code>/version</code> — показать версию.\n\n"
        "Задержка FAST меняется только при остановленном сканере. При изменении включённый LIVE автоматически выключается, "
        "чтобы новый режим был подтверждён повторным включением оплаты.",
        reply_markup=main_keyboard(),
    )


@router.message(Command("stagger"))
async def stagger_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    if await reject_changes_while_running_message(message):
        return
    if store.settings.payment_hold_saved_ids:
        await message.answer(PENDING_PAYMENT_HOLD_MESSAGE, reply_markup=main_keyboard())
        return

    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) == 1:
        await message.answer(
            f"⏱ Разница отправки FAST сейчас: <b>{effective_fast_volley_stagger_ms()} мс</b>.\n"
            f"Изменить: <code>/stagger 10</code> (0–{MAX_FAST_VOLLEY_STAGGER_MS} мс).",
            reply_markup=main_keyboard(),
        )
        return

    raw = parts[1].strip()
    try:
        value = int(raw)
    except ValueError:
        await message.answer(
            f"Нужно целое число миллисекунд от 0 до {MAX_FAST_VOLLEY_STAGGER_MS}. "
            "Пример: <code>/stagger 10</code>.",
            reply_markup=main_keyboard(),
        )
        return
    if not 0 <= value <= MAX_FAST_VOLLEY_STAGGER_MS:
        await message.answer(
            f"Допустимый диапазон: 0–{MAX_FAST_VOLLEY_STAGGER_MS} мс.",
            reply_markup=main_keyboard(),
        )
        return

    live_was_enabled = bool(store.settings.live_upgrades)
    store.settings.fast_volley_stagger_ms = value
    if live_was_enabled:
        store.settings.live_upgrades = False
        scanner.prepared.clear()
    await store.save()
    cluster_runtime.notify_state_changed()
    logger.info(
        "fast_volley_stagger_changed shooter_id=%s stagger_ms=%s live_disabled=%s",
        SHOOTER_ID, value, live_was_enabled,
    )
    record_payment_event(
        "fast_volley_stagger_changed",
        stagger_ms=value,
        live_disabled=live_was_enabled,
    )
    suffix = "\n🛡 LIVE выключен — включи оплату заново." if live_was_enabled else ""
    await message.answer(
        f"✅ Разница отправки FAST установлена: <b>{value} мс</b>.{suffix}",
        reply_markup=main_keyboard(),
    )


@router.message(F.text == BTN_LOG)
async def log_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    try:
        lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()[-60:]
    except FileNotFoundError:
        lines = ["Лог пока пуст."]
    if STRESS_REPORT_PATH.exists():
        try:
            report = json.loads(STRESS_REPORT_PATH.read_text(encoding="utf-8"))
            lines.extend([
                "",
                "--- LAST STRESS TEST ---",
                json.dumps(report, ensure_ascii=False, separators=(",", ":")),
            ])
        except (OSError, json.JSONDecodeError):
            pass
    text = "\n".join(lines)
    if len(text) > 3500:
        text = text[-3500:]
    await message.answer(f"<pre>{html.escape(text)}</pre>")


@router.message(F.text == BTN_RESET)
async def reset_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    await message.answer(
        "🗑 <b>ПОЛНЫЙ рабочий сброс</b> удалит канал, выбранные подарки, цели, LIVE, "
        "payment hold/guard, cooldown, resume-marker, кеши и временное состояние.\n\n"
        "⚠️ Даже неподтверждённый payment hold будет удалён. Используй это как аварийный "
        "сброс только когда понимаешь, что старый платёж не надо повторять.\n\n"
        "Сохранятся только авторизация владельца бота, TG_API_ID/TG_API_HASH/телефон, "
        "MTProto *.session и исторические логи.",
        reply_markup=reset_confirm_keyboard(),
    )


@router.callback_query(F.data.startswith("reset:"))
async def reset_confirm_handler(callback: CallbackQuery, state: FSMContext) -> None:
    if not await owner_guard_callback(callback):
        return
    action = (callback.data or "").split(":", 1)[1]
    if action == "yes":
        await scanner.stop("reset")
        await stress_tester.stop("reset")
        # Explicit emergency reset is the one place allowed to discard payment
        # holds/guards. Authorization/session files are never touched.
        await asyncio.to_thread(clear_full_operational_files)
        rate_limit.reset()
        await store.reset_operational()

        # Clear only operational memory.  The connected TelegramClient, its
        # authorization flag and every *.session database remain untouched.
        await state.clear()
        mtproto.clear_operational_cache()
        scanner.prepared.clear()
        scanner.triggered.clear()
        scanner.notified_missed.clear()
        scanner._groups.clear()
        scanner._counter_meta.clear()
        scanner._campaign_ids_by_slug.clear()
        scanner._slug_by_campaign_id.clear()
        scanner._payment_guard_keys.clear()
        scanner._payment_guard_id = None
        scanner._reset_critical_form_state()
        scanner._form_refresh_retry_after.clear()
        scanner._fast_fired = False
        scanner._fast_client = None
        scanner._fast_peer = None
        scanner._plan_dirty = True
        scanner.status_chat_id = None
        scanner.status_message_id = None
        scanner.status_updater.clear()
        stress_tester.status_chat_id = None
        stress_tester.status_message_id = None
        stress_tester.status_updater.clear()

        fresh_runtime = RuntimeState()
        runtime.__dict__.clear()
        runtime.__dict__.update(fresh_runtime.__dict__)
        logger.info(
            "full_operational_reset_complete authorization_preserved=true owner_bound=%s mtproto_configured=%s",
            store.settings.owner_user_id is not None,
            mtproto.configured(),
        )
        await write_diagnostics()
        await callback.answer("Сброшено; авторизация сохранена")
        if callback.message:
            await callback.message.answer(
                "🗑 Полный рабочий сброс выполнен. Payment hold/guard, cooldown и временное состояние очищены. Авторизация бота и Telegram-сессия сохранены.\n"
                "Сначала выбери канал, затем подарок и номер выстрела.",
                reply_markup=main_keyboard(),
            )
    else:
        await callback.answer("Отменено")


@router.message(Command("log_full"))
async def log_full_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    if log_full_lock.locked():
        await message.answer("⏳ /log_full уже формируется. Дождись завершения текущего экспорта.")
        return
    export_paths: list[Path] = []
    export_now = time.time()
    cutoff_epoch = export_now - LOG_FULL_WINDOW_SECONDS
    async with log_full_lock:
        try:
            export_paths = await asyncio.to_thread(build_full_log_exports, now_epoch=export_now)
            total = len(export_paths)
            for index, export_path in enumerate(export_paths, start=1):
                document = FSInputFile(export_path, filename=export_path.name)
                suffix = f" · часть {index}/{total}" if total > 1 else ""
                await message.answer_document(
                    document,
                    caption=f"📄 Лог за последние 24 часа · {APP_NAME} {APP_VERSION}{suffix}",
                )
            pruned, deleted = await asyncio.to_thread(
                prune_local_log_history_after_export,
                cutoff_epoch,
            )
            logger.info(
                "log_full_sent_and_old_history_pruned parts=%s pruned_files=%s deleted_files=%s window_hours=24",
                total,
                pruned,
                deleted,
            )
        except Exception as exc:
            logger.exception("log_full_failed")
            await message.answer(
                "❌ Не удалось полностью отправить лог. Старые локальные логи не очищены: "
                f"{html.escape(str(exc))}"
            )
        finally:
            for export_path in export_paths:
                with contextlib.suppress(OSError):
                    export_path.unlink()


@router.message(Command("version"))
async def version_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    await message.answer(f"{APP_NAME} {APP_VERSION}")


@router.message()
async def gift_name_lookup_handler(message: Message, state: FSMContext) -> None:
    if not await owner_guard_message(message):
        return
    if await state.get_state() is not None:
        return
    query = (message.text or "").strip()
    if not query or query.startswith("/"):
        return
    if runtime.active:
        await message.answer("Сканер активен. Для поиска другого подарка сначала останови его — так основной запрос не теряет скорость.")
        return
    if runtime.stress_active:
        await message.answer("Идёт стресс-тест. Сначала выключи тумблер, чтобы не искажать результат дополнительным MTProto-запросом.")
        return
    try:
        counter = await mtproto.resolve_slug(query, cache_seconds=0)
        if counter is None:
            await message.answer(
                "Не нашёл такой collectible slug. Пример корректного запроса: <code>DurovsGlasses</code> "
                "или ссылка <code>https://t.me/nft/DurovsGlasses-1</code>."
            )
            return

        # If this name corresponds to one selected regular gift, persist the binding.
        selected_ids = store.settings.selected_saved_ids
        if selected_ids and counter.base_gift_id:
            peer = await mtproto.resolve_channel()
            selected = await mtproto.fetch_saved_by_ids(peer, selected_ids)
            if any(_int_or_none(getattr(getattr(item, "gift", None), "id", None)) == counter.base_gift_id for item in selected.values()):
                store.settings.slug_map[str(counter.base_gift_id)] = counter.slug
                await store.save()

        total = f" из {counter.total}" if counter.total else ""
        await message.answer(
            f"🎁 <b>{html.escape(counter.title)}</b>\n"
            f"Slug: <code>{html.escape(counter.slug)}</code>\n"
            f"Последний актуальный номер: <b>{counter.current}</b>{html.escape(total)}"
        )
    except Exception as exc:
        logger.exception("gift_name_lookup_failed query=%s", query)
        await message.answer(f"❌ {html.escape(str(exc))}")


def _watchdog_worker() -> None:
    """Exit the process when the asyncio loop stops advancing.

    Docker's restart policy then starts bootstrap/main again. The thread is
    intentionally independent from asyncio, so a blocked event loop cannot
    keep reporting a healthy process forever.
    """
    global _EVENT_LOOP_LAST_TICK
    emergency_path = DATA_DIR / f"gift-hunter-{APP_VERSION}-watchdog.log"
    while not _WATCHDOG_STOP.wait(WATCHDOG_CHECK_SECONDS):
        age = time.monotonic() - _EVENT_LOOP_LAST_TICK
        if age <= WATCHDOG_STALL_SECONDS:
            continue
        line = (
            f"{time.strftime('%Y-%m-%d %H:%M:%S%z')} watchdog_stall "
            f"version={APP_VERSION} shooter_id={SHOOTER_ID} age_seconds={age:.3f}\n"
        )
        try:
            fd = os.open(
                emergency_path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o600,
            )
            try:
                os.write(fd, line.encode("utf-8", errors="replace"))
                os.write(fd, b"watchdog_stack_dump_begin\n")
                # Dump every Python thread before the hard restart. This gives
                # /log_full a concrete blocking stack if the event loop stalls
                # again instead of only reporting the 90-second symptom.
                with os.fdopen(os.dup(fd), "a", encoding="utf-8", closefd=True) as stream:
                    faulthandler.dump_traceback(file=stream, all_threads=True)
                    stream.flush()
                os.write(fd, b"watchdog_stack_dump_end\n")
            finally:
                os.close(fd)
        except (OSError, RuntimeError):
            pass
        try:
            config = cluster_runtime.applied_config or provision_store.load_config()
            safe_save_lifecycle(
                SHOOTER_ID,
                state="error",
                generation=config.generation,
                active_shooters=config.active_shooters,
                reason="event_loop_watchdog_timeout",
                version=APP_VERSION,
                pid=os.getpid(),
                stalled_seconds=round(age, 3),
            )
            provision_store.append_event(
                "watchdog_stall",
                shooter_id=SHOOTER_ID,
                version=APP_VERSION,
                stalled_seconds=round(age, 3),
            )
        except Exception:
            pass
        try:
            os.write(2, line.encode("utf-8", errors="replace"))
        except OSError:
            pass
        os._exit(70)


async def event_loop_tick_loop() -> None:
    global _EVENT_LOOP_LAST_TICK
    while True:
        _EVENT_LOOP_LAST_TICK = time.monotonic()
        await asyncio.sleep(1.0)


def _write_heartbeat_payload(payload: dict[str, Any]) -> None:
    temp = HEARTBEAT_PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(payload), encoding="utf-8")
    temp.replace(HEARTBEAT_PATH)


async def heartbeat_loop() -> None:
    while True:
        config = cluster_runtime.applied_config or ClusterConfig()
        payload = {
            "timestamp": time.time(),
            "version": APP_VERSION,
            "shooter_id": SHOOTER_ID,
            "active_shooters": config.active_shooters,
            "generation": config.generation,
            "scanner_active": runtime.active,
            "stress_test_active": runtime.stress_active,
            "rate_limit_remaining_seconds": rate_limit.remaining_seconds(),
            "rate_limit_blocked_until": rate_limit.blocked_until or None,
        }
        try:
            await asyncio.to_thread(_write_heartbeat_payload, payload)
        except OSError as exc:
            # Keep the task alive so a transient filesystem error cannot silently
            # kill heartbeat updates forever. A persistent error still makes the
            # Docker healthcheck stale and triggers a restart.
            logger.error("heartbeat_write_failed error=%s", exc)
        await asyncio.sleep(10)


async def write_diagnostics() -> None:
    config = cluster_runtime.applied_config or ClusterConfig()
    bus = cluster_runtime.bus
    now_monotonic = time.monotonic()
    remote_statuses = {
        str(shooter_id): {
            **status.payload(),
            "ready": status.ready,
            "age_seconds": max(0.0, now_monotonic - status.updated_monotonic),
        }
        for shooter_id, status in sorted(cluster_runtime.remote_statuses.items())
    }
    payload = {
        "version": APP_VERSION,
        "shooter_id": SHOOTER_ID,
        "active_shooters": active_shooter_count(),
        "cluster_udp_ready": bool(bus and bus.ready),
        "cluster_signal_ready": (
            bool(getattr(bus, "peers_ready", getattr(bus, "fully_connected", False)))
            if bus is not None else config.active_shooters == 1
        ),
        "cluster": {
            "configured": config.configured,
            "generation": config.generation,
            "active_shooters": config.active_shooters,
            "configured_token_ids": (
                await asyncio.to_thread(provision_store.configured_token_ids)
                if PRIMARY_SHOOTER else []
            ),
            "resolved_peer_ids": sorted(bus.resolved_peer_ids) if bus is not None else [],
            "expected_peer_ids": sorted(bus.expected_peer_ids) if bus is not None else [],
            "fully_connected": bool(bus and bus.fully_connected),
            "live_peer_ids": sorted(getattr(bus, "live_peer_ids", set())) if bus is not None else [],
            "peers_ready": bool(getattr(bus, "peers_ready", getattr(bus, "fully_connected", False))),
            "last_ping_age_seconds": (
                max(0.0, now_monotonic - float(getattr(bus, "last_ping_monotonic", 0.0)))
                if bus is not None and float(getattr(bus, "last_ping_monotonic", 0.0)) else None
            ),
            "remote_statuses": remote_statuses,
            "last_local_status": (
                {
                    **cluster_runtime.last_local_status.payload(),
                    "ready": cluster_runtime.last_local_status.ready,
                }
                if cluster_runtime.last_local_status is not None
                else None
            ),
        },
        "live_upgrades": store.settings.live_upgrades,
        "scanner_resume_marker_present": SCANNER_RESUME_PATH.exists(),
        "payment_guard_present": PAYMENT_GUARD_PATH.exists(),
        "max_upgrade_stars": MAX_UPGRADE_STARS,
        "fast_quiet_distance": FAST_QUIET_DISTANCE,
        "fast_disable_gc": FAST_DISABLE_GC,
        "adaptive_scan": ADAPTIVE_SCAN,
        "scan_start_interval_ms": SCAN_START_INTERVAL_MS,
        "scan_min_interval_ms": SCAN_MIN_INTERVAL_MS,
        "scan_max_interval_ms": SCAN_MAX_INTERVAL_MS,
        "scan_accelerate_every": SCAN_ACCELERATE_EVERY,
        "scan_accelerate_factor": SCAN_ACCELERATE_FACTOR,
        "scan_backoff_factor": SCAN_BACKOFF_FACTOR,
        "verify_delays_seconds": VERIFY_DELAYS_SECONDS,
        "stress_test_profile": {
            "duration_seconds": STRESS_TEST_DURATION_SECONDS,
            "phase_1": {"seconds": 60, "interval_ms": 300},
            "phase_2": {"seconds": 60, "interval_ms": 120},
            "phase_3": {"seconds": 180, "interval_ms": 0},
        },
        "rate_limit": {
            "remaining_seconds": rate_limit.remaining_seconds(),
            "blocked_until": rate_limit.blocked_until or None,
            "source": rate_limit.source,
        },
        "payment_audit": payment_audit_health.snapshot(),
        "runtime": asdict(runtime),
        "settings": {
            "owner_user_id": store.settings.owner_user_id,
            "channel_id": store.settings.channel_id,
            "channel_title": store.settings.channel_title,
            "selected_saved_ids": store.settings.selected_saved_ids,
            "target_numbers": store.settings.target_numbers,
            "volley_size": store.settings.volley_size,
            "fast_volley_stagger_ms": effective_fast_volley_stagger_ms(),
            "api_id_present": bool(store.settings.api_id),
            "api_hash_present": bool(store.settings.api_hash),
            "phone_present": bool(store.settings.phone),
            "slug_map": store.settings.slug_map,
            "payment_hold_saved_ids": store.settings.payment_hold_saved_ids,
            "payment_hold_targets": store.settings.payment_hold_targets,
            "payment_hold_reason": store.settings.payment_hold_reason,
            "payment_verification_url_present": bool(store.settings.payment_verification_url),
        },
    }
    try:
        temp = DIAGNOSTICS_PATH.with_suffix(".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(DIAGNOSTICS_PATH)
    except OSError as exc:
        logger.debug("diagnostics_write_failed error=%s", exc)


async def _auto_resume_scanner_after_crash(bot: Bot) -> bool:
    marker = await asyncio.to_thread(_load_scanner_resume_marker)
    if marker is None:
        return False

    # A SUBMITTED/legacy payment guard intentionally disables LIVE during module
    # startup. Never auto-resume across an ambiguous financial result.
    if store.settings.payment_hold_saved_ids or not store.settings.live_upgrades:
        logger.warning(
            "scanner_auto_resume_skipped live=%s holds=%s",
            store.settings.live_upgrades,
            store.settings.payment_hold_saved_ids,
        )
        await asyncio.to_thread(_clear_scanner_resume_marker)
        return False
    if not store.settings.selected_saved_ids or not store.settings.target_numbers:
        logger.warning("scanner_auto_resume_skipped reason=incomplete_settings")
        await asyncio.to_thread(_clear_scanner_resume_marker)
        return False

    try:
        await scanner.start()
    except Exception as exc:
        logger.exception("scanner_auto_resume_failed")
        await asyncio.to_thread(_clear_scanner_resume_marker)
        owner = store.settings.owner_user_id
        if owner is not None:
            with contextlib.suppress(Exception):
                await bot.send_message(
                    owner,
                    "⚠️ После аварийного рестарта не удалось автоматически восстановить "
                    f"сканер: {html.escape(str(exc))}",
                )
        return False

    logger.warning(
        "scanner_auto_resumed_after_crash targets=%s saved_ids=%s live=%s",
        store.settings.target_numbers,
        store.settings.selected_saved_ids,
        store.settings.live_upgrades,
    )
    record_payment_event(
        "scanner_auto_resumed_after_crash",
        targets=list(store.settings.target_numbers),
        saved_ids=list(store.settings.selected_saved_ids),
        live=bool(store.settings.live_upgrades),
        volley=effective_volley_size(),
        stagger_ms=effective_fast_volley_stagger_ms(),
    )
    owner = store.settings.owner_user_id
    if owner is not None:
        with contextlib.suppress(Exception):
            await bot.send_message(
                owner,
                "♻️ <b>Gift Hunter восстановлен после аварийного рестарта.</b>\n"
                "Сканер и LIVE-план автоматически пересобраны, FAST-формы получены заново.\n"
                f"Цель: <b>{', '.join('#' + str(x) for x in store.settings.target_numbers)}</b>",
            )
    return True


async def run_bot() -> None:
    global _bot_instance, _dispatcher_instance, _EVENT_LOOP_LAST_TICK
    if not BOT_TOKEN:
        raise RuntimeError("Environment variable BOT_TOKEN is required")
    logger.info("application_start version=%s shooter_id=%s", APP_VERSION, SHOOTER_ID)
    await store.save()
    config = cluster_runtime.applied_config or provision_store.load_config()
    safe_save_lifecycle(
        SHOOTER_ID,
        state="starting",
        generation=config.generation,
        active_shooters=config.active_shooters,
        reason="application_start",
        version=APP_VERSION,
        pid=os.getpid(),
    )
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    _bot_instance = bot
    dispatcher = Dispatcher()
    _dispatcher_instance = dispatcher
    dispatcher.include_router(router)
    heartbeat_task: asyncio.Task[None] | None = None
    payment_audit_task: asyncio.Task[None] | None = None
    tick_task: asyncio.Task[None] | None = None
    cluster_started = False
    failed = False
    _WATCHDOG_STOP.clear()
    _EVENT_LOOP_LAST_TICK = time.monotonic()
    watchdog_thread = threading.Thread(
        target=_watchdog_worker,
        name=f"gift-hunter-watchdog-{SHOOTER_ID}",
        daemon=True,
    )
    watchdog_thread.start()
    try:
        tick_task = asyncio.create_task(event_loop_tick_loop(), name="event-loop-tick")
        heartbeat_task = asyncio.create_task(heartbeat_loop(), name="heartbeat")
        payment_audit_task = asyncio.create_task(
            payment_audit_health_loop(), name="payment-audit-health"
        )
        await cluster_runtime.start()
        cluster_started = True
        config = cluster_runtime.applied_config or provision_store.load_config()
        safe_save_lifecycle(
            SHOOTER_ID,
            state="active",
            generation=config.generation,
            active_shooters=config.active_shooters,
            reason="bot_and_udp_started",
            version=APP_VERSION,
            pid=os.getpid(),
        )
        await _auto_resume_scanner_after_crash(bot)
        await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())
    except Exception as exc:
        failed = True
        config = cluster_runtime.applied_config or provision_store.load_config()
        with contextlib.suppress(Exception):
            safe_save_lifecycle(
                SHOOTER_ID,
                state="error",
                generation=config.generation,
                active_shooters=config.active_shooters,
                reason=type(exc).__name__,
                version=APP_VERSION,
                pid=os.getpid(),
            )
            record_cluster_event(
                "application_failed",
                error_type=type(exc).__name__,
                generation=config.generation,
                active_shooters=config.active_shooters,
            )
        raise
    finally:
        _WATCHDOG_STOP.set()
        watchdog_thread.join(timeout=1.0)
        with contextlib.suppress(Exception):
            await scanner.stop("shutdown")
        with contextlib.suppress(Exception):
            await stress_tester.stop("shutdown")
        if cluster_started:
            with contextlib.suppress(Exception):
                await cluster_runtime.stop()
        for task in (heartbeat_task, payment_audit_task, tick_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        with contextlib.suppress(Exception):
            await mtproto.disconnect()
        with contextlib.suppress(Exception):
            await bot.session.close()
        if not cluster_runtime.deactivation_in_progress and not failed:
            config = cluster_runtime.applied_config or provision_store.load_config()
            with contextlib.suppress(Exception):
                safe_save_lifecycle(
                    SHOOTER_ID,
                    state="stopped",
                    generation=config.generation,
                    active_shooters=config.active_shooters,
                    reason="process_shutdown",
                    version=APP_VERSION,
                    pid=os.getpid(),
                )
        _dispatcher_instance = None
        _bot_instance = None


def run_auth() -> None:
    s = store.settings
    if not (s.api_id and s.api_hash and s.phone):
        raise RuntimeError("Сначала укажи TG_API_ID, TG_API_HASH и номер телефона через бота")
    session = MTProtoService.session_base()
    print(f"{APP_NAME} {APP_VERSION}: авторизация MTProto")
    print("Код и пароль 2FA вводятся только здесь. Символы пароля могут не отображаться.")
    client = TelegramClient(
        session,
        int(s.api_id),
        str(s.api_hash),
        device_model=f"Gift Hunter {SHOOTER_ID} VPS",
        system_version="Linux",
        app_version=APP_VERSION,
        lang_code="ru",
        system_lang_code="ru-RU",
    )
    client.start(phone=str(s.phone))
    me = client.get_me()
    print(f"Авторизация успешна: {utils.get_display_name(me)} (id={me.id})")
    client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"{APP_NAME} {APP_VERSION}")
    parser.add_argument("command", nargs="?", default="bot", choices=["bot", "auth", "version"])
    return parser.parse_args()


def main() -> None:
    apply_fast_cpu_affinity()
    args = parse_args()
    if args.command == "auth":
        run_auth()
        return
    if args.command == "version":
        print(f"{APP_NAME} {APP_VERSION}")
        return
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
