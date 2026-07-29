from __future__ import annotations

import argparse
import asyncio
import contextlib
import html
import inspect
import json
import logging
import os
import re
import resource
import signal
import statistics
import socket
import sys
import time
import zipfile
from dataclasses import asdict, dataclass, field
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
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


APP_VERSION = "v0011"
APP_NAME = "Gift Hunter"
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
SETTINGS_PATH = DATA_DIR / "settings.json"
HEARTBEAT_PATH = DATA_DIR / "heartbeat.json"
LOG_PATH = DATA_DIR / f"gift-hunter-{APP_VERSION}.log"
DIAGNOSTICS_PATH = DATA_DIR / "diagnostics.json"
STRESS_REPORT_PATH = DATA_DIR / "stress-test-latest.json"
STRESS_HISTORY_PATH = DATA_DIR / "stress-tests.jsonl"
RATE_LIMIT_PATH = DATA_DIR / "rate-limit.json"
PENDING_PAYMENT_HOLD_MESSAGE = (
    "Есть платёж с неподтверждённым результатом. Повторная оплата и изменение "
    "связанных настроек заблокированы до сверки с Telegram."
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
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
PREPARE_REFRESH_SECONDS = env_int("PREPARE_REFRESH_SECONDS", 420, minimum=60)
MAX_UPGRADE_STARS = env_int("MAX_UPGRADE_STARS", 2000, minimum=0)
DIAGNOSTICS_INTERVAL_SECONDS = env_float("DIAGNOSTICS_INTERVAL_SECONDS", 1.0, minimum=0.5)
VERIFY_DELAYS_SECONDS = (0.10, 0.25, 0.50, 1.0, 2.0, 3.0)
STOP_AFTER_SUCCESS = parse_bool(os.getenv("STOP_AFTER_SUCCESS", "false"), False)
KEEP_ORIGINAL_DETAILS = parse_bool(os.getenv("KEEP_ORIGINAL_DETAILS", "true"), True)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

STRESS_TEST_DURATION_SECONDS = 300.0
STRESS_FIRST_PHASE_SECONDS = 60.0
STRESS_MAX_PHASE_START_SECONDS = 120.0
STRESS_FIRST_INTERVAL_MS = 300.0
STRESS_SECOND_INTERVAL_MS = 120.0
STRESS_MAX_INTERVAL_MS = 0.0
STRESS_STATUS_INTERVAL_SECONDS = 10.0
TASK_STOP_TIMEOUT_SECONDS = env_float("TASK_STOP_TIMEOUT_SECONDS", 4.0, minimum=1.0)
CATALOG_CONCURRENCY = env_int("CATALOG_CONCURRENCY", 6, minimum=1)


DATA_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging() -> logging.Logger:
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    if not any(isinstance(handler, RotatingFileHandler) for handler in root.handlers):
        file_handler = RotatingFileHandler(
            LOG_PATH,
            maxBytes=2_000_000,
            backupCount=3,
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
    slug_map: dict[str, str] = field(default_factory=dict)
    payment_hold_saved_ids: list[int] = field(default_factory=list)
    payment_hold_targets: dict[str, int] = field(default_factory=dict)
    payment_hold_reason: str | None = None
    payment_verification_url: str | None = None


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
        )
        return settings

    async def save(self) -> None:
        async with self._lock:
            payload = asdict(self.settings)
            payload["version"] = APP_VERSION
            temp = self.path.with_suffix(".tmp")
            temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.chmod(temp, 0o600)
            temp.replace(self.path)
            os.chmod(self.path, 0o600)

    async def reset_operational(self) -> None:
        if self.settings.payment_hold_saved_ids:
            raise RuntimeError(PENDING_PAYMENT_HOLD_MESSAGE)
        self.settings.selected_saved_ids = []
        self.settings.legacy_selected_gift_ids = []
        self.settings.target_numbers = []
        self.settings.live_upgrades = False
        self.settings.payment_hold_targets = {}
        self.settings.payment_hold_reason = None
        self.settings.payment_verification_url = None
        await self.save()


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
    stress_max_rate_per_s: float = 0.0
    stress_result: str | None = None


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


@dataclass
class UpgradeOutcome:
    status: str
    actual_num: int | None = None
    actual_slug: str | None = None
    verification_url: str | None = None
    detail: str | None = None


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

    @staticmethod
    def session_base() -> str:
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

    def configured(self) -> bool:
        s = self.store.settings
        return bool(s.api_id and s.api_hash and s.phone)

    async def get_client(self, *, reload: bool = False) -> TelegramClient:
        if not self.configured():
            raise RuntimeError("MTProto не настроен: нужны TG_API_ID, TG_API_HASH и номер телефона")
        async with self._client_lock:
            if reload and self.client is not None:
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
                    device_model="Gift Hunter VPS",
                    system_version="Linux",
                    app_version=APP_VERSION,
                    lang_code="ru",
                    system_lang_code="ru-RU",
                    auto_reconnect=True,
                    request_retries=1,
                    connection_retries=5,
                    flood_sleep_threshold=0,
                )
            if not self.client.is_connected():
                await self.client.connect()
            return self.client

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
        await self.require_authorized()
        input_saved = self.input_saved(peer, info.saved_id)
        if info.prepaid:
            return PreparedUpgrade(info.saved_id, input_saved, None, None, 0, True, time.monotonic())

        invoice = construct(
            types.InputInvoiceStarGiftUpgrade,
            stargift=input_saved,
            keep_original_details=KEEP_ORIGINAL_DETAILS,
        )
        try:
            form_request = construct(functions.payments.GetPaymentFormRequest, invoice=invoice, theme_params=None)
            form = await self.call(form_request)
        except errors.RPCError as exc:
            if "NO_PAYMENT_NEEDED" in str(exc).upper():
                return PreparedUpgrade(info.saved_id, input_saved, None, None, 0, True, time.monotonic())
            raise
        cost = sum_invoice_amount(getattr(form, "invoice", None)) or info.upgrade_cost
        if cost <= 0:
            raise RuntimeError("Telegram не вернул положительную стоимость улучшения")
        if MAX_UPGRADE_STARS and cost > MAX_UPGRADE_STARS:
            raise RuntimeError(f"Цена улучшения {cost} ⭐ превышает лимит {MAX_UPGRADE_STARS} ⭐")
        form_id = _int_or_none(getattr(form, "form_id", None))
        if not form_id:
            raise RuntimeError("Telegram не вернул form_id для оплаты улучшения")
        return PreparedUpgrade(info.saved_id, input_saved, invoice, form_id, cost, False, time.monotonic())

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

    async def execute_upgrade(self, peer: Any, info: SavedGiftInfo, prepared: PreparedUpgrade | None) -> UpgradeOutcome:
        await self.require_authorized()
        plan = prepared
        if plan is None or plan.saved_id != info.saved_id or time.monotonic() - plan.created_at > 540:
            plan = await self.prepare_upgrade(peer, info)

        async def submit(current: PreparedUpgrade) -> Any:
            if current.prepaid:
                request = construct(
                    functions.payments.UpgradeStarGiftRequest,
                    stargift=current.input_saved,
                    keep_original_details=KEEP_ORIGINAL_DETAILS,
                )
            else:
                request = construct(
                    functions.payments.SendStarsFormRequest,
                    form_id=int(current.form_id),
                    invoice=current.invoice,
                )
            return await self.call(request)

        async def handle_submit_error(exc: BaseException, *, retry: bool = False) -> UpgradeOutcome:
            if isinstance(exc, errors.FloodWaitError):
                raise exc
            code = self._rpc_code(exc)
            if code in {"FORM_SUBMIT_DUPLICATE", "STARGIFT_ALREADY_UPGRADED"}:
                actual_num, actual_slug = await self._verify_unique(peer, info.saved_id)
                if actual_num is not None:
                    return UpgradeOutcome("confirmed", actual_num, actual_slug)
                return UpgradeOutcome("unknown", detail=f"{code}: результат не удалось подтвердить")
            if isinstance(exc, errors.RPCError) and self._is_definitive_upgrade_error(code):
                return UpgradeOutcome("failed", detail=f"{code}: {exc}")
            actual_num, actual_slug = await self._verify_unique(peer, info.saved_id)
            if actual_num is not None:
                return UpgradeOutcome("confirmed", actual_num, actual_slug)
            prefix = "Повторный запрос" if retry else code
            return UpgradeOutcome("unknown", detail=f"{prefix}: {type(exc).__name__}: {exc}")

        try:
            result = await submit(plan)
            return await self._interpret_upgrade_result(peer, info.saved_id, result)
        except errors.FloodWaitError:
            raise
        except errors.RPCError as exc:
            code = self._rpc_code(exc)
            if code in {"FORM_EXPIRED", "STARS_FORM_AMOUNT_MISMATCH"}:
                try:
                    # Refreshing the form is part of the retry transaction.  It can
                    # fail with its own definitive Telegram error (for example an
                    # insufficient balance), so classify that error exactly like a
                    # submit failure instead of letting it escape to the scanner.
                    refreshed = await self.prepare_upgrade(peer, info)
                    result = await submit(refreshed)
                    return await self._interpret_upgrade_result(peer, info.saved_id, result)
                except BaseException as retry_exc:
                    if isinstance(retry_exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                        raise
                    return await handle_submit_error(retry_exc, retry=True)
            return await handle_submit_error(exc)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                raise
            return await handle_submit_error(exc)



def construct(cls: Any, **kwargs: Any) -> Any:
    """Instantiate generated Telethon classes while tolerating layer-specific optional fields."""
    try:
        signature = inspect.signature(cls.__init__)
    except (TypeError, ValueError):
        return cls(**kwargs)
    parameters = signature.parameters
    accepted = {key: value for key, value in kwargs.items() if key in parameters}
    return cls(**accepted)


mtproto = MTProtoService(store)


class Scanner:
    def __init__(self, service: MTProtoService, bot_getter: Any):
        self.service = service
        self.bot_getter = bot_getter
        self.task: asyncio.Task[None] | None = None
        self.monitor_task: asyncio.Task[None] | None = None
        self.stop_event = asyncio.Event()
        self.prepared: dict[int, PreparedUpgrade] = {}
        self.triggered: set[tuple[str, int]] = set()
        self.notified_missed: set[tuple[str, int]] = set()
        self._upgrade_lock = asyncio.Lock()
        self.status_chat_id: int | None = None
        self.status_message_id: int | None = None
        self._last_status_edit = 0.0
        self._last_status_text: str | None = None
        self._last_diagnostics_write = 0.0
        self._last_poll_started: float | None = None
        self._plan_dirty = True
        self._groups: dict[str, list[SavedGiftInfo]] = {}
        self._counter_meta: dict[str, GiftCounter] = {}
        self.rate = AdaptiveRateController(
            min_interval_ms=SCAN_MIN_INTERVAL_MS,
            max_interval_ms=SCAN_MAX_INTERVAL_MS,
            start_interval_ms=SCAN_START_INTERVAL_MS,
            accelerate_every=SCAN_ACCELERATE_EVERY,
            accelerate_factor=SCAN_ACCELERATE_FACTOR,
            backoff_factor=SCAN_BACKOFF_FACTOR,
            backoff_floor_ms=SCAN_BACKOFF_FLOOR_MS,
        )

    def attach_status_message(self, chat_id: int, message_id: int) -> None:
        self.status_chat_id = int(chat_id)
        self.status_message_id = int(message_id)
        self._last_status_edit = 0.0
        self._last_status_text = None

    async def refresh_status_message(self, *, force: bool = False) -> None:
        if self.status_chat_id is None or self.status_message_id is None:
            return
        now = time.monotonic()
        if not force and now - self._last_status_edit < 2.0:
            return
        text = await status_text()
        if not force and text == self._last_status_text:
            self._last_status_edit = now
            return
        bot = self.bot_getter()
        if bot is None:
            return
        try:
            await bot.edit_message_text(
                chat_id=self.status_chat_id,
                message_id=self.status_message_id,
                text=text,
            )
            self._last_status_text = text
            self._last_status_edit = now
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                self._last_status_text = text
                self._last_status_edit = now
            else:
                logger.debug("live_status_edit_failed error=%s", exc)
        except Exception as exc:
            logger.debug("live_status_edit_failed error=%s", exc)

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
        try:
            while runtime.active and not self.stop_event.is_set():
                await self._maybe_write_diagnostics()
                await self.refresh_status_message()
                await self._wait(1.0)
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
            raise RuntimeError(f"Все цели уже прошли. Текущий номер: {current}")

        for slug, counter in counters.items():
            runtime.current_by_slug[slug] = counter.current
            runtime.title_by_slug[slug] = counter.title

        self._groups = groups
        self._counter_meta = counters
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
                required = min(len(group), len(future_targets))
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

    async def start(self) -> None:
        if self.task and not self.task.done():
            return
        if not store.settings.selected_saved_ids:
            raise RuntimeError("Подарки не выбраны")
        if not store.settings.target_numbers:
            raise RuntimeError("Целевые номера не заданы")
        rate_limit.clear_if_expired()
        rate_limit.assert_available()
        if not await self.service.is_authorized():
            raise RuntimeError("Telegram-аккаунт не авторизован")

        # Reset the visible state first, but do not mark the scanner active until
        # channel, gifts, slug/counter and (for LIVE) payment forms have all been
        # validated synchronously.  This prevents a failed start from briefly
        # showing a green "active" status.
        self.stop_event = asyncio.Event()
        self.triggered.clear()
        self.notified_missed.clear()
        self._plan_dirty = True
        self._groups.clear()
        self._counter_meta.clear()
        self._last_poll_started = None
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

        try:
            peer = await self.service.resolve_channel()
            await self._load_plan(peer)
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

        runtime.active = True
        runtime.started_at = time.monotonic()
        runtime.last_error = None
        self._plan_dirty = False
        self.task = asyncio.create_task(self._run(peer), name="gift-scanner")
        self.monitor_task = asyncio.create_task(self._monitor_loop(), name="gift-scanner-monitor")
        logger.info(
            "scanner_started version=%s saved_ids=%s targets=%s live=%s adaptive=%s start_ms=%s min_ms=%s",
            APP_VERSION,
            store.settings.selected_saved_ids,
            store.settings.target_numbers,
            store.settings.live_upgrades,
            ADAPTIVE_SCAN,
            SCAN_START_INTERVAL_MS,
            SCAN_MIN_INTERVAL_MS,
        )

    async def stop(self, reason: str = "manual") -> None:
        self.stop_event.set()
        task = self.task
        monitor = self.monitor_task
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
        self.task = None
        self.monitor_task = None
        runtime.active = False
        runtime.started_at = None
        await self._maybe_write_diagnostics(force=True)
        await self.refresh_status_message(force=True)
        logger.info("scanner_stopped reason=%s", reason)

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
                    # Normal hot path: exactly one GetUniqueStarGift request per
                    # selected collectible type. No saved-gift fetch and no disk
                    # write is performed in the polling cycle.
                    for slug, meta in self._counter_meta.items():
                        counter = await self.service.fetch_counter_fast(
                            slug,
                            expected_gift_id=meta.base_gift_id,
                        )
                        counters[slug] = counter
                        runtime.current_by_slug[slug] = counter.current
                        runtime.title_by_slug[slug] = counter.title

                    current_max = max(counter.current for counter in counters.values())
                    future_targets = [value for value in store.settings.target_numbers if value > current_max]
                    if not future_targets:
                        store.settings.target_numbers = []
                        await store.save()
                        runtime.last_error = f"Все цели уже прошли. Текущий номер: {current_max}"
                        await self.notify(
                            f"⚠️ Все цели уже прошли. Текущий номер: <b>{current_max}</b>. Сканер остановлен."
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

                        # The trigger is deliberately the first action after the
                        # counter response. A second number check or owned-gift
                        # refresh here only makes the race slower.
                        if state.should_trigger:
                            key = (slug, target)
                            if key in self.triggered:
                                continue
                            if not store.settings.live_upgrades:
                                self.triggered.add(key)
                                await self.notify(
                                    f"🧪 DRY-RUN: <b>{html.escape(counter.title)}</b> сейчас #{counter.current}, "
                                    f"следующий номер — цель <b>#{target}</b>. Оплата выключена, улучшение не отправлено."
                                )
                            else:
                                await self._upgrade(peer, slug, counter, target, group)
                                self.triggered.add(key)
                            continue

                        for missed_target in sorted(set(store.settings.target_numbers)):
                            if missed_target > counter.current:
                                break
                            key = (slug, missed_target)
                            if key not in self.notified_missed:
                                self.notified_missed.add(key)
                                await self.notify(
                                    f"⚠️ Цель <b>{missed_target}</b> для <b>{html.escape(counter.title)}</b> уже прошла. "
                                    f"Текущий номер: <b>{counter.current}</b>."
                                )

                        if (
                            store.settings.live_upgrades
                            and state.distance is not None
                            and 1 < state.distance <= PREPARE_AHEAD
                        ):
                            future_targets = sorted(
                                {value for value in store.settings.target_numbers if value > counter.current}
                            )
                            required = min(len(group), len(future_targets))
                            for candidate in group[:required]:
                                existing = self.prepared.get(candidate.saved_id)
                                if existing is not None and time.monotonic() - existing.created_at <= PREPARE_REFRESH_SECONDS:
                                    continue
                                try:
                                    self.prepared[candidate.saved_id] = await self.service.prepare_upgrade(peer, candidate)
                                    logger.info(
                                        "upgrade_prepared slug=%s saved_id=%s target=%s cost=%s",
                                        slug,
                                        candidate.saved_id,
                                        target,
                                        self.prepared[candidate.saved_id].cost,
                                    )
                                except Exception as exc:
                                    logger.warning(
                                        "upgrade_prepare_failed slug=%s saved_id=%s error=%s",
                                        slug,
                                        candidate.saved_id,
                                        exc,
                                    )

                    if self.stop_event.is_set():
                        break

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
            self.task = None
            await self._maybe_write_diagnostics(force=True)
            await self.refresh_status_message(force=True)

    async def _upgrade(
        self,
        peer: Any,
        slug: str,
        counter: GiftCounter,
        target: int,
        group: list[SavedGiftInfo],
    ) -> None:
        async with self._upgrade_lock:
            candidates = [
                item
                for item in group
                if item.saved_id in store.settings.selected_saved_ids and item.can_upgrade
            ]
            if not candidates:
                raise RuntimeError("Нет доступного экземпляра подарка для улучшения")
            chosen = candidates[0]
            plan = self.prepared.get(chosen.saved_id)
            trigger_key = (slug, target)

            # Persist an in-flight hold before submitting a financial request. If
            # the process or network dies after Telegram accepts the payment, a
            # restart will reconcile this saved gift instead of paying twice.
            previous_ids = list(store.settings.payment_hold_saved_ids)
            previous_targets = dict(store.settings.payment_hold_targets)
            previous_reason = store.settings.payment_hold_reason
            previous_url = store.settings.payment_verification_url
            holds = set(previous_ids)
            holds.add(chosen.saved_id)
            store.settings.payment_hold_saved_ids = sorted(holds)
            store.settings.payment_hold_targets[str(chosen.saved_id)] = int(target)
            store.settings.payment_hold_reason = (
                f"LIVE-запрос отправляется: {counter.title}, цель #{target}, saved_id={chosen.saved_id}"
            )
            store.settings.payment_verification_url = None
            try:
                await store.save()
            except Exception as exc:
                store.settings.payment_hold_saved_ids = previous_ids
                store.settings.payment_hold_targets = previous_targets
                store.settings.payment_hold_reason = previous_reason
                store.settings.payment_verification_url = previous_url
                raise RuntimeError(
                    "Не удалось сохранить защиту от повторной оплаты; LIVE-запрос не отправлен"
                ) from exc

            self.triggered.add(trigger_key)
            logger.warning(
                "live_upgrade_submit slug=%s observed_current=%s target=%s saved_id=%s prepared=%s",
                slug, counter.current, target, chosen.saved_id, bool(plan),
            )
            try:
                outcome = await self.service.execute_upgrade(peer, chosen, plan)
            except errors.FloodWaitError:
                # Telegram rejected this method with FLOOD_WAIT, so no payment was
                # accepted. Remove the in-flight marker only after persisting it.
                with contextlib.suppress(ValueError):
                    store.settings.payment_hold_saved_ids.remove(chosen.saved_id)
                store.settings.payment_hold_targets.pop(str(chosen.saved_id), None)
                if not store.settings.payment_hold_saved_ids:
                    store.settings.payment_hold_reason = None
                    store.settings.payment_verification_url = None
                    runtime.pending_verification_url = None
                try:
                    await store.save()
                except Exception as exc:
                    # The pre-submit hold is still present on disk.  Restore the
                    # same guard in memory too, otherwise Reset/channel/gift
                    # controls could overwrite that safe on-disk state before a
                    # restart or reconciliation.
                    holds = set(store.settings.payment_hold_saved_ids)
                    holds.add(chosen.saved_id)
                    store.settings.payment_hold_saved_ids = sorted(holds)
                    store.settings.payment_hold_targets[str(chosen.saved_id)] = int(target)
                    store.settings.payment_hold_reason = (
                        f"FLOOD_WAIT до оплаты; снятие блокировки не сохранено, saved_id={chosen.saved_id}"
                    )
                    store.settings.live_upgrades = False
                    self.stop_event.set()
                    await self.notify(
                        "⚠️ FLOOD_WAIT получен до оплаты, но не удалось сохранить снятие защитной блокировки. "
                        "Сканер остановлен; открой «🎁 Подарки» перед повторным запуском."
                    )
                    raise RuntimeError("Не удалось сохранить состояние после FLOOD_WAIT") from exc
                self.triggered.discard(trigger_key)
                raise
            await self._finish_upgrade(chosen, counter, target, outcome)

    async def _finish_upgrade(
        self,
        chosen: SavedGiftInfo,
        counter: GiftCounter,
        target: int,
        outcome: UpgradeOutcome,
    ) -> None:
        if outcome.status == "confirmed" and outcome.actual_num is not None:
            actual_num = outcome.actual_num
            runtime.last_success = f"{counter.title}: цель {target}, получен {actual_num}"
            runtime.last_error = None
            runtime.pending_verification_url = None
            if actual_num == target:
                icon = "✅"
                verdict = "Целевой номер получен"
            else:
                icon = "⚠️"
                verdict = "Улучшение прошло, но из-за гонки выдан другой номер"

            await self.notify(
                f"{icon} <b>{html.escape(verdict)}</b>\n"
                f"Подарок: <b>{html.escape(counter.title)}</b>\n"
                f"Цель: <b>#{target}</b>\n"
                f"Получен: <b>#{actual_num}</b>"
                + (f"\nSlug: <code>{html.escape(outcome.actual_slug)}</code>" if outcome.actual_slug else "")
            )

            # Remove the attempted goal and every number that the confirmed
            # result has already passed, otherwise the scanner could spin forever
            # with only stale targets left after a race.
            store.settings.target_numbers = [
                value
                for value in store.settings.target_numbers
                if value != target and value > actual_num
            ]
            with contextlib.suppress(ValueError):
                store.settings.selected_saved_ids.remove(chosen.saved_id)
            with contextlib.suppress(ValueError):
                store.settings.payment_hold_saved_ids.remove(chosen.saved_id)
            store.settings.payment_hold_targets.pop(str(chosen.saved_id), None)
            store.settings.payment_verification_url = None
            if not store.settings.payment_hold_saved_ids:
                store.settings.payment_hold_reason = None

            save_error: Exception | None = None
            try:
                await store.save()
            except Exception as exc:
                # The pre-submit hold remains on disk.  Restore it in memory as
                # well, so no UI action can clear the protective state before a
                # restart/reconciliation.  The confirmed result itself is kept in
                # runtime and the scanner is stopped below.
                holds = set(store.settings.payment_hold_saved_ids)
                holds.add(chosen.saved_id)
                store.settings.payment_hold_saved_ids = sorted(holds)
                store.settings.payment_hold_targets[str(chosen.saved_id)] = int(target)
                store.settings.payment_hold_reason = (
                    f"Улучшение подтверждено, но итог не сохранён, saved_id={chosen.saved_id}"
                )
                save_error = exc
                logger.exception("confirmed_upgrade_state_save_failed")

            self.prepared.pop(chosen.saved_id, None)

            # Continue in memory. All required payment forms were prepared before
            # the trigger, so consecutive target numbers do not incur a saved-gift
            # reload or payment-form request between upgrades.
            group = self._groups.get(counter.slug, [])
            self._groups[counter.slug] = [item for item in group if item.saved_id != chosen.saved_id]
            runtime.current_by_slug[counter.slug] = max(
                runtime.current_by_slug.get(counter.slug, actual_num),
                actual_num,
            )
            if not self._groups[counter.slug]:
                self._groups.pop(counter.slug, None)
                self._counter_meta.pop(counter.slug, None)

            if save_error is not None:
                await self.notify(
                    "⚠️ Улучшение подтверждено, но запись настроек на диск не удалась. "
                    "Сканер остановлен; после перезапуска защитная сверка не даст списать Stars повторно."
                )
                self.stop_event.set()
            elif STOP_AFTER_SUCCESS or not store.settings.target_numbers or not store.settings.selected_saved_ids:
                self.stop_event.set()
            return

        detail = outcome.detail or "Telegram не подтвердил улучшение"
        runtime.last_error = detail
        store.settings.live_upgrades = False
        self.prepared.clear()
        self.stop_event.set()

        if outcome.status in {"verification", "unknown"}:
            # The in-flight hold was written before submission; keep it until a
            # later read proves which collectible number was actually assigned.
            holds = set(store.settings.payment_hold_saved_ids)
            holds.add(chosen.saved_id)
            store.settings.payment_hold_saved_ids = sorted(holds)
            store.settings.payment_hold_targets[str(chosen.saved_id)] = int(target)
            store.settings.payment_hold_reason = detail[:500]
        else:
            # A definitive failure means no payment was accepted.
            with contextlib.suppress(ValueError):
                store.settings.payment_hold_saved_ids.remove(chosen.saved_id)
            store.settings.payment_hold_targets.pop(str(chosen.saved_id), None)
            if not store.settings.payment_hold_saved_ids:
                store.settings.payment_hold_reason = None

        if outcome.status == "verification":
            runtime.pending_verification_url = outcome.verification_url
            store.settings.payment_verification_url = outcome.verification_url
        elif not store.settings.payment_hold_saved_ids:
            runtime.pending_verification_url = None
            store.settings.payment_verification_url = None

        save_error: Exception | None = None
        try:
            await store.save()
        except Exception as exc:
            # The pre-submit hold is already on disk.  Keep an equivalent guard
            # in memory even for a definitive failure: without it a later Reset or
            # channel change could overwrite the last known-safe file while this
            # process is still running.
            holds = set(store.settings.payment_hold_saved_ids)
            holds.add(chosen.saved_id)
            store.settings.payment_hold_saved_ids = sorted(holds)
            store.settings.payment_hold_targets[str(chosen.saved_id)] = int(target)
            store.settings.payment_hold_reason = (
                f"Результат {outcome.status} не удалось сохранить, saved_id={chosen.saved_id}: {detail[:300]}"
            )
            save_error = exc
            logger.exception("upgrade_result_state_save_failed status=%s", outcome.status)

        if outcome.status == "verification":
            text = (
                "⚠️ <b>Нужно подтверждение Telegram</b>\n"
                "Подарок и цель сохранены, повторная оплата не отправлялась."
            )
            if outcome.verification_url:
                text += f"\nОткрой: <code>{html.escape(outcome.verification_url)}</code>"
        elif outcome.status == "failed":
            text = (
                "❌ <b>Улучшение не выполнено</b>\n"
                f"Причина: <code>{html.escape(detail[:500])}</code>\n"
                "Подарок и цель сохранены."
            )
        else:
            text = (
                "⚠️ <b>Результат улучшения не подтверждён</b>\n"
                f"Детали: <code>{html.escape(detail[:500])}</code>\n"
                "Бот остановлен и не отправляет повторную оплату. Подарок и цель сохранены."
            )
        if save_error is not None:
            text += "\n⚠️ Дополнительно не удалось обновить файл настроек; защитная блокировка до отправки сохранена."
        await self.notify(text)

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
        self.stop_event = asyncio.Event()
        self.status_chat_id: int | None = None
        self.status_message_id: int | None = None
        self._last_status_update = 0.0

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
        self._last_status_update = 0.0
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
        self._last_status_update = 0.0

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
        runtime.stress_max_rate_per_s = 0.0
        runtime.stress_result = None
        runtime.current_by_slug = {counter.slug: counter.current}
        runtime.title_by_slug = {counter.slug: counter.title}

    async def stop(self, reason: str = "manual") -> None:
        task = self.task
        if not task or task.done():
            runtime.stress_active = False
            self.task = None
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
        if self.status_chat_id is None or self.status_message_id is None:
            return
        now = time.monotonic()
        if not force and now - self._last_status_update < STRESS_STATUS_INTERVAL_SECONDS:
            return
        bot = self.bot_getter()
        if bot is None:
            return
        try:
            await bot.edit_message_text(
                chat_id=self.status_chat_id,
                message_id=self.status_message_id,
                text=await stress_status_text(),
                reply_markup=None,
            )
            self._last_status_update = now
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                logger.debug("stress_status_edit_failed error=%s", exc)
        except Exception as exc:
            logger.debug("stress_status_edit_failed error=%s", exc)

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
                await self._refresh_status()

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
scanner = Scanner(mtproto, lambda: _bot_instance)
stress_tester = StressTester(mtproto, lambda: _bot_instance)


class SetupStates(StatesGroup):
    pin = State()
    api_id = State()
    api_hash = State()
    phone = State()
    targets = State()


router = Router()


BTN_CHANNEL = "📣 Канал"
BTN_GIFTS = "🎁 Подарки"
BTN_CHECK = "🔢 Проверить номера"
BTN_TARGETS = "🎯 Задать номера"
BTN_START = "▶️ Запустить"
BTN_STOP = "⛔ Остановить"
BTN_PAYMENT_OFF = "🛡 Оплата: ВЫКЛ"
BTN_PAYMENT_ON = "💳 Оплата: ВКЛ"
BTN_PING = "📡 Ping"
BTN_STRESS_OFF = "🧪 Стресс-тест: ВЫКЛ"
BTN_STRESS_ON = "🧪 Стресс-тест: ВКЛ"
BTN_LOG = "📄 Log"
BTN_RESET = "🗑 Сброс"


def main_keyboard() -> ReplyKeyboardMarkup:
    payment = BTN_PAYMENT_ON if store.settings.live_upgrades else BTN_PAYMENT_OFF
    start_stop = BTN_STOP if runtime.active else BTN_START
    stress_button = BTN_STRESS_ON if runtime.stress_active else BTN_STRESS_OFF
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CHANNEL), KeyboardButton(text=BTN_GIFTS)],
            [KeyboardButton(text=BTN_CHECK), KeyboardButton(text=BTN_TARGETS)],
            [KeyboardButton(text=start_stop), KeyboardButton(text=payment)],
            [KeyboardButton(text=BTN_PING), KeyboardButton(text=stress_button)],
            [KeyboardButton(text=BTN_LOG), KeyboardButton(text=BTN_RESET)],
        ],
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
            [InlineKeyboardButton(text="🗑 Сбросить выбор и цели", callback_data="reset:yes")],
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


async def safe_delete(message: Message) -> None:
    with contextlib.suppress(Exception):
        await message.delete()


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
                "Открой «🎁 Подарки», выбери конкретный экземпляр и задай цель.",
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
    await continue_setup(message, state)


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
    await continue_setup(message, state)


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
        "Отправь целевые номера через запятую, пробел или с новой строки.\n"
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
            + " Цели не изменены; сначала дождись сверки в «🎁 Подарки»."
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
    await state.clear()
    await message.answer(
        "🎯 Целевые номера: " + ", ".join(map(str, targets)) + "\n🛡 LIVE выключен — включи его заново после проверки цели.",
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
    """Return the global Telegram collectible catalog, independent of channels."""
    results, elapsed_ms = await service.fetch_global_catalog_numbers()
    results = sorted(results, key=lambda item: item.title.casefold())
    resolved = sum(1 for item in results if item.issued is not None)
    errors_count = len(results) - resolved
    lines = [
        f"🔢 <b>Последние выданные номера · {APP_VERSION}</b>",
        f"Проверено коллекций: <b>{len(results)}</b> · {elapsed_ms:.0f} мс",
    ]
    if errors_count:
        lines.append(f"Получено номеров: <b>{resolved}</b> · ошибок: <b>{errors_count}</b>")
    lines.append("")
    for item in results:
        if item.issued is None:
            lines.append(f"{html.escape(item.title)} — не определён")
        else:
            lines.append(f"{html.escape(item.title)} — {item.issued}")
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


def build_full_log_export() -> Path:
    """Build one owner-only ZIP containing all available bot logs."""
    export_path = DATA_DIR / f"gift-hunter-{APP_VERSION}-full-log.zip"
    candidates = sorted(
        (
            path
            for path in DATA_DIR.glob("gift-hunter-v*.log*")
            if path.is_file()
            and "-full-log" not in path.name
            and not path.name.endswith("-full.log")
        ),
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    extras = [
        path
        for path in (STRESS_REPORT_PATH, STRESS_HISTORY_PATH, DIAGNOSTICS_PATH, RATE_LIMIT_PATH)
        if path.exists() and path.is_file()
    ]
    manifest = (
        f"{APP_NAME} {APP_VERSION} full log export\n"
        f"generated_at={time.strftime('%Y-%m-%dT%H:%M:%S%z')}\n"
        f"log_files={len(candidates)}\n"
        f"extra_files={len(extras)}\n"
    )
    with zipfile.ZipFile(export_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("MANIFEST.txt", manifest)
        for path in candidates + extras:
            archive.write(path, arcname=path.name)
    return export_path


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
                f"Цель: <b>{target if target is not None else 'не задана'}</b>",
            ]
        )
        if state.distance is not None:
            if state.distance > 0:
                lines.append(f"До цели: <b>{state.distance}</b>")
            else:
                lines.append("Статус: <b>цель уже прошла</b>")
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
        await send_text_chunks(message, text, reply_markup=main_keyboard())
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
            + ("\n\n💳 <b>LIVE:</b> бот отправит оплату при текущем номере = цель − 1."
               if store.settings.live_upgrades
               else "\n\n🧪 <b>DRY-RUN:</b> Stars не списываются."),
            reply_markup=main_keyboard(),
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
        raise RuntimeError("Целевые номера не заданы")
    if any(not info.can_upgrade for info in infos):
        raise RuntimeError("Один из выбранных подарков больше нельзя улучшить. Обнови список подарков.")

    counter = await mtproto.counter_for_info(infos[0], peer=peer, cache_seconds=0)
    future_targets = sorted({target for target in store.settings.target_numbers if target > counter.current})
    if not future_targets:
        raise RuntimeError(f"Все цели уже прошли. Текущий номер: {counter.current}")

    operation_count = min(len(infos), len(future_targets))
    planned_infos = infos[:operation_count]
    planned_targets = future_targets[:operation_count]
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
    target_text = ", ".join(str(value) for value in planned_targets)
    warning = ""
    if len(future_targets) > len(infos):
        warning = (
            f"\n⚠️ Подарков меньше, чем будущих целей: подготовлено {operation_count} из "
            f"{len(future_targets)} операций."
        )

    return (
        "⚠️ <b>Подтверждение LIVE</b>\n"
        f"Канал: <b>{html.escape(store.settings.channel_title or '—')}</b>\n"
        f"Подарок: <b>{html.escape(counter.title)}</b>\n"
        f"Выбранных экземпляров: <b>{len(infos)}</b>\n"
        f"Текущий номер: <b>{counter.current}</b>\n"
        f"Подготовленные цели: <b>{target_text}</b>\n"
        f"Подготовлено операций: <b>{operation_count}</b>\n"
        f"Возможное списание: <b>{payment_text}</b>\n"
        f"Лимит одной операции: <b>{limit_text}</b>"
        f"{warning}\n\n"
        "Платёжные формы подготовлены заранее. Бот отправит улучшение, когда текущий номер "
        "станет равен ближайшей цели минус один. Точный номер не гарантируется из-за "
        "одновременных запросов других пользователей."
    )


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
        logger.info("live_disabled_by_toggle")
        await message.answer("🛡 Оплата выключена. Режим DRY-RUN.", reply_markup=main_keyboard())
        return
    try:
        summary = await live_preflight(prepare=True)
        store.settings.live_upgrades = True
        await store.save()
        logger.info("live_enabled_by_toggle")
        await message.answer(
            "💳 <b>Оплата включена — режим LIVE активирован.</b>\n"
            + summary.replace("⚠️ <b>Подтверждение LIVE</b>\n", ""),
            reply_markup=main_keyboard(),
        )
    except Exception as exc:
        store.settings.live_upgrades = False
        await store.save()
        logger.exception("live_preflight_failed")
        await message.answer(f"LIVE не включён: {html.escape(str(exc))}", reply_markup=main_keyboard())


@router.callback_query(F.data.startswith("payment:"))
async def payment_confirm_handler(callback: CallbackQuery) -> None:
    """Reject confirmation buttons left in chat by older versions.

    v0011 uses the reply-keyboard payment switch itself as confirmation, so a
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
        f"Максимум: <b>{runtime.stress_max_rate_per_s:.1f} проверок/с</b>",
        "Оплата: <b>принудительно не используется</b>",
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
    lines = [
        f"🎯 <b>{APP_NAME} {APP_VERSION}</b>",
        f"Сканер: {'🟢 активен' if runtime.active else '⚪ остановлен'}",
        f"Режим: {'LIVE' if store.settings.live_upgrades else 'DRY-RUN'}",
        f"MTProto: {'подключён' if authorized else 'не подключён'}",
        f"Канал: {html.escape(store.settings.channel_title or 'не выбран')}",
        f"Проверок: {runtime.checks}",
        f"Последний цикл: {runtime.last_cycle_ms:.0f} мс" if runtime.last_cycle_ms is not None else "Последний цикл: —",
        f"Адаптивный период: {runtime.adaptive_interval_ms:.0f} мс" if runtime.adaptive_interval_ms is not None else "Адаптивный период: —",
        f"Фактический шаг: {runtime.poll_gap_ms:.0f} мс" if runtime.poll_gap_ms is not None else "Фактический шаг: —",
        f"FloodWait: {runtime.flood_count}" + (f" · последний {runtime.last_flood_wait_s}с" if runtime.last_flood_wait_s is not None else ""),
        f"Cooldown: {runtime.rate_cooldown_cycles} циклов" if runtime.rate_cooldown_cycles else "Cooldown: нет",
        f"Uptime сканера: {uptime}с" if runtime.started_at else "Uptime сканера: —",
    ]
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
            lines.append(f"• {html.escape(title)}: текущий <b>{current}</b> · цель <b>{target or '—'}</b>")
    elif store.settings.target_numbers:
        lines.append("Цели: " + ", ".join(map(str, store.settings.target_numbers)))
    if runtime.last_error:
        lines.append(f"Ошибка: <code>{html.escape(runtime.last_error[:300])}</code>")
    if runtime.last_success:
        lines.append(f"Последний успех: {html.escape(runtime.last_success)}")
    verification_url = runtime.pending_verification_url or store.settings.payment_verification_url
    if verification_url:
        lines.append(f"Подтверждение: <code>{html.escape(verification_url)}</code>")
    return "\n".join(lines)


@router.message(F.text == BTN_PING)
async def ping_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    started = time.perf_counter()
    authorized = True if (runtime.active or runtime.stress_active) else await mtproto.is_authorized()
    latency = (time.perf_counter() - started) * 1000
    text = await status_text()
    text += f"\nПроверка MTProto: {latency:.0f} мс ({'OK' if authorized else 'нет авторизации'})"
    await message.answer(text, reply_markup=main_keyboard())


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
        "Сбросить выбранные подарки, цели и выключить LIVE? Авторизация Telegram сохранится.",
        reply_markup=reset_confirm_keyboard(),
    )


@router.callback_query(F.data.startswith("reset:"))
async def reset_confirm_handler(callback: CallbackQuery) -> None:
    if not await owner_guard_callback(callback):
        return
    action = (callback.data or "").split(":", 1)[1]
    if action == "yes":
        await scanner.stop("reset")
        await stress_tester.stop("reset")
        try:
            await store.reset_operational()
        except RuntimeError as exc:
            await callback.answer(str(exc)[:180], show_alert=True)
            return
        runtime.current_by_slug.clear()
        runtime.title_by_slug.clear()
        await callback.answer("Сброшено")
        if callback.message:
            await callback.message.answer("🗑 Выбор и цели сброшены.", reply_markup=main_keyboard())
    else:
        await callback.answer("Отменено")


@router.message(Command("log_full"))
async def log_full_handler(message: Message) -> None:
    if not await owner_guard_message(message):
        return
    try:
        export_path = build_full_log_export()
        document = FSInputFile(export_path, filename=export_path.name)
        await message.answer_document(
            document,
            caption=f"📄 Полный лог {APP_NAME} {APP_VERSION}",
        )
    except Exception as exc:
        logger.exception("log_full_failed")
        await message.answer(f"❌ Не удалось отправить полный лог: {html.escape(str(exc))}")


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


async def heartbeat_loop() -> None:
    while True:
        payload = {
            "timestamp": time.time(),
            "version": APP_VERSION,
            "scanner_active": runtime.active,
            "stress_test_active": runtime.stress_active,
            "rate_limit_remaining_seconds": rate_limit.remaining_seconds(),
            "rate_limit_blocked_until": rate_limit.blocked_until or None,
        }
        try:
            temp = HEARTBEAT_PATH.with_suffix(".tmp")
            temp.write_text(json.dumps(payload), encoding="utf-8")
            temp.replace(HEARTBEAT_PATH)
        except OSError as exc:
            # Keep the task alive so a transient filesystem error cannot silently
            # kill heartbeat updates forever. A persistent error still makes the
            # Docker healthcheck stale and triggers a restart.
            logger.error("heartbeat_write_failed error=%s", exc)
        await asyncio.sleep(10)


async def write_diagnostics() -> None:
    payload = {
        "version": APP_VERSION,
        "live_upgrades": store.settings.live_upgrades,
        "max_upgrade_stars": MAX_UPGRADE_STARS,
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
        "runtime": asdict(runtime),
        "settings": {
            "owner_user_id": store.settings.owner_user_id,
            "channel_id": store.settings.channel_id,
            "channel_title": store.settings.channel_title,
            "selected_saved_ids": store.settings.selected_saved_ids,
            "target_numbers": store.settings.target_numbers,
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


async def run_bot() -> None:
    global _bot_instance
    if not BOT_TOKEN:
        raise RuntimeError("Environment variable BOT_TOKEN is required")
    logger.info("application_start version=%s", APP_VERSION)
    await store.save()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    _bot_instance = bot
    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    heartbeat_task = asyncio.create_task(heartbeat_loop(), name="heartbeat")
    try:
        await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())
    finally:
        await scanner.stop("shutdown")
        await stress_tester.stop("shutdown")
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        await mtproto.disconnect()
        await bot.session.close()
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
        device_model="Gift Hunter VPS",
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
