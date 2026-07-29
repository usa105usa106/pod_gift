from __future__ import annotations

VERSION = "v0002"

from contextlib import suppress
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage


def validate_runtime_api() -> None:
    """Остановить запуск, если Telethon не содержит нужные Gift API."""
    try:
        from telethon import functions as _functions, types as _types
    except ImportError as exc:
        raise RuntimeError("Telethon не установлен. Проверь requirements.txt") from exc

    required = [
        (_functions.payments, "GetSavedStarGiftsRequest"),
        (_functions.payments, "GetSavedStarGiftRequest"),
        (_functions.payments, "GetStarGiftsRequest"),
        (_functions.payments, "GetUniqueStarGiftRequest"),
        (_functions.payments, "GetPaymentFormRequest"),
        (_functions.payments, "SendStarsFormRequest"),
        (_functions.payments, "UpgradeStarGiftRequest"),
        (_types, "InputSavedStarGiftChat"),
        (_types, "InputInvoiceStarGiftUpgrade"),
    ]
    missing = [name for namespace, name in required if not hasattr(namespace, name)]
    if missing:
        raise RuntimeError(
            "Установленная версия Telethon не поддерживает нужный слой Telegram API: "
            + ", ".join(missing)
        )


# ===== config.py =====
from pathlib import Path
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    """Environment configuration. Secrets stay outside Git/GitHub."""

    bot_token: SecretStr = Field(alias="BOT_TOKEN")
    data_dir: Path = Field(default=Path("/app/data"), alias="DATA_DIR")
    setup_pin: SecretStr | None = Field(default=None, alias="SETUP_PIN")

    max_upgrade_stars: int = Field(default=2000, alias="MAX_UPGRADE_STARS")
    scan_interval_ms: int = Field(default=400, alias="SCAN_INTERVAL_MS")
    catalog_concurrency: int = Field(default=6, alias="CATALOG_CONCURRENCY")
    stop_after_success: bool = Field(default=True, alias="STOP_AFTER_SUCCESS")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    @field_validator("scan_interval_ms")
    @classmethod
    def validate_scan_interval(cls, value: int) -> int:
        if value < 150:
            raise ValueError("SCAN_INTERVAL_MS must be >= 150")
        return value

    @field_validator("max_upgrade_stars")
    @classmethod
    def validate_max_price(cls, value: int) -> int:
        if value < 0:
            raise ValueError("MAX_UPGRADE_STARS must be >= 0")
        return value

    @property
    def bot_token_value(self) -> str:
        return self.bot_token.get_secret_value()

    @property
    def setup_pin_value(self) -> str | None:
        return self.setup_pin.get_secret_value() if self.setup_pin else None

# ===== models.py =====
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class UserSettings:
    owner_user_id: int | None = None
    api_id: int | None = None
    api_hash: str | None = None
    phone: str | None = None
    channel_id: int | None = None
    channel_title: str | None = None
    selected_gift_ids: list[int] = field(default_factory=list)
    target_numbers: list[int] = field(default_factory=list)
    live_upgrades: bool = False
    status_message_id: int | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "UserSettings":
        known = {name for name in cls.__dataclass_fields__}
        return cls(**{key: item for key, item in value.items() if key in known})

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner_user_id": self.owner_user_id,
            "api_id": self.api_id,
            "api_hash": self.api_hash,
            "phone": self.phone,
            "channel_id": self.channel_id,
            "channel_title": self.channel_title,
            "selected_gift_ids": list(self.selected_gift_ids),
            "target_numbers": list(self.target_numbers),
            "live_upgrades": self.live_upgrades,
            "status_message_id": self.status_message_id,
        }


@dataclass(frozen=True, slots=True)
class ChannelInfo:
    id: int
    title: str
    username: str | None = None


@dataclass(frozen=True, slots=True)
class GiftSnapshot:
    saved_id: int
    gift_id: int
    title: str
    can_upgrade: bool
    gift_num: int | None
    upgrade_stars: int | None
    prepaid_upgrade: bool


@dataclass(slots=True)
class GiftCollection:
    gift_id: int
    title: str
    items: list[GiftSnapshot] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.items)

    @property
    def numbers(self) -> list[int]:
        return sorted({item.gift_num for item in self.items if item.gift_num is not None})

    @property
    def upgrade_stars(self) -> int | None:
        values = [item.upgrade_stars for item in self.items if item.upgrade_stars is not None]
        return max(values) if values else None


@dataclass(frozen=True, slots=True)
class CatalogNumber:
    gift_id: int
    title: str
    issued: int | None
    total: int | None
    slug_base: str | None
    error: str | None = None


@dataclass(slots=True)
class ScannerRuntime:
    active: bool = False
    started_at_monotonic: float | None = None
    checks: int = 0
    last_cycle_ms: float | None = None
    last_error: str | None = None
    current_numbers: dict[int, int | None] = field(default_factory=dict)
    current_titles: dict[int, str] = field(default_factory=dict)
    last_success: str | None = None

# ===== domain.py =====
import re
import unicodedata
from collections import defaultdict
from datetime import timedelta
from typing import Iterable


_NUMBER_SPLIT = re.compile(r"[\s,;]+")


def parse_target_numbers(text: str, *, max_items: int = 100) -> list[int]:
    parts = [part for part in _NUMBER_SPLIT.split(text.strip()) if part]
    if not parts:
        raise ValueError("Укажи хотя бы один номер")
    if len(parts) > max_items:
        raise ValueError(f"Слишком много номеров: максимум {max_items}")

    numbers: set[int] = set()
    for part in parts:
        if not part.isdecimal():
            raise ValueError(f"Некорректное значение: {part}")
        number = int(part)
        if number <= 0 or number > 2_147_483_647:
            raise ValueError(f"Номер вне допустимого диапазона: {part}")
        numbers.add(number)
    return sorted(numbers)


def slug_base_from_title(title: str | None) -> str | None:
    if not title:
        return None
    normalized = unicodedata.normalize("NFKD", title)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = "".join(char for char in ascii_text if char.isalnum())
    return slug or None


def group_gifts(items: Iterable[GiftSnapshot]) -> list[GiftCollection]:
    grouped: dict[int, list[GiftSnapshot]] = defaultdict(list)
    for item in items:
        grouped[item.gift_id].append(item)
    collections = [
        GiftCollection(gift_id=gift_id, title=group[0].title, items=group)
        for gift_id, group in grouped.items()
    ]
    return sorted(collections, key=lambda item: item.title.casefold())


def should_upgrade(gift: GiftSnapshot, selected_gift_ids: set[int], targets: set[int]) -> bool:
    return (
        gift.can_upgrade
        and gift.gift_id in selected_gift_ids
        and gift.gift_num is not None
        and gift.gift_num in targets
    )



def validate_upgrade_payment(
    *,
    currency: str | None,
    amount: int,
    expected_amount: int | None,
    max_amount: int,
) -> str | None:
    """Return a rejection reason, or None when a Stars payment is safe to submit."""
    if currency != "XTR":
        return f"Неожиданная валюта оплаты: {currency}"
    if amount <= 0:
        return "Telegram вернул пустую или нулевую цену"
    if amount > max_amount:
        return f"Цена {amount} ⭐ выше лимита {max_amount} ⭐"
    if expected_amount is not None and amount != expected_amount:
        return f"Цена изменилась: ожидалось {expected_amount} ⭐, Telegram запросил {amount} ⭐"
    return None

def chunk_text(lines: Iterable[str], *, limit: int = 3900) -> list[str]:
    chunks: list[str] = []
    current = ""
    for line in lines:
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
        while len(line) > limit:
            chunks.append(line[:limit])
            line = line[limit:]
        current = line
    if current:
        chunks.append(current)
    return chunks


def format_uptime(seconds: float) -> str:
    total = max(0, int(seconds))
    delta = timedelta(seconds=total)
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}д")
    if hours or days:
        parts.append(f"{hours}ч")
    if minutes or hours or days:
        parts.append(f"{minutes}м")
    parts.append(f"{secs}с")
    return " ".join(parts)


def format_collection_line(collection: GiftCollection, selected: bool = False) -> str:
    mark = "✅" if selected else "▫️"
    numbers = collection.numbers
    if numbers:
        shown = ", ".join(f"#{number}" for number in numbers[:8])
        if len(numbers) > 8:
            shown += f" … +{len(numbers) - 8}"
    else:
        shown = "номер не предоставлен"
    price = f" · {collection.upgrade_stars} ⭐" if collection.upgrade_stars is not None else ""
    return f"{mark} {collection.title} — {collection.count} шт. · {shown}{price}"

# ===== storage.py =====
import asyncio
import json
import os
from pathlib import Path
from typing import Callable



class SettingsStore:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.path = data_dir / "settings.json"
        self._lock = asyncio.Lock()
        self._settings = UserSettings()

    async def load(self) -> UserSettings:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
                self._settings = UserSettings.from_dict(raw)
            except (OSError, ValueError, TypeError):
                self._settings = UserSettings()
        # Never keep paid mode armed after a restart or redeploy.
        self._settings.live_upgrades = False
        return self.snapshot()

    def snapshot(self) -> UserSettings:
        return UserSettings.from_dict(self._settings.to_dict())

    async def update(self, mutate: Callable[[UserSettings], None]) -> UserSettings:
        async with self._lock:
            mutate(self._settings)
            await asyncio.to_thread(self._write_atomic)
            return self.snapshot()

    async def replace(self, settings: UserSettings) -> UserSettings:
        async with self._lock:
            self._settings = settings
            await asyncio.to_thread(self._write_atomic)
            return self.snapshot()

    async def clear(self) -> None:
        async with self._lock:
            self._settings = UserSettings()
            if self.path.exists():
                await asyncio.to_thread(self.path.unlink)

    def _write_atomic(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self._settings.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.path)
        os.chmod(self.path, 0o600)

# ===== logging_setup.py =====
import logging
import logging.handlers
import queue
import threading
from collections import deque
from pathlib import Path


class MemoryRingHandler(logging.Handler):
    def __init__(self, capacity: int = 300):
        super().__init__()
        self.lines: deque[str] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
            with self._lock:
                self.lines.append(line)
        except Exception:
            self.handleError(record)

    def tail(self, limit: int = 30) -> list[str]:
        with self._lock:
            return list(self.lines)[-limit:]

    def clear(self) -> None:
        with self._lock:
            self.lines.clear()


class LogManager:
    def __init__(self, data_dir: Path, level: str):
        self.log_dir = data_dir / "logs"
        self.log_file = self.log_dir / "gift-hunter.log"
        self.level = getattr(logging, level.upper(), logging.INFO)
        self.memory = MemoryRingHandler()
        self._queue: queue.Queue[logging.LogRecord] = queue.Queue(-1)
        self._listener: logging.handlers.QueueListener | None = None
        self._file_handler: logging.Handler | None = None

    def start(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.memory.setFormatter(formatter)
        file_handler = logging.handlers.RotatingFileHandler(
            self.log_file,
            maxBytes=5_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        self._file_handler = file_handler
        self._listener = logging.handlers.QueueListener(
            self._queue, file_handler, self.memory, respect_handler_level=True
        )
        self._listener.start()

        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(self.level)
        root.addHandler(logging.handlers.QueueHandler(self._queue))

        for noisy in ("aiogram.event", "telethon.network", "telethon.client.updates"):
            logging.getLogger(noisy).setLevel(max(self.level, logging.WARNING))

    def stop(self) -> None:
        if self._listener:
            self._listener.stop()
            self._listener = None
        if self._file_handler:
            self._file_handler.close()
            self._file_handler = None

    def tail(self, limit: int = 30) -> list[str]:
        return self.memory.tail(limit)

    def flush(self) -> None:
        if self._file_handler:
            self._file_handler.flush()

    def clear(self) -> None:
        # Stop first so queued records are drained before files are truncated.
        was_running = self._listener is not None
        if was_running:
            self.stop()
        for path in self.log_dir.glob("gift-hunter.log*"):
            try:
                path.unlink()
            except OSError:
                pass
        self.memory.clear()
        if was_running:
            self.start()

# ===== telegram_user.py =====
import asyncio
import logging
from pathlib import Path
from time import perf_counter
from typing import Iterable

from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError, RPCError


logger = logging.getLogger(__name__)


class UserSessionError(RuntimeError):
    pass


class TelegramUserService:
    def __init__(self, data_dir: Path, settings_getter):
        self.data_dir = data_dir
        self.session_base = data_dir / "account"
        self._settings_getter = settings_getter
        self._client: TelegramClient | None = None
        self._client_key: tuple[int, str] | None = None
        self._connect_lock = asyncio.Lock()

    @property
    def session_file(self) -> Path:
        return self.session_base.with_suffix(".session")

    async def close(self) -> None:
        if self._client:
            await self._client.disconnect()
            self._client = None
            self._client_key = None

    async def client(self, *, require_authorized: bool = True) -> TelegramClient:
        settings: UserSettings = self._settings_getter()
        if not settings.api_id or not settings.api_hash:
            raise UserSessionError("TG_API_ID и TG_API_HASH ещё не заданы")
        key = (settings.api_id, settings.api_hash)
        async with self._connect_lock:
            if self._client is None or self._client_key != key:
                if self._client:
                    await self._client.disconnect()
                self.data_dir.mkdir(parents=True, exist_ok=True)
                self._client = TelegramClient(
                    str(self.session_base),
                    settings.api_id,
                    settings.api_hash,
                    request_retries=1,
                    connection_retries=3,
                    retry_delay=1,
                    flood_sleep_threshold=0,
                    auto_reconnect=True,
                    sequential_updates=True,
                )
                self._client_key = key
            if not self._client.is_connected():
                await self._client.connect()
        if require_authorized and not await self._client.is_user_authorized():
            raise UserSessionError("Пользовательская сессия не авторизована")
        return self._client

    async def check_authorized(self) -> tuple[bool, int | None, str | None]:
        try:
            client = await self.client(require_authorized=False)
            if not await client.is_user_authorized():
                return False, None, None
            me = await client.get_me()
            name = " ".join(part for part in [me.first_name, me.last_name] if part)
            return True, me.id, name or me.username
        except Exception as exc:
            logger.warning("authorization_check_failed error=%s", type(exc).__name__)
            return False, None, None

    async def logout_and_delete(self) -> bool:
        remote_logout_ok = True
        try:
            client = await self.client(require_authorized=False)
            if await client.is_user_authorized():
                await client.log_out()
        except Exception as exc:
            remote_logout_ok = False
            logger.warning("logout_failed error=%s", type(exc).__name__)
        finally:
            await self.close()
            for suffix in (".session", ".session-journal"):
                path = self.session_base.with_suffix(suffix)
                if path.exists():
                    try:
                        path.unlink()
                    except OSError:
                        logger.exception("session_delete_failed path=%s", path)
        return remote_logout_ok

    async def find_owner_channels(self) -> list[ChannelInfo]:
        client = await self.client()
        result: list[ChannelInfo] = []
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if (
                isinstance(entity, types.Channel)
                and bool(getattr(entity, "broadcast", False))
                and bool(getattr(entity, "creator", False))
            ):
                result.append(
                    ChannelInfo(
                        id=entity.id,
                        title=entity.title,
                        username=getattr(entity, "username", None),
                    )
                )
        return sorted(result, key=lambda item: item.title.casefold())

    async def get_input_channel(self, channel_id: int):
        client = await self.client()
        return await client.get_input_entity(channel_id)

    async def fetch_channel_gifts(self, channel_id: int) -> list[GiftSnapshot]:
        client = await self.client()
        peer = await client.get_input_entity(channel_id)
        offset = ""
        items: list[GiftSnapshot] = []
        while True:
            response = await client(
                functions.payments.GetSavedStarGiftsRequest(
                    peer=peer,
                    offset=offset,
                    limit=100,
                    exclude_unique=True,
                    exclude_unupgradable=True,
                    exclude_hosted=True,
                )
            )
            for saved in response.gifts:
                snapshot = self._saved_to_snapshot(saved)
                if snapshot and snapshot.can_upgrade:
                    items.append(snapshot)
            next_offset = getattr(response, "next_offset", None)
            if not next_offset:
                break
            offset = next_offset
        return items

    async def fetch_specific_gifts(
        self, channel_id: int, saved_ids: Iterable[int]
    ) -> list[GiftSnapshot]:
        client = await self.client()
        peer = await client.get_input_entity(channel_id)
        ids = list(dict.fromkeys(int(item) for item in saved_ids))
        result: list[GiftSnapshot] = []
        for start in range(0, len(ids), 100):
            inputs = [
                types.InputSavedStarGiftChat(peer=peer, saved_id=saved_id)
                for saved_id in ids[start : start + 100]
            ]
            response = await client(
                functions.payments.GetSavedStarGiftRequest(stargift=inputs)
            )
            for saved in response.gifts:
                snapshot = self._saved_to_snapshot(saved)
                if snapshot:
                    result.append(snapshot)
        return result

    async def fetch_catalog_numbers(self, concurrency: int = 6) -> tuple[list[CatalogNumber], float]:
        client = await self.client()
        started = perf_counter()
        catalog = await client(functions.payments.GetStarGiftsRequest(hash=0))
        gifts = [
            gift
            for gift in getattr(catalog, "gifts", [])
            if isinstance(gift, types.StarGift)
            and getattr(gift, "upgrade_stars", None) is not None
        ]
        semaphore = asyncio.Semaphore(max(1, concurrency))

        async def resolve(gift) -> CatalogNumber:
            title = getattr(gift, "title", None) or f"Gift {gift.id}"
            slug_base = slug_base_from_title(title)
            if not slug_base:
                return CatalogNumber(gift.id, title, None, None, None, "slug unavailable")
            try:
                async with semaphore:
                    response = await client(
                        functions.payments.GetUniqueStarGiftRequest(
                            slug=f"{slug_base}-1"
                        )
                    )
                unique = response.gift
                return CatalogNumber(
                    gift_id=gift.id,
                    title=title,
                    issued=getattr(unique, "availability_issued", None),
                    total=getattr(unique, "availability_total", None),
                    slug_base=slug_base,
                )
            except FloodWaitError as exc:
                return CatalogNumber(gift.id, title, None, None, slug_base, f"flood {exc.seconds}s")
            except RPCError as exc:
                return CatalogNumber(gift.id, title, None, None, slug_base, exc.__class__.__name__)

        results = await asyncio.gather(*(resolve(gift) for gift in gifts))
        results.sort(key=lambda item: item.title.casefold())
        return results, (perf_counter() - started) * 1000

    async def mtproto_ping_ms(self) -> float:
        client = await self.client()
        started = perf_counter()
        await client(functions.help.GetNearestDcRequest())
        return (perf_counter() - started) * 1000

    async def upgrade_gift(
        self,
        channel_id: int,
        snapshot: GiftSnapshot,
        *,
        live: bool,
        max_stars: int,
    ) -> tuple[bool, str, int | None]:
        """Upgrade exactly one verified gift. No blind retry is performed."""
        if snapshot.gift_num is None:
            return False, "Telegram не предоставил номер", None
        if not snapshot.can_upgrade:
            return False, "Подарок уже нельзя улучшить", None

        client = await self.client()
        peer = await client.get_input_entity(channel_id)
        stargift = types.InputSavedStarGiftChat(peer=peer, saved_id=snapshot.saved_id)

        if not live:
            return True, f"DRY-RUN: найден #{snapshot.gift_num}", 0

        # Prepaid upgrades use a direct method. If the hint is stale and payment
        # is required, Telegram returns PAYMENT_REQUIRED and we fall back below.
        if snapshot.prepaid_upgrade:
            try:
                await client(
                    functions.payments.UpgradeStarGiftRequest(
                        stargift=stargift,
                        keep_original_details=False,
                    )
                )
                return True, f"Улучшено #{snapshot.gift_num} (предоплачено)", 0
            except RPCError as exc:
                if "PAYMENT_REQUIRED" not in str(exc).upper():
                    raise

        invoice = types.InputInvoiceStarGiftUpgrade(
            stargift=stargift,
            keep_original_details=False,
        )
        form = await client(functions.payments.GetPaymentFormRequest(invoice=invoice))
        currency = getattr(form.invoice, "currency", None)
        prices = getattr(form.invoice, "prices", [])
        amount = sum(int(price.amount) for price in prices)
        payment_error = validate_upgrade_payment(
            currency=currency,
            amount=amount,
            expected_amount=snapshot.upgrade_stars,
            max_amount=max_stars,
        )
        if payment_error:
            logger.warning(
                "upgrade_payment_rejected saved_id=%s expected=%s actual=%s currency=%s reason=%s",
                snapshot.saved_id,
                snapshot.upgrade_stars,
                amount,
                currency,
                payment_error,
            )
            return False, payment_error, amount
        await client(
            functions.payments.SendStarsFormRequest(
                form_id=form.form_id,
                invoice=invoice,
            )
        )
        return True, f"Улучшено #{snapshot.gift_num}", amount

    @staticmethod
    def _saved_to_snapshot(saved) -> GiftSnapshot | None:
        gift = getattr(saved, "gift", None)
        if gift is None or isinstance(gift, types.StarGiftUnique):
            return None
        gift_id = getattr(gift, "id", None)
        saved_id = getattr(saved, "saved_id", None)
        if gift_id is None or saved_id is None:
            return None
        title = getattr(gift, "title", None) or f"Gift {gift_id}"
        prepaid = bool(
            getattr(saved, "upgrade_separate", False)
            or getattr(saved, "upgrade_stars", None) is not None
        )
        return GiftSnapshot(
            saved_id=int(saved_id),
            gift_id=int(gift_id),
            title=title,
            can_upgrade=bool(getattr(saved, "can_upgrade", False)),
            gift_num=getattr(saved, "gift_num", None),
            upgrade_stars=getattr(gift, "upgrade_stars", None),
            prepaid_upgrade=prepaid,
        )

# ===== scanner.py =====
import asyncio
import logging
from collections.abc import Awaitable, Callable
from time import monotonic, perf_counter
from typing import TYPE_CHECKING, Any

try:
    from telethon.errors import FloodWaitError, RPCError
except ImportError:  # Allows pure unit tests without network-installed integrations.
    class RPCError(Exception):
        pass

    class FloodWaitError(RPCError):
        seconds = 1


TelegramUserServiceType = Any

logger = logging.getLogger(__name__)

StatusCallback = Callable[[str], Awaitable[None]]
NotifyCallback = Callable[[str], Awaitable[None]]


class ScannerService:
    def __init__(
        self,
        config: AppConfig,
        store: SettingsStore,
        telegram: TelegramUserService,
    ):
        self.config = config
        self.store = store
        self.telegram = telegram
        self.runtime = ScannerRuntime()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._purchase_lock = asyncio.Lock()
        self._status_callback: StatusCallback | None = None
        self._notify_callback: NotifyCallback | None = None
        self._tracked_saved_ids: list[int] = []
        self._last_status_at = 0.0

    def set_callbacks(
        self,
        status_callback: StatusCallback,
        notify_callback: NotifyCallback,
    ) -> None:
        self._status_callback = status_callback
        self._notify_callback = notify_callback

    async def start(self) -> None:
        if self._task and not self._task.done():
            raise RuntimeError("Сканер уже работает")
        settings = self.store.snapshot()
        if not settings.channel_id:
            raise RuntimeError("Канал не выбран")
        if not settings.selected_gift_ids:
            raise RuntimeError("Подарки не выбраны")
        if not settings.target_numbers:
            raise RuntimeError("Целевые номера не заданы")

        all_gifts = await self.telegram.fetch_channel_gifts(settings.channel_id)
        selected = set(settings.selected_gift_ids)
        self._tracked_saved_ids = [gift.saved_id for gift in all_gifts if gift.gift_id in selected]
        if not self._tracked_saved_ids:
            raise RuntimeError("На канале нет выбранных неулучшенных подарков")

        self._stop.clear()
        self.runtime = ScannerRuntime(
            active=True,
            started_at_monotonic=monotonic(),
        )
        self._task = asyncio.create_task(self._run(), name="gift-scanner")
        logger.info(
            "scanner_started channel_id=%s gift_ids=%s targets=%s live=%s tracked=%s",
            settings.channel_id,
            settings.selected_gift_ids,
            settings.target_numbers,
            settings.live_upgrades,
            len(self._tracked_saved_ids),
        )
        await self._emit_status(force=True)

    async def stop(self, reason: str = "manual") -> None:
        self._stop.set()
        task = self._task
        if task and task is not asyncio.current_task():
            try:
                await asyncio.wait_for(task, timeout=5)
            except asyncio.TimeoutError:
                task.cancel()
            except asyncio.CancelledError:
                pass
        self.runtime.active = False
        logger.info("scanner_stopped reason=%s", reason)
        await self._emit_status(force=True)

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                cycle_started = perf_counter()
                settings = self.store.snapshot()
                if not settings.channel_id:
                    raise RuntimeError("Канал сброшен во время работы")
                try:
                    gifts = await self.telegram.fetch_specific_gifts(
                        settings.channel_id, self._tracked_saved_ids
                    )
                    self.runtime.checks += 1
                    changed = self._record_numbers(gifts)
                    selected = set(settings.selected_gift_ids)
                    targets = set(settings.target_numbers)
                    for gift in gifts:
                        if should_upgrade(gift, selected, targets):
                            success = await self._attempt(settings.channel_id, gift, targets)
                            if success and self.config.stop_after_success:
                                self._stop.set()
                                break
                    self.runtime.last_error = None
                    self.runtime.last_cycle_ms = (perf_counter() - cycle_started) * 1000
                    if changed:
                        await self._emit_status(force=True)
                    else:
                        await self._emit_status(force=False)
                except FloodWaitError as exc:
                    self.runtime.last_error = f"FloodWait {exc.seconds}с"
                    logger.warning("scanner_flood_wait seconds=%s", exc.seconds)
                    await self._notify(f"⏳ Telegram ограничил запросы на {exc.seconds}с. Сканер ждёт.")
                    await asyncio.sleep(exc.seconds)
                except (asyncio.TimeoutError, ConnectionError) as exc:
                    self.runtime.last_error = type(exc).__name__
                    logger.exception("scanner_network_error")
                    await asyncio.sleep(1)
                except RPCError as exc:
                    self.runtime.last_error = exc.__class__.__name__
                    logger.exception("scanner_rpc_error")
                    await self._notify(f"⚠️ Ошибка Telegram: {exc.__class__.__name__}")
                    await asyncio.sleep(1)
                except Exception as exc:
                    self.runtime.last_error = type(exc).__name__
                    logger.exception("scanner_fatal_error")
                    await self._notify(f"⛔ Сканер остановлен: {type(exc).__name__}: {exc}")
                    self._stop.set()
                    break

                elapsed = perf_counter() - cycle_started
                sleep_seconds = max(0.0, self.config.scan_interval_ms / 1000 - elapsed)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep_seconds)
                except asyncio.TimeoutError:
                    pass
        finally:
            self.runtime.active = False
            await self._emit_status(force=True)

    def _record_numbers(self, gifts: list[GiftSnapshot]) -> bool:
        changed = False
        for gift in gifts:
            previous = self.runtime.current_numbers.get(gift.saved_id)
            self.runtime.current_titles[gift.saved_id] = gift.title
            self.runtime.current_numbers[gift.saved_id] = gift.gift_num
            if previous != gift.gift_num:
                changed = True
                logger.info(
                    "number_changed saved_id=%s gift_id=%s title=%r old=%s new=%s",
                    gift.saved_id,
                    gift.gift_id,
                    gift.title,
                    previous,
                    gift.gift_num,
                )
        return changed

    async def _attempt(self, channel_id: int, gift: GiftSnapshot, targets: set[int]) -> bool:
        async with self._purchase_lock:
            # Re-fetch exactly this gift immediately before the paid call.
            current_items = await self.telegram.fetch_specific_gifts(channel_id, [gift.saved_id])
            if len(current_items) != 1:
                logger.warning("target_recheck_missing saved_id=%s", gift.saved_id)
                return False
            current = current_items[0]
            if (
                not current.can_upgrade
                or current.gift_num not in targets
                or current.gift_id != gift.gift_id
            ):
                logger.info(
                    "target_recheck_rejected saved_id=%s gift_id=%s number=%s",
                    current.saved_id,
                    current.gift_id,
                    current.gift_num,
                )
                return False

            # Re-read the switch immediately before the payment call. Turning it off
            # in the bot therefore closes the spending path without restarting.
            live = self.store.snapshot().live_upgrades
            logger.warning(
                "target_matched saved_id=%s gift_id=%s number=%s price=%s live=%s",
                current.saved_id,
                current.gift_id,
                current.gift_num,
                current.upgrade_stars,
                live,
            )
            try:
                ok, message, spent = await self.telegram.upgrade_gift(
                    channel_id,
                    current,
                    live=live,
                    max_stars=self.config.max_upgrade_stars,
                )
            except (asyncio.TimeoutError, ConnectionError):
                # Never retry a possibly-submitted payment blindly.
                self._stop.set()
                logger.exception("upgrade_result_unknown saved_id=%s", current.saved_id)
                await self._notify(
                    "⚠️ Результат оплаты неизвестен из-за сетевой ошибки. "
                    "Сканер остановлен — проверь подарок и баланс вручную."
                )
                return False

            if ok:
                self.runtime.last_success = message
                mode = "БОЕВОЙ" if live else "DRY-RUN"
                logger.warning(
                    "upgrade_success mode=%s saved_id=%s number=%s spent=%s",
                    mode,
                    current.saved_id,
                    current.gift_num,
                    spent,
                )
                await self._notify(
                    f"✅ {message}\n"
                    f"🎁 {current.title}\n"
                    f"💫 Номер: #{current.gift_num}\n"
                    f"💳 Потрачено: {spent or 0} ⭐\n"
                    f"Режим: {mode}"
                )
                return True

            logger.warning("upgrade_rejected saved_id=%s reason=%s", current.saved_id, message)
            await self._notify(f"⛔ Совпадение найдено, но улучшение отменено: {message}")
            self._stop.set()
            return False

    async def _emit_status(self, *, force: bool) -> None:
        if not self._status_callback:
            return
        now = monotonic()
        if not force and now - self._last_status_at < 2:
            return
        self._last_status_at = now
        try:
            await self._status_callback(self.status_text())
        except Exception:
            logger.exception("status_update_failed")

    async def _notify(self, text: str) -> None:
        if self._notify_callback:
            try:
                await self._notify_callback(text)
            except Exception:
                logger.exception("notification_failed")

    def status_text(self) -> str:
        settings = self.store.snapshot()
        state = "🟢 активен" if self.runtime.active else "⚪ остановлен"
        mode = "LIVE" if settings.live_upgrades else "DRY-RUN"
        by_title: dict[str, list[int | None]] = {}
        for saved_id, number in self.runtime.current_numbers.items():
            title = self.runtime.current_titles.get(saved_id, str(saved_id))
            by_title.setdefault(title, []).append(number)
        number_lines: list[str] = []
        for title, numbers in sorted(by_title.items()):
            known = sorted(number for number in numbers if number is not None)
            shown = ", ".join(f"#{number}" for number in known) if known else "—"
            number_lines.append(f"• {title}: {shown}")
        if not number_lines:
            number_lines.append("• номера ещё не получены")
        targets = ", ".join(map(str, settings.target_numbers)) or "не заданы"
        cycle = f"{self.runtime.last_cycle_ms:.0f} мс" if self.runtime.last_cycle_ms is not None else "—"
        error = f"\nОшибка: {self.runtime.last_error}" if self.runtime.last_error else ""
        return (
            f"🎯 Gift Hunter {VERSION}\n"
            f"Сканер: {state}\n"
            f"Режим: {mode}\n"
            f"Канал: {settings.channel_title or 'не выбран'}\n"
            f"Цели: {targets}\n"
            f"Проверок: {self.runtime.checks}\n"
            f"Последний цикл: {cycle}\n\n"
            + "\n".join(number_lines)
            + error
        )

# ===== services.py =====
import time
from dataclasses import dataclass, field

from aiogram import Bot



@dataclass(slots=True)
class AppServices:
    config: AppConfig
    bot: Bot
    store: SettingsStore
    telegram: TelegramUserService
    scanner: ScannerService
    logs: LogManager
    started_monotonic: float = field(default_factory=time.monotonic)

# ===== keyboards.py =====
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup


BTN_GIFTS = "🎁 Подарки"
BTN_CHECK_NUMBERS = "🔢 Проверить номера"
BTN_SET_NUMBERS = "🎯 Задать номера"
BTN_START = "▶️ Запустить"
BTN_STOP = "⛔ Остановить"
BTN_PING = "📡 Ping"
BTN_LOG = "📄 Log"
BTN_RESET = "🗑 Сброс"
BTN_MODE_OFF = "🛡 Оплата: ВЫКЛ"
BTN_MODE_ON = "💳 Оплата: ВКЛ"


def main_keyboard(active: bool, live_upgrades: bool = False) -> ReplyKeyboardMarkup:
    run_button = BTN_STOP if active else BTN_START
    mode_button = BTN_MODE_ON if live_upgrades else BTN_MODE_OFF
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_GIFTS), KeyboardButton(text=BTN_CHECK_NUMBERS)],
            [KeyboardButton(text=BTN_SET_NUMBERS), KeyboardButton(text=run_button)],
            [KeyboardButton(text=mode_button), KeyboardButton(text=BTN_PING)],
            [KeyboardButton(text=BTN_LOG), KeyboardButton(text=BTN_RESET)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Gift Hunter v0002",
    )



def setup_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_RESET)]],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Настройка Gift Hunter v0002",
    )

def auth_check_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Проверить авторизацию", callback_data="auth:check")]
        ]
    )


def channels_keyboard(channels: list[ChannelInfo]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=channel.title[:45], callback_data=f"channel:{channel.id}")]
        for channel in channels
    ]
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="channel:refresh")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def gifts_keyboard(collections: list[GiftCollection], selected: set[int]) -> InlineKeyboardMarkup:
    rows = []
    for collection in collections:
        mark = "✅" if collection.gift_id in selected else "▫️"
        label = f"{mark} {collection.title} · {collection.count} шт."
        rows.append(
            [InlineKeyboardButton(text=label[:60], callback_data=f"gift:{collection.gift_id}")]
        )
    rows.append(
        [
            InlineKeyboardButton(text="✅ Готово", callback_data="gift:done"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="gift:refresh"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def reset_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ Да, удалить всё", callback_data="reset:confirm")],
            [InlineKeyboardButton(text="Отмена", callback_data="reset:cancel")],
        ]
    )

# ===== handlers.py =====
import html
import json
import logging
import shutil
import socket
import tempfile
import zipfile
from pathlib import Path
from time import monotonic, perf_counter

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, Message, ReplyKeyboardRemove


logger = logging.getLogger(__name__)
router = Router(name="gift-hunter")


class SetupStates(StatesGroup):
    pin = State()
    api_id = State()
    api_hash = State()
    phone = State()
    target_numbers = State()


async def _delete_secret(message: Message) -> None:
    try:
        await message.delete()
    except TelegramBadRequest:
        pass


def _is_owner(message_or_callback, services: AppServices) -> bool:
    user = message_or_callback.from_user
    owner = services.store.snapshot().owner_user_id
    return bool(user and owner and user.id == owner)


async def _deny(event) -> None:
    if isinstance(event, CallbackQuery):
        await event.answer("Нет доступа", show_alert=True)
    else:
        await event.answer("Нет доступа")


async def _continue_setup(message: Message, state: FSMContext, services: AppServices) -> None:
    settings = services.store.snapshot()
    if not settings.api_id:
        await state.set_state(SetupStates.api_id)
        await message.answer("Отправь TG_API_ID с my.telegram.org. Сообщение будет удалено.", reply_markup=setup_keyboard())
        return
    if not settings.api_hash:
        await state.set_state(SetupStates.api_hash)
        await message.answer("Отправь TG_API_HASH. Сообщение будет удалено.", reply_markup=setup_keyboard())
        return
    if not settings.phone:
        await state.set_state(SetupStates.phone)
        await message.answer("Отправь номер телефона в формате +79991234567. Сообщение будет удалено.", reply_markup=setup_keyboard())
        return

    authorized, user_id, name = await services.telegram.check_authorized()
    if authorized:
        if user_id != settings.owner_user_id:
            await message.answer(
                "⛔ Сессия принадлежит другому Telegram-аккаунту. Нажми «Сброс» и авторизуй нужный аккаунт."
            )
            return
        await state.clear()
        await _discover_channel(message, services)
        return

    # The CLI opens the same SQLite session. Release the bot process handle first.
    await services.telegram.close()
    container_id = socket.gethostname()
    command = f"docker exec -it {container_id} python main.py auth"
    await state.clear()
    await message.answer(
        "Данные сохранены. Код входа нельзя отправлять в Telegram-чат: Telegram его аннулирует.\n\n"
        "Открой Termius на VPS и выполни:\n"
        f"<code>{command}</code>\n\n"
        "В Termius введи код и пароль 2FA. Затем нажми кнопку ниже.",
        reply_markup=auth_check_keyboard(),
        parse_mode="HTML",
    )


async def _discover_channel(message: Message, services: AppServices) -> None:
    channels = await services.telegram.find_owner_channels()
    if not channels:
        await message.answer(
            "Аккаунт подключён, но каналов, где он является владельцем, не найдено.",
            reply_markup=main_keyboard(services.scanner.runtime.active, services.store.snapshot().live_upgrades),
        )
        return
    if len(channels) == 1:
        channel = channels[0]
        await services.store.update(
            lambda value: (
                setattr(value, "channel_id", channel.id),
                setattr(value, "channel_title", channel.title),
            )
        )
        await message.answer(
            f"✅ Аккаунт подключён\n📢 Канал выбран автоматически: {channel.title}",
            reply_markup=main_keyboard(services.scanner.runtime.active, services.store.snapshot().live_upgrades),
        )
        return
    await message.answer("Выбери канал, где аккаунт является Owner:", reply_markup=channels_keyboard(channels))


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext, services: AppServices) -> None:
    if not message.from_user:
        return
    settings = services.store.snapshot()
    if settings.owner_user_id is None:
        if services.config.setup_pin_value:
            await state.set_state(SetupStates.pin)
            await message.answer("Введи SETUP_PIN из Coolify:")
            return
        await services.store.update(lambda value: setattr(value, "owner_user_id", message.from_user.id))
        logger.info("owner_bound user_id=%s", message.from_user.id)
    elif settings.owner_user_id != message.from_user.id:
        await _deny(message)
        return

    await _continue_setup(message, state, services)


@router.message(SetupStates.pin)
async def pin_handler(message: Message, state: FSMContext, services: AppServices) -> None:
    if not message.from_user:
        return
    pin = (message.text or "").strip()
    await _delete_secret(message)
    if pin != services.config.setup_pin_value:
        await message.answer("Неверный SETUP_PIN.")
        return
    await services.store.update(lambda value: setattr(value, "owner_user_id", message.from_user.id))
    logger.info("owner_bound_with_pin user_id=%s", message.from_user.id)
    await _continue_setup(message, state, services)


@router.message(SetupStates.api_id, F.text != BTN_RESET)
async def api_id_handler(message: Message, state: FSMContext, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    raw = (message.text or "").strip()
    await _delete_secret(message)
    if not raw.isdecimal() or int(raw) <= 0:
        await message.answer("TG_API_ID должен быть положительным числом.")
        return
    await services.store.update(lambda value: setattr(value, "api_id", int(raw)))
    await _continue_setup(message, state, services)


@router.message(SetupStates.api_hash, F.text != BTN_RESET)
async def api_hash_handler(message: Message, state: FSMContext, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    raw = (message.text or "").strip()
    await _delete_secret(message)
    if len(raw) < 20 or len(raw) > 128:
        await message.answer("TG_API_HASH выглядит некорректно.")
        return
    await services.store.update(lambda value: setattr(value, "api_hash", raw))
    await _continue_setup(message, state, services)


@router.message(SetupStates.phone, F.text != BTN_RESET)
async def phone_handler(message: Message, state: FSMContext, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    raw = (message.text or "").strip().replace(" ", "")
    await _delete_secret(message)
    if not raw.startswith("+") or not raw[1:].isdigit() or not 8 <= len(raw[1:]) <= 15:
        await message.answer("Формат номера: +79991234567")
        return
    await services.store.update(lambda value: setattr(value, "phone", raw))
    await _continue_setup(message, state, services)


@router.callback_query(F.data == "auth:check")
async def auth_check_handler(callback: CallbackQuery, state: FSMContext, services: AppServices) -> None:
    if not _is_owner(callback, services):
        await _deny(callback)
        return
    authorized, user_id, name = await services.telegram.check_authorized()
    settings = services.store.snapshot()
    if not authorized:
        await callback.answer("Сессия ещё не авторизована", show_alert=True)
        return
    if user_id != settings.owner_user_id:
        await callback.answer("Авторизован другой аккаунт", show_alert=True)
        return
    await callback.answer("Аккаунт подключён")
    if callback.message:
        await callback.message.answer(f"✅ Авторизация успешна: {name or user_id}")
        await _discover_channel(callback.message, services)
    await state.clear()


@router.callback_query(F.data.startswith("channel:"))
async def channel_handler(callback: CallbackQuery, services: AppServices) -> None:
    if not _is_owner(callback, services):
        await _deny(callback)
        return
    action = callback.data.split(":", 1)[1]
    channels = await services.telegram.find_owner_channels()
    if action == "refresh":
        if callback.message:
            await callback.message.edit_reply_markup(reply_markup=channels_keyboard(channels))
        await callback.answer("Обновлено")
        return
    channel_id = int(action)
    channel = next((item for item in channels if item.id == channel_id), None)
    if not channel:
        await callback.answer("Канал не найден", show_alert=True)
        return
    await services.store.update(
        lambda value: (
            setattr(value, "channel_id", channel.id),
            setattr(value, "channel_title", channel.title),
            setattr(value, "selected_gift_ids", []),
        )
    )
    await callback.answer("Канал выбран")
    if callback.message:
        await callback.message.answer(
            f"📢 Выбран канал: {channel.title}",
            reply_markup=main_keyboard(services.scanner.runtime.active, services.store.snapshot().live_upgrades),
        )


@router.message(F.text == BTN_GIFTS)
async def gifts_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    settings = services.store.snapshot()
    if not settings.channel_id:
        await _discover_channel(message, services)
        return
    started = perf_counter()
    gifts = await services.telegram.fetch_channel_gifts(settings.channel_id)
    collections = group_gifts(gifts)
    if not collections:
        await message.answer("На канале нет неулучшенных подарков, доступных для улучшения.")
        return
    lines = ["🎁 Подарки канала:", ""]
    selected = set(settings.selected_gift_ids)
    lines.extend(format_collection_line(item, item.gift_id in selected) for item in collections)
    lines.append(f"\nПроверено за {(perf_counter() - started) * 1000:.0f} мс")
    await message.answer("\n".join(lines), reply_markup=gifts_keyboard(collections, selected))


@router.callback_query(F.data.startswith("gift:"))
async def gift_callback_handler(callback: CallbackQuery, services: AppServices) -> None:
    if not _is_owner(callback, services):
        await _deny(callback)
        return
    action = callback.data.split(":", 1)[1]
    settings = services.store.snapshot()
    if not settings.channel_id:
        await callback.answer("Канал не выбран", show_alert=True)
        return
    if action == "done":
        await callback.answer("Выбор сохранён")
        if callback.message:
            await callback.message.answer(
                "Теперь задай номера через кнопку «🎯 Задать номера».",
                reply_markup=main_keyboard(services.scanner.runtime.active, services.store.snapshot().live_upgrades),
            )
        return
    gifts = await services.telegram.fetch_channel_gifts(settings.channel_id)
    collections = group_gifts(gifts)
    if action != "refresh":
        gift_id = int(action)
        selected = set(settings.selected_gift_ids)
        if gift_id in selected:
            selected.remove(gift_id)
        else:
            selected.add(gift_id)
        await services.store.update(lambda value: setattr(value, "selected_gift_ids", sorted(selected)))
    selected = set(services.store.snapshot().selected_gift_ids)
    if callback.message:
        await callback.message.edit_reply_markup(reply_markup=gifts_keyboard(collections, selected))
    await callback.answer("Обновлено")


@router.message(F.text == BTN_SET_NUMBERS)
async def set_numbers_handler(message: Message, state: FSMContext, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    await state.set_state(SetupStates.target_numbers)
    await message.answer("Отправь номера через запятую, пробел или с новой строки.\nПример: 3333, 3666, 4444")


@router.message(SetupStates.target_numbers, F.text != BTN_RESET)
async def target_numbers_handler(message: Message, state: FSMContext, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    try:
        numbers = parse_target_numbers(message.text or "")
    except ValueError as exc:
        await message.answer(str(exc))
        return
    await services.store.update(lambda value: setattr(value, "target_numbers", numbers))
    await state.clear()
    await message.answer(
        "🎯 Целевые номера: " + ", ".join(map(str, numbers)),
        reply_markup=main_keyboard(services.scanner.runtime.active, services.store.snapshot().live_upgrades),
    )


@router.message(F.text == BTN_CHECK_NUMBERS)
async def check_numbers_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    progress = await message.answer("🔢 Проверяю текущие номера всех коллекций…")
    try:
        items, elapsed_ms = await services.telegram.fetch_catalog_numbers(
            services.config.catalog_concurrency
        )
    except Exception as exc:
        logger.exception("catalog_scan_failed")
        await progress.edit_text(f"Ошибка проверки: {type(exc).__name__}: {exc}")
        return
    lines = [
        f"🔢 Последние выданные номера · {VERSION}",
        f"Проверено коллекций: {len(items)} · {elapsed_ms:.0f} мс",
        "",
    ]
    for item in items:
        if item.issued is not None:
            lines.append(f"{item.title} — {item.issued}")
        else:
            lines.append(f"{item.title} — нет данных")
    chunks = chunk_text(lines)
    await progress.edit_text(chunks[0])
    for chunk in chunks[1:]:
        await message.answer(chunk)
    logger.info("catalog_scan_completed count=%s elapsed_ms=%.0f", len(items), elapsed_ms)


@router.message(F.text == BTN_START)
async def scanner_start_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    try:
        status = await message.answer("Запускаю сканер…")
        await services.store.update(lambda value: setattr(value, "status_message_id", status.message_id))
        await services.scanner.start()
        await message.answer(
            "🟢 Сканер запущен. "
            + ("Платные улучшения включены." if services.store.snapshot().live_upgrades else "Сейчас DRY-RUN: Stars не списываются."),
            reply_markup=main_keyboard(True, services.store.snapshot().live_upgrades),
        )
    except Exception as exc:
        logger.exception("scanner_start_failed")
        await message.answer(f"Не удалось запустить: {exc}")


@router.message(F.text == BTN_STOP)
async def scanner_stop_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    await services.scanner.stop("manual")
    await message.answer("⛔ Сканер остановлен.", reply_markup=main_keyboard(False, services.store.snapshot().live_upgrades))


@router.message(F.text == BTN_PING)
async def ping_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    bot_started = perf_counter()
    await services.bot.get_me()
    bot_ms = (perf_counter() - bot_started) * 1000
    try:
        mtproto_ms = await services.telegram.mtproto_ping_ms()
        mtproto = f"{mtproto_ms:.0f} мс"
    except Exception:
        mtproto = "не подключён"
    runtime = services.scanner.runtime
    cycle = f"{runtime.last_cycle_ms:.0f} мс" if runtime.last_cycle_ms is not None else "—"
    await message.answer(
        f"📡 Ping · {VERSION}\n"
        f"Uptime: {format_uptime(monotonic() - services.started_monotonic)}\n"
        f"Bot API: {bot_ms:.0f} мс\n"
        f"MTProto: {mtproto}\n"
        f"Последний цикл: {cycle}\n"
        f"Сканер: {'активен' if runtime.active else 'остановлен'}\n"
        f"Оплата: {'ВКЛ' if services.store.snapshot().live_upgrades else 'ВЫКЛ'}"
    )


@router.message(F.text.in_({BTN_MODE_OFF, BTN_MODE_ON}))
async def live_mode_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    current = services.store.snapshot().live_upgrades
    if not current and services.scanner.runtime.active:
        await message.answer(
            "Сначала останови сканер. Боевой режим включается только при остановленном сканере.",
            reply_markup=main_keyboard(True, False),
        )
        return
    new_value = not current
    await services.store.update(lambda value: setattr(value, "live_upgrades", new_value))
    logger.warning("live_mode_changed enabled=%s", new_value)
    if new_value:
        text = (
            "💳 Оплата ВКЛ. При точном совпадении бот сможет списать Stars. "
            "Для начала охоты отдельно нажми «▶️ Запустить»."
        )
    else:
        text = "🛡 Оплата ВЫКЛ. Совпадения фиксируются без списания Stars."
    await message.answer(
        text,
        reply_markup=main_keyboard(services.scanner.runtime.active, new_value),
    )


@router.message(F.text == BTN_LOG)
async def log_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    lines = services.logs.tail(30)
    text = "📄 Последние события:\n\n" + ("\n".join(lines) if lines else "Лог пока пуст.")
    for chunk in chunk_text(text.splitlines()):
        await message.answer(f"<pre>{html.escape(chunk)}</pre>", parse_mode="HTML")


@router.message(Command("log_full"))
async def log_full_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    services.logs.flush()
    temp_dir = Path(tempfile.mkdtemp(prefix="gift-hunter-log-"))
    try:
        diagnostics = {
            "version": VERSION,
            "live_upgrades": services.store.snapshot().live_upgrades,
            "max_upgrade_stars": services.config.max_upgrade_stars,
            "scan_interval_ms": services.config.scan_interval_ms,
            "runtime": {
                "active": services.scanner.runtime.active,
                "checks": services.scanner.runtime.checks,
                "last_cycle_ms": services.scanner.runtime.last_cycle_ms,
                "last_error": services.scanner.runtime.last_error,
            },
            "settings": {
                "owner_user_id": services.store.snapshot().owner_user_id,
                "channel_id": services.store.snapshot().channel_id,
                "channel_title": services.store.snapshot().channel_title,
                "selected_gift_ids": services.store.snapshot().selected_gift_ids,
                "target_numbers": services.store.snapshot().target_numbers,
                "api_id_present": bool(services.store.snapshot().api_id),
                "api_hash_present": bool(services.store.snapshot().api_hash),
                "phone_present": bool(services.store.snapshot().phone),
            },
        }
        (temp_dir / "diagnostics.json").write_text(
            json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        for path in services.logs.log_dir.glob("gift-hunter.log*"):
            if path.is_file():
                shutil.copy2(path, temp_dir / path.name)
        archive = temp_dir.with_suffix(".zip")
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as output:
            for path in temp_dir.iterdir():
                output.write(path, arcname=path.name)
        await message.answer_document(
            FSInputFile(archive, filename=f"gift-hunter-{VERSION}-logs.zip"),
            caption="Полный лог без токенов, API_HASH, телефона, кода и session-файла.",
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        archive_path = temp_dir.with_suffix(".zip")
        if archive_path.exists():
            archive_path.unlink()


@router.message(F.text == BTN_RESET)
async def reset_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    await message.answer(
        "⚠️ Полный сброс остановит сканер, завершит MTProto-сессию и удалит настройки и логи.\n"
        "Канал, подарки и Telegram-аккаунт не удаляются.",
        reply_markup=reset_keyboard(),
    )


@router.callback_query(F.data.startswith("reset:"))
async def reset_callback_handler(callback: CallbackQuery, state: FSMContext, services: AppServices) -> None:
    if not _is_owner(callback, services):
        await _deny(callback)
        return
    action = callback.data.split(":", 1)[1]
    if action == "cancel":
        await callback.answer("Отменено")
        if callback.message:
            await callback.message.delete()
        return
    await callback.answer("Сбрасываю…")
    await services.scanner.stop("full_reset")
    remote_logout_ok = await services.telegram.logout_and_delete()
    await services.store.clear()
    await state.clear()
    services.logs.clear()
    if callback.message:
        warning = (
            ""
            if remote_logout_ok
            else "\n⚠️ Локальная сессия удалена, но Telegram не подтвердил удалённый logout. Проверь Настройки → Устройства."
        )
        await callback.message.answer(
            f"✅ Gift Hunter {VERSION} полностью сброшен.\nОтправь /start для новой настройки.{warning}",
            reply_markup=ReplyKeyboardRemove(),
        )


@router.message(Command("status"))
async def status_handler(message: Message, services: AppServices) -> None:
    if not _is_owner(message, services):
        await _deny(message)
        return
    await message.answer(
        services.scanner.status_text(),
        reply_markup=main_keyboard(services.scanner.runtime.active, services.store.snapshot().live_upgrades),
    )


@router.message()
async def fallback_handler(message: Message, services: AppServices) -> None:
    settings = services.store.snapshot()
    if settings.owner_user_id and not _is_owner(message, services):
        return
    await message.answer(
        "Используй кнопки меню или /start.",
        reply_markup=main_keyboard(services.scanner.runtime.active, services.store.snapshot().live_upgrades),
    )

# ===== auth CLI =====
import asyncio
import getpass
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError



async def run_auth_cli() -> int:
    config = AppConfig()
    store = SettingsStore(config.data_dir)
    settings = await store.load()
    if not settings.api_id or not settings.api_hash or not settings.phone:
        print("Сначала отправь TG_API_ID, TG_API_HASH и телефон боту через /start.")
        return 2

    print(f"Gift Hunter {VERSION}: авторизация {settings.phone}")
    client = TelegramClient(str(config.data_dir / "account"), settings.api_id, settings.api_hash)
    try:
        await client.connect()
        if await client.is_user_authorized():
            me = await client.get_me()
            print(f"Уже авторизован: {me.id} @{me.username or '-'}")
            return 0

        sent = await client.send_code_request(settings.phone)
        print("Код отправлен Telegram. Вводи его здесь, в Termius, не в чат бота.")
        code = input("Код: ").strip()
        try:
            await client.sign_in(
                phone=settings.phone,
                code=code,
                phone_code_hash=sent.phone_code_hash,
            )
        except SessionPasswordNeededError:
            password = getpass.getpass("Пароль 2FA: ")
            await client.sign_in(password=password)

        me = await client.get_me()
        if settings.owner_user_id and me.id != settings.owner_user_id:
            print(
                f"ОШИБКА: авторизован аккаунт {me.id}, а бот привязан к {settings.owner_user_id}. "
                "Сессия будет завершена."
            )
            await client.log_out()
            return 3
        print(f"Готово. Аккаунт авторизован: {me.id} @{me.username or '-'}")
        print("Вернись в чат бота и нажми «Проверить авторизацию».")
        return 0
    finally:
        if client.is_connected():
            await client.disconnect()
        session_file = config.data_dir / "account.session"
        if session_file.exists():
            session_file.chmod(0o600)

# ===== bot runtime =====
async def heartbeat(config: AppConfig) -> None:
    path = config.data_dir / "heartbeat.json"
    while True:
        config.data_dir.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_text(
            json.dumps({"timestamp": time.time(), "version": VERSION}),
            encoding="utf-8",
        )
        os.replace(temp, path)
        await asyncio.sleep(10)


async def main() -> None:
    validate_runtime_api()
    config = AppConfig()
    config.data_dir.mkdir(parents=True, exist_ok=True)
    logs = LogManager(config.data_dir, config.log_level)
    logs.start()
    logger = logging.getLogger(__name__)
    logger.info("application_start version=%s", VERSION)

    store = SettingsStore(config.data_dir)
    await store.load()
    bot = Bot(
        token=config.bot_token_value,
        default=DefaultBotProperties(),
    )
    telegram = TelegramUserService(config.data_dir, store.snapshot)
    scanner = ScannerService(config, store, telegram)
    services = AppServices(config, bot, store, telegram, scanner, logs)

    async def status_callback(text: str) -> None:
        settings = store.snapshot()
        if not settings.owner_user_id:
            return
        message_id = settings.status_message_id
        if message_id:
            try:
                await bot.edit_message_text(
                    chat_id=settings.owner_user_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=None,
                )
                return
            except Exception:
                logger.debug("status_edit_failed", exc_info=True)
        sent = await bot.send_message(
            settings.owner_user_id,
            text,
            reply_markup=main_keyboard(scanner.runtime.active, store.snapshot().live_upgrades),
        )
        await store.update(lambda value: setattr(value, "status_message_id", sent.message_id))

    async def notify_callback(text: str) -> None:
        settings = store.snapshot()
        if settings.owner_user_id:
            await bot.send_message(
                settings.owner_user_id,
                text,
                reply_markup=main_keyboard(scanner.runtime.active, store.snapshot().live_upgrades),
            )

    scanner.set_callbacks(status_callback, notify_callback)

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    heartbeat_task = asyncio.create_task(heartbeat(config), name="heartbeat")
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(bot, services=services, allowed_updates=dp.resolve_used_update_types())
    finally:
        await scanner.stop("shutdown")
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task
        await telegram.close()
        await bot.session.close()
        logs.stop()

# ===== command entrypoint =====

def run_healthcheck() -> int:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    heartbeat_path = data_dir / "heartbeat.json"
    if not heartbeat_path.exists():
        return 1
    try:
        payload = json.loads(heartbeat_path.read_text(encoding="utf-8"))
        timestamp = float(payload["timestamp"])
    except Exception:
        return 1
    return 0 if time.time() - timestamp < 45 else 1


def cli() -> None:
    import sys

    command = sys.argv[1].lower() if len(sys.argv) > 1 else "bot"
    if command == "bot":
        with suppress(KeyboardInterrupt):
            asyncio.run(main())
        return
    if command == "auth":
        raise SystemExit(asyncio.run(run_auth_cli()))
    if command == "healthcheck":
        raise SystemExit(run_healthcheck())
    raise SystemExit("Использование: python main.py [bot|auth|healthcheck]")


if __name__ == "__main__":
    cli()
