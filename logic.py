from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Sequence


SLUG_URL_RE = re.compile(r"(?:https?://)?t\.me/nft/([^/?#]+)", re.IGNORECASE)
TRAILING_NUMBER_RE = re.compile(r"^(.*?)-(\d+)$")
TOKEN_RE = re.compile(r"[A-Za-z0-9]+")




def parse_bool(value: object, default: bool = False) -> bool:
    """Parse booleans strictly instead of relying on Python truthiness."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "y", "да"}:
        return True
    if text in {"0", "false", "no", "off", "n", "нет", ""}:
        return False
    return default


def nearest_rank_percentile(values: Sequence[float], percentile: float) -> float | None:
    """Return the nearest-rank percentile for a non-empty sequence."""
    if not values:
        return None
    import math

    ordered = sorted(float(value) for value in values)
    p = min(1.0, max(0.0, float(percentile)))
    rank = max(1, math.ceil(p * len(ordered)))
    return ordered[min(len(ordered) - 1, rank - 1)]

def parse_target_numbers(text: str) -> list[int]:
    """Parse unique positive target numbers while preserving order."""
    values: list[int] = []
    seen: set[int] = set()
    for token in re.findall(r"\d+", text):
        value = int(token)
        if value <= 0 or value in seen:
            continue
        values.append(value)
        seen.add(value)
    return values


def slug_candidates(text: str) -> list[str]:
    """Return likely collectible base slugs from a title, slug or t.me/nft URL."""
    value = (text or "").strip()
    if not value:
        return []

    match = SLUG_URL_RE.search(value)
    if match:
        value = match.group(1)

    value = value.strip().strip("/@")
    numbered = TRAILING_NUMBER_RE.match(value)
    if numbered:
        value = numbered.group(1)

    candidates: list[str] = []

    def add(candidate: str) -> None:
        candidate = re.sub(r"[^A-Za-z0-9]", "", candidate)
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    # Exact alphanumeric spelling first, because Telegram slugs are case-sensitive.
    add(value)

    parts = TOKEN_RE.findall(value)
    if parts:
        add("".join(part[:1].upper() + part[1:] for part in parts))
        add("".join(parts))

    # Common smart-apostrophe and punctuation normalization, e.g. Durov's Glasses.
    cleaned = value.replace("’", "'").replace("`", "'")
    parts = TOKEN_RE.findall(cleaned)
    if parts:
        add("".join(part[:1].upper() + part[1:] for part in parts))

    return candidates


def next_target(targets: Sequence[int], current: int | None) -> int | None:
    if not targets:
        return None
    ordered = sorted(set(int(x) for x in targets if int(x) > 0))
    if current is None:
        return ordered[0] if ordered else None
    for target in ordered:
        if target > current:
            return target
    return None


@dataclass(frozen=True)
class TargetState:
    current: int | None
    target: int | None
    distance: int | None
    should_trigger: bool
    missed: bool


def evaluate_target(current: int | None, target: int | None) -> TargetState:
    if current is None or target is None:
        return TargetState(current, target, None, False, False)
    distance = target - current
    return TargetState(
        current=current,
        target=target,
        distance=distance,
        should_trigger=distance == 1,
        missed=distance <= 0,
    )


def sum_invoice_amount(invoice: object | None) -> int:
    if invoice is None:
        return 0
    total = 0
    prices = getattr(invoice, "prices", None) or []
    for price in prices:
        try:
            total += int(getattr(price, "amount", 0) or 0)
        except (TypeError, ValueError):
            continue
    return total


def find_object_by_class_name(root: object, class_name: str) -> object | None:
    """Find a Telethon TL object recursively without importing Telethon here."""
    seen: set[int] = set()

    def visit(value: object) -> object | None:
        if value is None:
            return None
        identity = id(value)
        if identity in seen:
            return None
        seen.add(identity)

        if value.__class__.__name__ == class_name:
            return value
        if isinstance(value, dict):
            for item in value.values():
                found = visit(item)
                if found is not None:
                    return found
            return None
        if isinstance(value, (list, tuple, set)):
            for item in value:
                found = visit(item)
                if found is not None:
                    return found
            return None
        data = getattr(value, "__dict__", None)
        if isinstance(data, dict):
            for item in data.values():
                found = visit(item)
                if found is not None:
                    return found
        return None

    return visit(root)


def chunks(items: Sequence[object], size: int) -> Iterable[Sequence[object]]:
    if size <= 0:
        raise ValueError("size must be positive")
    for index in range(0, len(items), size):
        yield items[index : index + size]

@dataclass
class AdaptiveRateController:
    """Adaptive sequential polling controller with conservative flood recovery.

    ``current_interval_ms`` is the desired period between starts of polling
    cycles. Request latency is subtracted from the sleep. After FLOOD_WAIT the
    controller holds the slower rate for a cooldown instead of immediately
    accelerating back into another limit.
    """

    min_interval_ms: float = 0.0
    max_interval_ms: float = 2000.0
    start_interval_ms: float = 120.0
    accelerate_every: int = 4
    accelerate_factor: float = 0.75
    backoff_factor: float = 2.0
    backoff_floor_ms: float = 100.0
    flood_cooldown_cycles: int = 80
    current_interval_ms: float = 0.0
    consecutive_successes: int = 0
    flood_count: int = 0
    cooldown_remaining: int = 0

    def __post_init__(self) -> None:
        self.min_interval_ms = max(0.0, float(self.min_interval_ms))
        self.max_interval_ms = max(self.min_interval_ms, float(self.max_interval_ms))
        self.start_interval_ms = min(
            self.max_interval_ms,
            max(self.min_interval_ms, float(self.start_interval_ms)),
        )
        self.accelerate_every = max(1, int(self.accelerate_every))
        self.accelerate_factor = min(0.99, max(0.10, float(self.accelerate_factor)))
        self.backoff_factor = max(1.10, float(self.backoff_factor))
        self.backoff_floor_ms = max(self.min_interval_ms, float(self.backoff_floor_ms))
        self.flood_cooldown_cycles = max(1, int(self.flood_cooldown_cycles))
        self.reset()

    def reset(self) -> None:
        self.current_interval_ms = self.start_interval_ms
        self.consecutive_successes = 0
        self.flood_count = 0
        self.cooldown_remaining = 0

    def on_success(self, *, critical: bool = False) -> float:
        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1
            self.consecutive_successes = 0
            return self.current_interval_ms

        self.consecutive_successes += 1
        threshold = max(1, self.accelerate_every // 2) if critical else self.accelerate_every
        if self.consecutive_successes < threshold:
            return self.current_interval_ms

        self.consecutive_successes = 0
        factor = self.accelerate_factor * (0.90 if critical else 1.0)
        candidate = self.current_interval_ms * factor
        if candidate - self.min_interval_ms < 1.0:
            candidate = self.min_interval_ms
        self.current_interval_ms = max(self.min_interval_ms, candidate)
        return self.current_interval_ms

    def on_flood(self, wait_seconds: float) -> float:
        self.flood_count += 1
        self.consecutive_successes = 0
        wait_seconds = max(0.0, float(wait_seconds))
        wait_based_floor = wait_seconds * 100.0
        candidate = max(
            self.backoff_floor_ms,
            wait_based_floor,
            self.current_interval_ms * self.backoff_factor,
        )
        self.current_interval_ms = min(self.max_interval_ms, candidate)
        self.cooldown_remaining = max(
            self.flood_cooldown_cycles,
            int(wait_seconds * 5),
        )
        return self.current_interval_ms

    def on_transient_error(self) -> float:
        self.consecutive_successes = 0
        candidate = max(self.backoff_floor_ms, self.current_interval_ms * 1.5)
        self.current_interval_ms = min(self.max_interval_ms, candidate)
        self.cooldown_remaining = max(self.cooldown_remaining, self.accelerate_every * 2)
        return self.current_interval_ms

    def sleep_after_cycle_ms(self, elapsed_ms: float) -> float:
        return max(0.0, self.current_interval_ms - max(0.0, float(elapsed_ms)))



def stress_test_interval_ms(
    elapsed_seconds: float,
    *,
    first_phase_seconds: float = 60.0,
    max_phase_starts_seconds: float = 120.0,
    first_interval_ms: float = 300.0,
    second_interval_ms: float = 120.0,
    maximum_interval_ms: float = 0.0,
) -> float:
    """Return the fixed polling period for the five-minute stress profile.

    Minute 1 uses the normal interval, minute 2 uses the faster interval, and
    from minute 3 onward there is no artificial sleep between sequential
    requests. Network latency remains the real lower bound.
    """
    elapsed = max(0.0, float(elapsed_seconds))
    first_end = max(0.0, float(first_phase_seconds))
    max_start = max(first_end, float(max_phase_starts_seconds))
    if elapsed < first_end:
        return max(0.0, float(first_interval_ms))
    if elapsed < max_start:
        return max(0.0, float(second_interval_ms))
    return max(0.0, float(maximum_interval_ms))
