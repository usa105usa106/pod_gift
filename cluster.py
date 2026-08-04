from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import socket
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable


MAX_SHOOTERS = 6
DEFAULT_FIRE_PORT = 45444


def _atomic_write(path: Path, data: str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    os.chmod(tmp, mode)
    tmp.replace(path)
    os.chmod(path, mode)


@dataclass(frozen=True)
class ClusterConfig:
    active_shooters: int = 1
    configured: bool = False
    generation: int = 1

    def normalized(self) -> "ClusterConfig":
        return ClusterConfig(
            active_shooters=min(MAX_SHOOTERS, max(1, int(self.active_shooters))),
            configured=bool(self.configured),
            generation=max(1, int(self.generation)),
        )


@dataclass(frozen=True)
class ConfigRead:
    config: ClusterConfig
    valid: bool
    error: str | None = None


class StableConfigGate:
    """Accept a changed config only after repeated identical valid reads.

    Active participants use ``require_new_generation=True`` so a damaged or
    manually rolled-back file can never be interpreted as a sleep command.
    """

    def __init__(
        self,
        current: ClusterConfig | None = None,
        *,
        required_reads: int = 2,
        require_new_generation: bool = True,
    ) -> None:
        self.current = current
        self.required_reads = max(2, int(required_reads))
        self.require_new_generation = bool(require_new_generation)
        self._candidate: ClusterConfig | None = None
        self._candidate_reads = 0

    def observe(self, result: ConfigRead) -> ClusterConfig | None:
        if not result.valid:
            self._candidate = None
            self._candidate_reads = 0
            return None
        config = result.config
        if self.current == config:
            self._candidate = None
            self._candidate_reads = 0
            return None
        if (
            self.current is not None
            and self.require_new_generation
            and config.generation <= self.current.generation
        ):
            self._candidate = None
            self._candidate_reads = 0
            return None
        if self._candidate == config:
            self._candidate_reads += 1
        else:
            self._candidate = config
            self._candidate_reads = 1
        if self._candidate_reads < self.required_reads:
            return None
        self.current = config
        self._candidate = None
        self._candidate_reads = 0
        return config


class ProvisionStore:
    """Shared cluster state plus optional isolated Bot API token storage.

    ``root`` is visible to every Hunter and contains only control-plane state.
    ``token_root`` is mounted only into Hunter 1; each secondary token lives in
    its own Docker volume and is not exposed through the shared directory.
    """

    def __init__(self, root: Path, token_root: Path | None = None):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.token_root = Path(token_root) if token_root is not None else None
        if self.token_root is not None:
            self.token_root.mkdir(parents=True, exist_ok=True)
        self.config_path = self.root / "cluster.json"
        self.generation_path = self.root / "cluster-generation.json"
        self.event_path = self.root / "cluster-events.jsonl"
        self._last_valid_config: ClusterConfig | None = None

    def read_config(self) -> ConfigRead:
        try:
            raw = json.loads(self.config_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("cluster.json must contain an object")
            active_shooters = int(raw["active_shooters"])
            generation = int(raw["generation"])
            configured = bool(raw.get("configured", False))
            if not 1 <= active_shooters <= MAX_SHOOTERS:
                raise ValueError("active_shooters is outside 1..6")
            if generation < 1:
                raise ValueError("generation must be positive")
            config = ClusterConfig(
                active_shooters=active_shooters,
                configured=configured,
                generation=generation,
            )
            self._last_valid_config = config
            return ConfigRead(config=config, valid=True)
        except (FileNotFoundError, OSError, KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
            fallback = self._last_valid_config or ClusterConfig()
            return ConfigRead(
                config=fallback,
                valid=False,
                error=f"{type(exc).__name__}: {exc}",
            )

    def load_config(self) -> ClusterConfig:
        """Return the latest valid config; never downgrade on a read error."""
        return self.read_config().config

    def _generation_candidates(self) -> list[int]:
        values: list[int] = []
        if self._last_valid_config is not None:
            values.append(int(self._last_valid_config.generation))
        try:
            raw = json.loads(self.config_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                value = int(raw.get("generation", 0))
                if value > 0:
                    values.append(value)
        except (FileNotFoundError, OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
        try:
            raw = json.loads(self.generation_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                value = int(raw.get("generation", 0))
                if value > 0:
                    values.append(value)
        except (FileNotFoundError, OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
        for shooter_id in range(1, MAX_SHOOTERS + 1):
            lifecycle = self.load_lifecycle(shooter_id)
            if lifecycle is None:
                continue
            try:
                value = int(lifecycle.get("generation", 0))
            except (TypeError, ValueError):
                continue
            if value > 0:
                values.append(value)
        return values

    def max_known_generation(self) -> int:
        """Recover the monotonic generation even if cluster.json was lost."""
        return max([1, *self._generation_candidates()])

    def _save_generation_marker(self, generation: int) -> None:
        generation = max(1, int(generation))
        current = 0
        try:
            raw = json.loads(self.generation_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                current = int(raw.get("generation", 0))
        except (FileNotFoundError, OSError, ValueError, TypeError, json.JSONDecodeError):
            pass
        if generation <= current:
            return
        _atomic_write(
            self.generation_path,
            json.dumps({"generation": generation, "updated_at": time.time()}, ensure_ascii=False, indent=2),
        )

    def save_config(self, config: ClusterConfig) -> ClusterConfig:
        normalized = config.normalized()
        _atomic_write(
            self.config_path,
            json.dumps(asdict(normalized), ensure_ascii=False, indent=2),
        )
        self._save_generation_marker(normalized.generation)
        self._last_valid_config = normalized
        return normalized

    def configure(self, active_shooters: int) -> ClusterConfig:
        config = ClusterConfig(
            active_shooters=active_shooters,
            configured=True,
            generation=self.max_known_generation() + 1,
        ).normalized()
        return self.save_config(config)

    def lifecycle_path(self, shooter_id: int) -> Path:
        shooter_id = min(MAX_SHOOTERS, max(1, int(shooter_id)))
        return self.root / f"hunter-{shooter_id}.lifecycle.json"

    def save_lifecycle(
        self,
        shooter_id: int,
        *,
        state: str,
        generation: int,
        active_shooters: int,
        reason: str = "",
        **fields: Any,
    ) -> Path:
        payload = {
            "timestamp": time.time(),
            "shooter_id": min(MAX_SHOOTERS, max(1, int(shooter_id))),
            "state": str(state),
            "generation": max(0, int(generation)),
            "active_shooters": min(MAX_SHOOTERS, max(1, int(active_shooters))),
            "reason": str(reason),
            **{
                str(key): self._safe_event_value(str(key), value)
                for key, value in fields.items()
            },
        }
        path = self.lifecycle_path(shooter_id)
        _atomic_write(path, json.dumps(payload, ensure_ascii=False, indent=2))
        return path

    def load_lifecycle(self, shooter_id: int) -> dict[str, Any] | None:
        try:
            raw = json.loads(self.lifecycle_path(shooter_id).read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return None
            return raw
        except (FileNotFoundError, OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def token_path(self, shooter_id: int) -> Path:
        shooter_id = min(MAX_SHOOTERS, max(1, int(shooter_id)))
        if self.token_root is None:
            return self.root / f"hunter-{shooter_id}.token"
        return self.token_root / f"hunter-{shooter_id}" / "bot.token"

    def save_token(self, shooter_id: int, token: str) -> Path:
        token = str(token).strip()
        if not token:
            raise ValueError("empty bot token")
        path = self.token_path(shooter_id)
        _atomic_write(path, token + "\n")
        return path

    def load_token(self, shooter_id: int) -> str:
        try:
            return self.token_path(shooter_id).read_text(encoding="utf-8").strip()
        except (FileNotFoundError, OSError):
            return ""

    def configured_token_ids(self) -> list[int]:
        return [i for i in range(2, MAX_SHOOTERS + 1) if bool(self.load_token(i))]

    @staticmethod
    def _safe_event_value(key: str, value: Any) -> Any:
        """Redact secret-like fields before they reach the shared event log."""
        lowered = str(key).lower()
        if any(marker in lowered for marker in ("token", "secret", "password", "api_hash")):
            if lowered.endswith("_present") or lowered.endswith("_ids"):
                return value
            return "[redacted]"
        if isinstance(value, dict):
            return {
                str(child_key): ProvisionStore._safe_event_value(str(child_key), child_value)
                for child_key, child_value in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [ProvisionStore._safe_event_value(key, item) for item in value]
        if isinstance(value, Path):
            return value.name
        return value

    def append_event(self, event: str, **fields: Any) -> Path:
        """Append one redacted structured cluster event from any container."""
        payload = {
            "timestamp": time.time(),
            "event": str(event),
            **{
                str(key): self._safe_event_value(str(key), value)
                for key, value in fields.items()
            },
        }
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        fd = os.open(
            self.event_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            os.write(fd, line.encode("utf-8", errors="replace"))
        finally:
            os.close(fd)
        os.chmod(self.event_path, 0o600)
        return self.event_path


@dataclass(frozen=True)
class ShooterStatus:
    shooter_id: int
    bot: bool
    mtproto: bool
    gift: bool
    shot: int | None
    plan: bool
    signal: bool
    live: bool
    active: bool
    generation: int = 0
    campaign_id: str | None = None
    updated_monotonic: float = 0.0

    @property
    def ready(self) -> bool:
        return all((self.bot, self.mtproto, self.gift, self.shot is not None, self.plan, self.signal, self.live, self.active))

    def payload(self) -> dict[str, Any]:
        return {
            "shooter_id": int(self.shooter_id),
            "bot": bool(self.bot),
            "mtproto": bool(self.mtproto),
            "gift": bool(self.gift),
            "shot": int(self.shot) if self.shot is not None else None,
            "plan": bool(self.plan),
            "signal": bool(self.signal),
            "live": bool(self.live),
            "active": bool(self.active),
            "generation": max(0, int(self.generation)),
            "campaign_id": str(self.campaign_id) if self.campaign_id else None,
        }

    @classmethod
    def from_payload(cls, data: dict[str, Any]) -> "ShooterStatus":
        shot_raw = data.get("shot")
        try:
            shot = int(shot_raw) if shot_raw is not None else None
        except (TypeError, ValueError):
            shot = None
        return cls(
            shooter_id=int(data.get("shooter_id", 0)),
            bot=bool(data.get("bot", False)),
            mtproto=bool(data.get("mtproto", False)),
            gift=bool(data.get("gift", False)),
            shot=shot,
            plan=bool(data.get("plan", False)),
            signal=bool(data.get("signal", False)),
            live=bool(data.get("live", False)),
            active=bool(data.get("active", False)),
            generation=max(0, int(data.get("generation", 0) or 0)),
            campaign_id=(str(data.get("campaign_id")).lower() if data.get("campaign_id") else None),
            updated_monotonic=time.monotonic(),
        )


def _sign(secret: str, payload: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()[:32]


def encode_status(secret: str, status: ShooterStatus) -> bytes:
    payload = json.dumps(status.payload(), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    encoded = base64.urlsafe_b64encode(payload).rstrip(b"=")
    return b"S|" + encoded + b"|" + _sign(secret, b"S|" + encoded).encode("ascii")


def decode_status(secret: str, packet: bytes) -> ShooterStatus | None:
    try:
        kind, encoded, signature = packet.split(b"|", 2)
        if kind != b"S":
            return None
        expected = _sign(secret, b"S|" + encoded).encode("ascii")
        if not hmac.compare_digest(signature, expected):
            return None
        padded = encoded + b"=" * (-len(encoded) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        if not isinstance(data, dict):
            return None
        status = ShooterStatus.from_payload(data)
        if not 1 <= status.shooter_id <= MAX_SHOOTERS:
            return None
        return status
    except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
        return None




def campaign_id_for(slug: str, base_gift_id: int | None) -> str:
    """Stable collection identity shared by independent Telegram accounts.

    The value is computed while the plan is prepared, never in the payment hot
    path.  The target number and config generation remain separate signed FIRE
    fields, so a packet cannot cross either collection or campaign generation.
    """
    normalized_slug = str(slug).strip().casefold()
    if not normalized_slug:
        raise ValueError("empty gift slug")
    base = int(base_gift_id or 0)
    payload = f"{base}|{normalized_slug}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def _valid_campaign_id(value: str) -> bool:
    return 16 <= len(value) <= 64 and all(ch in "0123456789abcdef" for ch in value.lower())


def _valid_nonce(value: str) -> bool:
    return 8 <= len(value) <= 64 and all(ch in "0123456789abcdef" for ch in value.lower())


def encode_ping(secret: str, nonce: str, requester_id: int) -> bytes:
    nonce = str(nonce).strip().lower()
    requester_id = int(requester_id)
    if not _valid_nonce(nonce) or not 1 <= requester_id <= MAX_SHOOTERS:
        raise ValueError("invalid ping packet")
    body = f"P|{nonce}|{requester_id}".encode("ascii")
    return body + b"|" + _sign(secret, body).encode("ascii")


def decode_ping(secret: str, packet: bytes) -> tuple[str, int] | None:
    try:
        kind, nonce_raw, requester_raw, signature = packet.split(b"|", 3)
        if kind != b"P":
            return None
        body = b"|".join((kind, nonce_raw, requester_raw))
        if not hmac.compare_digest(signature, _sign(secret, body).encode("ascii")):
            return None
        nonce = nonce_raw.decode("ascii").lower()
        requester_id = int(requester_raw)
        if not _valid_nonce(nonce) or not 1 <= requester_id <= MAX_SHOOTERS:
            return None
        return nonce, requester_id
    except (ValueError, TypeError, UnicodeDecodeError):
        return None


def encode_pong(secret: str, nonce: str, responder_id: int) -> bytes:
    nonce = str(nonce).strip().lower()
    responder_id = int(responder_id)
    if not _valid_nonce(nonce) or not 1 <= responder_id <= MAX_SHOOTERS:
        raise ValueError("invalid pong packet")
    body = f"Q|{nonce}|{responder_id}".encode("ascii")
    return body + b"|" + _sign(secret, body).encode("ascii")


def decode_pong(secret: str, packet: bytes) -> tuple[str, int] | None:
    try:
        kind, nonce_raw, responder_raw, signature = packet.split(b"|", 3)
        if kind != b"Q":
            return None
        body = b"|".join((kind, nonce_raw, responder_raw))
        if not hmac.compare_digest(signature, _sign(secret, body).encode("ascii")):
            return None
        nonce = nonce_raw.decode("ascii").lower()
        responder_id = int(responder_raw)
        if not _valid_nonce(nonce) or not 1 <= responder_id <= MAX_SHOOTERS:
            return None
        return nonce, responder_id
    except (ValueError, TypeError, UnicodeDecodeError):
        return None


def encode_config_notice(secret: str, generation: int, active_shooters: int) -> bytes:
    generation = int(generation)
    active_shooters = int(active_shooters)
    if generation < 1 or not 1 <= active_shooters <= MAX_SHOOTERS:
        raise ValueError("invalid config notice")
    body = f"C|{generation}|{active_shooters}".encode("ascii")
    return body + b"|" + _sign(secret, body).encode("ascii")


def decode_config_notice(secret: str, packet: bytes) -> tuple[int, int] | None:
    try:
        kind, generation_raw, active_raw, signature = packet.split(b"|", 3)
        if kind != b"C":
            return None
        body = b"|".join((kind, generation_raw, active_raw))
        if not hmac.compare_digest(signature, _sign(secret, body).encode("ascii")):
            return None
        generation = int(generation_raw)
        active_shooters = int(active_raw)
        if generation < 1 or not 1 <= active_shooters <= MAX_SHOOTERS:
            return None
        return generation, active_shooters
    except (ValueError, TypeError):
        return None


def encode_fire(
    secret: str,
    generation: int,
    campaign_id: str,
    shot: int,
    trigger: int,
) -> bytes:
    campaign_id = str(campaign_id).strip().lower()
    generation = int(generation)
    shot = int(shot)
    trigger = int(trigger)
    if (
        not _valid_campaign_id(campaign_id)
        or generation < 1
        or shot < 1
        or trigger != shot - 1
    ):
        raise ValueError("invalid fire packet")
    body = f"F|{generation}|{campaign_id}|{shot}|{trigger}".encode("ascii")
    return body + b"|" + _sign(secret, body).encode("ascii")


def decode_fire(secret: str, packet: bytes) -> tuple[int, str, int, int] | None:
    try:
        kind, generation, campaign_raw, shot, trigger, signature = packet.split(b"|", 5)
        if kind != b"F":
            return None
        body = b"|".join((kind, generation, campaign_raw, shot, trigger))
        if not hmac.compare_digest(signature, _sign(secret, body).encode("ascii")):
            return None
        campaign_id = campaign_raw.decode("ascii").lower()
        values = (int(generation), campaign_id, int(shot), int(trigger))
        if (
            values[0] < 1
            or not _valid_campaign_id(values[1])
            or values[2] < 1
            or values[3] != values[2] - 1
        ):
            return None
        return values
    except (ValueError, TypeError, UnicodeDecodeError):
        return None


class _Protocol(asyncio.DatagramProtocol):
    def __init__(self, receiver: Callable[[bytes, Any], None]):
        self.receiver = receiver
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Any) -> None:
        self.receiver(data, addr)


class ClusterBus:
    """UDP fire/status transport for containers on one Docker bridge network."""

    def __init__(
        self,
        *,
        shooter_id: int,
        secret: str,
        port: int,
        provision: ProvisionStore,
        on_fire: Callable[[str, int, int], None],
        on_status: Callable[[ShooterStatus], None],
        host_prefix: str = "hunter-",
        on_config_notice: Callable[[int, int], None] | None = None,
    ) -> None:
        self.shooter_id = min(MAX_SHOOTERS, max(1, int(shooter_id)))
        self.secret = secret
        self.port = int(port)
        self.provision = provision
        self.on_fire = on_fire
        self.on_status = on_status
        self.host_prefix = host_prefix
        self.on_config_notice = on_config_notice
        self.transport: asyncio.DatagramTransport | None = None
        self.protocol: _Protocol | None = None
        self.config = provision.load_config()
        self.peer_addresses: dict[int, tuple[str, int]] = {}
        self.primary_address: tuple[str, int] | None = None
        self._armed_packets: dict[tuple[str, int], bytes] = {}
        self._ping_sessions: dict[str, dict[str, Any]] = {}
        self.last_ping_results: dict[int, float | None] = {}
        self.last_ping_monotonic: float = 0.0

    @property
    def ready(self) -> bool:
        return self.transport is not None

    @property
    def expected_peer_ids(self) -> set[int]:
        return {
            peer_id
            for peer_id in range(1, self.config.active_shooters + 1)
            if peer_id != self.shooter_id
        }

    @property
    def resolved_peer_ids(self) -> set[int]:
        return set(self.peer_addresses)

    @property
    def fully_connected(self) -> bool:
        """Compatibility name: socket plus Docker DNS resolution."""
        if self.config.active_shooters <= 1:
            return self.ready
        return self.ready and self.resolved_peer_ids == self.expected_peer_ids

    @property
    def live_peer_ids(self) -> set[int]:
        return {peer_id for peer_id, rtt in self.last_ping_results.items() if rtt is not None}

    @property
    def peers_ready(self) -> bool:
        expected = set(range(1, self.config.active_shooters + 1))
        return self.ready and self.live_peer_ids == expected

    async def start(self) -> None:
        if self.transport is not None:
            return
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: _Protocol(self._receive),
            local_addr=("0.0.0.0", self.port),
            family=socket.AF_INET,
        )
        self.transport = transport  # type: ignore[assignment]
        self.protocol = protocol  # type: ignore[assignment]
        await self.refresh_config_and_peers()

    def close(self) -> None:
        if self.transport is not None:
            self.transport.close()
        self.transport = None
        self.protocol = None
        self.peer_addresses.clear()
        self.primary_address = None
        self._armed_packets.clear()
        self.last_ping_results.clear()
        self.last_ping_monotonic = 0.0
        for session in self._ping_sessions.values():
            event = session.get("event")
            if isinstance(event, asyncio.Event):
                event.set()
        self._ping_sessions.clear()

    async def _resolve_host(self, host: str) -> tuple[str, int] | None:
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, self.port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
        except OSError:
            return None
        if not infos:
            return None
        return infos[0][4]

    async def refresh_config_and_peers(self, config: ClusterConfig | None = None) -> ClusterConfig:
        previous_identity = (self.config.generation, self.config.active_shooters)
        if config is not None:
            self.config = config.normalized()
        if (self.config.generation, self.config.active_shooters) != previous_identity:
            self.last_ping_results.clear()
            self.last_ping_monotonic = 0.0
            self._armed_packets.clear()
        addresses: dict[int, tuple[str, int]] = {}
        for peer_id in range(1, self.config.active_shooters + 1):
            if peer_id == self.shooter_id:
                continue
            address = await self._resolve_host(f"{self.host_prefix}{peer_id}")
            if address is not None:
                addresses[peer_id] = address
        self.peer_addresses = addresses
        if self.shooter_id != 1:
            self.primary_address = await self._resolve_host(f"{self.host_prefix}1")
        else:
            self.primary_address = None
        return self.config

    def arm(self, campaign_id: str, shot: int) -> None:
        campaign_id = str(campaign_id).strip().lower()
        shot = int(shot)
        trigger = shot - 1
        self._armed_packets[(campaign_id, shot)] = encode_fire(
            self.secret, self.config.generation, campaign_id, shot, trigger
        )

    def disarm(self) -> None:
        self._armed_packets.clear()

    def broadcast_fire_nowait(self, *, campaign_id: str, shot: int, trigger: int) -> int:
        transport = self.transport
        if transport is None:
            return 0
        campaign_id = str(campaign_id).strip().lower()
        shot = int(shot)
        if int(trigger) != shot - 1:
            return 0
        key = (campaign_id, shot)
        packet = self._armed_packets.get(key)
        if packet is None:
            # FIRE packets are prebuilt while arming.  Never calculate HMAC or
            # touch control-plane state in the payment hot path.
            return 0
        sent = 0
        for address in self.peer_addresses.values():
            try:
                transport.sendto(packet, address)
                transport.sendto(packet, address)
            except (OSError, RuntimeError):
                continue
            sent += 1
        return sent

    def broadcast_config_notice_nowait(self, config: ClusterConfig) -> int:
        """Wake currently active peers so they re-read the shared config now."""
        transport = self.transport
        if transport is None:
            return 0
        packet = encode_config_notice(self.secret, config.generation, config.active_shooters)
        sent = 0
        # peer_addresses still describe the previous active set when Hunter 1
        # calls this before applying the new config. That is intentional: removed
        # participants receive the stop notice immediately.
        for address in self.peer_addresses.values():
            try:
                transport.sendto(packet, address)
                transport.sendto(packet, address)
            except (OSError, RuntimeError):
                continue
            sent += 1
        return sent

    def send_status_nowait(self, status: ShooterStatus) -> bool:
        transport = self.transport
        if transport is None:
            return False
        if self.shooter_id == 1:
            self.on_status(status)
            return True
        address = self.primary_address
        if address is None:
            return False
        try:
            transport.sendto(encode_status(self.secret, status), address)
        except (OSError, RuntimeError):
            return False
        return True

    async def ping_active_shooters(self, *, timeout_seconds: float = 0.5) -> dict[int, float | None]:
        """Ping every active shooter, including this process through its UDP socket.

        The returned values are RTT milliseconds. Missing or timed-out peers map to
        ``None``. No background heartbeat is started; this runs only on demand.
        """
        await self.refresh_config_and_peers()
        active_ids = list(range(1, self.config.active_shooters + 1))
        results: dict[int, float | None] = {peer_id: None for peer_id in active_ids}
        transport = self.transport
        if transport is None:
            return results

        nonce = secrets.token_hex(12)
        event = asyncio.Event()
        starts_ns: dict[int, int] = {}
        session: dict[str, Any] = {
            "event": event,
            "starts_ns": starts_ns,
            "results": results,
            "expected": set(),
        }
        self._ping_sessions[nonce] = session
        packet = encode_ping(self.secret, nonce, self.shooter_id)
        try:
            for peer_id in active_ids:
                address = ("127.0.0.1", self.port) if peer_id == self.shooter_id else self.peer_addresses.get(peer_id)
                if address is None:
                    continue
                starts_ns[peer_id] = time.perf_counter_ns()
                session["expected"].add(peer_id)
                try:
                    transport.sendto(packet, address)
                except (OSError, RuntimeError):
                    starts_ns.pop(peer_id, None)
                    session["expected"].discard(peer_id)
            if not session["expected"]:
                return results
            try:
                await asyncio.wait_for(event.wait(), timeout=max(0.05, float(timeout_seconds)))
            except asyncio.TimeoutError:
                pass
            snapshot = dict(results)
            self.last_ping_results = snapshot
            self.last_ping_monotonic = time.monotonic()
            return snapshot
        finally:
            self._ping_sessions.pop(nonce, None)

    def _receive(self, packet: bytes, _addr: Any) -> None:
        config_notice = decode_config_notice(self.secret, packet)
        if config_notice is not None:
            if self.on_config_notice is not None:
                self.on_config_notice(*config_notice)
            return
        fire = decode_fire(self.secret, packet)
        if fire is not None:
            generation, campaign_id, shot, trigger = fire
            if generation == self.config.generation:
                self.on_fire(campaign_id, shot, trigger)
            return
        status = decode_status(self.secret, packet)
        if status is not None and self.shooter_id == 1:
            self.on_status(status)
            return
        ping = decode_ping(self.secret, packet)
        if ping is not None:
            nonce, _requester_id = ping
            transport = self.transport
            if transport is not None:
                try:
                    transport.sendto(encode_pong(self.secret, nonce, self.shooter_id), _addr)
                except (OSError, RuntimeError):
                    pass
            return
        pong = decode_pong(self.secret, packet)
        if pong is None:
            return
        nonce, responder_id = pong
        session = self._ping_sessions.get(nonce)
        if session is None or responder_id not in session.get("expected", set()):
            return
        results = session.get("results")
        starts_ns = session.get("starts_ns")
        if not isinstance(results, dict) or not isinstance(starts_ns, dict):
            return
        if results.get(responder_id) is not None:
            return
        started_ns = starts_ns.get(responder_id)
        if started_ns is None:
            return
        results[responder_id] = max(0.0, (time.perf_counter_ns() - started_ns) / 1_000_000.0)
        expected = session.get("expected", set())
        if all(results.get(peer_id) is not None for peer_id in expected):
            event = session.get("event")
            if isinstance(event, asyncio.Event):
                event.set()
