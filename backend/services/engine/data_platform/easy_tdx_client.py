"""easy_tdx 长连接、节点选择与健康状态管理。"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

try:
    from easy_tdx import MacClient, TdxClient
    from easy_tdx.config import (
        get_best_host,
        get_best_mac_host,
        get_known_hosts,
        get_mac_hosts,
        get_port,
    )

    EASY_TDX_AVAILABLE = True
except ImportError:
    MacClient = TdxClient = None  # type: ignore[assignment,misc]
    EASY_TDX_AVAILABLE = False

_T = TypeVar("_T")
_ACTIVE_KEY = "quantmind:easy_tdx:active:{channel}"
_HEALTH_KEY = "quantmind:easy_tdx:health:{channel}"
_redis_client: Any | None = None
_redis_lock = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class EasyTdxClientManager:
    """每个进程维护一组连接，Redis 共享节点选择和运维健康状态。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._clients: dict[str, Any] = {}
        self._client_hosts: dict[str, str] = {}
        self._last_success_health_at: dict[tuple[str, str], float] = {}

    @property
    def available(self) -> bool:
        return EASY_TDX_AVAILABLE

    @property
    def library_version(self) -> str | None:
        if not self.available:
            return None
        try:
            return version("easy-tdx")
        except PackageNotFoundError:
            return "unknown"

    @staticmethod
    def _redis():
        global _redis_client
        if _redis_client is not None:
            return _redis_client
        try:
            import redis

            with _redis_lock:
                if _redis_client is None:
                    _redis_client = redis.from_url(
                        os.getenv("REDIS_URL", "redis://redis:6379/0"),
                        socket_timeout=2,
                    )
            return _redis_client
        except Exception:
            return None

    def hosts(self, channel: str) -> list[str]:
        self._require_available()
        if channel == "standard":
            return list(get_known_hosts())
        if channel == "mac":
            return list(get_mac_hosts())
        raise ValueError(f"不支持的 easy_tdx 通道: {channel}")

    def active_host(self, channel: str) -> str:
        self._require_available()
        client = self._redis()
        if client is not None:
            try:
                raw = client.get(_ACTIVE_KEY.format(channel=channel))
                if raw:
                    return raw.decode() if isinstance(raw, bytes) else str(raw)
            except Exception:
                pass
        return get_best_host() if channel == "standard" else get_best_mac_host()

    def _new_client(self, channel: str, host: str):
        timeout = float(os.getenv("EASY_TDX_TIMEOUT", "10"))
        cls = TdxClient if channel == "standard" else MacClient
        client = cls(host=host, timeout=timeout, heartbeat_interval=15.0)
        client.connect()
        return client

    def _client(self, channel: str):
        host = self.active_host(channel)
        current = self._clients.get(channel)
        if current is not None and self._client_hosts.get(channel) == host:
            return current
        if current is not None:
            try:
                current.close()
            except Exception:
                pass
        current = self._new_client(channel, host)
        self._clients[channel] = current
        self._client_hosts[channel] = host
        return current

    def execute(self, channel: str, operation: Callable[[Any], _T]) -> _T:
        """串行复用单连接；底层 easy_tdx 自身负责断线重连与跨节点容灾。"""
        started = time.monotonic()
        with self._lock:
            host = self.active_host(channel)
            try:
                client = self._client(channel)
                result = operation(client)
            except Exception as exc:
                self._record_health(
                    channel,
                    host,
                    ok=False,
                    latency_ms=(time.monotonic() - started) * 1000,
                    error=str(exc),
                )
                raise
            actual_host = str(getattr(client, "_host", host))
            if actual_host != host:
                self._client_hosts[channel] = actual_host
                redis_client = self._redis()
                if redis_client is not None:
                    try:
                        redis_client.set(
                            _ACTIVE_KEY.format(channel=channel), actual_host
                        )
                    except Exception:
                        pass
                host = actual_host
            self._record_health(
                channel,
                host,
                ok=True,
                latency_ms=(time.monotonic() - started) * 1000,
            )
            return result

    def list_servers(self, channel: str) -> list[dict[str, Any]]:
        active = self.active_host(channel)
        health = self._read_health(channel)
        port = int(get_port())
        candidates = self.hosts(channel)
        if active not in candidates:
            candidates.insert(0, active)
        return [
            {
                "channel": channel,
                "host": host,
                "port": port,
                "selected": host == active,
                **health.get(host, {"status": "unknown"}),
            }
            for host in candidates
        ]

    def test_servers(
        self,
        channel: str,
        host: str | None = None,
        timeout: float = 2.0,
    ) -> list[dict[str, Any]]:
        candidates = [host] if host else self.hosts(channel)
        allowed_hosts = self.hosts(channel)
        active = self.active_host(channel)
        if active not in allowed_hosts:
            allowed_hosts.append(active)
        unknown = [item for item in candidates if item not in allowed_hosts]
        if unknown:
            raise ValueError(f"节点不在 {channel} 候选池: {unknown[0]}")
        cls = TdxClient if channel == "standard" else MacClient
        started = time.monotonic()
        ranked = cls.ping_all(candidates, port=int(get_port()), timeout=timeout)
        reachable = {item_host: latency * 1000 for item_host, latency in ranked}
        elapsed_ms = (time.monotonic() - started) * 1000
        for candidate in candidates:
            ok = candidate in reachable
            self._record_health(
                channel,
                candidate,
                ok=ok,
                latency_ms=reachable.get(candidate, elapsed_ms),
                error=None if ok else "连接超时或协议握手失败",
            )
        return [
            item for item in self.list_servers(channel) if item["host"] in candidates
        ]

    def switch_server(self, channel: str, host: str) -> dict[str, Any]:
        allowed_hosts = self.hosts(channel)
        active = self.active_host(channel)
        if active not in allowed_hosts:
            allowed_hosts.append(active)
        if host not in allowed_hosts:
            raise ValueError(f"节点不在 {channel} 候选池: {host}")
        client = self._redis()
        if client is None:
            raise RuntimeError("Redis 不可用，无法在多个服务进程间共享节点切换")
        client.set(_ACTIVE_KEY.format(channel=channel), host)
        with self._lock:
            current = self._clients.pop(channel, None)
            self._client_hosts.pop(channel, None)
            if current is not None:
                try:
                    current.close()
                except Exception:
                    pass
        return {"channel": channel, "host": host, "switched_at": _now_iso()}

    def _read_health(self, channel: str) -> dict[str, dict[str, Any]]:
        client = self._redis()
        if client is None:
            return {}
        try:
            raw = client.hgetall(_HEALTH_KEY.format(channel=channel))
        except Exception:
            return {}
        result: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            host = key.decode() if isinstance(key, bytes) else str(key)
            payload = value.decode() if isinstance(value, bytes) else str(value)
            try:
                result[host] = json.loads(payload)
            except json.JSONDecodeError:
                continue
        return result

    def _record_health(
        self,
        channel: str,
        host: str,
        *,
        ok: bool,
        latency_ms: float,
        error: str | None = None,
    ) -> None:
        health_key = (channel, host)
        now = time.monotonic()
        if ok and now - self._last_success_health_at.get(health_key, 0) < 5:
            return
        if ok:
            self._last_success_health_at[health_key] = now
        client = self._redis()
        if client is None:
            return
        current = self._read_health(channel).get(host, {})
        failures = 0 if ok else int(current.get("consecutive_failures", 0)) + 1
        payload = {
            "status": "online" if ok else "offline",
            "latency_ms": round(latency_ms, 1),
            "checked_at": _now_iso(),
            "last_error": None if ok else (error or "unknown error")[:300],
            "consecutive_failures": failures,
        }
        try:
            client.hset(
                _HEALTH_KEY.format(channel=channel),
                host,
                json.dumps(payload, ensure_ascii=False),
            )
        except Exception:
            logger.debug("easy_tdx health write failed", exc_info=True)

    def _require_available(self) -> None:
        if not self.available:
            raise RuntimeError("easy-tdx 未安装，请先安装 requirements.txt")


_manager: EasyTdxClientManager | None = None
_manager_lock = threading.Lock()


def get_easy_tdx_manager() -> EasyTdxClientManager:
    global _manager
    if _manager is None:
        with _manager_lock:
            if _manager is None:
                _manager = EasyTdxClientManager()
    return _manager
