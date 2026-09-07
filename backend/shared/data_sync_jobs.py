"""Redis-backed 通用数据源同步任务状态。"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

KEY_PREFIX = "quantmind:data_sync:job:"
ACTIVE_KEY_PREFIX = "quantmind:data_sync:active:"
TTL_SECONDS = 24 * 3600
ACTIVE_STATUSES = {"queued", "running", "cancelling"}


class ActiveSyncJobError(RuntimeError):
    """Raised when the same market/source already has an active sync job."""

    def __init__(self, job: dict[str, Any]) -> None:
        self.job = job
        super().__init__(f"同步任务正在执行: {job.get('job_id', 'unknown')}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _redis():
    import redis

    return redis.from_url(
        os.getenv("REDIS_URL", "redis://redis:6379/0"), socket_timeout=3
    )


def _encode(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def _decode_text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _active_key(market: str, source_id: str) -> str:
    return f"{ACTIVE_KEY_PREFIX}{market.upper()}:{source_id.lower()}"


def _release_active_job(client: Any, job_id: str, market: str, source_id: str) -> None:
    key = _active_key(market, source_id)
    current = client.get(key)
    if current is not None and _decode_text(current) == job_id:
        client.delete(key)


def upsert_job(job_id: str, **fields: Any) -> None:
    payload = {"job_id": job_id, **fields}
    client = _redis()
    client.hset(
        KEY_PREFIX + job_id,
        mapping={key: _encode(value) for key, value in payload.items()},
    )
    client.expire(KEY_PREFIX + job_id, TTL_SECONDS)
    if fields.get("status") in {"completed", "failed", "cancelled"}:
        source_id = client.hget(KEY_PREFIX + job_id, "source_id")
        market = client.hget(KEY_PREFIX + job_id, "market")
        if source_id is not None and market is not None:
            _release_active_job(
                client,
                job_id,
                _decode_text(market),
                _decode_text(source_id),
            )


def find_active_job(market: str, source_id: str) -> dict[str, Any] | None:
    """Return the active same-market/source job and clean stale lock state."""
    client = _redis()
    key = _active_key(market, source_id)
    raw_job_id = client.get(key)
    if raw_job_id is not None:
        job_id = _decode_text(raw_job_id)
        job = get_job(job_id)
        if job and job.get("status") in ACTIVE_STATUSES:
            return job
        if job is None:
            # The lock is claimed immediately before the job hash is written.
            # Treat this tiny creation window as active to avoid a double start.
            return {
                "job_id": job_id,
                "market": market.upper(),
                "source_id": source_id.lower(),
                "status": "queued",
            }
        _release_active_job(client, job_id, market, source_id)

    # Pick up active jobs created before the lock mechanism was introduced.
    for job in list_jobs(200):
        if (
            str(job.get("market", "")).upper() == market.upper()
            and str(job.get("source_id", "")).lower() == source_id.lower()
            and job.get("status") in ACTIVE_STATUSES
        ):
            client.set(key, job["job_id"], nx=True, ex=TTL_SECONDS)
            return job
    return None


def create_job(
    *,
    source_id: str,
    market: str,
    datasets: list[str],
    days: int,
    symbols: list[str],
    publish_mode: str,
    with_pg: bool,
    with_qlib: bool,
    started_by: str,
) -> dict[str, Any]:
    active_job = find_active_job(market, source_id)
    if active_job is not None:
        raise ActiveSyncJobError(active_job)

    job_id = (
        f"sync-{source_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}-"
        f"{uuid.uuid4().hex[:6]}"
    )
    client = _redis()
    active_key = _active_key(market, source_id)
    if not client.set(active_key, job_id, nx=True, ex=60):
        active_job = find_active_job(market, source_id)
        raise ActiveSyncJobError(
            active_job
            or {
                "job_id": "unknown",
                "market": market.upper(),
                "source_id": source_id.lower(),
                "status": "queued",
            }
        )

    job = {
        "job_id": job_id,
        "source_id": source_id,
        "market": market,
        "status": "queued",
        "stage": "queued",
        "datasets": datasets,
        "days": days,
        "symbols": symbols,
        "publish_mode": publish_mode,
        "with_pg": with_pg,
        "with_qlib": with_qlib,
        "done": 0,
        "total": None,
        "current": "等待 worker 执行",
        "cancel_requested": False,
        "result": None,
        "error": None,
        "started_at": _now_iso(),
        "finished_at": None,
        "started_by": started_by,
    }
    try:
        upsert_job(
            job_id,
            **{key: value for key, value in job.items() if key != "job_id"},
        )
        client.expire(active_key, TTL_SECONDS)
    except Exception:
        _release_active_job(client, job_id, market, source_id)
        raise
    return job


def get_job(job_id: str) -> dict[str, Any] | None:
    try:
        raw = _redis().hgetall(KEY_PREFIX + job_id)
    except Exception:
        return None
    if not raw:
        return None
    job: dict[str, Any] = {}
    for raw_key, raw_value in raw.items():
        key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
        value = raw_value.decode() if isinstance(raw_value, bytes) else str(raw_value)
        try:
            job[key] = json.loads(value)
        except json.JSONDecodeError:
            job[key] = value
    return job


def list_jobs(limit: int = 50) -> list[dict[str, Any]]:
    try:
        keys = _redis().keys(KEY_PREFIX + "*")
    except Exception:
        return []
    jobs = []
    for key in keys:
        decoded = key.decode() if isinstance(key, bytes) else str(key)
        job = get_job(decoded.removeprefix(KEY_PREFIX))
        if job:
            jobs.append(job)
    jobs.sort(key=lambda item: str(item.get("started_at", "")), reverse=True)
    return jobs[:limit]


def request_cancel(job_id: str) -> bool:
    job = get_job(job_id)
    if not job or job.get("status") not in {"queued", "running"}:
        return False
    upsert_job(job_id, cancel_requested=True, status="cancelling")
    return True


def cancel_requested(job_id: str) -> bool:
    return bool((get_job(job_id) or {}).get("cancel_requested"))


def progress_callback(job_id: str):
    def _callback(event: str, **data: Any) -> None:
        if event == "start":
            upsert_job(
                job_id,
                status="running",
                stage="fetch",
                total=data.get("total"),
                current="开始拉取行情",
            )
        elif event == "symbol":
            upsert_job(
                job_id,
                stage="fetch",
                done=data.get("done", 0),
                total=data.get("total"),
                current=f"{data.get('dataset')} / {data.get('symbol')}",
            )
        elif event == "write":
            upsert_job(
                job_id,
                stage="write",
                current=f"写入 {data.get('dataset')} 影子分区",
            )

    return _callback
