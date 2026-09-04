"""Redis-backed 通用数据源同步任务状态。"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

KEY_PREFIX = "quantmind:data_sync:job:"
TTL_SECONDS = 24 * 3600


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _redis():
    import redis

    return redis.from_url(
        os.getenv("REDIS_URL", "redis://redis:6379/0"), socket_timeout=3
    )


def _encode(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def upsert_job(job_id: str, **fields: Any) -> None:
    payload = {"job_id": job_id, **fields}
    client = _redis()
    client.hset(
        KEY_PREFIX + job_id,
        mapping={key: _encode(value) for key, value in payload.items()},
    )
    client.expire(KEY_PREFIX + job_id, TTL_SECONDS)


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
    job_id = (
        f"sync-{source_id}-{datetime.now().strftime('%Y%m%d-%H%M%S')}-"
        f"{uuid.uuid4().hex[:6]}"
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
    upsert_job(job_id, **{key: value for key, value in job.items() if key != "job_id"})
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
