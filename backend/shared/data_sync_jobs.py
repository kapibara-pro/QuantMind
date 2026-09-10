"""Redis-backed 通用数据源同步任务状态。"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

KEY_PREFIX = "quantmind:data_sync:job:"
ACTIVE_KEY_PREFIX = "quantmind:data_sync:active:"
TTL_SECONDS = 24 * 3600
ACTIVE_STATUSES = {"queued", "running", "cancelling"}
CELERY_FAILURE_STATES = {"FAILURE", "REVOKED"}

# 任务心跳超时：worker 崩溃（容器被 OOM/磁盘写满杀死等）后不会有人回来收尾，
# 状态会永远停在 running/cancelling，前端「取消」也点不动。
# 超过该时长没有心跳即视为 worker 已丢失，由状态接口自愈为终态。
DEFAULT_JOB_STALE_SECONDS = 900
QUEUED_JOB_STALE_SECONDS_DEFAULT = 3600
QLIB_JOB_STALE_SECONDS_DEFAULT = 300


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
    return value if isinstance(value, str) else json.dumps(
        value,
        ensure_ascii=False,
        default=_json_default,
    )


def _json_default(value: Any) -> str | list[Any]:
    """Encode metadata values that commonly appear in worker results.

    Worker results may contain filesystem paths or timestamps from third-party
    libraries. These are status metadata, so preserving their readable value is
    preferable to failing the entire job finalization step.
    """
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, set):
        return list(value)
    return str(value)


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
    payload = {"job_id": job_id, "updated_at": _now_iso(), **fields}
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
    operation: str = "sync",
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
        "operation": operation,
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
        "progress": 0,
        "current": "等待 worker 执行",
        "cancel_requested": False,
        "result": None,
        "error": None,
        "started_at": _now_iso(),
        "updated_at": _now_iso(),
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


def reconcile_job(job_id: str) -> dict[str, Any] | None:
    """Turn worker loss or a dead Qlib heartbeat into a terminal job state."""
    job = get_job(job_id)
    if not job or job.get("status") not in ACTIVE_STATUSES:
        return job

    celery_task_id = str(job.get("celery_task_id") or "").strip()
    if celery_task_id:
        try:
            from backend.services.engine.qlib_app.celery_config import celery_app

            async_result = celery_app.AsyncResult(celery_task_id)
            if str(async_result.state).upper() in CELERY_FAILURE_STATES:
                reason = str(async_result.result or "Celery worker 异常退出")
                upsert_job(
                    job_id,
                    status="failed",
                    stage="worker_failed",
                    current=None,
                    error=reason,
                    finished_at=_now_iso(),
                )
                return get_job(job_id)
        except Exception:
            # Redis backend unavailable should not make the status endpoint fail.
            pass

    # 心跳超时自愈对所有阶段生效：worker 进程消失后没有任何人会把任务收尾，
    # 前端会一直卡在 running/cancelling（并阻塞后续同名任务）。
    stale_seconds = _stale_seconds_for(job)
    if _job_heartbeat_expired(job, stale_seconds):
        # 已经请求过取消：按「已取消」收尾，符合用户点取消时的预期；
        # 否则按失败收尾并说明原因，避免伪装成成功。
        cancelled = bool(job.get("cancel_requested")) or job.get("status") == "cancelling"
        upsert_job(
            job_id,
            status="cancelled" if cancelled else "failed",
            stage="cancelled" if cancelled else "worker_lost",
            current=None,
            error=(
                None
                if cancelled
                else (
                    f"任务超过 {stale_seconds} 秒没有心跳，worker 可能已退出"
                    "（内存不足或磁盘写满）"
                )
            ),
            finished_at=_now_iso(),
        )
        return get_job(job_id)
    return job


def _stale_seconds_for(job: dict[str, Any]) -> int:
    """心跳超时阈值。

    - 排队中：worker 可能正忙于长任务，放宽到 1 小时再判定为僵尸；
    - Qlib 构建：有心跳上报，5 分钟无心跳即视为 worker 丢失；
    - 其他阶段（含发布）：15 分钟，容忍大 parquet 文件的单文件处理。
    """
    if job.get("status") == "queued":
        default = QUEUED_JOB_STALE_SECONDS_DEFAULT
        env_name = "DATA_SYNC_QUEUED_STALE_SECONDS"
    elif str(job.get("stage") or "").startswith("qlib"):
        default = QLIB_JOB_STALE_SECONDS_DEFAULT
        env_name = "QLIB_JOB_STALE_SECONDS"
    else:
        default = DEFAULT_JOB_STALE_SECONDS
        env_name = "DATA_SYNC_JOB_STALE_SECONDS"
    try:
        return max(int(os.getenv(env_name, str(default))), 60)
    except ValueError:
        return default


def _job_heartbeat_expired(job: dict[str, Any], stale_seconds: int) -> bool:
    raw_updated = str(job.get("updated_at") or job.get("started_at") or "")
    try:
        updated = datetime.fromisoformat(raw_updated.replace("Z", "+00:00"))
    except ValueError:
        return False
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - updated).total_seconds() > stale_seconds


def reconcile_jobs(limit: int = 50) -> list[dict[str, Any]]:
    jobs = list_jobs(limit)
    return [reconcile_job(str(job["job_id"])) or job for job in jobs]


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
    """提交取消请求。

    幂等：重复点击「取消」不会报错，也不会把已进入取消中的任务卡住。
    worker 已经消失（心跳超时）时直接落终态，避免任务永远停在 cancelling。
    """
    job = get_job(job_id)
    if not job or job.get("status") not in ACTIVE_STATUSES:
        return False

    # 先按取消前的心跳判断 worker 是否还在：upsert 会把 updated_at 刷成现在，
    # 之后再判断就永远“不超时”了。
    worker_lost = _job_heartbeat_expired(job, _stale_seconds_for(job))
    upsert_job(job_id, cancel_requested=True, status="cancelling")

    if worker_lost:
        # 没有 worker 会再读取消标记，直接收尾，否则任务永远停在 cancelling。
        upsert_job(
            job_id,
            status="cancelled",
            stage="cancelled",
            current=None,
            error=None,
            finished_at=_now_iso(),
        )
    else:
        # worker 仍可能已异常退出（Celery 任务失败/被撤销），交给自愈逻辑兜底。
        reconcile_job(job_id)
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
                progress=0,
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
        elif event == "publish_validating":
            upsert_job(
                job_id,
                status="running",
                stage="quality_gate",
                done=0,
                current="校验三套日线发布包",
            )
        elif event == "publish_start":
            upsert_job(
                job_id,
                status="running",
                stage="publish_prepare",
                done=0,
                total=data.get("total"),
                progress=0,
                current="质量校验通过，准备正式分区",
            )
        elif event == "publish_prepare":
            done = data.get("done", 0)
            total = data.get("total") or 0
            upsert_job(
                job_id,
                stage="publish_prepare",
                done=done,
                total=total,
                progress=round(100 * done / total) if total else 0,
                current=data.get("current")
                or f"整理 {data.get('dataset')} / {data.get('date')}",
            )
        elif event == "publish_partition":
            done = data.get("done", 0)
            total = data.get("total") or 0
            upsert_job(
                job_id,
                stage="publish",
                done=done,
                total=total,
                progress=round(100 * done / total) if total else 0,
                current=f"发布 {data.get('dataset')} / {data.get('date')}",
            )
        elif event == "publish_qlib":
            progress = data.get("progress")
            done = data.get("done", 0)
            total = data.get("total") or 0
            overall = (
                round(100 * (done + float(progress or 0) / 100) / total)
                if total
                else int(progress or 0)
            )
            upsert_job(
                job_id,
                stage="qlib",
                substage=data.get("phase") or "build",
                done=done,
                total=total,
                progress=overall,
                current=data.get("current") or "重建并切换 Qlib 训练数据",
            )
        elif event == "publish_pg":
            done = data.get("done", 0)
            total = data.get("total") or 0
            upsert_job(
                job_id,
                stage="pg",
                done=done,
                total=total,
                progress=round(100 * done / total) if total else 0,
                current="更新 PostgreSQL 行情投影",
            )
        elif event == "publish_complete":
            upsert_job(
                job_id,
                stage="finalizing",
                done=data.get("done", 0),
                total=data.get("total"),
                progress=100,
                current="写入发布清单",
            )

    return _callback
