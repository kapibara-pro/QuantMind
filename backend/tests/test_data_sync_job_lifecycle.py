"""数据源同步任务的生命周期自愈测试。

事故背景：easy_tdx 正式发布把数据盘写满后，Docker 无法创建容器快照、Redis 拒绝
写入、Celery worker 全部退出。发布任务因此永久停在 cancelling：进度不再更新，
点「取消」也因为 request_cancel 只接受 queued/running 而直接报 409。

这里锁定修复后的行为：
1. worker 心跳超时后，running/cancelling 任务由状态接口自愈为终态；
2. 已请求取消的任务自愈为 cancelled（而不是 failed）；
3. 重复点「取消」幂等，worker 已失联时立即落 cancelled；
4. 正常心跳中的任务不会被误杀。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from backend.shared import data_sync_jobs


def _iso(minutes_ago: float) -> str:
    return (
        (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago))
        .isoformat()
        .replace("+00:00", "Z")
    )


def _job(**overrides: Any) -> dict[str, Any]:
    job: dict[str, Any] = {
        "job_id": "sync-easy_tdx-20260909-224206-4a2226",
        "source_id": "easy_tdx",
        "operation": "publish",
        "market": "A",
        "status": "cancelling",
        "stage": "publish_prepare",
        "progress": 44,
        "current": "整理 SZ300735.parquet",
        "cancel_requested": True,
        "updated_at": _iso(120),
        "started_at": _iso(600),
    }
    job.update(overrides)
    return job


@pytest.fixture()
def store(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, Any]]:
    """用内存字典替换 Redis，单测不依赖真实 Redis。"""
    state: dict[str, dict[str, Any]] = {}

    def _get_job(job_id: str) -> dict[str, Any] | None:
        return state.get(job_id)

    def _upsert_job(job_id: str, **fields: Any) -> None:
        current = state.setdefault(job_id, {"job_id": job_id})
        current.update(fields)
        current["updated_at"] = data_sync_jobs._now_iso()

    monkeypatch.setattr(data_sync_jobs, "get_job", _get_job)
    monkeypatch.setattr(data_sync_jobs, "upsert_job", _upsert_job)
    return state


def test_stale_cancelling_job_is_reaped_as_cancelled(store):
    store["job"] = _job()

    result = data_sync_jobs.reconcile_job("job")

    assert result is not None
    assert result["status"] == "cancelled"
    assert result["stage"] == "cancelled"
    assert result["finished_at"]


def test_stale_running_job_is_reaped_as_failed_with_reason(store):
    store["job"] = _job(status="running", cancel_requested=False, error=None)

    result = data_sync_jobs.reconcile_job("job")

    assert result is not None
    assert result["status"] == "failed"
    assert result["stage"] == "worker_lost"
    assert "心跳" in (result["error"] or "")


def test_healthy_job_is_not_reaped(store):
    store["job"] = _job(updated_at=_iso(0))

    result = data_sync_jobs.reconcile_job("job")

    assert result is not None
    assert result["status"] == "cancelling"
    assert not result.get("finished_at")


def test_qlib_stage_uses_tighter_stale_threshold(store):
    store["job"] = _job(
        stage="qlib_build", status="running", cancel_requested=False, updated_at=_iso(10)
    )

    result = data_sync_jobs.reconcile_job("job")

    assert result is not None
    assert result["status"] == "failed"


def test_queued_job_tolerates_long_wait(store):
    # worker 可能正忙于别的长任务，排队 15 分钟不应被判为僵尸
    store["job"] = _job(status="queued", cancel_requested=False, updated_at=_iso(15))

    result = data_sync_jobs.reconcile_job("job")

    assert result is not None
    assert result["status"] == "queued"
    assert not result.get("finished_at")


def test_queued_job_is_reaped_when_nothing_picks_it_up(store):
    store["job"] = _job(status="queued", cancel_requested=False, updated_at=_iso(120))

    result = data_sync_jobs.reconcile_job("job")

    assert result is not None
    assert result["status"] == "failed"


def test_request_cancel_is_idempotent_while_waiting(store):
    store["job"] = _job(status="running", cancel_requested=False, updated_at=_iso(0))

    assert data_sync_jobs.request_cancel("job") is True
    assert store["job"]["status"] == "cancelling"

    # 重复点击取消不应报错，也不应把仍在运行的任务改成失败
    assert data_sync_jobs.request_cancel("job") is True
    assert store["job"]["status"] == "cancelling"


def test_request_cancel_finalizes_when_worker_is_gone(store):
    store["job"] = _job(status="running", cancel_requested=False, updated_at=_iso(120))

    assert data_sync_jobs.request_cancel("job") is True

    assert store["job"]["status"] == "cancelled"
    assert store["job"]["finished_at"]


def test_request_cancel_rejects_finished_job(store):
    store["job"] = _job(status="completed", cancel_requested=False)

    assert data_sync_jobs.request_cancel("job") is False


def test_request_cancel_rejects_unknown_job(store):
    assert data_sync_jobs.request_cancel("missing") is False
