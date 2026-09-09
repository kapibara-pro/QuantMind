from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException


def test_sync_job_encoding_accepts_paths_and_dates():
    from backend.shared import data_sync_jobs

    encoded = data_sync_jobs._encode(
        {"path": Path("/tmp/release/manifest.json"), "date": datetime(2026, 9, 9)}
    )

    assert json.loads(encoded) == {
        "path": "/tmp/release/manifest.json",
        "date": "2026-09-09T00:00:00",
    }


@pytest.mark.asyncio
async def test_run_ashare_now_uses_current_request_and_returns_tracked_job(
    monkeypatch: pytest.MonkeyPatch,
):
    from backend.services.api.routers.admin import data_platform, sync_schedule

    saved = {
        "enabled": True,
        "time": "03:00",
        "days": 5,
        "datasets": ["daily_unadjusted"],
        "source_id": "quantdb",
        "publish_mode": "official",
        "with_qlib": False,
    }
    monkeypatch.setattr(
        sync_schedule,
        "_scheduler",
        lambda: (
            {"A": "QuantDB A股"},
            None,
            lambda _market: saved,
            None,
            None,
        ),
    )
    received = []

    async def _fake_create(payload, current_user):
        received.append((payload, current_user))
        return {
            "success": True,
            "data": {
                "job": {
                    "job_id": "sync-easy_tdx-test",
                    "status": "queued",
                }
            },
        }

    monkeypatch.setattr(data_platform, "create_data_source_sync_job", _fake_create)
    payload = sync_schedule.SyncScheduleRequest(
        enabled=True,
        time="22:30",
        days=3,
        datasets=["min1_kline", "min5_kline"],
        source_id="easy_tdx",
        publish_mode="shadow",
    )

    response = await sync_schedule.run_market_schedule_now(
        "a",
        payload,
        {"username": "admin"},
    )

    request, user = received[0]
    assert request.source_id == "easy_tdx"
    assert request.datasets == ["min1_kline", "min5_kline"]
    assert request.days == 3
    assert user == {"username": "admin"}
    assert response["data"]["job"]["job_id"] == "sync-easy_tdx-test"
    assert response["data"]["status"] == "queued"


@pytest.mark.asyncio
async def test_run_ashare_now_without_body_uses_saved_schedule(
    monkeypatch: pytest.MonkeyPatch,
):
    from backend.services.api.routers.admin import data_platform, sync_schedule

    saved = {
        "enabled": True,
        "time": "22:30",
        "days": 8,
        "datasets": ["min5_kline"],
        "source_id": "easy_tdx",
        "publish_mode": "shadow",
        "with_qlib": False,
    }
    monkeypatch.setattr(
        sync_schedule,
        "_scheduler",
        lambda: (
            {"A": "QuantDB A股"},
            None,
            lambda _market: saved,
            None,
            None,
        ),
    )
    received = []

    async def _fake_create(payload, _current_user):
        received.append(payload)
        return {
            "success": True,
            "data": {"job": {"job_id": "sync-saved", "status": "queued"}},
        }

    monkeypatch.setattr(data_platform, "create_data_source_sync_job", _fake_create)

    await sync_schedule.run_market_schedule_now(
        "A",
        None,
        {"username": "admin"},
    )

    assert received[0].source_id == "easy_tdx"
    assert received[0].datasets == ["min5_kline"]
    assert received[0].days == 8


@pytest.mark.asyncio
async def test_create_sync_job_rejects_same_source_active_job(
    monkeypatch: pytest.MonkeyPatch,
):
    from backend.services.api.routers.admin import data_platform
    from backend.services.engine.data_platform import source_catalog
    from backend.shared import data_source_config, data_sync_jobs

    monkeypatch.setattr(
        source_catalog,
        "get_source_descriptor",
        lambda _source_id: SimpleNamespace(markets=["A"], configurable=True),
    )
    monkeypatch.setattr(data_source_config, "is_source_enabled", lambda *_: True)
    active_job = {
        "job_id": "sync-easy_tdx-active",
        "source_id": "easy_tdx",
        "market": "A",
        "status": "running",
    }

    def _raise_active(**_kwargs):
        raise data_sync_jobs.ActiveSyncJobError(active_job)

    monkeypatch.setattr(data_sync_jobs, "create_job", _raise_active)

    with pytest.raises(HTTPException) as exc_info:
        await data_platform.create_data_source_sync_job(
            data_platform.DataSourceSyncRequest(
                source_id="easy_tdx",
                market="A",
                publish_mode="shadow",
            ),
            {"username": "admin"},
        )

    assert exc_info.value.status_code == 409
    assert "sync-easy_tdx-active" in str(exc_info.value.detail)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload_kwargs", "expected_detail"),
    [
        (
            {
                "source_id": "quantdb",
                "operation": "publish",
                "publish_mode": "official",
            },
            "仅 easy_tdx 支持独立发布操作",
        ),
        (
            {"source_id": "easy_tdx", "operation": "publish"},
            "发布操作必须使用 official 模式",
        ),
        (
            {
                "source_id": "easy_tdx",
                "operation": "publish",
                "publish_mode": "official",
                "with_qlib": False,
            },
            "必须同时更新 Qlib",
        ),
        (
            {
                "source_id": "easy_tdx",
                "datasets": ["daily_unadjusted", "daily_forward"],
                "publish_mode": "official",
                "with_qlib": True,
            },
            "缺少: daily_backward",
        ),
    ],
)
async def test_sync_job_validates_publication_contract(
    monkeypatch: pytest.MonkeyPatch,
    payload_kwargs: dict,
    expected_detail: str,
):
    from backend.services.api.routers.admin import data_platform
    from backend.services.engine.data_platform import source_catalog
    from backend.shared import data_source_config

    monkeypatch.setattr(
        source_catalog,
        "get_source_descriptor",
        lambda _source_id: SimpleNamespace(markets=["A"], configurable=True),
    )
    monkeypatch.setattr(data_source_config, "is_source_enabled", lambda *_: True)

    with pytest.raises(HTTPException) as exc_info:
        await data_platform.create_data_source_sync_job(
            data_platform.DataSourceSyncRequest(market="A", **payload_kwargs),
            {"username": "admin"},
        )

    assert exc_info.value.status_code == 400
    assert expected_detail in str(exc_info.value.detail)


def test_schedule_rejects_projection_without_publication():
    from backend.services.api.routers.admin import sync_schedule

    payload = sync_schedule.SyncScheduleRequest(
        enabled=True,
        source_id="easy_tdx",
        publish_mode="shadow",
        with_qlib=True,
    )

    with pytest.raises(HTTPException, match="仅采集模式不能更新"):
        sync_schedule._validate_schedule_payload("A", payload)


@pytest.mark.asyncio
async def test_publish_job_is_dispatched_to_dedicated_qlib_queue(monkeypatch):
    from backend.services.api.routers.admin import data_platform
    from backend.services.engine.data_platform import source_catalog
    from backend.services.engine.qlib_app.celery_config import celery_app
    from backend.shared import data_source_config, data_sync_jobs

    monkeypatch.setattr(
        source_catalog,
        "get_source_descriptor",
        lambda _source_id: SimpleNamespace(markets=["A"], configurable=True),
    )
    monkeypatch.setattr(data_source_config, "is_source_enabled", lambda *_: True)
    monkeypatch.setattr(
        data_sync_jobs,
        "create_job",
        lambda **kwargs: {"job_id": "publish-queued", **kwargs},
    )
    updates: list[dict] = []
    monkeypatch.setattr(
        data_sync_jobs,
        "upsert_job",
        lambda _job_id, **fields: updates.append(fields),
    )
    sent: list[dict] = []

    def _send_task(name, **kwargs):
        sent.append({"name": name, **kwargs})
        return SimpleNamespace(id="celery-publish-1")

    monkeypatch.setattr(celery_app, "send_task", _send_task)

    await data_platform.create_data_source_sync_job(
        data_platform.DataSourceSyncRequest(
            source_id="easy_tdx",
            operation="publish",
            market="A",
            publish_mode="official",
            with_pg=True,
            with_qlib=True,
        ),
        {"username": "admin"},
    )

    assert sent[0]["queue"] == "qlib_build"
    assert updates[-1]["celery_task_id"] == "celery-publish-1"


class _JobRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: str, ex=None, nx=False):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def delete(self, key: str):
        self.values.pop(key, None)

    def hset(self, key: str, mapping: dict[str, str]):
        self.hashes.setdefault(key, {}).update(mapping)

    def hget(self, key: str, field: str):
        return self.hashes.get(key, {}).get(field)

    def hgetall(self, key: str):
        return self.hashes.get(key, {})

    def expire(self, _key: str, _seconds: int):
        return True

    def keys(self, prefix: str):
        start = prefix.removesuffix("*")
        return [key for key in self.hashes if key.startswith(start)]


def test_upsert_job_persists_path_values_in_result(monkeypatch: pytest.MonkeyPatch):
    from backend.shared import data_sync_jobs

    redis = _JobRedis()
    monkeypatch.setattr(data_sync_jobs, "_redis", lambda: redis)

    data_sync_jobs.upsert_job(
        "sync-publish-result",
        status="completed",
        result={"manifest": Path("/data/easy_tdx/releases/manifest.json")},
    )

    stored = json.loads(
        redis.hashes[data_sync_jobs.KEY_PREFIX + "sync-publish-result"]["result"]
    )
    assert stored["manifest"] == "/data/easy_tdx/releases/manifest.json"


def test_same_source_job_lock_is_released_after_terminal_status(
    monkeypatch: pytest.MonkeyPatch,
):
    from backend.shared import data_sync_jobs

    redis = _JobRedis()
    monkeypatch.setattr(data_sync_jobs, "_redis", lambda: redis)
    kwargs = {
        "source_id": "easy_tdx",
        "market": "A",
        "datasets": ["min5_kline"],
        "days": 5,
        "symbols": [],
        "publish_mode": "shadow",
        "with_pg": False,
        "with_qlib": False,
        "started_by": "admin",
    }

    first = data_sync_jobs.create_job(**kwargs)
    with pytest.raises(data_sync_jobs.ActiveSyncJobError) as exc_info:
        data_sync_jobs.create_job(**kwargs)
    assert exc_info.value.job["job_id"] == first["job_id"]

    data_sync_jobs.upsert_job(first["job_id"], status="completed")
    second = data_sync_jobs.create_job(**kwargs)

    assert second["job_id"] != first["job_id"]


def test_qlib_progress_reports_overall_percent_and_detail(monkeypatch):
    from backend.shared import data_sync_jobs

    redis = _JobRedis()
    monkeypatch.setattr(data_sync_jobs, "_redis", lambda: redis)
    job = data_sync_jobs.create_job(
        source_id="easy_tdx",
        operation="publish",
        market="A",
        datasets=["daily_unadjusted", "daily_forward", "daily_backward"],
        days=5,
        symbols=[],
        publish_mode="official",
        with_pg=True,
        with_qlib=True,
        started_by="admin",
    )
    callback = data_sync_jobs.progress_callback(job["job_id"])

    callback("publish_start", total=5)
    callback(
        "publish_qlib",
        done=3,
        total=5,
        progress=50,
        phase="write",
        current="正在写入 Qlib 特征 1200/5000",
    )

    current = data_sync_jobs.get_job(job["job_id"])
    assert current["progress"] == 70
    assert current["substage"] == "write"
    assert current["current"] == "正在写入 Qlib 特征 1200/5000"


def test_stale_qlib_job_is_closed_as_worker_lost(monkeypatch):
    from backend.shared import data_sync_jobs

    redis = _JobRedis()
    monkeypatch.setattr(data_sync_jobs, "_redis", lambda: redis)
    monkeypatch.setenv("QLIB_JOB_STALE_SECONDS", "60")
    job = data_sync_jobs.create_job(
        source_id="easy_tdx",
        operation="publish",
        market="A",
        datasets=["daily_unadjusted", "daily_forward", "daily_backward"],
        days=5,
        symbols=[],
        publish_mode="official",
        with_pg=True,
        with_qlib=True,
        started_by="admin",
    )
    data_sync_jobs.upsert_job(job["job_id"], status="running", stage="qlib")
    stale = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    redis.hashes[data_sync_jobs.KEY_PREFIX + job["job_id"]]["updated_at"] = json.dumps(
        stale
    )

    reconciled = data_sync_jobs.reconcile_job(job["job_id"])

    assert reconciled["status"] == "failed"
    assert reconciled["stage"] == "worker_lost"
    assert "没有心跳" in reconciled["error"]
    assert redis.get(data_sync_jobs._active_key("A", "easy_tdx")) is None


def test_celery_failure_state_closes_active_job(monkeypatch):
    from backend.services.engine.qlib_app.celery_config import celery_app
    from backend.shared import data_sync_jobs

    redis = _JobRedis()
    monkeypatch.setattr(data_sync_jobs, "_redis", lambda: redis)
    job = data_sync_jobs.create_job(
        source_id="easy_tdx",
        operation="publish",
        market="A",
        datasets=["daily_unadjusted", "daily_forward", "daily_backward"],
        days=5,
        symbols=[],
        publish_mode="official",
        with_pg=True,
        with_qlib=True,
        started_by="admin",
    )
    data_sync_jobs.upsert_job(
        job["job_id"],
        status="running",
        stage="qlib",
        celery_task_id="celery-lost-1",
    )
    monkeypatch.setattr(
        celery_app,
        "AsyncResult",
        lambda _task_id: SimpleNamespace(
            state="FAILURE", result="Worker exited prematurely"
        ),
    )

    reconciled = data_sync_jobs.reconcile_job(job["job_id"])

    assert reconciled["status"] == "failed"
    assert reconciled["stage"] == "worker_failed"
    assert reconciled["error"] == "Worker exited prematurely"
    assert redis.get(data_sync_jobs._active_key("A", "easy_tdx")) is None
