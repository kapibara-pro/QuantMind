from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException


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
