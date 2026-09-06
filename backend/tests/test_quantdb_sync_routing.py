from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.scripts.quantdb_daily_sync import (
    DEFAULT_SYNC_DATASETS,
    V1_DATASETS,
    V2_DATASETS,
)
from backend.shared.quantdb_sync_jobs import build_dataset_results


def test_quantdb_sync_registry_includes_minute_datasets():
    registered = {
        item["sub_category"] for item in [*V1_DATASETS, *V2_DATASETS]
    }
    default_sync = {item["sub_category"] for item in DEFAULT_SYNC_DATASETS}

    assert {"min1_kline", "min5_kline"} <= registered
    assert not {"min1_kline", "min5_kline"} & default_sync


def test_unprocessed_dataset_is_not_reported_as_up_to_date():
    results = build_dataset_results(
        ["min5_kline"],
        {"parquet": {"synced": 0, "up_to_date": 0, "errors": []}},
    )

    assert results == [
        {
            "dataset": "min5_kline",
            "status": "failed",
            "downloaded": 0,
            "error": "同步流程未处理该数据集",
        }
    ]


def test_explicit_dataset_outcome_is_preserved():
    results = build_dataset_results(
        ["min1_kline", "min5_kline"],
        {
            "parquet": {
                "per_dataset": {
                    "min1_kline": {"status": "synced", "downloaded": 2},
                    "min5_kline": {"status": "up_to_date", "downloaded": 0},
                }
            }
        },
    )

    assert [item["status"] for item in results] == ["synced", "up_to_date"]
    assert results[0]["downloaded"] == 2


def test_generic_task_marks_quantdb_dataset_failure(monkeypatch):
    from backend.scripts import quantdb_daily_sync
    from backend.services.engine.tasks import celery_tasks
    from backend.shared import data_sync_jobs

    job = {
        "job_id": "sync-test",
        "source_id": "quantdb",
        "datasets": ["min5_kline"],
        "with_pg": False,
        "with_qlib": False,
    }
    updates: list[dict] = []
    monkeypatch.setattr(data_sync_jobs, "get_job", lambda *_: job)
    monkeypatch.setattr(data_sync_jobs, "cancel_requested", lambda *_: False)
    monkeypatch.setattr(data_sync_jobs, "progress_callback", lambda *_: None)
    monkeypatch.setattr(
        data_sync_jobs,
        "upsert_job",
        lambda _job_id, **fields: updates.append(fields),
    )
    monkeypatch.setattr(
        quantdb_daily_sync,
        "run_daily_sync",
        lambda **_: {
            "parquet": {
                "errors": ["min5_kline: 1 个对象下载失败"],
                "per_dataset": {
                    "min5_kline": {
                        "status": "failed",
                        "downloaded": 0,
                        "errors": 1,
                    }
                },
            },
            "sources": {},
        },
    )

    response = celery_tasks.run_data_source_sync.run("sync-test")

    assert response["status"] == "failed"
    assert updates[-1]["status"] == "failed"
    assert "min5_kline" in updates[-1]["error"]


def test_generic_task_marks_total_easy_tdx_failure(monkeypatch):
    from backend.services.engine.data_platform import easy_tdx_sync
    from backend.services.engine.tasks import celery_tasks
    from backend.shared import data_sync_jobs

    job = {
        "job_id": "sync-test",
        "source_id": "easy_tdx",
        "datasets": ["min1_kline"],
        "days": 5,
        "symbols": ["SH600036"],
        "publish_mode": "shadow",
    }
    updates: list[dict] = []
    monkeypatch.setattr(data_sync_jobs, "get_job", lambda *_: job)
    monkeypatch.setattr(data_sync_jobs, "cancel_requested", lambda *_: False)
    monkeypatch.setattr(data_sync_jobs, "progress_callback", lambda *_: None)
    monkeypatch.setattr(
        data_sync_jobs,
        "upsert_job",
        lambda _job_id, **fields: updates.append(fields),
    )
    monkeypatch.setattr(
        easy_tdx_sync,
        "sync",
        lambda **_: {
            "datasets": {
                "min1_kline": {
                    "rows": 0,
                    "files": 0,
                    "failed_symbols": 1,
                }
            },
            "errors": [{"error": "节点无响应"}],
            "error_count": 1,
        },
    )

    response = celery_tasks.run_data_source_sync.run("sync-test")

    assert response["status"] == "failed"
    assert updates[-1]["status"] == "failed"
    assert "节点无响应" in updates[-1]["error"]


@pytest.mark.asyncio
async def test_quantdb_sync_endpoint_rejects_disabled_source(monkeypatch):
    from backend.services.api.routers.admin import quantdb_console
    from backend.shared import data_source_config

    monkeypatch.setattr(data_source_config, "is_source_enabled", lambda *_: False)

    with pytest.raises(HTTPException) as exc_info:
        await quantdb_console.sync_datasets(
            quantdb_console.SyncDatasetsRequest(datasets=["min5_kline"]),
            current_user={"username": "admin"},
        )

    assert exc_info.value.status_code == 409
    assert "QuantDB 数据源未启用" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_generic_update_check_rejects_disabled_source(monkeypatch):
    from backend.services.api.routers.admin import data_platform
    from backend.shared import data_source_config

    monkeypatch.setattr(data_source_config, "is_source_enabled", lambda *_: False)

    with pytest.raises(HTTPException) as exc_info:
        await data_platform.check_source_updates(
            "quantdb",
            data_platform.SourceUpdateCheckRequest(datasets=["min5_kline"]),
            current_user={"username": "admin"},
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "quantdb 数据源未启用"
