"""市场定时同步配置 API。

GET  /api/v1/admin/data-platform/sync-schedule            全部市场定时配置
GET  /api/v1/admin/data-platform/sync-schedule/{market}   单市场配置
POST /api/v1/admin/data-platform/sync-schedule/{market}   保存单市场配置
POST /api/v1/admin/data-platform/sync-schedule/{market}/run  立即触发一次同步（测试用）
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator

from backend.services.api.user_app.middleware.auth import require_admin

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_admin)])


class SyncScheduleRequest(BaseModel):
    enabled: bool = False
    time: str = Field("03:00", description="每天触发时间 HH:MM（Asia/Shanghai，建议凌晨执行如 03:00）")
    days: int = Field(5, ge=1, le=365, description="同步最近 N 个交易日（BC 为自然日）")
    datasets: list[str] = Field(default_factory=list, description="要同步的数据集；空=按默认全量")
    source_id: str = Field("quantdb", description="数据源标识；A 股可选 quantdb/easy_tdx")
    publish_mode: str = Field("shadow", description="落盘发布模式；easy_tdx 当前仅支持 shadow")
    with_qlib: bool = Field(False, description="同步后重建 Qlib 缓存")

    @field_validator("time")
    @classmethod
    def _validate_time(cls, v: str) -> str:
        from datetime import datetime

        try:
            datetime.strptime(v.strip(), "%H:%M")
        except ValueError:
            raise ValueError("time 必须是 HH:MM 格式（如 22:30）")
        return v.strip()

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in {"quantdb", "easy_tdx"}:
            raise ValueError("source_id 必须是 quantdb 或 easy_tdx")
        return value

    @field_validator("publish_mode")
    @classmethod
    def _validate_publish_mode(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in {"shadow", "official"}:
            raise ValueError("publish_mode 必须是 shadow 或 official")
        return value


def _scheduler():
    from backend.services.engine.tasks.market_sync_scheduler import (
        MARKETS,
        get_all_schedules,
        get_schedule,
        run_market_sync,
        save_schedule,
    )

    return MARKETS, get_all_schedules, get_schedule, save_schedule, run_market_sync


def _validate_schedule_payload(market: str, payload: SyncScheduleRequest) -> None:
    if market != "A" and payload.source_id != "quantdb":
        raise HTTPException(status_code=400, detail="easy_tdx 仅支持 A 股市场")
    if payload.source_id == "easy_tdx" and payload.publish_mode != "shadow":
        raise HTTPException(status_code=400, detail="easy_tdx 第一版仅支持影子落盘")
    if payload.source_id == "easy_tdx" and payload.with_qlib:
        raise HTTPException(status_code=400, detail="easy_tdx 尚不能直接重建 Qlib")
    if payload.source_id == "easy_tdx" and payload.datasets:
        from backend.services.engine.data_platform.easy_tdx_sync import DATASETS

        unknown = [name for name in payload.datasets if name not in DATASETS]
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"easy_tdx 不支持数据集: {unknown[0]}",
            )


@router.get("/sync-schedule")
async def list_schedules(current_user: dict = Depends(require_admin)):
    MARKETS, get_all_schedules, *_ = _scheduler()
    schedules = get_all_schedules()
    return {
        "success": True,
        "data": {
            "schedules": [
                {"market": m, "label": MARKETS[m], **schedules[m]} for m in MARKETS
            ]
        },
    }


@router.get("/sync-schedule/{market}")
async def get_market_schedule(market: str, current_user: dict = Depends(require_admin)):
    MARKETS, _, get_schedule, *_ = _scheduler()
    market = market.upper()
    if market not in MARKETS:
        raise HTTPException(status_code=404, detail=f"未知市场: {market}")
    return {"success": True, "data": {"market": market, "label": MARKETS[market], **get_schedule(market)}}


@router.post("/sync-schedule/{market}")
async def save_market_schedule(
    market: str,
    payload: SyncScheduleRequest,
    current_user: dict = Depends(require_admin),
):
    MARKETS, _, _, save_schedule, _ = _scheduler()
    market = market.upper()
    if market not in MARKETS:
        raise HTTPException(status_code=404, detail=f"未知市场: {market}")
    _validate_schedule_payload(market, payload)
    saved = save_schedule(
        market,
        {
            "enabled": payload.enabled,
            "time": payload.time,
            "days": payload.days,
            "datasets": payload.datasets,
            "source_id": payload.source_id,
            "publish_mode": payload.publish_mode,
            "with_qlib": payload.with_qlib,
        },
    )
    return {
        "success": True,
        "data": {"market": market, "label": MARKETS[market], **saved},
    }


@router.post("/sync-schedule/{market}/run")
async def run_market_schedule_now(
    market: str,
    payload: SyncScheduleRequest | None = None,
    current_user: dict = Depends(require_admin),
):
    """立即触发一次同步；请求体为空时使用已保存配置。"""
    MARKETS, _, get_schedule, _, run_market_sync = _scheduler()
    market = market.upper()
    if market not in MARKETS:
        raise HTTPException(status_code=404, detail=f"未知市场: {market}")
    schedule = payload or SyncScheduleRequest(**get_schedule(market))
    if not schedule.enabled:
        raise HTTPException(status_code=400, detail="该市场定时同步未启用，请先保存配置")
    _validate_schedule_payload(market, schedule)

    cfg = schedule.model_dump()
    if market == "A":
        from backend.services.api.routers.admin.data_platform import (
            DataSourceSyncRequest,
            create_data_source_sync_job,
        )

        response = await create_data_source_sync_job(
            DataSourceSyncRequest(
                source_id=schedule.source_id,
                market=market,
                datasets=list(schedule.datasets),
                days=schedule.days,
                publish_mode=schedule.publish_mode,
                with_pg=False,
                with_qlib=schedule.with_qlib,
            ),
            current_user,
        )
        job = response["data"]["job"]
        return {
            "success": True,
            "data": {
                "market": market,
                "label": MARKETS[market],
                "status": job["status"],
                "job": job,
            },
        }

    from backend.services.engine.qlib_app.celery_config import celery_app

    celery_app.send_task(
        "engine.tasks.run_market_scheduled_sync",
        args=[market, cfg],
        queue="qlib_backtest_srv",
    )
    return {
        "success": True,
        "data": {"market": market, "label": MARKETS[market], "status": "dispatched"},
    }
