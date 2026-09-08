"""管理员 - 同花顺每日快照目录与数据预览。"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text

from backend.services.api.user_app.middleware.auth import require_admin
from backend.shared.database_manager_v2 import get_session
from backend.shared.stock_utils import StockCodeUtil

router = APIRouter(dependencies=[Depends(require_admin)])


THS_GROUPS: tuple[dict[str, str], ...] = (
    {"id": "selection", "name": "股票选股", "category_id": "1"},
    {"id": "emotion", "name": "市场情绪", "category_id": "2"},
    {"id": "auction", "name": "盘前竞价", "category_id": "3"},
    {"id": "sector", "name": "板块分析", "category_id": "4"},
)

THS_DATASETS: tuple[dict[str, str], ...] = (
    {
        "dataset": "ticker_catalog",
        "name": "A股标的目录",
        "group": "selection",
        "note": "沪深京 A 股代码、名称及交易所目录",
    },
    {
        "dataset": "valuation_snapshot",
        "name": "估值快照",
        "group": "selection",
        "note": "PE、PB、PS、PCF 等每日估值指标",
    },
    {
        "dataset": "limit_up_pool",
        "name": "涨停池",
        "group": "emotion",
        "note": "当日涨停股票及封板信息",
    },
    {
        "dataset": "limit_down_pool",
        "name": "跌停池",
        "group": "emotion",
        "note": "当日跌停股票明细",
    },
    {
        "dataset": "limit_break_pool",
        "name": "炸板池",
        "group": "emotion",
        "note": "当日打开涨停股票明细",
    },
    {
        "dataset": "limit_up_ladder",
        "name": "连板天梯",
        "group": "emotion",
        "note": "连续涨停层级和梯队结构",
    },
    {
        "dataset": "anomaly_list",
        "name": "盘中异动",
        "group": "emotion",
        "note": "价格和成交异动股票列表",
    },
    {
        "dataset": "skyrocket_list",
        "name": "飙升榜",
        "group": "emotion",
        "note": "日内关注度快速上升股票",
    },
    {
        "dataset": "hot_stock_list",
        "name": "热股榜",
        "group": "emotion",
        "note": "当日热门股票排名",
    },
    {
        "dataset": "hot_stock_list_history",
        "name": "历史热股榜",
        "group": "emotion",
        "note": "指定业务日期的热股排名",
    },
    {
        "dataset": "dragon_tiger_all",
        "name": "龙虎榜",
        "group": "emotion",
        "note": "当日全部榜型的龙虎榜数据",
    },
    {
        "dataset": "auction_snapshot",
        "name": "集合竞价终态",
        "group": "auction",
        "note": "09:35 保存的全市场集合竞价终态",
    },
    {
        "dataset": "auction_short_term_benchmark",
        "name": "短线风向标",
        "group": "auction",
        "note": "盘前短线情绪标签和比例基准",
    },
    {
        "dataset": "index_catalog_cn_concept",
        "name": "概念板块目录",
        "group": "sector",
        "note": "同花顺概念指数目录",
    },
    {
        "dataset": "index_catalog_industry",
        "name": "行业板块目录",
        "group": "sector",
        "note": "同花顺行业指数目录",
    },
    {
        "dataset": "index_catalog_region",
        "name": "地域板块目录",
        "group": "sector",
        "note": "同花顺地域指数目录",
    },
    {
        "dataset": "index_catalog_tszs",
        "name": "特色指数目录",
        "group": "sector",
        "note": "同花顺特色指数目录",
    },
    {
        "dataset": "index_snapshot",
        "name": "指数行情快照",
        "group": "sector",
        "note": "配置指数的每日价格快照",
    },
    {
        "dataset": "index_constituents",
        "name": "指数成分快照",
        "group": "sector",
        "note": "配置指数的当前成分股快照",
    },
)

_DATASET_NAMES = {item["dataset"] for item in THS_DATASETS}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def _snapshot_table_exists(session: Any) -> bool:
    result = await session.execute(
        text("SELECT to_regclass('public.qm_ths_daily_snapshots') IS NOT NULL")
    )
    return bool(result.scalar())


@router.get("/catalog")
async def get_ths_snapshot_catalog() -> dict[str, Any]:
    """返回固定数据集目录及 PostgreSQL 中的实际历史覆盖情况。"""
    async with get_session(read_only=True) as session:
        table_exists = await _snapshot_table_exists(session)
        stats: dict[str, dict[str, Any]] = {}
        if table_exists:
            result = await session.execute(
                text(
                    """
                    WITH latest AS (
                        SELECT dataset, MAX(snapshot_date) AS latest_date
                        FROM qm_ths_daily_snapshots
                        GROUP BY dataset
                    )
                    SELECT s.dataset,
                           MIN(s.snapshot_date) AS start_date,
                           MAX(s.snapshot_date) AS end_date,
                           COUNT(DISTINCT s.snapshot_date) AS snapshot_days,
                           COUNT(*) AS rows_total,
                           COUNT(*) FILTER (
                               WHERE s.snapshot_date = latest.latest_date
                           ) AS latest_rows,
                           MAX(s.captured_at) AS updated_at
                    FROM qm_ths_daily_snapshots s
                    JOIN latest ON latest.dataset = s.dataset
                    GROUP BY s.dataset, latest.latest_date
                    """
                )
            )
            stats = {
                str(row["dataset"]): dict(row)
                for row in result.mappings().all()
            }

    datasets = []
    for spec in THS_DATASETS:
        stat = stats.get(spec["dataset"], {})
        datasets.append(
            {
                **spec,
                "storage_type": "postgres_snapshot",
                "available": bool(stat),
                "start_date": stat.get("start_date"),
                "end_date": stat.get("end_date"),
                "snapshot_days": int(stat.get("snapshot_days") or 0),
                "rows_total": int(stat.get("rows_total") or 0),
                "latest_rows": int(stat.get("latest_rows") or 0),
                "updated_at": stat.get("updated_at"),
            }
        )

    groups = []
    for group in THS_GROUPS:
        members = [item for item in datasets if item["group"] == group["id"]]
        groups.append(
            {
                **group,
                "dataset_count": len(members),
                "available_count": sum(1 for item in members if item["available"]),
                "rows_total": sum(item["rows_total"] for item in members),
            }
        )

    return {
        "success": True,
        "data": {
            "source": "ths",
            "storage_type": "postgres_snapshot",
            "table_ready": table_exists,
            "api_key_configured": bool(
                os.getenv("HITHINK_FINANCE_API_KEY", "").strip()
            ),
            "schedule_enabled": os.getenv(
                "HITHINK_FINANCE_SNAPSHOT_ENABLED", "false"
            ).lower()
            == "true",
            "groups": groups,
            "datasets": datasets,
            "timestamp": _now_iso(),
        },
    }


@router.get("/preview")
async def preview_ths_snapshot(
    dataset: str = Query(..., min_length=1, max_length=64),
    snapshot_date: date | None = Query(default=None),
    symbol: str | None = Query(default=None, max_length=32),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    """预览指定数据集最近或指定日期的原始 JSONB 快照。"""
    if dataset not in _DATASET_NAMES:
        raise HTTPException(
            status_code=400, detail=f"未知同花顺数据集: {dataset}"
        )

    async with get_session(read_only=True) as session:
        if not await _snapshot_table_exists(session):
            return {
                "success": True,
                "data": {
                    "dataset": dataset,
                    "snapshot_date": snapshot_date,
                    "rows_total": 0,
                    "payload_fields": [],
                    "data": [],
                    "timestamp": _now_iso(),
                },
            }

        target_date = snapshot_date
        if target_date is None:
            target_date = (
                await session.execute(
                    text(
                        "SELECT MAX(snapshot_date) FROM qm_ths_daily_snapshots "
                        "WHERE dataset = :dataset"
                    ),
                    {"dataset": dataset},
                )
            ).scalar()

        conditions = ["dataset = :dataset", "snapshot_date = :snapshot_date"]
        params: dict[str, Any] = {
            "dataset": dataset,
            "snapshot_date": target_date,
            "limit": limit,
        }
        if symbol and symbol.strip():
            raw_symbol = symbol.strip()
            normalized = StockCodeUtil.to_prefix(raw_symbol)
            conditions.append(
                "(symbol = :symbol OR scope_key ILIKE :search "
                "OR payload->>'thscode' ILIKE :search "
                "OR payload->>'name' ILIKE :search)"
            )
            params["symbol"] = normalized
            params["search"] = f"%{raw_symbol}%"
        where = " AND ".join(conditions)

        count_result = await session.execute(
            text(f"SELECT COUNT(*) FROM qm_ths_daily_snapshots WHERE {where}"),
            params,
        )
        rows_total = int(count_result.scalar() or 0)
        rows_result = await session.execute(
            text(
                f"""
                SELECT id, snapshot_date, dataset, scope_key, symbol, as_of_ms,
                       payload, source_request_id, row_count, status,
                       schema_version, captured_at
                FROM qm_ths_daily_snapshots
                WHERE {where}
                ORDER BY symbol NULLS LAST, scope_key, id
                LIMIT :limit
                """
            ),
            params,
        )
        rows = [dict(row) for row in rows_result.mappings().all()]

    payload_fields = sorted(
        {
            str(key)
            for row in rows
            if isinstance(row.get("payload"), dict)
            for key in row["payload"]
        }
    )
    return {
        "success": True,
        "data": {
            "dataset": dataset,
            "snapshot_date": target_date,
            "rows_total": rows_total,
            "payload_fields": payload_fields,
            "data": rows,
            "timestamp": _now_iso(),
        },
    }
