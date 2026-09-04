"""
管理员 - 数据平台路由
========================

GET  /api/v1/admin/data-platform/markets                 列出支持的市场
GET  /api/v1/admin/data-platform/sources                 列出所有已注册数据源
GET  /api/v1/admin/data-platform/sources/{name}/health   单源所有字段健康
GET  /api/v1/admin/data-platform/health-matrix?market=A  市场 × 字段 × 源 健康矩阵
GET  /api/v1/admin/data-platform/field-coverage          字段覆盖表（YAML 路由 + 实际可用）
GET  /api/v1/admin/data-platform/quality-alerts          告警列表（分页 + 过滤）
POST /api/v1/admin/data-platform/quality-alerts/{id}/ack 标记告警已处理
POST /api/v1/admin/data-platform/sources/{name}/sync     触发指定源同步（占位）
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.api.user_app.middleware.auth import require_admin

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(require_admin)])  # 路由器级认证兜底


# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# Alpha Agent 市场 → H5 数据文件路径（仅 crypto 5min 仍用 H5）
_ALPHA_AGENT_H5_MAP: dict[str, str] = {
    "crypto": "/app/db/crypto_data/5min_pv.h5",
}


def _market_local_stats(market: str) -> dict | None:
    """从各市场本地 parquet 读取统计（行数/标的/日期范围）。

    支持 parquet 单源市场：a_share / futures / hong_kong / us_stock。
    """
    hub_cls_map = {
        "a_share": "quantdb_hub.QuantDBDataHub",
        "futures": "quantfutures_hub.QuantFuturesDataHub",
        "hong_kong": "quanthk_hub.QuantHKDataHub",
        "us_stock": "quantus_hub.QuantUSDataHub",
    }
    hub_ref = hub_cls_map.get(market)
    if not hub_ref:
        return None
    try:
        import duckdb
        from backend.services.engine.data_platform import (
            quantus_hub,
            quantbc_hub,
            quantdb_hub,
            quantfutures_hub,
            quanthk_hub,
        )

        hub = {
            "quantus_hub.QuantUSDataHub": quantus_hub.QuantUSDataHub,
            "quanthk_hub.QuantHKDataHub": quanthk_hub.QuantHKDataHub,
            "quantfutures_hub.QuantFuturesDataHub": quantfutures_hub.QuantFuturesDataHub,
            "quantdb_hub.QuantDBDataHub": quantdb_hub.QuantDBDataHub,
        }[hub_ref].get_instance()
        fwd = hub.data_dir / "1_kline_data" / "daily_forward"
        if not fwd.is_dir():
            return None
        con = duckdb.connect(config={"memory_limit": "2GB", "threads": "2"})
        try:
            df = con.execute(
                f"""
                SELECT COUNT(*) AS rows,
                       COUNT(DISTINCT symbol) AS symbols,
                       MIN(CAST(time AS DATE)) AS start_date,
                       MAX(CAST(time AS DATE)) AS end_date
                FROM read_parquet('{fwd / "dt=*" / "data.parquet"}', hive_partitioning=1)
                """
            ).fetchdf()
        finally:
            con.close()
        if df.empty or not df.iloc[0]["rows"]:
            return None
        row = df.iloc[0]
        return {
            "rows": int(row["rows"]),
            "symbols": int(row["symbols"]),
            "start_date": str(row["start_date"])[:10],
            "end_date": str(row["end_date"])[:10],
            "file_size_mb": round(sum(p.stat().st_size for p in fwd.rglob("*.parquet")) / 1024 / 1024, 1),
        }
    except Exception:
        return None


def _get_routing():
    """延迟引入 data_platform，避免 API 启动时强依赖 engine 模块。"""
    from backend.services.engine.data_platform.aggregator import FieldRoutingTable
    return FieldRoutingTable()


def _get_registry():
    from backend.services.engine.data_platform.adapters import register_all
    from backend.services.engine.data_platform.registry import get_registry
    register_all()
    return get_registry()


def _get_monitor():
    from backend.services.engine.data_platform.monitor import get_monitor
    try:
        import redis  # type: ignore
        url = os.getenv("REDIS_URL", "redis://quantmind-redis:6379/0")
        client = redis.from_url(url, socket_timeout=2)
        return get_monitor(redis_client=client)
    except Exception as exc:  # noqa: BLE001
        logger.warning("redis init failed, falling back to in-memory monitor: %s", exc)
        return get_monitor()


# ---------------------------------------------------------------------------
# 数据新鲜度：以「预期最新交易日 vs 数据实际最新 trade_date」计算
# 尽量避免把“最后一次成功拉取”当作“数据新鲜”，否则回填也会被判为当天新鲜。
# ---------------------------------------------------------------------------
# 可映射到本地分区数据(dt=YYYYMMDD)的字段 → 相对数据集目录
_FIELD_LOCAL_PARTITION: dict[str, str] = {
    "daily_kline": "1_kline_data/daily_forward",
    "index_kline": "1_kline_data/index_daily",
    "valuation": "5_technical_derived/valuation",
    "technical_indicators": "5_technical_derived/technical_indicators",
    "market_sentiment": "5_technical_derived/market_sentiment",
    "margin_trading": "2_base_sector/margin_trading",
}


def _market_hub(market: str):
    """按市场解析本地量化数据枢纽（读其 data_dir）。"""
    from backend.services.engine.data_platform import quantdb_hub, quanthk_hub, quantus_hub
    cls = {
        "A": quantdb_hub.QuantDBDataHub,
        "HK": quanthk_hub.QuantHKDataHub,
        "US": quantus_hub.QuantUSDataHub,
    }.get(market)
    try:
        return cls.get_instance() if cls else None
    except Exception:
        return None


def _expected_trade_date():
    """预期最新交易日：今天；周六回退到周五，周日回退到上周五。"""
    from datetime import timedelta
    d = date.today()
    wd = d.weekday()  # 周一=0 ... 周日=6
    if wd == 5:
        d -= timedelta(days=1)
    elif wd == 6:
        d -= timedelta(days=2)
    return d


def _latest_partition_date_str(root, rel_dir: str):
    """扫描 dt=YYYYMMDD 分区目录，返回最新日期串 YYYYMMDD；无则 None。"""
    d = Path(root) / rel_dir
    if not d.is_dir():
        return None
    dates = [p.name[3:] for p in d.iterdir()
             if p.is_dir() and p.name.startswith("dt=")]
    return max(dates) if dates else None


def _db_engine():
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus as _q
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        host = os.getenv("DB_MASTER_HOST", "quantmind-db")
        port = os.getenv("DB_MASTER_PORT", "5432")
        user = os.getenv("DB_USER", "quantmind")
        pwd = _q(os.getenv("DB_PASSWORD", "quantmind"))
        name = os.getenv("DB_NAME", "quantmind")
        db_url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
    elif "asyncpg" in db_url:
        db_url = db_url.replace("asyncpg", "psycopg2")
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(db_url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
@router.get("/markets")
async def list_markets(current_user: dict = Depends(require_admin)):
    try:
        rt = _get_routing()
        return {
            "success": True,
            "data": {
                "markets": rt.list_markets(),
                "timestamp": _now_iso(),
            },
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.get("/sources")
async def list_sources(current_user: dict = Depends(require_admin)):
    try:
        rt = _get_routing()
        reg = _get_registry()
        monitor = _get_monitor()
        out: list[dict[str, Any]] = []
        for name in reg.list_sources():
            adapter = reg.get(name)
            # 统计该源覆盖的字段（去重）
            covered_fields: set[str] = set()
            for m in rt.list_markets():
                for f in rt.list_fields(m):
                    route = rt.get_route(m, f)
                    if name in route.ordered_sources and adapter.supports(f, m):
                        covered_fields.add(f)
            # 用 daily_kline 作为代表抓 health 摘要
            health = monitor.get_health(name, "daily_kline")
            out.append({
                "name": name,
                "class": adapter.__class__.__name__,
                "markets": adapter.markets,
                "field_count": len(adapter.fields),
                "covered_field_count": len(covered_fields),
                "health_summary": {
                    "last_success_at": health.get("last_success_at"),
                    "last_error_at": health.get("last_error_at"),
                    "last_error_msg": health.get("last_error_msg"),
                    "error_rate_1h": health.get("error_rate_1h"),
                    "avg_latency_ms": health.get("avg_latency_ms"),
                },
                "metadata": adapter.describe(),
            })
        from backend.services.engine.data_platform.source_catalog import (
            list_source_descriptors,
        )

        return {
            "success": True,
            "data": {
                "sources": out,
                "sync_sources": list_source_descriptors(set(reg.list_sources())),
                "timestamp": _now_iso(),
            },
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# 数据源控制面：能力目录、更新检查、同步任务、easy_tdx 节点池
# ---------------------------------------------------------------------------
class SourceUpdateCheckRequest(BaseModel):
    datasets: list[str] = Field(default_factory=list)


class DataSourceSyncRequest(BaseModel):
    source_id: str
    market: str = "A"
    datasets: list[str] = Field(default_factory=list)
    days: int = Field(5, ge=1, le=3650)
    symbols: list[str] = Field(default_factory=list)
    publish_mode: str = "shadow"
    with_pg: bool = False
    with_qlib: bool = False


class EasyTdxServerTestRequest(BaseModel):
    channel: str = "mac"
    host: str | None = None
    timeout: float = Field(2.0, ge=0.2, le=10.0)


class EasyTdxServerSwitchRequest(BaseModel):
    channel: str = "mac"
    host: str


@router.get("/sources/{source_id}/datasets")
async def source_datasets(
    source_id: str,
    current_user: dict = Depends(require_admin),
):
    try:
        if source_id == "easy_tdx":
            from backend.services.engine.data_platform.easy_tdx_sync import (
                list_datasets,
            )

            datasets = list_datasets()
            data_root = os.getenv("QM_EASY_TDX_DATA_DIR", "/data/easy_tdx")
        elif source_id == "quantdb":
            from backend.services.api.routers.admin.quantdb_console import DATASETS

            datasets = [
                {
                    "dataset": item.dataset,
                    "label": item.name,
                    "default": item.dataset
                    not in {"min1_kline", "min5_kline", "tick_data"},
                }
                for item in DATASETS
            ]
            data_root = os.getenv("QM_QUANTDB_DATA_DIR", "/data/quantdb")
        else:
            raise HTTPException(status_code=404, detail=f"未知数据源: {source_id}")
        return {
            "success": True,
            "data": {
                "source_id": source_id,
                "data_dir": data_root,
                "datasets": datasets,
                "timestamp": _now_iso(),
            },
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}") from exc


@router.post("/sources/{source_id}/check-updates")
async def check_source_updates(
    source_id: str,
    payload: SourceUpdateCheckRequest,
    current_user: dict = Depends(require_admin),
):
    try:
        if source_id == "easy_tdx":
            import asyncio

            from backend.services.engine.data_platform.easy_tdx_sync import (
                check_updates,
            )

            result = await asyncio.get_running_loop().run_in_executor(
                None, check_updates, payload.datasets or None
            )
            result["timestamp"] = _now_iso()
            return {"success": True, "data": result}
        if source_id == "quantdb":
            from backend.services.api.routers.admin.quantdb_console import (
                get_remote_diff,
            )

            response = await get_remote_diff(
                datasets=",".join(payload.datasets) if payload.datasets else None,
                current_user=current_user,
            )
            response.setdefault("data", {})["source_id"] = "quantdb"
            return response
        raise HTTPException(status_code=404, detail=f"未知数据源: {source_id}")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error("source update check failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/sync-jobs")
async def create_data_source_sync_job(
    payload: DataSourceSyncRequest,
    current_user: dict = Depends(require_admin),
):
    from backend.services.engine.data_platform.source_catalog import (
        get_source_descriptor,
    )

    try:
        descriptor = get_source_descriptor(payload.source_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if payload.market.upper() not in descriptor.markets:
        raise HTTPException(
            status_code=400,
            detail=f"{payload.source_id} 不支持市场 {payload.market}",
        )
    if payload.datasets:
        if payload.source_id == "easy_tdx":
            from backend.services.engine.data_platform.easy_tdx_sync import DATASETS

            known_datasets = set(DATASETS)
        else:
            from backend.services.api.routers.admin.quantdb_console import (
                DATASETS as QUANTDB_DATASETS,
            )

            known_datasets = {item.dataset for item in QUANTDB_DATASETS}
        unknown = [name for name in payload.datasets if name not in known_datasets]
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"{payload.source_id} 不支持数据集: {unknown[0]}",
            )
    if payload.source_id == "easy_tdx":
        if payload.publish_mode != "shadow":
            raise HTTPException(status_code=400, detail="easy_tdx 第一版仅支持影子落盘")
        if payload.with_pg or payload.with_qlib:
            raise HTTPException(
                status_code=400,
                detail="easy_tdx 尚未完成因子质量门禁，不能直接写 PG 或 Qlib",
            )

    from backend.services.engine.qlib_app.celery_config import celery_app
    from backend.shared.data_sync_jobs import create_job, upsert_job

    try:
        job = create_job(
            source_id=payload.source_id,
            market=payload.market.upper(),
            datasets=list(payload.datasets),
            days=payload.days,
            symbols=list(payload.symbols),
            publish_mode=payload.publish_mode,
            with_pg=payload.with_pg,
            with_qlib=payload.with_qlib,
            started_by=current_user.get("username")
            or current_user.get("user_id")
            or "admin",
        )
        queue = (
            os.getenv("QUANTDB_SYNC_QUEUE", "quantdb_sync")
            if payload.source_id == "quantdb"
            else os.getenv("QLIB_CELERY_QUEUE", "qlib_backtest_srv")
        )
        celery_app.send_task(
            "engine.tasks.run_data_source_sync",
            kwargs={"job_id": job["job_id"]},
            queue=queue,
        )
    except Exception as exc:  # noqa: BLE001
        if "job" in locals():
            upsert_job(
                job["job_id"],
                status="failed",
                stage="enqueue_failed",
                error=str(exc),
                finished_at=_now_iso(),
            )
        raise HTTPException(status_code=502, detail=f"同步任务派发失败: {exc}") from exc
    return {"success": True, "data": {"job": job}}


@router.get("/sync-jobs")
async def list_data_source_sync_jobs(
    limit: int = Query(50, ge=1, le=200),
    current_user: dict = Depends(require_admin),
):
    from backend.shared.data_sync_jobs import list_jobs

    return {
        "success": True,
        "data": {"jobs": list_jobs(limit), "timestamp": _now_iso()},
    }


@router.get("/sync-jobs/{job_id}")
async def get_data_source_sync_job(
    job_id: str,
    current_user: dict = Depends(require_admin),
):
    from backend.shared.data_sync_jobs import get_job

    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"任务不存在: {job_id}")
    return {"success": True, "data": {"job": job}}


@router.post("/sync-jobs/{job_id}/cancel")
async def cancel_data_source_sync_job(
    job_id: str,
    current_user: dict = Depends(require_admin),
):
    from backend.shared.data_sync_jobs import request_cancel

    if not request_cancel(job_id):
        raise HTTPException(status_code=409, detail="任务不存在或已经结束")
    return {
        "success": True,
        "data": {"job_id": job_id, "status": "cancelling"},
    }


@router.get("/sources/easy_tdx/servers")
async def list_easy_tdx_servers(current_user: dict = Depends(require_admin)):
    from backend.services.engine.data_platform.easy_tdx_client import (
        get_easy_tdx_manager,
    )

    manager = get_easy_tdx_manager()
    try:
        return {
            "success": True,
            "data": {
                "available": manager.available,
                "version": manager.library_version,
                "channels": {
                    channel: manager.list_servers(channel)
                    for channel in ("standard", "mac")
                }
                if manager.available
                else {},
                "timestamp": _now_iso(),
            },
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/sources/easy_tdx/servers/test")
async def test_easy_tdx_servers(
    payload: EasyTdxServerTestRequest,
    current_user: dict = Depends(require_admin),
):
    import asyncio

    from backend.services.engine.data_platform.easy_tdx_client import (
        get_easy_tdx_manager,
    )

    try:
        manager = get_easy_tdx_manager()
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: manager.test_servers(
                payload.channel, payload.host, payload.timeout
            ),
        )
        return {"success": True, "data": {"servers": result}}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/sources/easy_tdx/servers/switch")
async def switch_easy_tdx_server(
    payload: EasyTdxServerSwitchRequest,
    current_user: dict = Depends(require_admin),
):
    from backend.services.engine.data_platform.easy_tdx_client import (
        get_easy_tdx_manager,
    )

    try:
        result = get_easy_tdx_manager().switch_server(payload.channel, payload.host)
        return {"success": True, "data": result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/sources/{name}/health")
async def source_health(
    name: str,
    current_user: dict = Depends(require_admin),
):
    try:
        rt = _get_routing()
        monitor = _get_monitor()
        per_field: dict[str, Any] = {}
        for m in rt.list_markets():
            for f in rt.list_fields(m):
                route = rt.get_route(m, f)
                if name not in route.ordered_sources:
                    continue
                per_field[f"{m}/{f}"] = monitor.get_health(name, f)
        return {
            "success": True,
            "data": {
                "source": name,
                "fields": per_field,
                "timestamp": _now_iso(),
            },
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.get("/health-matrix")
async def health_matrix(
    market: str = Query("A", description="A / HK / US"),
    current_user: dict = Depends(require_admin),
):
    """字段 × 源 健康矩阵，前端用来渲染健康卡片/色块。"""
    try:
        rt = _get_routing()
        monitor = _get_monitor()
        reg = _get_registry()
        m = market.upper()
        fields = rt.list_fields(m)
        sources_seen: set[str] = set()
        cells: list[dict[str, Any]] = []
        field_tiers: dict[str, str] = {}
        for f in fields:
            route = rt.get_route(m, f)
            field_tiers[f] = route.tier
            for src in route.ordered_sources:
                sources_seen.add(src)
                health = monitor.get_health(src, f)
                registered = src in reg.list_sources()
                cells.append({
                    "field": f,
                    "source": src,
                    "is_primary": src == route.primary,
                    "registered": registered,
                    "last_success_at": health.get("last_success_at"),
                    "last_error_at": health.get("last_error_at"),
                    "error_rate_1h": float(health.get("error_rate_1h", 0) or 0),
                    "avg_latency_ms": float(health.get("avg_latency_ms", 0) or 0),
                    "fallback_triggered_count": int(health.get("fallback_triggered_count", 0) or 0),
                })
        return {
            "success": True,
            "data": {
                "market": m,
                "fields": fields,
                "field_tiers": field_tiers,
                "sources": sorted(sources_seen),
                "cells": cells,
                "timestamp": _now_iso(),
            },
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.get("/field-coverage")
async def field_coverage(current_user: dict = Depends(require_admin)):
    """所有市场 × 字段 × (primary, fallbacks, consensus, cleanup, tier)。"""
    try:
        rt = _get_routing()
        out: dict[str, list[dict[str, Any]]] = {}
        for m in rt.list_markets():
            rows = []
            for f in rt.list_fields(m):
                r = rt.get_route(m, f)
                rows.append({
                    "field": f,
                    "tier": r.tier,
                    "primary": r.primary,
                    "fallbacks": r.fallbacks,
                    "consensus": r.consensus,
                    "cleanup": r.cleanup,
                })
            out[m] = rows
        return {"success": True, "data": {"coverage": out, "timestamp": _now_iso()}}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# 告警
# ---------------------------------------------------------------------------
class AckRequest(BaseModel):
    note: Optional[str] = None


@router.get("/quality-alerts")
async def list_quality_alerts(
    severity: Optional[str] = None,
    market: Optional[str] = None,
    field: Optional[str] = None,
    acknowledged: Optional[bool] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(require_admin),
):
    from sqlalchemy import text as sql_text
    try:
        engine = _db_engine()
        clauses = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if severity:
            clauses.append("severity = :severity")
            params["severity"] = severity
        if market:
            clauses.append("market = :market")
            params["market"] = market.upper()
        if field:
            clauses.append("field = :field")
            params["field"] = field
        if acknowledged is not None:
            clauses.append("acknowledged = :ack")
            params["ack"] = bool(acknowledged)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with engine.begin() as conn:
            total = conn.execute(
                sql_text(f"SELECT COUNT(*) FROM data_quality_alerts {where}"),
                params,
            ).scalar() or 0
            rows = conn.execute(
                sql_text(
                    f"""
                    SELECT id, alert_type, severity, market, field, source, symbol,
                           trade_date, message, details, acknowledged, acknowledged_by,
                           acknowledged_at, created_at
                    FROM data_quality_alerts
                    {where}
                    ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                params,
            ).fetchall()
        items = [
            {
                "id": r[0], "alert_type": r[1], "severity": r[2],
                "market": r[3], "field": r[4], "source": r[5], "symbol": r[6],
                "trade_date": r[7].isoformat() if r[7] else None,
                "message": r[8], "details": r[9],
                "acknowledged": bool(r[10]),
                "acknowledged_by": r[11],
                "acknowledged_at": r[12].isoformat() if r[12] else None,
                "created_at": r[13].isoformat() if r[13] else None,
            }
            for r in rows
        ]
        return {
            "success": True,
            "data": {"total": total, "items": items, "timestamp": _now_iso()},
        }
    except Exception as exc:  # noqa: BLE001
        logger.error("list_quality_alerts failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.post("/quality-alerts/{alert_id}/ack")
async def ack_quality_alert(
    alert_id: int,
    payload: AckRequest = AckRequest(),  # body 可空
    current_user: dict = Depends(require_admin),
):
    from sqlalchemy import text as sql_text
    try:
        user_id = str(current_user.get("user_id") or current_user.get("id") or "admin")
        engine = _db_engine()
        with engine.begin() as conn:
            updated = conn.execute(
                sql_text(
                    """
                    UPDATE data_quality_alerts
                    SET acknowledged = TRUE,
                        acknowledged_by = :uid,
                        acknowledged_at = NOW()
                    WHERE id = :id
                    """
                ),
                {"uid": user_id, "id": alert_id},
            ).rowcount
        if not updated:
            raise HTTPException(status_code=404, detail=f"alert {alert_id} not found")
        return {"success": True, "data": {"alert_id": alert_id, "acknowledged_by": user_id}}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# 同步触发（占位，D8 cron 接入）
# ---------------------------------------------------------------------------
class SyncRequest(BaseModel):
    market: str = "A"
    field: str = "daily_kline"
    symbols: list[str] = []


@router.post("/sources/{name}/sync")
async def trigger_sync(
    name: str,
    payload: SyncRequest,
    current_user: dict = Depends(require_admin),
):
    """触发指定数据源对若干 symbol 的拉取（同步执行；MVP 阶段串行）。"""
    try:
        rt = _get_routing()
        reg = _get_registry()
        if name not in reg.list_sources():
            raise HTTPException(status_code=404, detail=f"source {name} not registered")
        try:
            route = rt.get_route(payload.market, payload.field)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        if name not in route.ordered_sources:
            raise HTTPException(
                status_code=400,
                detail=f"{name} not configured for {payload.market}/{payload.field}",
            )

        from backend.services.engine.data_platform.aggregator import FieldAggregator
        from backend.services.engine.data_platform.cleaner import DataCleaner
        agg = FieldAggregator(
            registry=reg, routing=rt, monitor=_get_monitor(), cleaner=DataCleaner(),
        )

        results: list[dict[str, Any]] = []
        for sym in payload.symbols[:50]:  # 限制 batch
            try:
                res = agg.fetch(
                    market=payload.market, field=payload.field, symbol=sym,
                )
                results.append({
                    "symbol": sym, "ok": True,
                    "source_used": res.source_used, "rows": len(res.data),
                    "cleaning": res.cleaning_report,
                })
            except Exception as exc:  # noqa: BLE001
                results.append({"symbol": sym, "ok": False, "error": str(exc)})
        return {"success": True, "data": {"source": name, "results": results}}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# 一键同步：对当前 market 的所有声明源依次触发同一组 symbols
# ---------------------------------------------------------------------------
class SweepRequest(BaseModel):
    market: str = "A"
    field: str = "daily_kline"
    symbols: list[str] = []
    include_fallbacks: bool = True


@router.post("/sweep")
async def sweep_market(
    payload: SweepRequest,
    current_user: dict = Depends(require_admin),
):
    """
    对 market×field 路由声明的所有源（primary + 可选 fallbacks）依次触发一次 fetch。

    用途：刚部署或长期未运行时，手动点亮"健康矩阵"——让监控里有真实的成功/错误样本。
    单次最多 20 个 symbol，串行执行；返回每个 source × symbol 的结果摘要。
    """
    try:
        rt = _get_routing()
        reg = _get_registry()
        try:
            route = rt.get_route(payload.market, payload.field)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        sources: list[str] = [route.primary]
        if payload.include_fallbacks:
            sources.extend([s for s in (route.fallbacks or []) if s not in sources])
        sources = [s for s in sources if s in reg.list_sources()]
        if not sources:
            raise HTTPException(
                status_code=400,
                detail=f"{payload.market}/{payload.field} 路由源均未注册",
            )

        from backend.services.engine.data_platform.aggregator import FieldAggregator
        from backend.services.engine.data_platform.cleaner import DataCleaner
        from datetime import date, timedelta

        monitor = _get_monitor()
        agg = FieldAggregator(
            registry=reg, routing=rt, monitor=monitor, cleaner=DataCleaner(),
        )

        symbols = [s.strip() for s in payload.symbols if s and s.strip()][:20]
        if not symbols:
            raise HTTPException(status_code=400, detail="symbols 不能为空")

        end = date.today()
        start = end - timedelta(days=14)

        per_source: list[dict[str, Any]] = []
        ok_total = 0
        fail_total = 0
        for src in sources:
            adapter = reg.get(src)
            sym_results: list[dict[str, Any]] = []
            for sym in symbols:
                t0 = _now_iso()
                try:
                    df = None
                    if hasattr(adapter, "fetch_field"):
                        try:
                            df = adapter.fetch_field(
                                payload.field, sym, start=start, end=end,
                            )
                        except (NotImplementedError, Exception) as field_exc:
                            # fetch_field 未实现该字段，回退到 fetch_daily
                            msg = str(field_exc).lower()
                            if ("not implemented" in msg or "未实现" in msg
                                    or isinstance(field_exc, NotImplementedError)):
                                df = None
                            else:
                                raise
                    if df is None and hasattr(adapter, "fetch_daily"):
                        df = adapter.fetch_daily(sym, start, end)
                    rows = 0 if df is None else len(df)
                    monitor.record_success(src, payload.field, latency_ms=0.0)
                    sym_results.append({"symbol": sym, "ok": True, "rows": rows, "started": t0})
                    ok_total += 1
                except Exception as exc:  # noqa: BLE001
                    monitor.record_error(src, payload.field, error=str(exc))
                    sym_results.append({"symbol": sym, "ok": False, "error": str(exc)[:200], "started": t0})
                    fail_total += 1
            per_source.append({"source": src, "results": sym_results})

        # 顺便走一次聚合：让"主源 + cleanup + consensus"链路也被记录
        agg_results: list[dict[str, Any]] = []
        for sym in symbols:
            try:
                res = agg.fetch(
                    market=payload.market, field=payload.field, symbol=sym,
                    start=start, end=end,
                )
                agg_results.append({
                    "symbol": sym, "ok": True,
                    "source_used": res.source_used,
                    "consensus_sources": res.consensus_sources,
                    "rows": len(res.data),
                })
            except Exception as exc:  # noqa: BLE001
                agg_results.append({"symbol": sym, "ok": False, "error": str(exc)[:200]})

        return {
            "success": True,
            "data": {
                "market": payload.market,
                "field": payload.field,
                "sources": sources,
                "symbols": symbols,
                "summary": {"ok": ok_total, "failed": fail_total},
                "per_source": per_source,
                "aggregated": agg_results,
                "timestamp": _now_iso(),
            },
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# 日常数据同步管理
# ---------------------------------------------------------------------------
class DailySyncRequest(BaseModel):
    market: str = "A"
    source_id: str = "quantdb"
    symbols: list[str] = Field(default_factory=list)
    datasets: list[str] = Field(default_factory=list)
    days: int = Field(5, ge=1, le=3650)
    publish_mode: str = "shadow"
    calibrate: bool = True


@router.post("/daily-sync")
async def trigger_daily_sync(
    payload: DailySyncRequest,
    current_user: dict = Depends(require_admin),
):
    """异步提交统一数据同步任务到 Celery，立即返回 task_id。

    A 股可选择 QuantDB 或 easy_tdx；easy_tdx 固定写入独立影子目录。
    """
    try:
        if payload.source_id != "quantdb":
            return await create_data_source_sync_job(
                DataSourceSyncRequest(
                    source_id=payload.source_id,
                    market=payload.market,
                    datasets=payload.datasets,
                    days=payload.days,
                    symbols=payload.symbols,
                    publish_mode=payload.publish_mode,
                ),
                current_user,
            )
        from backend.services.engine.tasks.celery_tasks import daily_data_sync_task

        symbols_str = ",".join(payload.symbols) if payload.symbols else ""
        task = daily_data_sync_task.delay(
            market=payload.market,
            symbols=symbols_str,
            incremental=True,
            calibrate=payload.calibrate,
        )
        return {
            "success": True,
            "data": {
                "task_id": task.id,
                "status": "submitted",
                "message": f"同步任务已提交 (task_id={task.id})，后台执行中",
            },
        }
    except Exception as exc:  # noqa: BLE001
        logger.error("daily_sync submit failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.get("/daily-sync/status/{task_id}")
async def get_daily_sync_task_status(
    task_id: str,
    current_user: dict = Depends(require_admin),
):
    """查询 Celery 异步同步任务的状态和结果。"""
    try:
        from celery.result import AsyncResult
        from backend.services.engine.tasks.celery_tasks import celery_app

        result = AsyncResult(task_id, app=celery_app)
        resp: dict[str, Any] = {
            "task_id": task_id,
            "status": result.status,  # PENDING / STARTED / SUCCESS / FAILURE
        }
        if result.ready():
            if result.successful():
                resp["result"] = result.get()
            else:
                resp["error"] = str(result.result)
                resp["traceback"] = result.traceback
        else:
            info = result.info or {}
            if isinstance(info, dict):
                resp["progress"] = info
        return {"success": True, "data": resp}
    except Exception as exc:  # noqa: BLE001
        logger.error("task status query failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.get("/sync-status")
async def get_sync_status(current_user: dict = Depends(require_admin)):
    """获取当前数据同步状态摘要。"""
    try:
        import asyncio
        from backend.scripts.daily_data_sync import get_sync_status

        loop = asyncio.get_event_loop()
        status = await loop.run_in_executor(None, get_sync_status)
        return {"success": True, "data": status}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.get("/sync-progress")
async def get_sync_progress(current_user: dict = Depends(require_admin)):
    """获取当前同步执行进度（步骤级）。"""
    try:
        from backend.scripts.daily_data_sync import get_sync_progress
        progress = get_sync_progress()
        return {"success": True, "data": progress}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.post("/update-investment-data")
async def update_investment_data_endpoint(
    version: str = "",
    current_user: dict = Depends(require_admin),
):
    """下载最新 investment_data qlib_bin 并解压更新。"""
    try:
        import asyncio
        import functools
        from backend.scripts.daily_data_sync import update_investment_data

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, functools.partial(update_investment_data, version=version)
        )
        return {"success": True, "data": result}
    except Exception as exc:  # noqa: BLE001
        logger.error("update_investment_data failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.post("/update-feature-parquet")
async def update_feature_parquet_endpoint(
    year: int = Query(0, description="指定年份 (默认: 当前年份)"),
    current_user: dict = Depends(require_admin),
):
    _ = year, current_user
    raise HTTPException(
        status_code=410,
        detail="特征快照生成已停用；A 股模型训练改为直读 QuantDB 因子源",
    )


# ---------------------------------------------------------------------------
# 数据新鲜度
# ---------------------------------------------------------------------------
@router.get("/freshness")
async def get_freshness(
    market: str = Query("A", description="A / HK / US"),
    current_user: dict = Depends(require_admin),
):
    """按市场返回每个源×字段的数据新鲜度。

    口径：能解析到本地分区数据的字段，用「预期最新交易日 − 数据最新 trade_date」计算；
    其余字段/源退回「最后一次成功拉取距今」（仅作最近调用健康参考）。
    """
    try:
        from datetime import datetime as _dt

        rt = _get_routing()
        monitor = _get_monitor()
        reg = _get_registry()
        m = market.upper()

        now = _dt.now(timezone.utc)
        expected_td = _expected_trade_date()
        hub = _market_hub(m)
        items: list[dict[str, Any]] = []

        for f in rt.list_fields(m):
            route = rt.get_route(m, f)
            all_sources = [route.primary] + [s for s in (route.fallbacks or []) if s != route.primary]
            # 可映射字段：同一 field 的所有源共享同一份本地数据日期
            data_date_str = None
            rel_dir = _FIELD_LOCAL_PARTITION.get(f)
            if rel_dir and hub is not None:
                data_date_str = _latest_partition_date_str(hub.data_dir, rel_dir)
            for src in all_sources:
                if src not in reg.list_sources():
                    continue
                health = monitor.get_health(src, f)
                last_ok = health.get("last_success_at")
                days_stale = None
                freshness = "unknown"
                # 1) 优先：数据最新交易日口径
                if data_date_str:
                    try:
                        dd = _dt.strptime(data_date_str, "%Y%m%d").date()
                        days_stale = (expected_td - dd).days
                        freshness = "fresh" if days_stale <= 0 else (
                            "stale" if days_stale <= 3 else "outdated")
                    except Exception:
                        pass
                # 2) 无法确定数据日期时，退回最近一次成功拉取
                if freshness in ("unknown",) and last_ok:
                    try:
                        last_dt = _dt.fromisoformat(last_ok.replace("Z", "+00:00"))
                        days_stale = (now - last_dt).days
                        freshness = "fresh" if days_stale == 0 else (
                            "stale" if days_stale <= 3 else "outdated")
                    except Exception:
                        pass
                items.append({
                    "field": f,
                    "source": src,
                    "is_primary": src == route.primary,
                    "last_success_at": last_ok,
                    "last_error_at": health.get("last_error_at"),
                    "days_stale": days_stale,
                    "freshness": freshness,
                    "avg_latency_ms": float(health.get("avg_latency_ms", 0) or 0),
                    "error_rate_1h": float(health.get("error_rate_1h", 0) or 0),
                })

        return {
            "success": True,
            "data": {
                "market": m,
                "items": items,
                "timestamp": _now_iso(),
            },
        }
    except Exception as exc:  # noqa: BLE001
        logger.error("get_freshness failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# 源在线状态
# ---------------------------------------------------------------------------
@router.get("/online-status")
async def get_online_status(current_user: dict = Depends(require_admin)):
    """快速检测所有适配器的在线/离线状态。"""
    try:
        import time as _time
        from backend.services.engine.data_platform.base import InvalidFieldRequest

        reg = _get_registry()
        items: list[dict[str, Any]] = []

        for name in reg.list_sources():
            adapter = reg.get(name)
            status = "unknown"
            latency_ms = None
            error_msg = None

            # 轻量检测：尝试 fetch_meta 或直接标记
            t0 = _time.monotonic()
            try:
                # 用第一个 market 做 fetch_meta 检测
                if adapter.markets:
                    test_market = adapter.markets[0]
                    adapter.fetch_meta(test_market)
                status = "online"
                latency_ms = round((_time.monotonic() - t0) * 1000, 1)
            except InvalidFieldRequest:
                # 适配器不提供 fetch_meta（如 easyquotation 仅实时），但声明了字段
                # 则视为功能在线（真实拉取质量由健康矩阵的 error_rate 再判断）。
                status = "online" if adapter.fields else "unavailable"
                latency_ms = round((_time.monotonic() - t0) * 1000, 1)
            except NotImplementedError:
                # 未实现 fetch_meta 同上：有字段即在线
                status = "online" if adapter.fields else "unavailable"
                latency_ms = round((_time.monotonic() - t0) * 1000, 1)
            except Exception as exc:
                # 真实连通性 / 依赖缺失异常 → 判离线或异常，不再误报在线
                latency_ms = round((_time.monotonic() - t0) * 1000, 1)
                msg = str(exc).lower()
                if any(k in msg for k in (
                    "not installed", "未安装", "未配置", "未找到", "not found",
                    "404", "timeout", "timed out", "time out", "refused", "连接",
                    "connection", "resolve", "dns", "unreachable",
                )):
                    status = "unavailable"
                else:
                    status = "error"
                error_msg = str(exc)[:200]

            items.append({
                "name": name,
                "class": adapter.__class__.__name__,
                "markets": adapter.markets,
                "fields": sorted(adapter.fields),
                "status": status,
                "latency_ms": latency_ms,
                "error": error_msg,
                "checked_at": _now_iso(),
            })

        return {
            "success": True,
            "data": {
                "items": items,
                "total": len(items),
                "online": sum(1 for i in items if i["status"] == "online"),
                "offline": sum(1 for i in items if i["status"] in ("error", "unavailable")),
                "timestamp": _now_iso(),
            },
        }
    except Exception as exc:  # noqa: BLE001
        logger.error("get_online_status failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# Alpha Agent 市场数据同步（RD-Agent 因子挖掘用）
# ---------------------------------------------------------------------------
@router.get("/alpha-agent-markets")
async def list_alpha_agent_markets(current_user: dict = Depends(require_admin)):
    """列出 Alpha Agent 支持的市场及数据就绪状态。"""
    try:
        from pathlib import Path
        from backend.services.engine.rd_agent.market_adapters import list_markets, get_adapter

        # 市场 → H5 数据文件路径（仅 crypto 5min 用）
        h5_map = _ALPHA_AGENT_H5_MAP

        markets = list_markets()
        for m in markets:
            mid = m["market_id"]
            try:
                adapter = get_adapter(mid)
                m["data_ready"] = adapter.is_data_ready()
            except Exception:
                m["data_ready"] = False

            # parquet 单源市场（A股/期货/港股/美股）：数据统一从本地 Quant parquet 读取，
            # 不依赖 PostgreSQL。
            if mid in ("a_share", "futures", "hong_kong", "us_stock"):
                m["h5_info"] = _market_local_stats(mid)
                m["data_source"] = "parquet" if m["h5_info"] else None
            else:
                # crypto 5min：仍用 H5 管线（无 5min parquet）
                h5_path = h5_map.get(mid)
                if h5_path and Path(h5_path).exists():
                    try:
                        import pandas as pd
                        df = pd.read_hdf(h5_path, key="data")
                        dates = df.index.get_level_values("datetime")
                        instruments = df.index.get_level_values("instrument")
                        m["h5_info"] = {
                            "rows": len(df),
                            "symbols": int(instruments.nunique()),
                            "start_date": str(dates.min().date()),
                            "end_date": str(dates.max().date()),
                            "file_size_mb": round(Path(h5_path).stat().st_size / 1024 / 1024, 1),
                        }
                        m["data_source"] = "h5"
                    except Exception:
                        m["h5_info"] = None
                else:
                    m["h5_info"] = None

            # 读取 Qlib 目录详情（路径统一由市场适配器解析，消除硬编码漂移）
            qlib_dir = None
            try:
                adapter = get_adapter(mid)
                qlib_dir = adapter.get_qlib_provider_uri()
            except Exception:
                qlib_dir = None
            if qlib_dir and Path(qlib_dir).is_dir():
                try:
                    p = Path(qlib_dir)
                    cal_files = list((p / "calendars").iterdir()) if (p / "calendars").is_dir() else []
                    feat_count = len(list((p / "features").iterdir())) if (p / "features").is_dir() else 0
                    m["qlib_info"] = {
                        "qlib_dir": str(qlib_dir),
                        "calendar_files": [f.name for f in cal_files],
                        "feature_dirs": feat_count,
                    }
                except Exception:
                    m["qlib_info"] = None
            else:
                m["qlib_info"] = None

        return {"success": True, "data": {"markets": markets, "timestamp": _now_iso()}}
    except Exception as exc:  # noqa: BLE001
        logger.error("list_alpha_agent_markets failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


# ---------------------------------------------------------------------------
# 港美股基本面数据同步 (PE/PB/ROE/EPS)
# ---------------------------------------------------------------------------
class FundamentalsSyncRequest(BaseModel):
    market: str = "ALL"  # HK / US / ALL
    dry_run: bool = False


@router.post("/sync-fundamentals")
async def sync_fundamentals(
    payload: FundamentalsSyncRequest,
    current_user: dict = Depends(require_admin),
):
    """同步港美股基本面数据 (PE/PB/ROE/EPS/股息率/市值) 从 yfinance/akshare。"""
    try:
        import asyncio
        from backend.scripts.sync_market_fundamentals import sync_market_fundamentals

        loop = asyncio.get_event_loop()
        market = payload.market.upper()

        if market == "ALL":
            results = {}
            for m in ["HK", "US"]:
                result = await loop.run_in_executor(
                    None, lambda _m=m: sync_market_fundamentals(_m, payload.dry_run)
                )
                results[m] = result
            return {"success": True, "data": {"results": results, "dry_run": payload.dry_run}}
        else:
            result = await loop.run_in_executor(
                None, lambda: sync_market_fundamentals(market, payload.dry_run)
            )
            return {"success": True, "data": {"result": result, "dry_run": payload.dry_run}}
    except Exception as exc:  # noqa: BLE001
        logger.error("sync_fundamentals failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")


@router.post("/sync-alpha-agent-market")
async def sync_alpha_agent_market(
    market: str = Query(..., description="市场 ID: a_share, crypto, hong_kong, us_stock"),
    force: bool = Query(False, description="强制重新下载"),
    current_user: dict = Depends(require_admin),
):
    """同步 Alpha Agent 市场数据（下载 + 转 Qlib 格式）。异步执行。"""
    try:
        from backend.services.engine.rd_agent.market_adapters import get_adapter

        adapter = get_adapter(market)

        # A 股数据通过 QuantDB 单源管理（数据管理页「直接增量同步（QuantDB）」）
        if market == "a_share":
            return {
                "success": True,
                "data": {
                    "market": market,
                    "status": "skipped",
                    "message": "A 股数据由 QuantDB 单源管理，请使用「直接增量同步（QuantDB）」功能",
                },
            }

        # 检查是否已就绪（非强制模式下跳过）
        if not force and adapter.is_data_ready():
            return {
                "success": True,
                "data": {
                    "market": market,
                    "market_name": adapter.market_name,
                    "status": "already_ready",
                    "message": f"{adapter.market_name}数据已就绪",
                },
            }

        # 异步执行数据准备
        import asyncio

        def _do_sync():
            return adapter.prepare_data()

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _do_sync)

        if result:
            # 数据同步成功后，自动触发特征计算
            feature_msg = ""
            try:
                import subprocess
                script_path = Path(__file__).resolve().parents[3] / "scripts" / "update_market_features.py"
                if not script_path.exists():
                    script_path = Path("/app/backend/scripts/update_market_features.py")
                if script_path.exists():
                    proc = subprocess.run(
                        ["python", str(script_path), "--market", market],
                        capture_output=True, text=True, timeout=600, check=False,
                    )
                    if proc.returncode == 0:
                        feature_msg = "，特征快照已更新"
                    else:
                        feature_msg = "，特征计算失败"
                        logger.warning("Feature computation failed for %s: %s", market, proc.stderr[-500:])
            except Exception as e:
                feature_msg = "，特征计算异常"
                logger.warning("Feature computation error for %s: %s", market, e)

            return {
                "success": True,
                "data": {
                    "market": market,
                    "market_name": adapter.market_name,
                    "status": "completed",
                    "message": f"{adapter.market_name}数据同步完成{feature_msg}",
                },
            }
        else:
            return {
                "success": False,
                "data": {
                    "market": market,
                    "market_name": adapter.market_name,
                    "status": "failed",
                    "message": f"{adapter.market_name}数据同步失败，请检查日志",
                },
            }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.error("sync_alpha_agent_market failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"failed: {exc}")
