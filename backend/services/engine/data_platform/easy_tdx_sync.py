"""easy_tdx 行情增量同步与更新检查。"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.services.engine.data_platform.adapters.easy_tdx_adapter import (
    EasyTdxAdapter,
)
from backend.shared.stock_utils import StockCodeUtil

DATASETS: dict[str, dict[str, Any]] = {
    "daily_unadjusted": {
        "label": "日线（不复权）",
        "adjust": "none",
        "default": True,
    },
    "daily_forward": {
        "label": "日线（前复权）",
        "adjust": "qfq",
        "default": True,
    },
    "daily_backward": {
        "label": "日线（后复权）",
        "adjust": "hfq",
        "default": False,
    },
    "stock_list": {
        "label": "沪深股票清单（北交所可由 QuantDB 补齐）",
        "adjust": None,
        "default": True,
    },
}


def data_dir() -> Path:
    return Path(os.getenv("QM_EASY_TDX_DATA_DIR", "/data/easy_tdx"))


def list_datasets() -> list[dict[str, Any]]:
    root = data_dir()
    items = []
    for dataset, meta in DATASETS.items():
        if dataset == "stock_list":
            path = root / "stock_list" / "instrument_list.parquet"
            synced = path.is_file()
            end_date = None
            partitions = 1 if synced else 0
        else:
            path = root / "1_kline_data" / dataset
            dates = _partition_dates(path)
            synced = bool(dates)
            end_date = _format_partition_date(dates[-1]) if dates else None
            partitions = len(dates)
        items.append(
            {
                "dataset": dataset,
                **meta,
                "synced": synced,
                "end_date": end_date,
                "partitions": partitions,
            }
        )
    return items


def check_updates(datasets: list[str] | None = None) -> dict[str, Any]:
    selected = _validate_datasets(datasets)
    adapter = EasyTdxAdapter()
    today = date.today()
    remote_latest: date | None = None
    benchmark_errors: list[str] = []
    for symbol in ("SH600000", "SZ000001"):
        try:
            frame = adapter.fetch_daily(
                symbol, today - timedelta(days=20), today, adjust="none"
            )
            latest = pd.to_datetime(frame["trade_date"]).dt.date.max()
            remote_latest = max(remote_latest, latest) if remote_latest else latest
        except Exception as exc:  # noqa: BLE001
            benchmark_errors.append(f"{symbol}: {exc}")
    if remote_latest is None:
        raise RuntimeError("easy_tdx 基准行情不可用: " + "; ".join(benchmark_errors))

    root = data_dir()
    items: list[dict[str, Any]] = []
    summary = {
        "total_datasets": len(selected),
        "up_to_date": 0,
        "updates_available": 0,
        "not_synced": 0,
        "unknown": 0,
    }
    for dataset in selected:
        if dataset == "stock_list":
            local_file = root / "stock_list" / "instrument_list.parquet"
            local_updated = (
                datetime.fromtimestamp(local_file.stat().st_mtime, timezone.utc).date()
                if local_file.exists()
                else None
            )
            if local_updated is None:
                status = "not_synced"
            elif local_updated < remote_latest:
                status = "updates_available"
            else:
                status = "up_to_date"
            local_end = local_updated.isoformat() if local_updated else None
            local_files = int(local_file.exists())
            local_rows, local_symbols = _parquet_coverage(local_file)
        else:
            partition_root = root / "1_kline_data" / dataset
            dates = _partition_dates(partition_root)
            local_end = _format_partition_date(dates[-1]) if dates else None
            local_files = len(dates)
            latest_file = (
                partition_root / f"dt={dates[-1]}" / "data.parquet" if dates else None
            )
            local_rows, local_symbols = _parquet_coverage(latest_file)
            if not dates:
                status = "not_synced"
            elif dates[-1] < remote_latest.strftime("%Y%m%d"):
                status = "updates_available"
            else:
                status = "up_to_date"
        summary[status] += 1
        items.append(
            {
                "dataset": dataset,
                "name": DATASETS[dataset]["label"],
                "status": status,
                "local": {
                    "synced": status != "not_synced",
                    "files": local_files,
                    "end_date": local_end,
                    "latest_rows": local_rows,
                    "latest_symbols": local_symbols,
                },
                "remote": {
                    "end_date": remote_latest.isoformat(),
                    "benchmark_symbols": ["SH600000", "SZ000001"],
                },
                "new_files": _estimated_missing_days(local_end, remote_latest, status),
            }
        )
    return {
        "source_id": "easy_tdx",
        "data_dir": str(root),
        "datasets": items,
        "summary": summary,
        "remote_latest_trade_date": remote_latest.isoformat(),
    }


def sync(
    *,
    datasets: list[str] | None = None,
    days: int = 5,
    symbols: list[str] | None = None,
    publish_mode: str = "shadow",
    progress_cb: Callable[..., None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """拉取 easy_tdx 行情并写入独立影子目录。"""
    if publish_mode != "shadow":
        raise ValueError("easy_tdx 第一版只允许 publish_mode=shadow")
    selected = _validate_datasets(datasets)
    adapter = EasyTdxAdapter()
    universe, meta = _resolve_universe(adapter, symbols)
    daily_datasets = [name for name in selected if name != "stock_list"]
    total = len(universe) * len(daily_datasets)
    if progress_cb:
        progress_cb("start", total=total)

    root = data_dir()
    root.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "source_id": "easy_tdx",
        "publish_mode": publish_mode,
        "data_dir": str(root),
        "symbols_total": len(universe),
        "datasets": {},
    }

    if "stock_list" in selected:
        if progress_cb:
            progress_cb("write", dataset="stock_list")
        stock_path = root / "stock_list" / "instrument_list.parquet"
        stock_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(meta, stock_path)
        result["datasets"]["stock_list"] = {"rows": len(meta), "files": 1}

    start_date = date.today() - timedelta(days=max(days * 2 + 10, 20))
    done = 0
    errors: list[dict[str, str]] = []
    for dataset in daily_datasets:
        frames: list[pd.DataFrame] = []
        adjust = str(DATASETS[dataset]["adjust"])
        for symbol in universe:
            if should_cancel and should_cancel():
                result["cancelled"] = True
                result["errors"] = errors
                return result
            try:
                frame = adapter.fetch_daily(
                    symbol,
                    start_date,
                    date.today(),
                    adjust=adjust,
                )
                frames.append(frame.tail(days))
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    {"dataset": dataset, "symbol": symbol, "error": str(exc)[:300]}
                )
            done += 1
            if progress_cb:
                progress_cb(
                    "symbol",
                    dataset=dataset,
                    symbol=symbol,
                    done=done,
                    total=total,
                )
        if progress_cb:
            progress_cb("write", dataset=dataset)
        merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        written = _write_daily_partitions(root, dataset, merged)
        result["datasets"][dataset] = {
            "rows": len(merged),
            "partitions": len(written),
            "failed_symbols": sum(1 for item in errors if item["dataset"] == dataset),
        }

    result["errors"] = errors[:100]
    result["error_count"] = len(errors)
    return result


def _resolve_universe(
    adapter: EasyTdxAdapter, symbols: list[str] | None
) -> tuple[list[str], pd.DataFrame]:
    if symbols:
        universe = sorted({StockCodeUtil.to_prefix(symbol) for symbol in symbols})
        invalid = [
            symbol
            for symbol in universe
            if len(symbol) != 8
            or symbol[:2] not in {"SH", "SZ", "BJ"}
            or not symbol[2:].isdigit()
        ]
        if invalid:
            raise ValueError(f"无效 A 股代码: {invalid[0]}")
        meta = pd.DataFrame(
            {
                "symbol": universe,
                "code": [symbol[2:] for symbol in universe],
                "exchange": [symbol[:2] for symbol in universe],
                "market": "A",
                "is_active": True,
                "source": "request",
            }
        )
        return universe, meta

    meta = adapter.fetch_meta("A")
    # easy_tdx 完整清单暂不含北交所；保留 QuantDB 本地清单作为 BJ 补充。
    try:
        from backend.services.engine.data_platform.quantdb_hub import QuantDBDataHub

        raw = QuantDBDataHub.get_instance().fetch_stock_list()
        if raw is not None and not raw.empty:
            symbol_column = "Symbol" if "Symbol" in raw.columns else "symbol"
            bj_mask = raw[symbol_column].astype(str).str.upper().str.contains("BJ")
            bj = raw[bj_mask].copy()
            if not bj.empty:
                supplement = pd.DataFrame(
                    {
                        "symbol": bj[symbol_column].map(StockCodeUtil.to_prefix),
                        "code": bj[symbol_column].map(StockCodeUtil.to_prefix).str[2:],
                        "exchange": "BSE",
                        "name": bj.get("Name", ""),
                        "market": "A",
                        "is_active": True,
                        "source": "quantdb_stock_master",
                    }
                )
                meta = pd.concat([meta, supplement], ignore_index=True)
    except Exception:
        pass
    meta["symbol"] = meta["symbol"].map(StockCodeUtil.to_prefix)
    meta = meta.drop_duplicates("symbol", keep="first").reset_index(drop=True)
    return sorted(meta["symbol"].tolist()), meta


def _write_daily_partitions(
    root: Path, dataset: str, frame: pd.DataFrame
) -> list[Path]:
    if frame.empty:
        return []
    frame = frame.copy()
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    written: list[Path] = []
    for trade_date, part in frame.groupby("trade_date"):
        partition = root / "1_kline_data" / dataset / f"dt={trade_date:%Y%m%d}"
        path = partition / "data.parquet"
        partition.mkdir(parents=True, exist_ok=True)
        payload = part
        if path.exists():
            existing = pd.read_parquet(path)
            payload = pd.concat([existing, part], ignore_index=True)
        payload = payload.drop_duplicates(["trade_date", "symbol"], keep="last")
        payload = payload.sort_values("symbol").reset_index(drop=True)
        _atomic_write(payload, path)
        written.append(path)
    return written


def _atomic_write(frame: pd.DataFrame, path: Path) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temp, index=False, compression="zstd")
    temp.replace(path)


def _validate_datasets(datasets: list[str] | None) -> list[str]:
    selected = datasets or [name for name, meta in DATASETS.items() if meta["default"]]
    unknown = [name for name in selected if name not in DATASETS]
    if unknown:
        raise ValueError(f"easy_tdx 不支持数据集: {unknown[0]}")
    return list(dict.fromkeys(selected))


def _partition_dates(path: Path) -> list[str]:
    if not path.is_dir():
        return []
    return sorted(
        entry.name[3:]
        for entry in path.iterdir()
        if entry.is_dir() and entry.name.startswith("dt=") and len(entry.name) == 11
    )


def _format_partition_date(value: str) -> str:
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def _parquet_coverage(path: Path | None) -> tuple[int, int]:
    if path is None or not path.is_file():
        return 0, 0
    try:
        frame = pd.read_parquet(path, columns=["symbol"])
        return len(frame), int(frame["symbol"].nunique())
    except Exception:
        return 0, 0


def _estimated_missing_days(
    local_end: str | None, remote_latest: date, status: str
) -> int:
    if status == "up_to_date":
        return 0
    if not local_end:
        return 1
    try:
        return max((remote_latest - date.fromisoformat(local_end)).days, 1)
    except ValueError:
        return 1
