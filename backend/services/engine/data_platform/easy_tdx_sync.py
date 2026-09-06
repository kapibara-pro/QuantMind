"""easy_tdx 行情增量同步与更新检查。"""

from __future__ import annotations

import json
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
    "min5_kline": {
        "label": "5 分钟线（不复权）",
        "freq": "5min",
        "default": False,
    },
    "min1_kline": {
        "label": "1 分钟线（不复权）",
        "freq": "1min",
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
        elif _is_minute_dataset(dataset):
            coverage = _minute_dataset_coverage(root, dataset)
            synced = coverage["files"] > 0
            end_date = coverage["end_at"]
            partitions = coverage["files"]
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
    remote_markers = _remote_update_markers(adapter, selected, today)
    remote_latest = max(marker["trade_date"] for marker in remote_markers.values())

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
        remote_marker = remote_markers[dataset]
        remote_trade_date = remote_marker["trade_date"]
        if dataset == "stock_list":
            local_file = root / "stock_list" / "instrument_list.parquet"
            local_updated = (
                datetime.fromtimestamp(local_file.stat().st_mtime, timezone.utc).date()
                if local_file.exists()
                else None
            )
            if local_updated is None:
                status = "not_synced"
            elif local_updated < remote_trade_date:
                status = "updates_available"
            else:
                status = "up_to_date"
            local_end = local_updated.isoformat() if local_updated else None
            local_files = int(local_file.exists())
            local_rows, local_symbols = _parquet_coverage(local_file)
        elif _is_minute_dataset(dataset):
            coverage = _minute_dataset_coverage(root, dataset)
            local_end = coverage["end_at"]
            local_files = coverage["files"]
            local_rows = coverage["rows"]
            local_symbols = coverage["symbols"]
            if not local_files:
                status = "not_synced"
            elif (
                coverage["scope"] == "full"
                and coverage["failed_symbols"] == 0
                and _cursor_is_current(local_end, remote_marker["cursor"])
            ):
                status = "up_to_date"
            else:
                status = "updates_available"
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
            elif dates[-1] < remote_trade_date.strftime("%Y%m%d"):
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
                    "end_date": remote_trade_date.isoformat(),
                    "end_at": remote_marker["cursor"],
                    "benchmark_symbols": ["SH600000", "SZ000001"],
                },
                "new_files": _estimated_missing_days(
                    local_end, remote_trade_date, status
                ),
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
    daily_datasets = [
        name
        for name in selected
        if name != "stock_list" and not _is_minute_dataset(name)
    ]
    minute_datasets = [name for name in selected if _is_minute_dataset(name)]
    total = len(universe) * (len(daily_datasets) + len(minute_datasets))
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

    requested_start = date.today() - timedelta(days=max(days * 2 + 10, 20))
    for dataset in minute_datasets:
        freq = str(DATASETS[dataset]["freq"])
        rows_received = 0
        files_written = 0
        symbols_succeeded = 0
        latest_values: list[pd.Timestamp] = []
        errors_before = len(errors)
        for symbol in universe:
            if should_cancel and should_cancel():
                result["cancelled"] = True
                result["errors"] = errors[:100]
                result["error_count"] = len(errors)
                return result

            path = _minute_symbol_path(root, dataset, symbol)
            local_latest = _latest_minute_timestamp(path)
            start_date = (
                local_latest.date() - timedelta(days=1)
                if local_latest is not None
                else requested_start
            )
            try:
                frame = adapter.fetch_minute(
                    symbol,
                    start_date,
                    date.today(),
                    freq=freq,
                )
                if local_latest is None:
                    frame = _tail_trade_days(frame, days)
                _write_minute_symbol(path, symbol, frame)
                rows_received += len(frame)
                files_written += 1
                symbols_succeeded += 1
                latest_values.append(pd.to_datetime(frame["datetime"]).max())
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
        dataset_error_count = len(errors) - errors_before
        previous_state = _read_minute_state(root, dataset)
        latest_at = _latest_iso_timestamp(
            latest_values, previous_state.get("latest_at")
        )
        state = {
            "dataset": dataset,
            "frequency": freq,
            "scope": "partial" if symbols else "full",
            "checked_through": latest_at,
            "latest_at": latest_at,
            "symbols_total": len(universe),
            "symbols_succeeded": symbols_succeeded,
            "failed_symbols": dataset_error_count,
            "rows_received": rows_received,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_minute_state(root, dataset, state)
        result["datasets"][dataset] = {
            "rows": rows_received,
            "files": files_written,
            "symbols": symbols_succeeded,
            "failed_symbols": dataset_error_count,
            "end_at": latest_at,
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


def _remote_update_markers(
    adapter: EasyTdxAdapter, selected: list[str], today: date
) -> dict[str, dict[str, Any]]:
    markers: dict[str, dict[str, Any]] = {}
    benchmark_symbols = ("SH600000", "SZ000001")
    daily_datasets = [name for name in selected if not _is_minute_dataset(name)]
    if daily_datasets:
        latest_date: date | None = None
        errors: list[str] = []
        for symbol in benchmark_symbols:
            try:
                frame = adapter.fetch_daily(
                    symbol, today - timedelta(days=20), today, adjust="none"
                )
                current = pd.to_datetime(frame["trade_date"]).dt.date.max()
                latest_date = max(latest_date, current) if latest_date else current
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{symbol}: {exc}")
        if latest_date is None:
            raise RuntimeError("easy_tdx 日线基准行情不可用: " + "; ".join(errors))
        for dataset in daily_datasets:
            markers[dataset] = {
                "trade_date": latest_date,
                "cursor": latest_date.isoformat(),
            }

    frequencies = {
        str(DATASETS[name]["freq"])
        for name in selected
        if _is_minute_dataset(name)
    }
    for freq in frequencies:
        latest_at: pd.Timestamp | None = None
        errors = []
        for symbol in benchmark_symbols:
            try:
                frame = adapter.fetch_minute(
                    symbol,
                    today - timedelta(days=10),
                    today,
                    freq=freq,
                )
                current = pd.to_datetime(frame["datetime"]).max()
                latest_at = max(latest_at, current) if latest_at is not None else current
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{symbol}: {exc}")
        if latest_at is None:
            raise RuntimeError(
                f"easy_tdx {freq} 基准行情不可用: " + "; ".join(errors)
            )
        for dataset in selected:
            if DATASETS[dataset].get("freq") == freq:
                markers[dataset] = {
                    "trade_date": latest_at.date(),
                    "cursor": latest_at.isoformat(),
                }
    return markers


def _is_minute_dataset(dataset: str) -> bool:
    return bool(DATASETS.get(dataset, {}).get("freq"))


def _minute_dataset_dir(root: Path, dataset: str) -> Path:
    return root / "1_kline_data" / dataset


def _minute_symbol_path(root: Path, dataset: str, symbol: str) -> Path:
    prefix = StockCodeUtil.to_prefix(symbol)
    return _minute_dataset_dir(root, dataset) / f"{prefix}.parquet"


def _minute_state_path(root: Path, dataset: str) -> Path:
    return _minute_dataset_dir(root, dataset) / "_sync_state.json"


def _read_minute_state(root: Path, dataset: str) -> dict[str, Any]:
    path = _minute_state_path(root, dataset)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _write_minute_state(
    root: Path, dataset: str, state: dict[str, Any]
) -> None:
    path = _minute_state_path(root, dataset)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temp.replace(path)


def _minute_dataset_coverage(root: Path, dataset: str) -> dict[str, Any]:
    directory = _minute_dataset_dir(root, dataset)
    files = list(directory.glob("*.parquet")) if directory.is_dir() else []
    state = _read_minute_state(root, dataset)
    end_at = state.get("checked_through") or state.get("latest_at")
    if not end_at and files:
        latest = _latest_minute_timestamp(max(files, key=lambda path: path.stat().st_mtime))
        end_at = latest.isoformat() if latest is not None else None
    return {
        "files": len(files),
        "symbols": len(files),
        "rows": int(state.get("rows_received") or 0),
        "end_at": end_at,
        "scope": state.get("scope"),
        "failed_symbols": int(state.get("failed_symbols") or 0),
    }


def _cursor_is_current(local: str | None, remote: str) -> bool:
    if not local:
        return False
    try:
        return pd.Timestamp(local) >= pd.Timestamp(remote)
    except (TypeError, ValueError):
        return False


def _latest_minute_timestamp(path: Path) -> pd.Timestamp | None:
    if not path.is_file():
        return None
    for column in ("datetime", "time", "trade_date"):
        try:
            values = pd.read_parquet(path, columns=[column])
            latest = pd.to_datetime(values[column], errors="coerce").max()
            if pd.notna(latest):
                return pd.Timestamp(latest)
        except Exception:  # noqa: BLE001
            continue
    return None


def _tail_trade_days(frame: pd.DataFrame, days: int) -> pd.DataFrame:
    if frame.empty:
        return frame
    trade_dates = pd.to_datetime(frame["trade_date"]).dt.date
    selected_dates = sorted(set(trade_dates))[-days:]
    return frame.loc[trade_dates.isin(selected_dates)].reset_index(drop=True)


def _write_minute_symbol(path: Path, symbol: str, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    incoming = frame.copy()
    if "datetime" not in incoming.columns:
        raise ValueError("easy_tdx 分钟线缺少 datetime 列")
    incoming["datetime"] = pd.to_datetime(incoming["datetime"])
    incoming["trade_date"] = incoming["datetime"].dt.date
    incoming["symbol"] = StockCodeUtil.to_prefix(symbol)

    payload = incoming
    if path.is_file():
        existing = pd.read_parquet(path)
        if "datetime" not in existing.columns and "time" in existing.columns:
            existing = existing.rename(columns={"time": "datetime"})
        if "datetime" in existing.columns:
            existing["datetime"] = pd.to_datetime(existing["datetime"])
            existing["trade_date"] = existing["datetime"].dt.date
            existing["symbol"] = StockCodeUtil.to_prefix(symbol)
            payload = pd.concat([existing, incoming], ignore_index=True)

    payload = payload.drop_duplicates(["symbol", "datetime"], keep="last")
    payload = payload.sort_values("datetime").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(payload, path)


def _latest_iso_timestamp(
    values: list[pd.Timestamp], previous: Any = None
) -> str | None:
    candidates = [pd.Timestamp(value) for value in values if pd.notna(value)]
    if previous:
        try:
            candidates.append(pd.Timestamp(previous))
        except (TypeError, ValueError):
            pass
    return max(candidates).isoformat() if candidates else None


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
        return max((remote_latest - date.fromisoformat(local_end[:10])).days, 1)
    except ValueError:
        return 1
