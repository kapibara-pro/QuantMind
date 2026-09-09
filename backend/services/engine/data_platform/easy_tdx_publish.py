"""Publish validated easy_tdx bars into the canonical A-share data path."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.shared.stock_utils import StockCodeUtil

PUBLISH_DATASETS = (
    "daily_unadjusted",
    "daily_forward",
    "daily_backward",
)
PUBLISH_INDEX_DATASETS = ("index_daily",)
PUBLISH_MINUTE_DATASETS = ("min1_kline", "min5_kline")
_KLINE_COLUMNS = (
    "symbol",
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
)
_KEY_COLUMNS = ("symbol", "time")
_MINUTE_COLUMNS = _KLINE_COLUMNS
# QuantDB 的既有指数消费者会读取这两个可选字段。easy_tdx 不一定提供
# 它们，但正式目录必须保持稳定 schema，避免 DuckDB 视图在全新目录上绑定失败。
_INDEX_COLUMNS = _KLINE_COLUMNS + ("preClose", "Category")


def source_data_dir() -> Path:
    return Path(os.getenv("QM_EASY_TDX_DATA_DIR", "/data/easy_tdx"))


def target_data_dir() -> Path:
    return Path(os.getenv("QM_QUANTDB_DATA_DIR", "/data/quantdb"))


def publication_status() -> dict[str, Any]:
    source_latest = _latest_common_partition(source_data_dir(), PUBLISH_DATASETS)
    target_latest = _latest_common_partition(target_data_dir(), PUBLISH_DATASETS)
    manifest = _read_latest_manifest(source_data_dir())
    datasets: dict[str, dict[str, Any]] = {}
    for dataset in (*PUBLISH_DATASETS, *PUBLISH_INDEX_DATASETS):
        datasets[dataset] = {
            "source_latest_date": _format_date(
                _latest_partition(source_data_dir(), dataset)
            ),
            "published_latest_date": _format_date(
                _latest_partition(target_data_dir(), dataset)
            ),
        }
    for dataset in PUBLISH_MINUTE_DATASETS:
        source = _minute_coverage(source_data_dir(), dataset)
        target = _minute_coverage(target_data_dir(), dataset)
        datasets[dataset] = {
            "source_latest_at": source["latest_at"],
            "published_latest_at": target["latest_at"],
            "source_files": source["files"],
            "published_files": target["files"],
            "source_failed_symbols": source["failed_symbols"],
        }
    index_source_latest = _latest_partition(source_data_dir(), "index_daily")
    index_target_latest = _latest_partition(target_data_dir(), "index_daily")
    index_pending = bool(
        index_source_latest
        and (not index_target_latest or index_source_latest > index_target_latest)
    )
    minute_pending = any(
        (details.get("source_files") or 0) > (details.get("published_files") or 0)
        or (
            details.get("source_latest_at")
            and details.get("source_latest_at") != details.get("published_latest_at")
        )
        for details in datasets.values()
        if "source_files" in details
    )
    return {
        "source_latest_date": _format_date(source_latest),
        "published_latest_date": _format_date(target_latest),
        "source_index_latest_date": _format_date(
            _latest_partition(source_data_dir(), "index_daily")
        ),
        "published_index_latest_date": _format_date(
            _latest_partition(target_data_dir(), "index_daily")
        ),
        "datasets": datasets,
        "last_release": manifest,
        "publish_available": bool(
            (source_latest and source_latest > (target_latest or ""))
            or index_pending
            or minute_pending
        ),
    }


def publish(
    *,
    with_pg: bool = True,
    with_qlib: bool = True,
    progress_cb: Callable[..., None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Validate, merge, and publish the complete easy_tdx release bundle.

    The shadow directory remains the source of the release. QuantDB-only datasets
    (financials, valuation, and L1/L2 factors) are never modified.
    """
    source_root = source_data_dir()
    target_root = target_data_dir()
    with _publication_lock(target_root):
        return _publish_locked(
            source_root=source_root,
            target_root=target_root,
            with_pg=with_pg,
            with_qlib=with_qlib,
            progress_cb=progress_cb,
            should_cancel=should_cancel,
        )


def _publish_locked(
    *,
    source_root: Path,
    target_root: Path,
    with_pg: bool,
    with_qlib: bool,
    progress_cb: Callable[..., None] | None,
    should_cancel: Callable[[], bool] | None,
) -> dict[str, Any]:
    release_id = _release_id()
    started_at = _now_iso()

    if progress_cb:
        progress_cb("publish_validating", release_id=release_id)
    target_latest = _latest_common_partition(target_root, PUBLISH_DATASETS)
    quality = validate_release(source_root, after_date=target_latest)
    dates = quality["dates"]
    if should_cancel and should_cancel():
        return {"cancelled": True, "release_id": release_id, "quality": quality}

    publish_root = target_root / ".easy_tdx_publish" / release_id
    staging_root = publish_root / "staging"
    backup_root = publish_root / "backup"
    manifest_path = source_root / "releases" / release_id / "manifest.json"
    index_dates = _new_partition_dates(source_root, target_root, "index_daily")
    minute_files = _minute_source_files(source_root)
    _validate_minute_release(source_root)
    total = (
        len(dates) * len(PUBLISH_DATASETS)
        + len(index_dates)
        + len(minute_files)
        + int(with_qlib)
        + int(with_pg)
    )
    done = 0
    prepared_done = 0
    if progress_cb:
        progress_cb("publish_start", total=total, release_id=release_id)

    prepared: list[dict[str, Any]] = []
    for dataset in PUBLISH_DATASETS:
        for partition_date in dates:
            if should_cancel and should_cancel():
                _remove_tree(publish_root)
                return {
                    "cancelled": True,
                    "release_id": release_id,
                    "quality": quality,
                }
            source_file = _partition_file(source_root, dataset, partition_date)
            target_file = _partition_file(target_root, dataset, partition_date)
            staged_file = _partition_file(staging_root, dataset, partition_date)
            source_frame = _normalize_source_partition(source_file, partition_date)
            target_frame = (
                _normalize_target_partition(target_file, partition_date)
                if target_file.is_file()
                else pd.DataFrame(columns=_KLINE_COLUMNS)
            )
            merged = _merge_partition(target_frame, source_frame)
            _write_parquet(merged, staged_file)
            prepared.append(
                {
                    "dataset": dataset,
                    "date": partition_date,
                    "source_rows": len(source_frame),
                    "target_rows_before": len(target_frame),
                    "target_rows_after": len(merged),
                    "target_file": target_file,
                    "staged_file": staged_file,
                    "sha256": _sha256(staged_file),
                }
            )
            prepared_done += 1
            if progress_cb:
                progress_cb(
                    "publish_prepare",
                    done=prepared_done,
                    total=total,
                    dataset=dataset,
                    date=partition_date,
                )

    for dataset in PUBLISH_INDEX_DATASETS:
        for partition_date in index_dates:
            if should_cancel and should_cancel():
                _remove_tree(publish_root)
                return {"cancelled": True, "release_id": release_id, "quality": quality}
            source_file = _partition_file(source_root, dataset, partition_date)
            target_file = _partition_file(target_root, dataset, partition_date)
            staged_file = _partition_file(staging_root, dataset, partition_date)
            source_frame = _normalize_index_partition(source_file, partition_date)
            target_frame = (
                _normalize_index_partition(target_file, partition_date)
                if target_file.is_file()
                else pd.DataFrame(columns=_INDEX_COLUMNS)
            )
            merged = _merge_index_partition(target_frame, source_frame)
            _write_parquet(merged, staged_file)
            prepared.append(
                {
                    "dataset": dataset,
                    "date": partition_date,
                    "source_rows": len(source_frame),
                    "target_rows_before": len(target_frame),
                    "target_rows_after": len(merged),
                    "target_file": target_file,
                    "staged_file": staged_file,
                    "sha256": _sha256(staged_file),
                }
            )
            prepared_done += 1
            if progress_cb:
                progress_cb(
                    "publish_prepare",
                    done=prepared_done,
                    total=total,
                    dataset=dataset,
                    date=partition_date,
                )

    for dataset, source_file in minute_files:
        if should_cancel and should_cancel():
            _remove_tree(publish_root)
            return {"cancelled": True, "release_id": release_id, "quality": quality}
        relative = source_file.relative_to(source_root / "1_kline_data" / dataset)
        target_file = target_root / "1_kline_data" / dataset / f"{_suffix_filename(relative.name)}"
        staged_file = staging_root / "1_kline_data" / dataset / f"{_suffix_filename(relative.name)}"
        source_frame = _normalize_minute_file(source_file)
        target_frame = (
            _normalize_minute_file(target_file, symbol=target_file.stem)
            if target_file.is_file()
            else pd.DataFrame(columns=_MINUTE_COLUMNS)
        )
        merged = _merge_minute(target_frame, source_frame)
        _write_parquet(merged, staged_file)
        prepared.append(
            {
                "dataset": dataset,
                "date": (
                    _format_date(
                        str(merged["time"].dt.date.max()).replace("-", "")
                    )
                    if not merged.empty
                    else None
                ),
                "source_rows": len(source_frame),
                "target_rows_before": len(target_frame),
                "target_rows_after": len(merged),
                "target_file": target_file,
                "staged_file": staged_file,
                "backup_file_path": (
                    publish_root
                    / "backup"
                    / "1_kline_data"
                    / dataset
                    / target_file.name
                ),
                "sha256": _sha256(staged_file),
            }
        )
        prepared_done += 1
        if progress_cb:
            progress_cb(
                "publish_prepare",
                done=prepared_done,
                total=total,
                dataset=dataset,
                current=f"整理 {source_file.name}",
            )

    applied: list[dict[str, Any]] = []
    try:
        for item in prepared:
            if should_cancel and should_cancel():
                _rollback(applied)
                _remove_tree(publish_root)
                return {
                    "cancelled": True,
                    "release_id": release_id,
                    "quality": quality,
                }
            target_file = item["target_file"]
            staged_file = item["staged_file"]
            backup_file = item.get("backup_file_path") or _partition_file(
                backup_root, item["dataset"], item["date"]
            )
            target_file.parent.mkdir(parents=True, exist_ok=True)
            existed = target_file.is_file()
            if existed:
                backup_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target_file, backup_file)
            os.replace(staged_file, target_file)
            item["backup_file"] = backup_file if existed else None
            applied.append(item)
            done += 1
            if progress_cb:
                progress_cb(
                    "publish_partition",
                    done=done,
                    total=total,
                    dataset=item["dataset"],
                    date=item["date"],
                )
    except Exception:
        _rollback(applied)
        _remove_tree(publish_root)
        raise
    finally:
        _remove_tree(staging_root)

    if should_cancel and should_cancel():
        _rollback(applied)
        _remove_tree(publish_root)
        return {"cancelled": True, "release_id": release_id, "quality": quality}

    qlib_result: dict[str, Any] | None = None
    if with_qlib:
        if progress_cb:
            progress_cb("publish_qlib", done=done, total=total)
        try:
            qlib_result = _build_qlib(
                target_root,
                dates[-1],
                progress_cb=(
                    lambda percent, **details: progress_cb(
                        "publish_qlib",
                        done=done,
                        total=total,
                        progress=percent,
                        **details,
                    )
                    if progress_cb
                    else None
                ),
            )
        except Exception:
            # The live Qlib directory is switched only after a complete build.
            # Restore canonical parquet too so readers stay on one release.
            _rollback(applied)
            _remove_tree(publish_root)
            raise
        done += 1

    # Parquet + Qlib are the training release. PG is a retryable serving
    # projection, so a PG failure is surfaced as partial without undoing a
    # successfully switched training release.
    committed = True
    pg_result: dict[str, Any] | None = None
    warnings: list[str] = []
    if with_pg:
        if progress_cb:
            progress_cb("publish_pg", done=done, total=total)
        from backend.scripts.quantdb_daily_sync import fill_pg_from_parquet

        try:
            pg_result = fill_pg_from_parquet(
                start_date=datetime.strptime(dates[0], "%Y%m%d").date(),
                end_date=datetime.strptime(dates[-1], "%Y%m%d").date(),
            )
        except Exception as exc:  # noqa: BLE001 - release remains retryable
            pg_result = {"status": "error", "reason": str(exc)}
        if pg_result.get("status") != "ok":
            warnings.append(
                "PostgreSQL 行情投影未完整更新，可在修复数据库后重新发布"
            )
        done += 1

    _write_minute_publish_state(target_root, source_root)

    result = {
        "status": "partial" if warnings else "ok",
        "committed": committed,
        "source_id": "easy_tdx",
        "publish_mode": "official",
        "release_id": release_id,
        "source_data_dir": str(source_root),
        "target_data_dir": str(target_root),
        "started_at": started_at,
        "finished_at": _now_iso(),
        "published_date_range": {
            "start": _format_date(dates[0]),
            "end": _format_date(dates[-1]),
        },
        "published_index_date_range": {
            "start": _format_date(index_dates[0]) if index_dates else None,
            "end": _format_date(index_dates[-1]) if index_dates else None,
        },
        "published_minute_files": len(minute_files),
        "quality": quality,
        "partitions": [
            {
                key: value
                for key, value in item.items()
                if key not in {
                    "target_file",
                    "staged_file",
                    "backup_file",
                    "backup_file_path",
                }
            }
            for item in prepared
        ],
        "qlib": qlib_result,
        "pg": pg_result,
        "warnings": warnings,
    }
    _write_json(result, manifest_path)
    _write_json(
        {"release_id": release_id, "manifest": str(manifest_path)},
        source_root / "releases" / "latest.json",
    )
    if progress_cb:
        progress_cb("publish_complete", done=done, total=total)
    _prune_publish_runs(target_root)
    return result


def validate_release(
    root: Path | None = None, *, after_date: str | None = None
) -> dict[str, Any]:
    root = root or source_data_dir()
    all_date_sets = {
        dataset: set(_partition_dates(root, dataset)) for dataset in PUBLISH_DATASETS
    }
    missing = [dataset for dataset, dates in all_date_sets.items() if not dates]
    if missing:
        raise ValueError(f"缺少可发布的 easy_tdx 日线数据集: {', '.join(missing)}")

    date_sets = all_date_sets
    if after_date:
        newer_date_sets = {
            dataset: {date for date in dates if date > after_date}
            for dataset, dates in all_date_sets.items()
        }
        if any(newer_date_sets.values()):
            date_sets = newer_date_sets
        elif all(after_date in dates for dates in all_date_sets.values()):
            # Keep same-day retries available when Qlib or PostgreSQL projection
            # failed after the canonical parquet partition was already switched.
            date_sets = {
                dataset: {after_date} for dataset in PUBLISH_DATASETS
            }
        else:
            raise ValueError(
                f"没有晚于正式行情 {_format_date(after_date)} 的 easy_tdx "
                "日线数据，且三套日线不满足同日重试条件"
            )

    common = sorted(set.intersection(*date_sets.values()))
    if not common:
        raise ValueError("三套 easy_tdx 日线没有共同交易日，禁止发布")
    latest_by_dataset = {dataset: max(dates) for dataset, dates in date_sets.items()}
    if len(set(latest_by_dataset.values())) != 1:
        detail = ", ".join(
            f"{dataset}={_format_date(value)}"
            for dataset, value in latest_by_dataset.items()
        )
        raise ValueError(f"三套 easy_tdx 日线最新日期不一致: {detail}")
    if len({frozenset(dates) for dates in date_sets.values()}) != 1:
        raise ValueError("三套 easy_tdx 日线分区日期集合不一致，禁止跳过缺失分区发布")

    minimum_symbols = int(os.getenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "3000"))
    minimum_coverage = float(
        os.getenv("QM_EASY_TDX_PUBLISH_MIN_COVERAGE", "0.98")
    )
    reports: list[dict[str, Any]] = []
    for partition_date in common:
        counts: dict[str, int] = {}
        frames: dict[str, pd.DataFrame] = {}
        for dataset in PUBLISH_DATASETS:
            frame = _normalize_source_partition(
                _partition_file(root, dataset, partition_date), partition_date
            )
            _validate_ohlcv(frame, dataset, partition_date)
            counts[dataset] = int(frame["symbol"].nunique())
            frames[dataset] = frame
        expected = max(counts.values())
        if expected < minimum_symbols:
            raise ValueError(
                f"{_format_date(partition_date)} 标的数 {expected} 小于发布门槛 "
                f"{minimum_symbols}，可能是抽样同步"
            )
        low_coverage = {
            dataset: count / expected
            for dataset, count in counts.items()
            if count / expected < minimum_coverage
        }
        if low_coverage:
            detail = ", ".join(
                f"{dataset}={ratio:.2%}" for dataset, ratio in low_coverage.items()
            )
            raise ValueError(
                f"{_format_date(partition_date)} 三套日线标的覆盖不一致: {detail}"
            )
        common_symbols = set.intersection(
            *(set(frame["symbol"]) for frame in frames.values())
        )
        common_coverage = len(common_symbols) / expected
        if common_coverage < minimum_coverage:
            raise ValueError(
                f"{_format_date(partition_date)} 三套日线共同标的覆盖率 "
                f"{common_coverage:.2%} 低于发布门槛 {minimum_coverage:.2%}"
            )
        reports.append(
            {
                "date": _format_date(partition_date),
                "symbol_counts": counts,
                "common_symbols": len(common_symbols),
            }
        )
    return {
        "status": "passed",
        "dates": common,
        "latest_date": _format_date(common[-1]),
        "after_date": _format_date(after_date),
        "ignored_existing_partitions": sum(
            len(dates) for dates in all_date_sets.values()
        )
        - sum(len(dates) for dates in date_sets.values()),
        "minimum_symbols": minimum_symbols,
        "minimum_coverage": minimum_coverage,
        "partitions": reports,
    }


def _normalize_source_partition(path: Path, partition_date: str) -> pd.DataFrame:
    if not path.is_file():
        raise ValueError(f"easy_tdx 分区不存在: {path}")
    frame = pd.read_parquet(path)
    if "datetime" in frame.columns and "time" not in frame.columns:
        frame = frame.rename(columns={"datetime": "time"})
    missing = [column for column in _KLINE_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"easy_tdx 分区字段缺失 {path}: {', '.join(missing)}")
    frame = frame.loc[:, list(_KLINE_COLUMNS)].copy()
    frame["symbol"] = frame["symbol"].map(StockCodeUtil.to_suffix)
    return _normalize_values(frame, partition_date)


def _normalize_target_partition(path: Path, partition_date: str) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    missing = [column for column in _KLINE_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"正式行情分区字段缺失 {path}: {', '.join(missing)}")
    frame = frame.loc[:, list(_KLINE_COLUMNS)].copy()
    frame["symbol"] = frame["symbol"].map(StockCodeUtil.to_suffix)
    return _normalize_values(frame, partition_date)


def _normalize_values(frame: pd.DataFrame, partition_date: str) -> pd.DataFrame:
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
    for column in _KLINE_COLUMNS[2:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    invalid_symbols = ~frame["symbol"].str.fullmatch(r"\d{6}\.(SH|SZ|BJ)", na=False)
    if invalid_symbols.any():
        sample = frame.loc[invalid_symbols, "symbol"].head(3).tolist()
        raise ValueError(f"分区含无效股票代码: {sample}")
    expected = datetime.strptime(partition_date, "%Y%m%d").date()
    wrong_date = frame["time"].isna() | (frame["time"].dt.date != expected)
    if wrong_date.any():
        raise ValueError(f"dt={partition_date} 含 {int(wrong_date.sum())} 行跨分区时间")
    return frame.sort_values(list(_KEY_COLUMNS)).reset_index(drop=True)


def _validate_ohlcv(frame: pd.DataFrame, dataset: str, partition_date: str) -> None:
    if frame.empty:
        raise ValueError(f"{dataset}/dt={partition_date} 是空分区")
    if frame.duplicated(list(_KEY_COLUMNS)).any():
        raise ValueError(f"{dataset}/dt={partition_date} 存在重复 symbol+time")
    if frame["symbol"].duplicated().any():
        raise ValueError(f"{dataset}/dt={partition_date} 存在单标的多条日线")
    prices = frame[["open", "high", "low", "close"]]
    invalid = (
        prices.isna().any(axis=1)
        | (prices <= 0).any(axis=1)
        | (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
        | frame["volume"].isna()
        | (frame["volume"] < 0)
        | frame["amount"].isna()
        | (frame["amount"] < 0)
    )
    if invalid.any():
        raise ValueError(
            f"{dataset}/dt={partition_date} 有 {int(invalid.sum())} 行 OHLCV 不合法"
        )


def _merge_partition(target: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    payload = pd.concat([target, source], ignore_index=True)
    payload = payload.drop_duplicates(list(_KEY_COLUMNS), keep="last")
    payload = payload.sort_values(list(_KEY_COLUMNS)).reset_index(drop=True)
    return payload.loc[:, list(_KLINE_COLUMNS)]


def _normalize_index_partition(path: Path, partition_date: str) -> pd.DataFrame:
    """Normalize an index partition while retaining QuantDB compatibility fields."""
    frame = pd.read_parquet(path)
    if "time" not in frame.columns:
        if "datetime" in frame.columns:
            frame = frame.rename(columns={"datetime": "time"})
        elif "trade_date" in frame.columns:
            frame = frame.rename(columns={"trade_date": "time"})
    if "preClose" not in frame.columns:
        for alias in ("pre_close", "preclose", "prev_close"):
            if alias in frame.columns:
                frame = frame.rename(columns={alias: "preClose"})
                break
    if "preClose" not in frame.columns:
        frame["preClose"] = pd.NA
    if "Category" not in frame.columns:
        for alias in ("category", "category_id"):
            if alias in frame.columns:
                frame = frame.rename(columns={alias: "Category"})
                break
    if "Category" not in frame.columns:
        frame["Category"] = pd.NA
    missing = [column for column in _KLINE_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"指数分区字段缺失 {path}: {', '.join(missing)}")
    frame = frame.loc[:, list(_INDEX_COLUMNS)].copy()
    frame["symbol"] = frame["symbol"].map(StockCodeUtil.to_suffix)
    # 先保留可选字段，再由规范化函数按 symbol+time 排序，确保字段不发生错位。
    frame = _normalize_values(frame, partition_date)
    frame["preClose"] = pd.to_numeric(frame["preClose"], errors="coerce")
    return frame.loc[:, list(_INDEX_COLUMNS)]


def _normalize_minute_file(path: Path, *, symbol: str | None = None) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if "time" not in frame.columns and "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "time"})
    if "amount" not in frame.columns:
        frame["amount"] = 0.0
    if "symbol" not in frame.columns:
        frame["symbol"] = symbol or path.stem
    missing = [column for column in _MINUTE_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"分钟文件字段缺失 {path}: {', '.join(missing)}")
    frame = frame.loc[:, list(_MINUTE_COLUMNS)].copy()
    frame["symbol"] = frame["symbol"].map(StockCodeUtil.to_suffix)
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
    if frame["time"].isna().any():
        raise ValueError(f"分钟文件含无效时间: {path}")
    for column in _MINUTE_COLUMNS[2:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_values(list(_KEY_COLUMNS)).reset_index(drop=True)


def _merge_minute(target: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    payload = pd.concat([target, source], ignore_index=True)
    payload = payload.drop_duplicates(list(_KEY_COLUMNS), keep="last")
    return payload.sort_values(list(_KEY_COLUMNS)).reset_index(drop=True).loc[
        :, list(_MINUTE_COLUMNS)
    ]


def _merge_index_partition(target: pd.DataFrame, source: pd.DataFrame) -> pd.DataFrame:
    payload = pd.concat([target, source], ignore_index=True)
    payload = payload.drop_duplicates(list(_KEY_COLUMNS), keep="last")
    return payload.sort_values(list(_KEY_COLUMNS)).reset_index(drop=True).loc[
        :, list(_INDEX_COLUMNS)
    ]


def _build_qlib(
    target_root: Path,
    expected_latest: str,
    progress_cb: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    from backend.services.engine.qlib_data_builder import QlibDataBuilder

    configured = os.getenv("QLIB_PROVIDER_URI", "").strip()
    live_dir = Path(configured or "/data/qlib/cn_data")
    stage_dir = live_dir.parent / f".{live_dir.name}.stage-{uuid.uuid4().hex[:8]}"
    backup_dir = live_dir.parent / f".{live_dir.name}.backup-{uuid.uuid4().hex[:8]}"
    _remove_tree(stage_dir)
    builder = QlibDataBuilder.for_market(
        "CN", data_dir=target_root, qlib_dir=stage_dir
    )
    try:
        build_result = builder.build_all(
            incremental=False,
            progress_cb=progress_cb,
        )
        status = builder.get_status()
        calendar_file = stage_dir / "calendars" / "day.txt"
        calendar = [line.strip() for line in calendar_file.read_text().splitlines() if line.strip()]
        expected = _format_date(expected_latest)
        if not calendar or calendar[-1] < expected:
            raise RuntimeError(
                f"Qlib 日历未覆盖发布日: latest={calendar[-1] if calendar else None}, "
                f"expected={expected}"
            )
        if not status.get("instrument_count") or not status.get("feature_symbol_count"):
            raise RuntimeError("Qlib 构建结果缺少标的或特征文件")

        live_dir.parent.mkdir(parents=True, exist_ok=True)
        moved_live = False
        try:
            if live_dir.exists():
                os.replace(live_dir, backup_dir)
                moved_live = True
            os.replace(stage_dir, live_dir)
        except Exception:
            if moved_live and backup_dir.exists() and not live_dir.exists():
                os.replace(backup_dir, live_dir)
            raise
        _remove_tree(backup_dir)
        return {
            "status": "ok",
            "provider_uri": str(live_dir),
            "latest_date": calendar[-1],
            **build_result,
        }
    finally:
        _remove_tree(stage_dir)


def _rollback(applied: list[dict[str, Any]]) -> None:
    for item in reversed(applied):
        target_file: Path = item["target_file"]
        backup_file: Path | None = item.get("backup_file")
        if backup_file and backup_file.is_file():
            os.replace(backup_file, target_file)
        elif target_file.exists():
            target_file.unlink()


def _partition_file(root: Path, dataset: str, partition_date: str) -> Path:
    return root / "1_kline_data" / dataset / f"dt={partition_date}" / "data.parquet"


def _partition_dates(root: Path, dataset: str) -> list[str]:
    dataset_root = root / "1_kline_data" / dataset
    if not dataset_root.is_dir():
        return []
    return sorted(
        path.name.removeprefix("dt=")
        for path in dataset_root.glob("dt=????????")
        if path.is_dir()
        and path.name.removeprefix("dt=").isdigit()
        and (path / "data.parquet").is_file()
    )


def _latest_partition(root: Path, dataset: str) -> str | None:
    dates = _partition_dates(root, dataset)
    return dates[-1] if dates else None


def _new_partition_dates(source_root: Path, target_root: Path, dataset: str) -> list[str]:
    source_dates = set(_partition_dates(source_root, dataset))
    target_dates = set(_partition_dates(target_root, dataset))
    if not source_dates:
        return []
    newer = source_dates - target_dates
    if newer:
        return sorted(newer)
    latest = max(source_dates)
    return [latest] if latest in target_dates else []


def _minute_source_files(root: Path) -> list[tuple[str, Path]]:
    result: list[tuple[str, Path]] = []
    for dataset in PUBLISH_MINUTE_DATASETS:
        directory = root / "1_kline_data" / dataset
        if not directory.is_dir():
            continue
        result.extend((dataset, path) for path in sorted(directory.glob("*.parquet")))
    return result


def _validate_minute_release(root: Path) -> None:
    minimum_symbols = int(os.getenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "3000"))
    for dataset in PUBLISH_MINUTE_DATASETS:
        directory = root / "1_kline_data" / dataset
        files = list(directory.glob("*.parquet")) if directory.is_dir() else []
        if files and len(files) < minimum_symbols:
            raise ValueError(
                f"{dataset} 标的数 {len(files)} 小于发布门槛 {minimum_symbols}，"
                "可能是抽样同步"
            )


def _suffix_filename(name: str) -> str:
    stem = Path(name).stem
    return f"{StockCodeUtil.to_suffix(stem)}.parquet"


def _minute_coverage(root: Path, dataset: str) -> dict[str, Any]:
    directory = root / "1_kline_data" / dataset
    files = list(directory.glob("*.parquet")) if directory.is_dir() else []
    state_path = directory / "_sync_state.json"
    publication_state_path = directory / "_publication_state.json"
    state: dict[str, Any] = {}
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        try:
            state = json.loads(publication_state_path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            pass
    latest_at = state.get("latest_at") or state.get("checked_through")
    if not latest_at and files:
        latest_values: list[pd.Timestamp] = []
        for path in files[: min(len(files), 20)]:
            try:
                columns = pd.read_parquet(path, engine="pyarrow").columns
                time_column = "datetime" if "datetime" in columns else "time"
                frame = pd.read_parquet(path, columns=[time_column])
                latest_values.append(pd.to_datetime(frame[time_column]).max())
            except Exception:
                continue
        if latest_values:
            latest_at = max(latest_values).isoformat()
    return {
        "files": len(files),
        "latest_at": latest_at,
        "failed_symbols": int(state.get("failed_symbols") or 0),
    }


def _write_minute_publish_state(target_root: Path, source_root: Path) -> None:
    for dataset in PUBLISH_MINUTE_DATASETS:
        source_state = _minute_coverage(source_root, dataset)
        if not source_state["files"]:
            continue
        path = target_root / "1_kline_data" / dataset / "_publication_state.json"
        _write_json(
            {
                "dataset": dataset,
                "latest_at": source_state["latest_at"],
                "files": source_state["files"],
                "published_at": _now_iso(),
            },
            path,
        )


def _latest_common_partition(root: Path, datasets: tuple[str, ...]) -> str | None:
    date_sets = [set(_partition_dates(root, dataset)) for dataset in datasets]
    if not date_sets or any(not dates for dates in date_sets):
        return None
    common = set.intersection(*date_sets)
    return max(common) if common else None


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False, compression="zstd")


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    os.replace(temp, path)


def _read_latest_manifest(root: Path) -> dict[str, Any] | None:
    latest = root / "releases" / "latest.json"
    try:
        pointer = json.loads(latest.read_text(encoding="utf-8"))
        manifest = Path(pointer["manifest"])
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        return {
            "release_id": payload.get("release_id"),
            "finished_at": payload.get("finished_at"),
            "published_date_range": payload.get("published_date_range"),
            "qlib": payload.get("qlib"),
            "pg": payload.get("pg"),
        }
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _release_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"easy-tdx-{stamp}-{uuid.uuid4().hex[:6]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_date(value: str | None) -> str | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def _remove_tree(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)


@contextmanager
def _publication_lock(target_root: Path) -> Iterator[None]:
    """Serialize manual, tracked, and beat-triggered publications."""
    import fcntl

    target_root.mkdir(parents=True, exist_ok=True)
    lock_path = target_root / ".easy_tdx_publish.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("已有 easy_tdx 发布任务正在执行，请稍后重试") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _prune_publish_runs(target_root: Path) -> None:
    try:
        keep = max(int(os.getenv("QM_EASY_TDX_PUBLISH_BACKUPS", "3")), 0)
    except ValueError:
        keep = 3
    root = target_root / ".easy_tdx_publish"
    if not root.is_dir():
        return
    runs = sorted((path for path in root.iterdir() if path.is_dir()), reverse=True)
    for stale in runs[keep:]:
        try:
            _remove_tree(stale)
        except OSError:
            pass
