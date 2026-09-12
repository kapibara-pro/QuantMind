"""按标的 / 单文件布局数据集的真实数据区间。

分区布局（``dt=YYYYMMDD/data.parquet``）的区间能直接从目录名读出；
按标的布局（``{SYMBOL}.parquet``）的路径里没有任何日期信息，必须读
parquet 数据块尾部的统计信息（min/max statistics）才能得到真实区间。

这里只读 footer、不解码数据页：实测 5000+ 文件约 2-4 秒，结果带 TTL
缓存；管理台目录/差异接口会先用 :func:`prewarm_bounds` 并发预热。
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# 单个数据集 5000+ 个文件仍要打开 5000+ 次 footer，缓存 10 分钟，
# 避免页面重复加载时反复扫盘。
RANGE_CACHE_TTL_SECONDS = 600.0
DEFAULT_SCAN_WORKERS = 8
# 防御性上限：异常目录（如误挂载）不应把管理台接口拖死。
MAX_SCAN_FILES = 30000

_cache: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _as_datetime(value: Any) -> datetime | None:
    """把 parquet 统计值（Timestamp / str(YYYYMMDD) / ISO）统一成 datetime。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    for fmt in ("%Y%m%d", "%Y%m%d%H%M%S", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _file_bounds(path: Path, column: str) -> tuple[datetime | None, datetime | None] | None:
    """读单个 parquet 的 footer，返回 (最早, 最晚)；列缺失或不可读时返回 None。"""
    import pyarrow.parquet as pq

    try:
        parquet = pq.ParquetFile(path)
    except Exception as exc:  # noqa: BLE001 - 单个坏文件不应中断整体统计
        logger.debug("quantdb range: 无法读取 %s: %s", path, exc)
        return None

    names = parquet.schema_arrow.names
    if column not in names:
        return None
    index = names.index(column)

    metadata = parquet.metadata
    low: datetime | None = None
    high: datetime | None = None
    for row_group in range(metadata.num_row_groups):
        stats = metadata.row_group(row_group).column(index).statistics
        if stats is None or not stats.has_min_max:
            continue
        moment = _as_datetime(stats.min)
        if moment is not None and (low is None or moment < low):
            low = moment
        moment = _as_datetime(stats.max)
        if moment is not None and (high is None or moment > high):
            high = moment
    return low, high


def _scan_directory(directory: Path, column: str) -> dict[str, Any]:
    if not directory.is_dir():
        return {}
    files = sorted(f for f in directory.glob("*.parquet") if f.is_file())
    if not files:
        return {}

    truncated = len(files) > MAX_SCAN_FILES
    if truncated:
        logger.warning(
            "quantdb range: %s 文件数 %s 超过上限，只扫描前 %s 个",
            directory,
            len(files),
            MAX_SCAN_FILES,
        )
        files = files[:MAX_SCAN_FILES]

    low: datetime | None = None
    high: datetime | None = None
    covered = 0
    for path in files:
        bounds = _file_bounds(path, column)
        if bounds is None:
            continue
        file_low, file_high = bounds
        if file_low is None and file_high is None:
            continue
        covered += 1
        if file_low is not None and (low is None or file_low < low):
            low = file_low
        if file_high is not None and (high is None or file_high > high):
            high = file_high

    if low is None or high is None:
        return {}

    payload: dict[str, Any] = {
        # 与分区布局保持一致，统一用 YYYYMMDD，便于和远端 end_date 直接比较。
        "start_date": low.strftime("%Y%m%d"),
        "end_date": high.strftime("%Y%m%d"),
        "covered_files": covered,
    }
    if truncated:
        payload["range_truncated"] = True
    # 日内数据补上精确到秒的边界，前端用 tooltip 展示"最新到哪一根"。
    if low.time() != datetime.min.time() or high.time() != datetime.min.time():
        payload["start_at"] = low.isoformat(timespec="seconds")
        payload["end_at"] = high.isoformat(timespec="seconds")
    return payload


def dataset_time_bounds(
    root: Path, spec: Any, *, use_cache: bool = True
) -> dict[str, Any]:
    """返回数据集真实区间；spec 未声明 date_column 或读不到时返回 {}。"""
    column = getattr(spec, "date_column", None)
    if not column:
        return {}

    key = (str(root), str(getattr(spec, "rel_dir", "")), str(column))
    if use_cache:
        with _cache_lock:
            hit = _cache.get(key)
        if hit is not None and time.monotonic() - hit[0] < RANGE_CACHE_TTL_SECONDS:
            return dict(hit[1])

    try:
        bounds = _scan_directory(root / spec.rel_dir, column)
    except Exception as exc:  # noqa: BLE001 - 统计失败不应影响目录展示
        logger.warning("quantdb range: 扫描 %s 失败: %s", getattr(spec, "dataset", "?"), exc)
        bounds = {}

    if use_cache:
        with _cache_lock:
            _cache[key] = (time.monotonic(), bounds)
    return dict(bounds)


def prewarm_bounds(
    root: Path, specs: Iterable[Any], workers: int = DEFAULT_SCAN_WORKERS
) -> None:
    """并发预热多个数据集的区间缓存（单数据集内顺序扫，避免线程爆炸）。"""
    targets = [s for s in specs if getattr(s, "date_column", None)]
    if not targets:
        return

    def _run(spec: Any) -> None:
        try:
            dataset_time_bounds(root, spec)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "quantdb range: 预热 %s 失败: %s", getattr(spec, "dataset", "?"), exc
            )

    if len(targets) == 1:
        _run(targets[0])
        return
    with ThreadPoolExecutor(max_workers=min(workers, len(targets))) as pool:
        list(pool.map(_run, targets))


def clear_cache() -> None:
    """清空区间缓存（同步/发布后或测试用）。"""
    with _cache_lock:
        _cache.clear()
