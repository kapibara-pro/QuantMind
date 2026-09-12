"""按标的 / 单文件布局数据集的真实数据区间。

分区布局（``dt=YYYYMMDD/data.parquet``）的区间能直接从目录名读出；
按标的布局（``{SYMBOL}.parquet``）的路径里没有任何日期信息，只能读
parquet 数据块尾部的统计信息（min/max statistics）才能得到真实区间。

扫描一个数据集要打开 5000+ 次文件 footer，单次约 1-5 秒、全部按标的
数据集合计约 30 秒，因此这里**不与请求同步**：

1. 接口只从内存缓存（或磁盘快照）读值，未命中先显示空；
2. :func:`prewarm_bounds` 发现过期/缺失时起后台线程单飞刷新；
3. 结果落盘到 ``<data_dir>/.sync_state/catalog_ranges.json``，容器重启
   后仍能立即展示，不必重新扫盘。

分钟线的"最新进度"由 TTL 决定（默认 6 小时），同步完成后重新加载页面
即可看到更新，无需等待接口变慢。
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# 数据只在同步/发布时变化，缓存 6 小时足够；到期后由后台线程刷新。
RANGE_CACHE_TTL_SECONDS = 6 * 3600.0
# 扫描结果为空（如列名不匹配）时缩短重试间隔，避免一直空着。
EMPTY_RANGE_RETRY_SECONDS = 600.0
# 防御性上限：异常目录（如误挂载）不应拖垮后台刷新。
MAX_SCAN_FILES = 30000

SNAPSHOT_REL_PATH = Path(".sync_state") / "catalog_ranges.json"

_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()
_loaded_roots: set[str] = set()
_refreshing: set[str] = set()


def _rel_key(spec: Any) -> str:
    return f"{getattr(spec, 'rel_dir', '')}|{getattr(spec, 'date_column', '')}"


def _cache_key(root: Path, spec: Any) -> str:
    return f"{root}|{_rel_key(spec)}"


def _snapshot_path(root: Path) -> Path:
    return root / SNAPSHOT_REL_PATH


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


def _load_snapshot(root: Path) -> None:
    """把落盘快照读进内存（每个 root 只读一次）。"""
    with _cache_lock:
        if str(root) in _loaded_roots:
            return
        _loaded_roots.add(str(root))

    path = _snapshot_path(root)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("quantdb range: 读取快照 %s 失败: %s", path, exc)
        return

    entries = raw.get("datasets") if isinstance(raw, dict) else None
    if not isinstance(entries, dict):
        return
    with _cache_lock:
        for key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            value = entry.get("value")
            ts = entry.get("ts")
            if isinstance(value, dict) and isinstance(ts, (int, float)):
                _cache.setdefault(f"{root}|{key}", (float(ts), value))


def _write_snapshot(root: Path) -> None:
    """best-effort 落盘：失败只记日志，不影响接口。"""
    path = _snapshot_path(root)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"updated_at": time.time(), "datasets": {}}
        prefix = f"{root}|"
        with _cache_lock:
            for key, (ts, value) in _cache.items():
                if not key.startswith(prefix):
                    continue
                payload["datasets"][key[len(prefix):]] = {"ts": ts, "value": value}
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(path)
    except OSError as exc:
        logger.warning("quantdb range: 写入快照 %s 失败: %s", path, exc)


def dataset_time_bounds(root: Path, spec: Any) -> dict[str, Any]:
    """只读缓存返回数据集区间；未命中或未声明 date_column 时返回 {}。"""
    if not getattr(spec, "date_column", None):
        return {}
    _load_snapshot(root)
    with _cache_lock:
        hit = _cache.get(_cache_key(root, spec))
    return dict(hit[1]) if hit else {}


def _scan_specs(root: Path, specs: Iterable[Any]) -> None:
    for spec in specs:
        column = getattr(spec, "date_column", None)
        if not column:
            continue
        try:
            bounds = _scan_directory(root / spec.rel_dir, column)
        except Exception as exc:  # noqa: BLE001 - 统计失败不应影响目录展示
            logger.warning(
                "quantdb range: 扫描 %s 失败: %s", getattr(spec, "dataset", "?"), exc
            )
            continue
        key = _cache_key(root, spec)
        with _cache_lock:
            # 扫描失败时保留旧值，避免区间在页面上闪成空。
            if bounds or key not in _cache:
                _cache[key] = (time.time(), bounds)
        # 每个数据集扫完就落盘：即使刷新途中进程重启，已完成的部分也不丢。
        _write_snapshot(root)


def refresh_bounds(root: Path, specs: Iterable[Any]) -> None:
    """同步扫描全部数据集并落盘（供后台线程或运维脚本调用）。"""
    targets = [s for s in specs if getattr(s, "date_column", None)]
    if not targets:
        return
    started = time.monotonic()
    _scan_specs(root, targets)
    _write_snapshot(root)
    logger.info(
        "quantdb range: 刷新 %s 个数据集区间完成，耗时 %.1fs",
        len(targets),
        time.monotonic() - started,
    )


def prewarm_bounds(root: Path, specs: Iterable[Any], *, force: bool = False) -> bool:
    """载入磁盘快照；过期或缺失时起后台线程刷新。永不阻塞调用方。

    返回 True 表示已触发一次后台刷新（同 root 单飞，不会重复起线程）。
    """
    targets = [s for s in specs if getattr(s, "date_column", None)]
    if not targets:
        return False

    _load_snapshot(root)
    now = time.time()
    with _cache_lock:
        stale = force
        if not stale:
            for spec in targets:
                hit = _cache.get(_cache_key(root, spec))
                if hit is None:
                    stale = True
                    break
                ttl = RANGE_CACHE_TTL_SECONDS if hit[1] else EMPTY_RANGE_RETRY_SECONDS
                if now - hit[0] >= ttl:
                    stale = True
                    break
        if not stale or str(root) in _refreshing:
            return False
        _refreshing.add(str(root))

    def _run() -> None:
        try:
            refresh_bounds(root, targets)
        except Exception as exc:  # noqa: BLE001
            logger.warning("quantdb range: 后台刷新失败: %s", exc)
        finally:
            with _cache_lock:
                _refreshing.discard(str(root))

    threading.Thread(target=_run, name="quantdb-range-refresh", daemon=True).start()
    return True


def clear_cache() -> None:
    """清空内存缓存（测试用；磁盘快照需另行删除）。"""
    with _cache_lock:
        _cache.clear()
        _loaded_roots.clear()


def _main() -> None:
    """运维入口：手动刷新区间快照并打印结果。

    ``python -m backend.shared.quantdb_range --data-dir /data/quantdb``
    """
    import argparse
    import os

    from backend.shared.quantdb_datasets import DATASETS

    parser = argparse.ArgumentParser(description="刷新 QuantDB 数据集区间快照")
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("QM_QUANTDB_DATA_DIR", "/data/quantdb"),
        help="QuantDB 本地数据目录",
    )
    args = parser.parse_args()

    root = Path(args.data_dir)
    started = time.monotonic()
    refresh_bounds(root, DATASETS)
    print(f"刷新完成，耗时 {time.monotonic() - started:.1f}s，快照：{_snapshot_path(root)}")
    for spec in DATASETS:
        bounds = dataset_time_bounds(root, spec)
        if not bounds:
            continue
        print(
            f"  {spec.dataset:<20} {bounds.get('start_date')} -> {bounds.get('end_date')}"
            f"  files={bounds.get('covered_files')}"
            + (f"  end_at={bounds['end_at']}" if bounds.get("end_at") else "")
        )


if __name__ == "__main__":
    _main()
