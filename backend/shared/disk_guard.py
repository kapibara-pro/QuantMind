"""磁盘余量守卫：避免同步/发布把数据盘写满，进而拖垮 Redis 与 Celery。

事故背景：easy_tdx 正式发布在磁盘接近写满时继续写入，最终 `/www` 用尽，
Docker 无法创建容器快照、Redis 无法落盘（stop-writes-on-bgsave-error），
Celery worker 全部退出，发布任务永久停在 running/cancelling。
因此在任务开始前先做一次余量检查，用明确的错误信息快速失败。
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

# 最低可用空间（GiB）：低于该值直接拒绝启动数据任务。
DEFAULT_MIN_FREE_GIB = 5.0


class InsufficientDiskSpaceError(RuntimeError):
    """数据盘可用空间不足时抛出。"""


def min_free_bytes() -> int:
    raw = os.getenv("QM_DATA_MIN_FREE_GIB")
    try:
        gib = float(raw) if raw not in (None, "") else DEFAULT_MIN_FREE_GIB
    except (TypeError, ValueError):
        gib = DEFAULT_MIN_FREE_GIB
    return int(max(gib, 0) * 1024**3)


def ensure_disk_headroom(*paths: str | Path) -> None:
    """校验目标路径所在分区剩余空间，不足则抛出带明细的异常。

    路径不存在时向上回溯到最近的已存在父目录，避免因为目录尚未创建而漏检。
    """
    required = min_free_bytes()
    if required <= 0:
        return

    checked: set[str] = set()
    for raw_path in paths:
        if raw_path is None:
            continue
        probe = Path(raw_path)
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        key = str(probe)
        if key in checked:
            continue
        checked.add(key)
        try:
            usage = shutil.disk_usage(probe)
        except OSError:
            # 无法探测时不阻断任务（例如路径在远端挂载上）。
            continue
        if usage.free < required:
            raise InsufficientDiskSpaceError(
                f"数据盘可用空间不足：{probe} 剩余 "
                f"{usage.free / 1024**3:.1f} GiB，低于所需 "
                f"{required / 1024**3:.1f} GiB。"
                "请先清理磁盘（历史发布备份、离线安装包、旧日志等）后再执行。"
            )
