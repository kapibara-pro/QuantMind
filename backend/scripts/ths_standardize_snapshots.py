"""Create and backfill the standardized 同花顺 snapshot layer.

The command is safe to rerun. Raw snapshots remain untouched and the
standardized table is updated by the same idempotent keys as daily collection.
"""

from __future__ import annotations

from backend.services.engine.data_platform.ths_snapshots import build_default_store


def main() -> int:
    store = build_default_store()
    store.ensure_schema()
    print("同花顺标准化快照表已创建，历史原始快照已完成回填")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
