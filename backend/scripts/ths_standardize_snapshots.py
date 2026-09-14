"""Create and backfill the standardized 同花顺 snapshot layer.

The command is safe to rerun. Raw snapshots remain untouched and the
standardized table is updated by the same idempotent keys as daily collection.
"""

from __future__ import annotations

import argparse

from backend.services.engine.data_platform.ths_snapshots import build_default_store


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="replace all derived rows from the preserved raw snapshot table",
    )
    args = parser.parse_args()
    store = build_default_store()
    store.ensure_schema()
    if args.rebuild:
        result = store.rebuild_standardized()
        print(
            "同花顺标准化快照已重建："
            f"原始 {result['raw_rows']} 行，标准化 {result['standardized_rows']} 行"
        )
    else:
        print("同花顺标准化快照表已创建，历史原始快照已完成回填")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
