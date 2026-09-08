"""手动执行同花顺每日快照。

用法：
    HITHINK_FINANCE_API_KEY=... python backend/scripts/ths_daily_snapshot.py
    HITHINK_FINANCE_API_KEY=... python backend/scripts/ths_daily_snapshot.py --auction

API Key 只从环境变量读取，不接受命令行参数，避免出现在 shell history 或进程列表。
"""

from __future__ import annotations

import argparse
import json

from backend.services.engine.data_platform.ths_snapshots import (
    run_auction_snapshot,
    run_daily_snapshot,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="保存同花顺每日历史快照")
    parser.add_argument("--auction", action="store_true", help="只采集竞价快照和短线基准")
    args = parser.parse_args()
    result = run_auction_snapshot() if args.auction else run_daily_snapshot()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
