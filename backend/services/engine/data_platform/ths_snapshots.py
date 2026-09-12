"""同花顺金融数据每日快照采集与持久化。

该模块只负责三件事：调用同花顺公开 REST API、把上游代码转换为
QuantMind 内部的前缀式代码、将原始业务 JSON 以幂等方式落入 PostgreSQL。
选股/情绪/板块指标的派生计算放在下游，避免采集任务改变上游口径。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Protocol
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import text

from backend.shared.stock_utils import StockCodeUtil
from backend.shared.runtime_secrets import get_secret

logger = logging.getLogger(__name__)

SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_BASE_URL = "https://fuyao.aicubes.cn"
SNAPSHOT_SCHEMA_VERSION = "ths_snapshot_v1"
MARKET_SCOPE = "__market__"


class ThsSnapshotError(RuntimeError):
    """同花顺快照请求或持久化失败。"""

    def __init__(self, message: str, *, retryable: bool = True, code: Any = None) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.code = code


class SnapshotClient(Protocol):
    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]: ...


@dataclass(frozen=True)
class SnapshotRecord:
    snapshot_date: date
    dataset: str
    scope_key: str
    symbol: str | None
    as_of_ms: int | None
    payload: dict[str, Any]
    request_id: str | None
    row_count: int


class ThsFinanceClient:
    """同花顺 REST 客户端。

    API Key 仅从环境变量读取，任何异常信息和日志都不会包含请求头。
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.api_key = (api_key or get_secret("HITHINK_FINANCE_API_KEY")).strip()
        if not self.api_key:
            raise ThsSnapshotError("HITHINK_FINANCE_API_KEY 未配置", retryable=False)
        self.base_url = (
            base_url or os.getenv("HITHINK_FINANCE_BASE_URL", DEFAULT_BASE_URL)
        ).rstrip("/")
        self.timeout = timeout or float(os.getenv("HITHINK_FINANCE_TIMEOUT_SECONDS", "20"))
        self.max_retries = max(
            0,
            int(
                max_retries
                if max_retries is not None
                else os.getenv("HITHINK_FINANCE_MAX_RETRIES", "3")
            ),
        )
        self._client = client

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.startswith("/api/"):
            raise ThsSnapshotError(f"非法同花顺路径: {path}", retryable=False)
        request_id = uuid.uuid4().hex
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                if self._client is None:
                    with httpx.Client(base_url=self.base_url, timeout=self.timeout) as client:
                        response = client.get(
                            path, params=params, headers={"X-api-key": self.api_key}
                        )
                else:
                    response = self._client.get(
                        path, params=params, headers={"X-api-key": self.api_key}
                    )
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict):
                    raise ThsSnapshotError("同花顺响应不是 JSON 对象")
                code = body.get("code")
                if code == 0:
                    return body
                # 4001/5xxx 可退避重试；参数、权限和数据未准备好不重试。
                try:
                    code_number = int(code)
                except (TypeError, ValueError):
                    code_number = None
                if code_number != 4001 and not (
                    code_number is not None and 5000 <= code_number < 6000
                ):
                    raise ThsSnapshotError(
                        f"同花顺业务错误 code={code}", retryable=False, code=code
                    )
                raise ThsSnapshotError(
                    f"同花顺服务暂不可用 code={code}", retryable=True, code=code
                )
            except ThsSnapshotError as exc:
                if not exc.retryable:
                    raise
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(min(2**attempt, 8))
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if exc.response.status_code < 500 and exc.response.status_code != 429:
                    raise ThsSnapshotError(
                        f"同花顺 HTTP 错误 status={exc.response.status_code}",
                        retryable=False,
                    ) from exc
                if attempt >= self.max_retries:
                    break
                time.sleep(min(2**attempt, 8))
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(min(2**attempt, 8))
        raise ThsSnapshotError(
            f"同花顺请求失败 path={path} request_id={request_id}: {last_error}"
        ) from last_error


def _as_ms(value: Any) -> int | None:
    return int(value) if isinstance(value, (int, float)) else None


def _prefix_symbol(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = StockCodeUtil.to_prefix(value)
    return normalized if normalized != value or normalized[:2] in {"SH", "SZ", "BJ"} else None


def _item_symbol(item: dict[str, Any]) -> str | None:
    for key in ("thscode", "symbol", "ticker"):
        symbol = _prefix_symbol(item.get(key))
        if symbol:
            return symbol
    return None


def _fallback_scope(item: dict[str, Any], index: int) -> str:
    """为指数/板块等非股票列表项生成稳定且不冲突的范围键。"""
    for key in ("thscode", "symbol", "ticker", "code", "index_code", "name"):
        value = item.get(key)
        if isinstance(value, (str, int, float)) and str(value).strip():
            return f"{MARKET_SCOPE}:{str(value).strip().upper()}"
    digest = hashlib.sha1(
        json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    return f"{MARKET_SCOPE}:{index}:{digest}"


def _records(
    *,
    snapshot_date: date,
    dataset: str,
    body: dict[str, Any],
    scope_prefix: str | None = None,
) -> list[SnapshotRecord]:
    data = body.get("data") or {}
    if not isinstance(data, dict):
        data = {"value": data}
    request_id = body.get("request_id")
    as_of_ms = _as_ms(data.get("timestamp"))
    items = data.get("item")
    if isinstance(items, list):
        records: list[SnapshotRecord] = []
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                item = {"value": item}
            symbol = _item_symbol(item)
            scope_key = symbol or _fallback_scope(item, index)
            if scope_prefix:
                scope_key = f"{scope_prefix}:{scope_key}"
            records.append(
                SnapshotRecord(
                    snapshot_date=snapshot_date,
                    dataset=dataset,
                    scope_key=scope_key,
                    symbol=symbol,
                    as_of_ms=as_of_ms,
                    payload=item,
                    request_id=request_id,
                    row_count=len(items),
                )
            )
        if records:
            return records
    return [
        SnapshotRecord(
            snapshot_date=snapshot_date,
            dataset=dataset,
            scope_key=MARKET_SCOPE,
            symbol=None,
            as_of_ms=as_of_ms,
            payload=data,
            request_id=request_id,
            row_count=len(items) if isinstance(items, list) else 1,
        )
    ]


class ThsSnapshotStore:
    """原始快照存储。

    使用 ``scope_key`` 解决市场级 JSON 与标的级 JSON 的统一唯一键问题。
    同一天同一数据集同一标的重复执行时覆盖该行，保证任务可安全重跑。
    """

    CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS qm_ths_daily_snapshots (
        id BIGSERIAL PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        dataset TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        symbol TEXT,
        as_of_ms BIGINT,
        payload JSONB NOT NULL,
        source_request_id TEXT,
        row_count INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'success',
        schema_version TEXT NOT NULL DEFAULT 'ths_snapshot_v1',
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (snapshot_date, dataset, scope_key)
    )
    """
    CREATE_VIEWS = (
        """
        CREATE OR REPLACE VIEW v_ths_daily_snapshot AS
        SELECT snapshot_date AS trade_date, dataset, scope_key, symbol, as_of_ms,
               payload, source_request_id, row_count, status, schema_version,
               captured_at, 'ths'::TEXT AS source
        FROM qm_ths_daily_snapshots
        """,
        """
        CREATE OR REPLACE VIEW v_ths_stock_selection_daily AS
        SELECT * FROM v_ths_daily_snapshot
        WHERE dataset IN ('ticker_catalog', 'valuation_snapshot', 'financial_snapshot')
        """,
        """
        CREATE OR REPLACE VIEW v_ths_emotion_daily AS
        SELECT * FROM v_ths_daily_snapshot
        WHERE dataset IN (
          'limit_up_pool', 'limit_down_pool', 'limit_break_pool',
          'limit_up_ladder', 'anomaly_list', 'skyrocket_list',
          'hot_stock_list', 'hot_stock_list_history', 'dragon_tiger_all'
        )
        """,
        """
        CREATE OR REPLACE VIEW v_ths_auction_daily AS
        SELECT * FROM v_ths_daily_snapshot
        WHERE dataset IN ('auction_snapshot', 'auction_short_term_benchmark')
        """,
        """
        CREATE OR REPLACE VIEW v_ths_sector_daily AS
        SELECT * FROM v_ths_daily_snapshot
        WHERE dataset LIKE 'index_catalog_%'
           OR dataset IN ('index_snapshot', 'index_constituents')
        """,
    )

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def ensure_schema(self) -> None:
        with self.engine.begin() as connection:
            connection.execute(text(self.CREATE_TABLE))
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_qm_ths_snapshot_date "
                    "ON qm_ths_daily_snapshots (snapshot_date)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_qm_ths_snapshot_dataset "
                    "ON qm_ths_daily_snapshots (dataset, snapshot_date)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_qm_ths_snapshot_symbol "
                    "ON qm_ths_daily_snapshots (symbol, snapshot_date)"
                )
            )
            for view_sql in self.CREATE_VIEWS:
                connection.execute(text(view_sql))

    def upsert(self, records: Iterable[SnapshotRecord]) -> int:
        rows = list(records)
        if not rows:
            return 0
        statement = text(
            """
            INSERT INTO qm_ths_daily_snapshots
              (snapshot_date, dataset, scope_key, symbol, as_of_ms, payload,
               source_request_id, row_count, status, schema_version, captured_at)
            VALUES
              (:snapshot_date, :dataset, :scope_key, :symbol, :as_of_ms,
               CAST(:payload AS JSONB), :source_request_id, :row_count,
               'success', :schema_version, NOW())
            ON CONFLICT (snapshot_date, dataset, scope_key) DO UPDATE SET
              symbol = EXCLUDED.symbol,
              as_of_ms = EXCLUDED.as_of_ms,
              payload = EXCLUDED.payload,
              source_request_id = EXCLUDED.source_request_id,
              row_count = EXCLUDED.row_count,
              status = EXCLUDED.status,
              schema_version = EXCLUDED.schema_version,
              captured_at = EXCLUDED.captured_at
            """
        )
        params = [
            {
                "snapshot_date": row.snapshot_date,
                "dataset": row.dataset,
                "scope_key": row.scope_key,
                "symbol": row.symbol,
                "as_of_ms": row.as_of_ms,
                "payload": json.dumps(row.payload, ensure_ascii=False, separators=(",", ":")),
                "source_request_id": row.request_id,
                "row_count": row.row_count,
                "schema_version": SNAPSHOT_SCHEMA_VERSION,
            }
            for row in rows
        ]
        with self.engine.begin() as connection:
            connection.execute(statement, params)
        return len(rows)


def _batched(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


class ThsDailySnapshotCollector:
    """采集盘后选股、情绪和板块快照。"""

    def __init__(self, client: SnapshotClient, store: ThsSnapshotStore) -> None:
        self.client = client
        self.store = store

    def _save(
        self,
        snapshot_date: date,
        dataset: str,
        body: dict[str, Any],
        scope_prefix: str | None = None,
    ) -> int:
        return self.store.upsert(
            _records(
                snapshot_date=snapshot_date,
                dataset=dataset,
                body=body,
                scope_prefix=scope_prefix,
            )
        )

    def _save_pool(
        self, snapshot_date: date, dataset: str, path: str, params: dict[str, Any]
    ) -> int:
        """按 page/size 取尽专题池，避免 200 条上限造成静默截断。"""
        page = 1
        total = 0
        while True:
            page_body = self.client.get(path, {**params, "page": page})
            total += self._save(snapshot_date, dataset, page_body)
            pagination = (page_body.get("data") or {}).get("pagination") or {}
            pages = int(pagination.get("pages") or page)
            if page >= pages:
                return total
            page += 1

    def _symbols(self) -> tuple[list[str], dict[str, Any] | None]:
        configured = [
            x.strip()
            for x in os.getenv("HITHINK_FINANCE_SYMBOLS", "").split(",")
            if x.strip()
        ]
        if configured:
            return [StockCodeUtil.to_suffix(x) for x in configured], None
        body = self.client.get(
            "/api/meta/tickers/list",
            {
                "exchange": "SH,SZ,BJ",
                "asset_type": "a-share",
                "limit": 10000,
                "offset": 0,
            },
        )
        symbols = [
            item["thscode"]
            for item in body.get("data", {}).get("item", [])
            if isinstance(item, dict) and item.get("thscode")
        ]
        return symbols, body

    def collect(self, snapshot_date: date | None = None) -> dict[str, Any]:
        target = snapshot_date or datetime.now(SHANGHAI).date()
        self.store.ensure_schema()
        summary: dict[str, Any] = {
            "snapshot_date": target.isoformat(),
            "datasets": {},
            "status": "success",
        }

        def save(
            dataset: str, body: dict[str, Any], scope_prefix: str | None = None
        ) -> None:
            summary["datasets"][dataset] = (
                summary["datasets"].get(dataset, 0)
                + self._save(target, dataset, body, scope_prefix=scope_prefix)
            )

        symbols, meta = self._symbols()
        if meta:
            save("ticker_catalog", meta)

        for batch in _batched(symbols, 100):
            save(
                "valuation_snapshot",
                self.client.get(
                    "/api/a-share/valuations/snapshot",
                    {"thscodes": ",".join(batch)},
                ),
            )

        date_ms = int(
            datetime.combine(target, datetime.min.time(), tzinfo=SHANGHAI).timestamp()
            * 1000
        )
        for dataset, path in {
            "limit_up_pool": "/api/a-share/special-data/limit-up-pool",
            "limit_down_pool": "/api/a-share/special-data/limit-down-pool",
            "limit_break_pool": "/api/a-share/special-data/limit-break-pool",
        }.items():
            summary["datasets"][dataset] = self._save_pool(
                target, dataset, path, {"date_ms": date_ms, "size": 200}
            )
        for dataset, path, params in (
            ("limit_up_ladder", "/api/a-share/special-data/limit-up-ladder", None),
            ("anomaly_list", "/api/a-share/special-data/anomaly-analysis-list", None),
            ("skyrocket_list", "/api/a-share/special-data/skyrocket-list", {"period": "day"}),
            ("hot_stock_list", "/api/a-share/special-data/hot-stock-list", {"period": "day"}),
            (
                "dragon_tiger_all",
                "/api/a-share/special-data/dragon-tiger-list",
                {"date": target.isoformat(), "board_type": "all"},
            ),
        ):
            save(dataset, self.client.get(path, params))
        save(
            "hot_stock_list_history",
            self.client.get(
                "/api/a-share/special-data/hot-stock-list-history",
                {"date": target.isoformat()},
            ),
        )
        for tag in ("cn_concept", "region", "tszs", "industry"):
            save(
                f"index_catalog_{tag}",
                self.client.get(
                    "/api/a-share-index/catalog/ths-index-list", {"tag": tag}
                ),
            )
        index_codes = [
            x.strip()
            for x in os.getenv(
                "HITHINK_FINANCE_INDEX_CODES",
                "000300.SH,000001.SH,399001.SZ,399006.SZ",
            ).split(",")
            if x.strip()
        ]
        for batch in _batched(index_codes, 100):
            save(
                "index_snapshot",
                self.client.get(
                    "/api/a-share-index/prices/snapshot",
                    {"thscodes": ",".join(batch)},
                ),
            )
        for code in index_codes:
            save(
                "index_constituents",
                self.client.get(
                    "/api/a-share-index/constituents/ths-stock-list", {"thscode": code}
                ),
                scope_prefix=f"index:{code}",
            )
        return summary

    def collect_auction(self, snapshot_date: date | None = None) -> dict[str, Any]:
        target = snapshot_date or datetime.now(SHANGHAI).date()
        self.store.ensure_schema()
        symbols, _ = self._symbols()
        count = 0
        for batch in _batched(symbols, 100):
            body = self.client.get(
                "/api/a-share/auction/snapshot",
                {"thscodes": ",".join(batch), "stage": "final"},
            )
            count += self._save(target, "auction_snapshot", body)
        count += self._save(
            target,
            "auction_short_term_benchmark",
            self.client.get(
                "/api/a-share/auction/short-term-benchmark",
                {"date": target.isoformat()},
            ),
        )
        return {"status": "success", "snapshot_date": target.isoformat(), "rows": count}


def build_default_store() -> ThsSnapshotStore:
    from sqlalchemy import create_engine

    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url.startswith("postgresql+asyncpg://"):
        database_url = database_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if not database_url:
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        name = os.getenv("DB_NAME", "quantmind")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "")
        database_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return ThsSnapshotStore(create_engine(database_url, pool_pre_ping=True))


def run_daily_snapshot() -> dict[str, Any]:
    client = ThsFinanceClient()
    return ThsDailySnapshotCollector(client, build_default_store()).collect()


def run_auction_snapshot() -> dict[str, Any]:
    client = ThsFinanceClient()
    return ThsDailySnapshotCollector(client, build_default_store()).collect_auction()
