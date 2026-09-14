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
import re
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

# The upstream APIs use slightly different names for the same concept. These
# aliases form the stable preview contract while ``extra`` keeps unmapped data.
STANDARD_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "symbol": ("thscode", "symbol", "ticker", "stock_code", "code"),
    "name": ("name", "stock_name", "security_name", "index_name"),
    "index_code": ("index_code", "indexcode", "ths_index_code"),
    "exchange": ("exchange", "market", "market_code"),
    "category": ("category", "category_name", "tag", "type"),
    "rank": ("rank", "ranking", "อันดับ", "position"),
    "price": ("price", "now", "last", "latest", "close"),
    "change_pct": ("change_pct", "change_percent", "pct_chg", "changeRate", "涨跌幅"),
    "change_amount": ("change", "change_amount", "price_change", "涨跌额"),
    "volume": ("volume", "vol", "成交量"),
    "amount": ("amount", "turnover", "成交额"),
    "turnover_pct": ("turnover_pct", "turnover_rate", "换手率"),
    "market_cap": ("market_cap", "market_value", "总市值", "流通市值"),
    "pe_ttm": ("pe_ttm", "pe", "pe_ratio", "市盈率"),
    "pe_mrq": ("pe_mrq", "pe_dynamic"),
    "pb_mrq": ("pb_mrq", "pb", "pb_ratio", "市净率"),
    "ps_ttm": ("ps_ttm", "ps", "ps_ratio", "市销率"),
    "pcf_ttm": ("pcf_ttm", "pcf", "pcf_ratio", "市现率"),
    "limit_up_count": ("limit_up_count", "up_limit_count", "涨停数"),
    "limit_down_count": ("limit_down_count", "down_limit_count", "跌停数"),
    "consecutive_limit_count": ("consecutive_limit_count", "limit_days", "连板数"),
    "seal_amount": ("seal_amount", "封单金额", "封板资金"),
    "sentiment_score": ("sentiment_score", "emotion_score", "情绪分"),
    "metric_value": ("metric_value", "value", "ratio", "percent", "指标值"),
    "label": ("label", "tag", "signal", "标签"),
    "weight": ("weight", "weight_pct", "权重"),
}

STANDARD_NUMERIC_FIELDS = {
    "price", "change_pct", "change_amount", "volume", "amount", "turnover_pct",
    "market_cap", "pe_ttm", "pe_mrq", "pb_mrq", "ps_ttm", "pcf_ttm",
    "seal_amount", "sentiment_score", "weight", "metric_value",
}
STANDARD_INTEGER_FIELDS = {
    "rank", "limit_up_count", "limit_down_count", "consecutive_limit_count",
}

STANDARD_DATASET_COLUMNS: dict[str, tuple[str, ...]] = {
    "ticker_catalog": ("symbol", "name", "exchange", "category"),
    "valuation_snapshot": (
        "symbol", "name", "price", "pe_ttm", "pe_mrq", "pb_mrq", "ps_ttm",
        "pcf_ttm", "market_cap"
    ),
    "limit_up_pool": (
        "symbol", "name", "price", "change_pct", "volume", "amount", "turnover_pct",
        "consecutive_limit_count", "seal_amount"
    ),
    "limit_down_pool": (
        "symbol", "name", "price", "change_pct", "volume", "amount", "turnover_pct"
    ),
    "limit_break_pool": (
        "symbol", "name", "price", "change_pct", "volume", "amount",
        "turnover_pct", "seal_amount"
    ),
    "limit_up_ladder": (
        "symbol", "name", "price", "change_pct", "rank", "consecutive_limit_count", "amount"
    ),
    "anomaly_list": ("symbol", "name", "price", "change_pct", "volume", "amount", "label"),
    "skyrocket_list": ("symbol", "name", "price", "change_pct", "rank", "label"),
    "hot_stock_list": ("symbol", "name", "rank", "price", "change_pct", "label"),
    "hot_stock_list_history": ("symbol", "name", "rank", "price", "change_pct", "label"),
    "dragon_tiger_all": ("symbol", "name", "rank", "price", "change_pct", "amount", "label"),
    "auction_snapshot": (
        "symbol", "name", "price", "change_pct", "volume", "amount", "turnover_pct", "label"
    ),
    "auction_short_term_benchmark": (
        "name", "label", "metric_value", "sentiment_score", "change_pct",
        "limit_up_count", "limit_down_count"
    ),
    "index_catalog_cn_concept": ("index_code", "name", "exchange", "category"),
    "index_catalog_industry": ("index_code", "name", "exchange", "category"),
    "index_catalog_region": ("index_code", "name", "exchange", "category"),
    "index_catalog_tszs": ("index_code", "name", "exchange", "category"),
    "index_snapshot": ("index_code", "name", "price", "change_pct", "volume", "amount"),
    "index_constituents": ("index_code", "symbol", "name", "rank", "weight"),
}

STANDARD_TABLE = "qm_ths_standardized_snapshots"


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


def _coerce_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip().replace(",", "").replace("%", "")
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _coerce_int(value: Any) -> int | None:
    number = _coerce_number(value)
    return int(number) if number is not None else None


def standardize_snapshot_record(record: SnapshotRecord) -> dict[str, Any]:
    """Map one raw response item into the stable cross-dataset schema."""
    payload = record.payload if isinstance(record.payload, dict) else {}
    consumed: set[str] = set()
    values: dict[str, Any] = {
        "snapshot_date": record.snapshot_date,
        "dataset": record.dataset,
        "scope_key": record.scope_key,
        "symbol": record.symbol,
        "as_of_ms": record.as_of_ms,
        "row_order": None,
    }
    for field, aliases in STANDARD_FIELD_ALIASES.items():
        value = None
        for alias in aliases:
            if alias in payload and payload[alias] is not None:
                value = payload[alias]
                consumed.add(alias)
                break
        if field == "symbol" and record.symbol:
            value = record.symbol
        if field in STANDARD_NUMERIC_FIELDS:
            value = _coerce_number(value)
        elif field in STANDARD_INTEGER_FIELDS:
            value = _coerce_int(value)
        elif value is not None and not isinstance(value, (str, bool)):
            value = str(value)
        values[field] = value
    if record.dataset.startswith("index_catalog_") or record.dataset == "index_snapshot":
        if not values.get("index_code"):
            values["index_code"] = payload.get("thscode") or payload.get("indexcode")
        values["symbol"] = None
    elif record.dataset == "index_constituents":
        values["index_code"] = record.scope_key.removeprefix("index:").split(":", 1)[0]
    values["extra"] = {
        str(key): value for key, value in payload.items() if key not in consumed
    }
    return values


def standard_columns(dataset: str) -> tuple[str, ...]:
    """Return the ordered, user-facing columns for a dataset."""
    return STANDARD_DATASET_COLUMNS.get(
        dataset,
        ("symbol", "name", "index_code", "rank", "price", "change_pct", "amount", "label"),
    )


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
    CREATE_STANDARD_TABLE = f"""
    CREATE TABLE IF NOT EXISTS {STANDARD_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        dataset TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        symbol TEXT,
        name TEXT,
        index_code TEXT,
        exchange TEXT,
        category TEXT,
        rank INTEGER,
        price DOUBLE PRECISION,
        change_pct DOUBLE PRECISION,
        change_amount DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        amount DOUBLE PRECISION,
        turnover_pct DOUBLE PRECISION,
        market_cap DOUBLE PRECISION,
        pe_ttm DOUBLE PRECISION,
        pe_mrq DOUBLE PRECISION,
        pb_mrq DOUBLE PRECISION,
        ps_ttm DOUBLE PRECISION,
        pcf_ttm DOUBLE PRECISION,
        limit_up_count INTEGER,
        limit_down_count INTEGER,
        consecutive_limit_count INTEGER,
        seal_amount DOUBLE PRECISION,
        sentiment_score DOUBLE PRECISION,
        metric_value DOUBLE PRECISION,
        label TEXT,
        weight DOUBLE PRECISION,
        as_of_ms BIGINT,
        row_order INTEGER,
        extra JSONB NOT NULL DEFAULT '{{}}'::jsonb,
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
        f"""
        CREATE OR REPLACE VIEW v_ths_standardized_daily AS
        SELECT snapshot_date AS trade_date, dataset, scope_key, symbol, name,
               index_code, exchange, category, rank, price, change_pct,
               change_amount, volume, amount, turnover_pct, market_cap,
               pe_ttm, pe_mrq, pb_mrq, ps_ttm, pcf_ttm, limit_up_count,
               limit_down_count, consecutive_limit_count, seal_amount,
               sentiment_score, metric_value, label, weight, as_of_ms,
               row_order, extra, captured_at, 'ths'::TEXT AS source
        FROM {STANDARD_TABLE}
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
            connection.execute(text(self.CREATE_STANDARD_TABLE))
            connection.execute(
                text(
                    f"ALTER TABLE {STANDARD_TABLE} "
                    "ADD COLUMN IF NOT EXISTS metric_value DOUBLE PRECISION"
                )
            )
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
            connection.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS idx_{STANDARD_TABLE}_dataset_date "
                    f"ON {STANDARD_TABLE} (dataset, snapshot_date)"
                )
            )
            connection.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS idx_{STANDARD_TABLE}_symbol_date "
                    f"ON {STANDARD_TABLE} (symbol, snapshot_date)"
                )
            )
            for view_sql in self.CREATE_VIEWS:
                connection.execute(text(view_sql))
            for dataset in STANDARD_DATASET_COLUMNS:
                view_name = f"v_ths_std_{re.sub(r'[^a-z0-9_]', '_', dataset.lower())}"
                connection.execute(
                    text(
                        f"CREATE OR REPLACE VIEW {view_name} AS "
                        f"SELECT * FROM {STANDARD_TABLE} "
                        f"WHERE dataset = '{dataset}'"
                    )
                )

        self._backfill_standardized()

    def _upsert_standardized(self, records: Iterable[SnapshotRecord]) -> int:
        rows = [standardize_snapshot_record(record) for record in records]
        if not rows:
            return 0
        columns = (
            "snapshot_date", "dataset", "scope_key", "symbol", "name", "index_code",
            "exchange", "category", "rank", "price", "change_pct", "change_amount",
            "volume", "amount", "turnover_pct", "market_cap", "pe_ttm", "pe_mrq",
            "pb_mrq", "ps_ttm", "pcf_ttm", "limit_up_count", "limit_down_count",
            "consecutive_limit_count", "seal_amount", "sentiment_score", "label", "weight",
            "metric_value", "as_of_ms", "row_order", "extra",
        )
        placeholders = ", ".join(
            f"CAST(:{column} AS JSONB)" if column == "extra" else f":{column}"
            for column in columns
        )
        statement = text(
            f"""
            INSERT INTO {STANDARD_TABLE} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (snapshot_date, dataset, scope_key) DO UPDATE SET
              symbol = EXCLUDED.symbol, name = EXCLUDED.name,
              index_code = EXCLUDED.index_code, exchange = EXCLUDED.exchange,
              category = EXCLUDED.category, rank = EXCLUDED.rank,
              price = EXCLUDED.price, change_pct = EXCLUDED.change_pct,
              change_amount = EXCLUDED.change_amount, volume = EXCLUDED.volume,
              amount = EXCLUDED.amount, turnover_pct = EXCLUDED.turnover_pct,
              market_cap = EXCLUDED.market_cap, pe_ttm = EXCLUDED.pe_ttm,
              pe_mrq = EXCLUDED.pe_mrq, pb_mrq = EXCLUDED.pb_mrq,
              ps_ttm = EXCLUDED.ps_ttm, pcf_ttm = EXCLUDED.pcf_ttm,
              limit_up_count = EXCLUDED.limit_up_count,
              limit_down_count = EXCLUDED.limit_down_count,
              consecutive_limit_count = EXCLUDED.consecutive_limit_count,
              seal_amount = EXCLUDED.seal_amount,
              sentiment_score = EXCLUDED.sentiment_score,
              metric_value = EXCLUDED.metric_value,
              label = EXCLUDED.label, weight = EXCLUDED.weight,
              as_of_ms = EXCLUDED.as_of_ms, row_order = EXCLUDED.row_order,
              extra = EXCLUDED.extra, captured_at = NOW()
            """
        )
        params = [
            {**row, "extra": json.dumps(row["extra"], ensure_ascii=False, separators=(",", ":"))}
            for row in rows
        ]
        with self.engine.begin() as connection:
            connection.execute(statement, params)
        return len(rows)

    def _backfill_standardized(self) -> None:
        """Make deployments with existing raw snapshots immediately previewable."""
        with self.engine.begin() as connection:
            raw_rows = connection.execute(
                text(
                    f"""
                    SELECT r.snapshot_date, r.dataset, r.scope_key, r.symbol,
                           r.as_of_ms, r.payload
                    FROM qm_ths_daily_snapshots r
                    LEFT JOIN {STANDARD_TABLE} n
                      ON n.snapshot_date = r.snapshot_date
                     AND n.dataset = r.dataset
                     AND n.scope_key = r.scope_key
                    WHERE n.id IS NULL
                    """
                )
            ).mappings().all()
        if not raw_rows:
            return
        records = [
            SnapshotRecord(
                snapshot_date=row["snapshot_date"],
                dataset=row["dataset"],
                scope_key=row["scope_key"],
                symbol=row["symbol"],
                as_of_ms=row["as_of_ms"],
                payload=row["payload"] or {},
                request_id=None,
                row_count=0,
            )
            for row in raw_rows
        ]
        self._upsert_standardized(records)

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
        self._upsert_standardized(rows)
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
