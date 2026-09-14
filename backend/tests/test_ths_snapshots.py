from __future__ import annotations

from datetime import date

import pytest

from backend.services.engine.data_platform.ths_snapshots import (
    MARKET_SCOPE,
    SnapshotRecord,
    ThsFinanceClient,
    ThsSnapshotError,
    ThsDailySnapshotCollector,
    _records,
    standard_columns,
    standardize_snapshot_record,
    standardize_snapshot_records,
)


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def get(self, path: str, params: dict | None = None) -> dict:
        self.calls.append((path, params or {}))
        if path.endswith("/tickers/list"):
            return {"code": 0, "request_id": "meta", "data": {"timestamp": 1, "item": [{"thscode": "600519.SH", "ticker": "600519", "name": "贵州茅台"}]}}
        if path.endswith("/catalog/ths-index-list"):
            return {"code": 0, "request_id": "catalog", "data": {"timestamp": 1, "item": [{"thscode": "886042.TI", "name": "示例板块"}]}}
        if path.endswith("/constituents/ths-stock-list"):
            return {"code": 0, "request_id": "constituents", "data": {"timestamp": 1, "item": [{"thscode": "600519.SH", "ticker": "600519", "name": "贵州茅台"}]}}
        if path.endswith("/valuations/snapshot"):
            return {"code": 0, "request_id": "valuation", "data": {"timestamp": 1, "item": [{"thscode": "600519.SH", "ticker": "600519", "pe_ttm": 20.0}]}}
        return {"code": 0, "request_id": path, "data": {"timestamp": 1, "item": []}}


class FakeStore:
    def __init__(self) -> None:
        self.records: list[SnapshotRecord] = []
        self.schema_calls = 0

    def ensure_schema(self) -> None:
        self.schema_calls += 1

    def upsert(self, records) -> int:
        rows = list(records)
        self.records.extend(rows)
        return len(rows)


def test_records_normalize_symbol_and_market_scope() -> None:
    records = _records(
        snapshot_date=date(2026, 9, 7),
        dataset="valuation_snapshot",
        body={"request_id": "req-1", "data": {"timestamp": 123, "item": [{"thscode": "600519.SH", "pe_ttm": 20}]}},
    )
    assert records[0].symbol == "SH600519"
    assert records[0].scope_key == "SH600519"
    assert records[0].as_of_ms == 123

    market = _records(
        snapshot_date=date(2026, 9, 7),
        dataset="index_catalog_industry",
        body={"request_id": "req-2", "data": {"timestamp": 456, "item": []}},
    )
    assert market[0].scope_key == MARKET_SCOPE
    assert market[0].symbol is None

    index_rows = _records(
        snapshot_date=date(2026, 9, 7),
        dataset="index_catalog_industry",
        body={
            "request_id": "req-3",
            "data": {
                "item": [
                    {"thscode": "886042.TI", "name": "示例板块A"},
                    {"thscode": "886043.TI", "name": "示例板块B"},
                ]
            },
        },
    )
    assert len(index_rows) == 2
    assert index_rows[0].scope_key != index_rows[1].scope_key

    scoped_rows = _records(
        snapshot_date=date(2026, 9, 7),
        dataset="index_constituents",
        scope_prefix="index:000300.SH",
        body={
            "data": {"item": [{"thscode": "600519.SH", "name": "贵州茅台"}]}
        },
    )
    assert scoped_rows[0].scope_key == "index:000300.SH:SH600519"


def test_client_requires_key_and_rejects_non_api_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    with pytest.raises(ThsSnapshotError, match="未配置"):
        ThsFinanceClient()

    client = ThsFinanceClient(api_key="test-key")
    with pytest.raises(ThsSnapshotError, match="非法同花顺路径"):
        client.get("/docs")


def test_standardize_snapshot_record_keeps_unknown_fields_in_extra() -> None:
    record = _records(
        snapshot_date=date(2026, 9, 7),
        dataset="valuation_snapshot",
        body={
            "data": {
                "item": [{
                    "thscode": "600519.SH",
                    "name": "贵州茅台",
                    "pe_ttm": "20.5",
                    "pb": 12.1,
                    "vendor_only": "kept",
                }]
            }
        },
    )[0]
    normalized = standardize_snapshot_record(record)
    assert normalized["symbol"] == "SH600519"
    assert normalized["pe_ttm"] == 20.5
    assert normalized["pb_mrq"] == 12.1
    assert normalized["extra"] == {"vendor_only": "kept"}


def test_standardize_index_catalog_uses_index_code() -> None:
    record = _records(
        snapshot_date=date(2026, 9, 7),
        dataset="index_catalog_industry",
        body={"data": {"item": [{"thscode": "886042.TI", "name": "示例板块"}]}},
    )[0]
    normalized = standardize_snapshot_record(record)
    assert normalized["symbol"] is None
    assert normalized["index_code"] == "886042.TI"


def test_standardize_hot_stock_uses_ranking_metrics() -> None:
    record = _records(
        snapshot_date=date(2026, 9, 11),
        dataset="hot_stock_list",
        body={
            "data": {
                "item": [{
                    "thscode": "600519.SH",
                    "name": "贵州茅台",
                    "rank": 3,
                    "heat": 9821.5,
                    "rank_change": -2,
                    "rank_trend": "down",
                }]
            }
        },
    )[0]

    normalized = standardize_snapshot_record(record)
    assert standard_columns("hot_stock_list") == (
        "symbol", "name", "rank", "heat", "rank_change", "rank_trend"
    )
    assert normalized["heat"] == 9821.5
    assert normalized["rank_change"] == -2
    assert normalized["rank_trend"] == "down"
    assert normalized["extra"] == {}


def test_standardize_auction_fields_and_tags() -> None:
    auction = _records(
        snapshot_date=date(2026, 9, 11),
        dataset="auction_snapshot",
        body={
            "data": {
                "item": [{
                    "thscode": "002912.SZ",
                    "name": "新雷能",
                    "auction_price": 20.12,
                    "auction_pct": 3.5,
                    "auction_volume": 1200,
                    "auction_amount": 2414400,
                    "auction_unmatched": 200,
                    "auction_turnover_pct": 0.8,
                    "auction_volume_ratio": 2.1,
                    "auction_yesterday_ratio_pct": 130,
                    "pre_close_price": 19.44,
                    "open_price": 20.12,
                    "last_price": 20.3,
                    "float_market_cap": 1000000000,
                }]
            }
        },
    )[0]
    normalized = standardize_snapshot_record(auction)
    assert normalized["symbol"] == "SZ002912"
    assert normalized["auction_price"] == 20.12
    assert normalized["auction_volume_ratio"] == 2.1
    assert normalized["pre_close_price"] == 19.44

    benchmark = _records(
        snapshot_date=date(2026, 9, 11),
        dataset="auction_short_term_benchmark",
        body={
            "data": {
                "item": [{
                    "thscode": "002912.SZ",
                    "name": "新雷能",
                    "auction_pct": 3.5,
                    "tags": ["强势", "高开"],
                }]
            }
        },
    )[0]
    benchmark_row = standardize_snapshot_record(benchmark)
    assert benchmark_row["tags"] == ["强势", "高开"]
    assert benchmark_row["extra"] == {}


def test_standardize_limit_up_ladder_expands_each_stock() -> None:
    record = SnapshotRecord(
        snapshot_date=date(2026, 9, 12),
        dataset="limit_up_ladder",
        scope_key=MARKET_SCOPE,
        symbol=None,
        as_of_ms=123,
        payload={
            "date": "2026-09-11",
            "boards": {
                "two_board": [
                    {
                        "name": "新雷能",
                        "ticker": "002912",
                        "thscode": "002912.SZ",
                        "board_num": 2,
                        "sign_level": 0,
                        "seal_nextday": True,
                    }
                ],
                "three_board": [
                    {
                        "name": "示例股票",
                        "ticker": "600001",
                        "thscode": "600001.SH",
                        "board_num": 3,
                        "sign_level": 1,
                        "seal_nextday": False,
                    }
                ],
            },
        },
        request_id="ladder",
        row_count=30,
    )

    rows = standardize_snapshot_records(record)
    assert len(rows) == 2
    assert rows[0]["event_date"] == date(2026, 9, 11)
    assert rows[0]["board_name"] == "two_board"
    assert rows[0]["board_num"] == 2
    assert rows[0]["symbol"] == "SZ002912"
    assert rows[0]["sign_level"] == 0
    assert rows[0]["seal_nextday"] is True
    assert rows[0]["scope_key"] == "ladder:2026-09-11:two_board:SZ002912"
    assert rows[1]["row_order"] == 1


def test_standardize_wrapped_items_and_skip_empty_wrappers() -> None:
    wrapped = SnapshotRecord(
        snapshot_date=date(2026, 9, 12),
        dataset="limit_down_pool",
        scope_key=MARKET_SCOPE,
        symbol=None,
        as_of_ms=None,
        payload={
            "timestamp": 123,
            "item": [{
                "thscode": "600001.SH",
                "name": "示例股票",
                "last_price": 9.9,
                "price_change_ratio_pct": -10,
                "turnover_ratio_pct": 2.5,
            }],
        },
        request_id=None,
        row_count=1,
    )
    rows = standardize_snapshot_records(wrapped)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "SH600001"
    assert rows[0]["price"] == 9.9
    assert rows[0]["change_pct"] == -10
    assert rows[0]["turnover_pct"] == 2.5
    assert rows[0]["as_of_ms"] == 123

    empty = SnapshotRecord(
        snapshot_date=wrapped.snapshot_date,
        dataset="limit_down_pool",
        scope_key=MARKET_SCOPE,
        symbol=None,
        as_of_ms=None,
        payload={"item": []},
        request_id=None,
        row_count=0,
    )
    assert standardize_snapshot_records(empty) == []


def test_daily_collector_is_idempotent_at_store_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HITHINK_FINANCE_SYMBOLS", "SH600519")
    monkeypatch.setenv("HITHINK_FINANCE_INDEX_CODES", "000300.SH")
    client = FakeClient()
    store = FakeStore()
    result = ThsDailySnapshotCollector(client, store).collect(date(2026, 9, 7))

    assert result["status"] == "success"
    assert store.schema_calls == 1
    assert result["datasets"]["valuation_snapshot"] == 1
    assert "auction_final" not in result["datasets"]
    assert any(row.symbol == "SH600519" for row in store.records)
    assert any(path.endswith("/valuations/snapshot") for path, _ in client.calls)
