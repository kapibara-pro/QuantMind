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
