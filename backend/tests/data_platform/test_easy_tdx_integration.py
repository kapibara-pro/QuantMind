from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from backend.services.engine.data_platform.adapters.easy_tdx_adapter import (
    _split_symbol,
    _standardize_bars,
)
from backend.services.engine.data_platform.source_catalog import (
    get_source_descriptor,
    list_source_descriptors,
)


def test_source_catalog_exposes_easy_tdx_when_dependency_is_missing():
    sources = {
        item["source_id"]: item for item in list_source_descriptors({"quantdb_local"})
    }

    assert sources["quantdb"]["registered"] is True
    assert sources["easy_tdx"]["registered"] is False
    assert sources["easy_tdx"]["transport"] == "tcp"
    assert sources["easy_tdx"]["managed_service"] is True
    assert "realtime_pull" in sources["easy_tdx"]["delivery_modes"]
    assert get_source_descriptor("easy_tdx").adapter_name == "easy_tdx"


def test_easy_tdx_symbol_conversion_uses_prefix_internally():
    assert _split_symbol("SH600036") == (1, "600036", "SH600036")
    assert _split_symbol("600036.SH") == (1, "600036", "SH600036")
    assert _split_symbol("000001") == (0, "000001", "SZ000001")
    assert _split_symbol("BJ830001") == (2, "830001", "BJ830001")


def test_standardize_bars_maps_easy_tdx_columns_to_platform_schema():
    raw = pd.DataFrame(
        [
            {
                "datetime": pd.Timestamp("2026-09-03"),
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "vol": 1000,
                "amount": 10500,
            }
        ]
    )

    result = _standardize_bars(raw, "600036.SH", "easy_tdx")

    assert result.iloc[0]["symbol"] == "SH600036"
    assert result.iloc[0]["trade_date"] == date(2026, 9, 3)
    assert result.iloc[0]["volume"] == 1000
    assert result.iloc[0]["source"] == "easy_tdx"
    assert result.iloc[0]["datetime"] == pd.Timestamp("2026-09-03")


def test_easy_tdx_stock_list_field_uses_metadata(monkeypatch):
    from backend.services.engine.data_platform.adapters.easy_tdx_adapter import (
        EasyTdxAdapter,
    )

    adapter = EasyTdxAdapter()
    expected = pd.DataFrame([{"symbol": "SH600036", "name": "招商银行"}])
    monkeypatch.setattr(adapter, "fetch_meta", lambda market: expected)

    result = adapter.fetch_field("stock_list", "SH600036")

    assert result.equals(expected)


class _FakeAdapter:
    def fetch_meta(self, market: str) -> pd.DataFrame:
        assert market == "A"
        return pd.DataFrame(
            [
                {
                    "symbol": "SH600036",
                    "code": "600036",
                    "exchange": "SSE",
                    "name": "招商银行",
                    "market": "A",
                    "is_active": True,
                    "source": "easy_tdx",
                }
            ]
        )

    def fetch_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjust: str,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "symbol": "SH600036",
                    "trade_date": date(2026, 9, 3),
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "volume": 1000,
                    "amount": 10500,
                    "adj_factor": 1.0,
                    "source": "easy_tdx",
                }
            ]
        )


def test_easy_tdx_sync_writes_only_source_specific_shadow_partitions(
    tmp_path, monkeypatch
):
    from backend.services.engine.data_platform import easy_tdx_sync

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(tmp_path / "easy_tdx"))
    monkeypatch.setattr(easy_tdx_sync, "EasyTdxAdapter", _FakeAdapter)

    result = easy_tdx_sync.sync(
        datasets=["daily_unadjusted", "stock_list"],
        days=5,
        symbols=["600036.SH"],
    )

    daily_file = (
        tmp_path
        / "easy_tdx"
        / "1_kline_data"
        / "daily_unadjusted"
        / "dt=20260903"
        / "data.parquet"
    )
    stock_file = tmp_path / "easy_tdx" / "stock_list" / "instrument_list.parquet"
    assert daily_file.is_file()
    assert stock_file.is_file()
    assert result["publish_mode"] == "shadow"
    assert result["datasets"]["daily_unadjusted"]["rows"] == 1
    assert pd.read_parquet(daily_file).iloc[0]["symbol"] == "SH600036"


def test_easy_tdx_update_check_compares_remote_trade_date_to_local_partition(
    tmp_path, monkeypatch
):
    from backend.services.engine.data_platform import easy_tdx_sync

    root = tmp_path / "easy_tdx"
    stale = root / "1_kline_data" / "daily_forward" / "dt=20260902"
    stale.mkdir(parents=True)
    pd.DataFrame([{"symbol": "SH600036"}]).to_parquet(
        stale / "data.parquet", index=False
    )
    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(root))
    monkeypatch.setattr(easy_tdx_sync, "EasyTdxAdapter", _FakeAdapter)

    result = easy_tdx_sync.check_updates(["daily_forward"])

    assert result["remote_latest_trade_date"] == "2026-09-03"
    assert result["summary"]["updates_available"] == 1
    assert result["datasets"][0]["status"] == "updates_available"


class _FakeRedis:
    def __init__(self) -> None:
        self.values = {}
        self.hashes = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, **kwargs):
        self.values[key] = str(value).encode()
        return True

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[str(field).encode()] = str(value).encode()

    def hgetall(self, key):
        return self.hashes.get(key, {})


class _FakeClient:
    instances = []

    def __init__(self, host, **kwargs):
        self._host = host
        self.closed = False
        self.instances.append(self)

    def connect(self):
        return None

    def close(self):
        self.closed = True

    @staticmethod
    def ping_all(hosts, port, timeout):
        return [(host, 0.01 + index / 100) for index, host in enumerate(hosts)]


def test_easy_tdx_manager_persists_health_and_switches_connection(monkeypatch):
    from backend.services.engine.data_platform import easy_tdx_client

    fake_redis = _FakeRedis()
    _FakeClient.instances = []
    monkeypatch.setattr(easy_tdx_client, "EASY_TDX_AVAILABLE", True)
    monkeypatch.setattr(easy_tdx_client, "MacClient", _FakeClient)
    monkeypatch.setattr(
        easy_tdx_client,
        "get_mac_hosts",
        lambda: ["10.0.0.1", "10.0.0.2"],
        raising=False,
    )
    monkeypatch.setattr(
        easy_tdx_client,
        "get_best_mac_host",
        lambda: "10.0.0.1",
        raising=False,
    )
    monkeypatch.setattr(easy_tdx_client, "get_port", lambda: 7709, raising=False)

    manager = easy_tdx_client.EasyTdxClientManager()
    monkeypatch.setattr(manager, "_redis", lambda: fake_redis)

    servers = manager.test_servers("mac", timeout=0.2)
    assert [server["status"] for server in servers] == ["online", "online"]
    assert servers[0]["latency_ms"] == 10.0

    assert manager.execute("mac", lambda client: client._host) == "10.0.0.1"
    switched = manager.switch_server("mac", "10.0.0.2")
    assert switched["host"] == "10.0.0.2"
    assert manager.execute("mac", lambda client: client._host) == "10.0.0.2"
    assert len(_FakeClient.instances) == 2
    assert _FakeClient.instances[0].closed is True


def test_easy_tdx_manager_records_failed_connection_health(monkeypatch):
    from backend.services.engine.data_platform import easy_tdx_client

    class _FailingClient(_FakeClient):
        def connect(self):
            raise TimeoutError("connect timeout")

    fake_redis = _FakeRedis()
    monkeypatch.setattr(easy_tdx_client, "EASY_TDX_AVAILABLE", True)
    monkeypatch.setattr(easy_tdx_client, "MacClient", _FailingClient)
    monkeypatch.setattr(
        easy_tdx_client,
        "get_best_mac_host",
        lambda: "10.0.0.1",
        raising=False,
    )

    manager = easy_tdx_client.EasyTdxClientManager()
    monkeypatch.setattr(manager, "_redis", lambda: fake_redis)

    with pytest.raises(TimeoutError, match="connect timeout"):
        manager.execute("mac", lambda client: client._host)

    health = manager._read_health("mac")["10.0.0.1"]
    assert health["status"] == "offline"
    assert health["consecutive_failures"] == 1
    assert health["last_error"] == "connect timeout"
