from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from backend.services.engine.data_platform.adapters.easy_tdx_adapter import (
    EasyTdxAdapter,
    _minute_request_count,
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
    assert "min5_kline" in sources["easy_tdx"]["datasets"]
    assert "min1_kline" in sources["easy_tdx"]["datasets"]
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

    def fetch_minute(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        freq: str,
    ) -> pd.DataFrame:
        minute = 1 if freq == "1min" else 5
        return pd.DataFrame(
            [
                {
                    "symbol": "SH600036",
                    "datetime": pd.Timestamp("2026-09-03 09:30")
                    + pd.Timedelta(minutes=minute),
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


def test_easy_tdx_minute_request_count_covers_more_than_800_bars():
    today = date.today()
    start = today - timedelta(days=40)

    min5_count = _minute_request_count(start, today, "5min")
    min1_count = _minute_request_count(start, today, "1min")

    assert min5_count > 800
    assert min1_count == min5_count * 5


def test_easy_tdx_minute_fetch_preserves_tdx_close_timestamp(monkeypatch):
    from backend.services.engine.data_platform.adapters import easy_tdx_adapter

    captured: dict[str, object] = {}

    class _Client:
        def get_stock_kline(self, *args, **kwargs):
            captured.update(kwargs)
            return pd.DataFrame(
                [
                    {
                        "datetime": pd.Timestamp.combine(
                            date.today(), pd.Timestamp("15:00").time()
                        ),
                        "open": 10.0,
                        "high": 10.1,
                        "low": 9.9,
                        "close": 10.0,
                        "vol": 100,
                        "amount": 1000,
                    }
                ]
            )

    class _Manager:
        def execute(self, channel, operation):
            assert channel == "mac"
            return operation(_Client())

    monkeypatch.setattr(easy_tdx_adapter, "EASY_TDX_AVAILABLE", True)
    adapter = EasyTdxAdapter()
    adapter._manager = _Manager()

    frame = adapter.fetch_minute(
        "SH600036", date.today(), date.today(), freq="1min"
    )

    assert captured["bar_time"] == "start"
    assert frame.iloc[-1]["datetime"].strftime("%H:%M") == "15:00"


def test_easy_tdx_sync_writes_min1_and_min5_symbol_files(tmp_path, monkeypatch):
    from backend.services.engine.data_platform import easy_tdx_sync

    root = tmp_path / "easy_tdx"
    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(root))
    monkeypatch.setattr(easy_tdx_sync, "EasyTdxAdapter", _FakeAdapter)

    result = easy_tdx_sync.sync(
        datasets=["min1_kline", "min5_kline"],
        days=5,
        symbols=["600036.SH"],
    )

    for dataset in ("min1_kline", "min5_kline"):
        path = root / "1_kline_data" / dataset / "SH600036.parquet"
        assert path.is_file()
        frame = pd.read_parquet(path)
        assert frame.iloc[0]["symbol"] == "SH600036"
        assert frame.iloc[0]["source"] == "easy_tdx"
        assert result["datasets"][dataset]["files"] == 1


def test_easy_tdx_minute_sync_resumes_from_local_cursor_and_deduplicates(
    tmp_path, monkeypatch
):
    from backend.services.engine.data_platform import easy_tdx_sync

    class _IncrementalAdapter(_FakeAdapter):
        calls: list[date] = []

        def fetch_minute(self, symbol, start, end, *, freq):
            self.calls.append(start)
            rows = [
                {
                    "symbol": "SH600036",
                    "datetime": pd.Timestamp("2026-09-03 15:00"),
                    "trade_date": date(2026, 9, 3),
                    "close": 10.5,
                    "source": "easy_tdx",
                }
            ]
            if len(self.calls) > 1:
                rows.append(
                    {
                        "symbol": "SH600036",
                        "datetime": pd.Timestamp("2026-09-04 15:00"),
                        "trade_date": date(2026, 9, 4),
                        "close": 10.8,
                        "source": "easy_tdx",
                    }
                )
            return pd.DataFrame(rows)

    root = tmp_path / "easy_tdx"
    _IncrementalAdapter.calls = []
    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(root))
    monkeypatch.setattr(easy_tdx_sync, "EasyTdxAdapter", _IncrementalAdapter)

    for _ in range(2):
        easy_tdx_sync.sync(
            datasets=["min5_kline"], days=5, symbols=["SH600036"]
        )

    path = root / "1_kline_data" / "min5_kline" / "SH600036.parquet"
    frame = pd.read_parquet(path)
    assert frame["datetime"].nunique() == 2
    assert _IncrementalAdapter.calls[1] == date(2026, 9, 2)


def test_easy_tdx_minute_update_check_uses_full_sync_watermark(
    tmp_path, monkeypatch
):
    from backend.services.engine.data_platform import easy_tdx_sync

    root = tmp_path / "easy_tdx"
    path = root / "1_kline_data" / "min5_kline" / "SH600036.parquet"
    path.parent.mkdir(parents=True)
    _FakeAdapter().fetch_minute(
        "SH600036", date(2026, 9, 1), date(2026, 9, 3), freq="5min"
    ).to_parquet(path, index=False)
    easy_tdx_sync._write_minute_state(
        root,
        "min5_kline",
        {
            "scope": "full",
            "checked_through": "2026-09-03T09:35:00",
            "failed_symbols": 0,
            "rows_received": 1,
        },
    )
    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(root))
    monkeypatch.setattr(easy_tdx_sync, "EasyTdxAdapter", _FakeAdapter)

    result = easy_tdx_sync.check_updates(["min5_kline"])

    assert result["summary"]["up_to_date"] == 1
    assert result["datasets"][0]["status"] == "up_to_date"


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
