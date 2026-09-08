from __future__ import annotations

import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


def _chunk(symbol: str, dates: list[str], close: float) -> pd.DataFrame:
    rows = []
    for raw_date in dates:
        rows.append(
            {
                "symbol": symbol,
                "d": pd.Timestamp(raw_date).date(),
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1000.0,
                "amount": close * 1000,
                "raw_open": close,
                "raw_high": close,
                "raw_low": close,
                "raw_close": close,
                "factor": 1.0,
            }
        )
    return pd.DataFrame(rows)


def test_qlib_bulk_build_streams_chunks_and_keeps_split_symbol(tmp_path, monkeypatch):
    from backend.services.engine.qlib_data_builder import QlibDataBuilder

    qlib_dir = tmp_path / "qlib"
    calendar_dir = qlib_dir / "calendars"
    calendar_dir.mkdir(parents=True)
    (calendar_dir / "day.txt").write_text("2026-09-04\n2026-09-07\n", encoding="utf-8")

    chunks = [
        _chunk("000001.SZ", ["2026-09-04"], 10.0),
        pd.concat(
            [
                _chunk("000001.SZ", ["2026-09-07"], 11.0),
                _chunk("600036.SH", ["2026-09-07"], 12.0),
            ],
            ignore_index=True,
        ),
        pd.DataFrame(),
    ]
    captured: dict[str, object] = {}

    class _Cursor:
        def fetch_df_chunk(self, *, vectors_per_chunk):
            captured.setdefault("vectors", []).append(vectors_per_chunk)
            return chunks.pop(0)

    class _Connection:
        def execute(self, query, params):
            captured["query"] = query
            captured["params"] = params
            return _Cursor()

        def close(self):
            captured["closed"] = True

    def _connect(*, config):
        captured["config"] = config
        return _Connection()

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=_connect))
    monkeypatch.setenv("QLIB_DUCKDB_FETCH_ROWS", "2048")
    hub = SimpleNamespace(data_dir=tmp_path / "quantdb", available=True)
    builder = QlibDataBuilder(hub, qlib_dir, market="CN")
    monkeypatch.setattr(builder, "_multiplicative_factor", lambda *_: None)
    progress: list[tuple[int, dict]] = []

    result = builder.build_features_bulk(
        symbols=["sz000001", "sh600036"],
        progress_cb=lambda percent, **details: progress.append((percent, details)),
    )

    assert result == {"updated": 2, "skipped": 0}
    assert captured["config"] == {
        "memory_limit": "3GB",
        "threads": "2",
        "temp_directory": str(qlib_dir.parent / ".duckdb_tmp"),
    }
    assert captured["params"] == ["000001.SZ", "600036.SH"]
    assert captured["closed"] is True
    assert any(details.get("phase") == "read" for _, details in progress)
    assert any(details.get("phase") == "write" for _, details in progress)

    path = qlib_dir / "features" / "sz000001" / "close.day.bin"
    values = np.fromfile(path, dtype="<f4")
    assert values.tolist() == [0.0, 10.0, 11.0]


def test_celery_visibility_timeout_and_qlib_queue_are_explicit():
    from backend.services.engine.qlib_app.celery_config import celery_app

    assert celery_app.conf.broker_transport_options["visibility_timeout"] == 43200
    assert (
        celery_app.conf.result_backend_transport_options["visibility_timeout"] == 43200
    )
    queues = {queue.name for queue in celery_app.conf.task_queues}
    assert "qlib_build" in queues
    assert celery_app.conf.task_routes["engine.tasks.update_qlib_cache"] == {
        "queue": "qlib_build"
    }


def test_qlib_bulk_build_runs_real_duckdb_join(tmp_path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    from backend.services.engine.qlib_data_builder import QlibDataBuilder

    data_dir = tmp_path / "quantdb"
    qlib_dir = tmp_path / "qlib"
    (qlib_dir / "calendars").mkdir(parents=True)
    (qlib_dir / "calendars" / "day.txt").write_text(
        "2026-09-04\n2026-09-07\n", encoding="utf-8"
    )

    rows = pd.DataFrame(
        {
            "symbol": ["600036.SH", "600036.SH"],
            "time": pd.to_datetime(["2026-09-04", "2026-09-07"]),
            "open": [10.0, 10.5],
            "high": [10.2, 10.8],
            "low": [9.8, 10.3],
            "close": [10.1, 10.7],
            "volume": [1000.0, 1200.0],
            "amount": [10100.0, 12840.0],
        }
    )
    for dataset in ("daily_backward", "daily_unadjusted"):
        partition = data_dir / "1_kline_data" / dataset / "dt=20260907"
        partition.mkdir(parents=True)
        rows.to_parquet(partition / "data.parquet", index=False)

    monkeypatch.setenv("QLIB_DUCKDB_MEMORY_LIMIT", "64MB")
    monkeypatch.setenv("QLIB_DUCKDB_THREADS", "1")
    monkeypatch.setenv("QLIB_DUCKDB_FETCH_ROWS", "2048")
    hub = SimpleNamespace(data_dir=data_dir, available=True)
    builder = QlibDataBuilder(hub, qlib_dir, market="CN")
    monkeypatch.setattr(builder, "_multiplicative_factor", lambda *_: None)

    result = builder.build_features_bulk(symbols=["sh600036"])

    assert result == {"updated": 1, "skipped": 0}
    close_path = qlib_dir / "features" / "sh600036" / "close.day.bin"
    close_values = np.fromfile(close_path, dtype="<f4")
    assert close_values.tolist() == pytest.approx([0.0, 10.1, 10.7])
