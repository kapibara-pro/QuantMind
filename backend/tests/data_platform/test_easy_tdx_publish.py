from __future__ import annotations

import pandas as pd
import pytest

from backend.services.engine.data_platform import easy_tdx_publish


def _shadow_frame(
    symbol: str, close: float = 10.0, partition_date: str = "20260907"
) -> pd.DataFrame:
    timestamp = pd.Timestamp(partition_date).replace(hour=15)
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "datetime": timestamp,
                "trade_date": timestamp.date(),
                "open": close - 0.2,
                "high": close + 0.3,
                "low": close - 0.4,
                "close": close,
                "volume": 1000,
                "amount": close * 1000,
                "adj_factor": 1.0,
                "source": "easy_tdx",
            }
        ]
    )


def _official_frame(
    symbol: str, close: float = 10.0, partition_date: str = "20260907"
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "time": pd.Timestamp(partition_date).replace(hour=15),
                "open": close - 0.2,
                "high": close + 0.3,
                "low": close - 0.4,
                "close": close,
                "volume": 1000,
                "amount": close * 1000,
            }
        ]
    )


def _write_partition(
    root, dataset: str, frame: pd.DataFrame, partition_date: str = "20260907"
) -> None:
    path = (
        root
        / "1_kline_data"
        / dataset
        / f"dt={partition_date}"
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_publish_converts_schema_and_source_wins_without_dropping_fallback(
    tmp_path, monkeypatch
):
    source = tmp_path / "easy_tdx"
    target = tmp_path / "quantdb"
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(source, dataset, _shadow_frame("SH600036", close=12.0))
        existing = pd.concat(
            [
                _official_frame("600036.SH", close=10.0),
                _official_frame("000001.SZ", close=5.0),
            ],
            ignore_index=True,
        )
        _write_partition(target, dataset, existing)

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(target))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")

    result = easy_tdx_publish.publish(with_pg=False, with_qlib=False)

    published = pd.read_parquet(
        target
        / "1_kline_data"
        / "daily_forward"
        / "dt=20260907"
        / "data.parquet"
    )
    assert published.columns.tolist() == list(easy_tdx_publish._KLINE_COLUMNS)
    assert set(published["symbol"]) == {"600036.SH", "000001.SZ"}
    assert published.loc[published["symbol"] == "600036.SH", "close"].item() == 12.0
    assert result["published_date_range"]["end"] == "2026-09-07"
    assert result["quality"]["status"] == "passed"
    assert easy_tdx_publish.publication_status()["publish_available"] is False


def test_publish_adds_index_and_minute_bars_to_canonical_data(tmp_path, monkeypatch):
    source = tmp_path / "easy_tdx"
    target = tmp_path / "quantdb"
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(source, dataset, _shadow_frame("SH600036"))
    _write_partition(source, "index_daily", _shadow_frame("SH000001"))
    for dataset in easy_tdx_publish.PUBLISH_MINUTE_DATASETS:
        minute_dir = source / "1_kline_data" / dataset
        minute_dir.mkdir(parents=True, exist_ok=True)
        _shadow_frame("SH600036").to_parquet(
            minute_dir / "SH600036.parquet", index=False
        )

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(target))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")

    result = easy_tdx_publish.publish(with_pg=False, with_qlib=False)

    index_file = target / "1_kline_data/index_daily/dt=20260907/data.parquet"
    assert index_file.is_file()
    assert pd.read_parquet(index_file).iloc[0]["symbol"] == "000001.SH"
    for dataset in easy_tdx_publish.PUBLISH_MINUTE_DATASETS:
        minute_file = target / f"1_kline_data/{dataset}/600036.SH.parquet"
        assert minute_file.is_file()
        assert pd.read_parquet(minute_file).iloc[0]["symbol"] == "600036.SH"
    assert result["published_index_date_range"]["end"] == "2026-09-07"
    assert result["published_minute_files"] == 2



def test_quality_gate_rejects_incomplete_daily_bundle(tmp_path, monkeypatch):
    source = tmp_path / "easy_tdx"
    for dataset in ("daily_unadjusted", "daily_forward"):
        _write_partition(source, dataset, _shadow_frame("SH600036"))
    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")

    with pytest.raises(ValueError, match="daily_backward"):
        easy_tdx_publish.validate_release()


def test_quality_gate_rejects_sample_sync_below_symbol_threshold(
    tmp_path, monkeypatch
):
    source = tmp_path / "easy_tdx"
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(source, dataset, _shadow_frame("SH600036"))
    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "3000")

    with pytest.raises(ValueError, match="可能是抽样同步"):
        easy_tdx_publish.validate_release()


def test_publish_ignores_old_sample_partitions_before_official_latest(
    tmp_path, monkeypatch
):
    source = tmp_path / "easy_tdx"
    target = tmp_path / "quantdb"
    latest = pd.concat(
        [
            _shadow_frame("SH600036", close=12.0),
            _shadow_frame("SZ000001", close=8.0),
        ],
        ignore_index=True,
    )
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(
            source,
            dataset,
            _shadow_frame("SH600036", partition_date="20260824"),
            "20260824",
        )
        _write_partition(source, dataset, latest)
        _write_partition(
            target,
            dataset,
            _official_frame(
                "600036.SH", close=10.0, partition_date="20260904"
            ),
            "20260904",
        )

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(target))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "2")

    result = easy_tdx_publish.publish(with_pg=False, with_qlib=False)

    assert result["published_date_range"] == {
        "start": "2026-09-07",
        "end": "2026-09-07",
    }
    assert result["quality"]["after_date"] == "2026-09-04"
    assert result["quality"]["ignored_existing_partitions"] == 3
    assert not (
        target
        / "1_kline_data"
        / "daily_unadjusted"
        / "dt=20260824"
        / "data.parquet"
    ).exists()
    assert (
        target
        / "1_kline_data"
        / "daily_unadjusted"
        / "dt=20260907"
        / "data.parquet"
    ).is_file()


def test_quality_gate_rejects_mismatched_symbol_sets(tmp_path, monkeypatch):
    source = tmp_path / "easy_tdx"
    _write_partition(
        source,
        "daily_unadjusted",
        pd.concat(
            [_shadow_frame("SH600036"), _shadow_frame("SZ000001")],
            ignore_index=True,
        ),
    )
    _write_partition(
        source,
        "daily_forward",
        pd.concat(
            [_shadow_frame("SH600036"), _shadow_frame("SZ000001")],
            ignore_index=True,
        ),
    )
    _write_partition(
        source,
        "daily_backward",
        pd.concat(
            [_shadow_frame("SH600036"), _shadow_frame("BJ920000")],
            ignore_index=True,
        ),
    )
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_COVERAGE", "0.98")

    with pytest.raises(ValueError, match="共同标的覆盖率"):
        easy_tdx_publish.validate_release(source)


def test_publish_lock_rejects_concurrent_release(tmp_path):
    target = tmp_path / "quantdb"

    with easy_tdx_publish._publication_lock(target):
        with pytest.raises(RuntimeError, match="已有 easy_tdx 发布任务"):
            with easy_tdx_publish._publication_lock(target):
                pass


def test_publish_rolls_back_partitions_when_cancelled_before_commit(
    tmp_path, monkeypatch
):
    source = tmp_path / "easy_tdx"
    target = tmp_path / "quantdb"
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(source, dataset, _shadow_frame("SH600036", close=12.0))
        _write_partition(target, dataset, _official_frame("600036.SH", close=10.0))

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(target))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")
    cancelled = False

    def progress(event: str, **_kwargs):
        nonlocal cancelled
        if event == "publish_partition":
            cancelled = True

    result = easy_tdx_publish.publish(
        with_pg=False,
        with_qlib=False,
        progress_cb=progress,
        should_cancel=lambda: cancelled,
    )

    assert result["cancelled"] is True
    published = pd.read_parquet(
        target
        / "1_kline_data"
        / "daily_unadjusted"
        / "dt=20260907"
        / "data.parquet"
    )
    assert published["close"].item() == 10.0


def test_pg_partial_is_reported_without_rolling_back_training_release(
    tmp_path, monkeypatch
):
    from backend.scripts import quantdb_daily_sync

    source = tmp_path / "easy_tdx"
    target = tmp_path / "quantdb"
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(source, dataset, _shadow_frame("SH600036", close=12.0))
        _write_partition(target, dataset, _official_frame("600036.SH", close=10.0))

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(target))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")
    monkeypatch.setattr(
        quantdb_daily_sync,
        "fill_pg_from_parquet",
        lambda **_kwargs: {"status": "partial", "failed_batches": ["2026-09-07"]},
    )

    result = easy_tdx_publish.publish(with_pg=True, with_qlib=False)

    assert result["status"] == "partial"
    assert result["committed"] is True
    assert result["warnings"]
    published = pd.read_parquet(
        target
        / "1_kline_data"
        / "daily_forward"
        / "dt=20260907"
        / "data.parquet"
    )
    assert published["close"].item() == 12.0


def test_qlib_failure_rolls_back_official_partitions(tmp_path, monkeypatch):
    source = tmp_path / "easy_tdx"
    target = tmp_path / "quantdb"
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(source, dataset, _shadow_frame("SH600036", close=12.0))
        _write_partition(target, dataset, _official_frame("600036.SH", close=10.0))

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(target))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")
    monkeypatch.setattr(
        easy_tdx_publish,
        "_build_qlib",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("build failed")),
    )

    with pytest.raises(RuntimeError, match="build failed"):
        easy_tdx_publish.publish(with_pg=False, with_qlib=True)

    published = pd.read_parquet(
        target
        / "1_kline_data"
        / "daily_backward"
        / "dt=20260907"
        / "data.parquet"
    )
    assert published["close"].item() == 10.0


def test_publish_rebuilds_qlib_calendar_through_latest_easy_tdx_date(
    tmp_path, monkeypatch
):
    source = tmp_path / "easy_tdx"
    target = tmp_path / "quantdb"
    qlib = tmp_path / "qlib" / "cn_data"
    for dataset in easy_tdx_publish.PUBLISH_DATASETS:
        _write_partition(source, dataset, _shadow_frame("SH600036", close=12.0))

    monkeypatch.setenv("QM_EASY_TDX_DATA_DIR", str(source))
    monkeypatch.setenv("QM_QUANTDB_DATA_DIR", str(target))
    monkeypatch.setenv("QM_EASY_TDX_PUBLISH_MIN_SYMBOLS", "1")
    monkeypatch.setenv("QLIB_PROVIDER_URI", str(qlib))

    result = easy_tdx_publish.publish(with_pg=False, with_qlib=True)

    assert result["status"] == "ok"
    assert result["qlib"]["latest_date"] == "2026-09-07"
    assert (qlib / "calendars" / "day.txt").read_text().strip() == "2026-09-07"
    assert (qlib / "features" / "sh600036" / "close.day.bin").is_file()
