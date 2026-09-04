"""市场定时同步调度器测试。

覆盖 _normalize 按市场合并默认配置（MARKET_DEFAULT_SCHEDULES）与
显式保存配置的覆盖行为，Redis 用桩对象替代。
"""

from __future__ import annotations

import pytest

from backend.services.engine.tasks.market_sync_scheduler import (
    DEFAULT_SCHEDULE,
    MARKET_DEFAULT_SCHEDULES,
    get_schedule,
    save_schedule,
)


class _StubRedis:
    """最小 Redis 桩：仅 get/set/exists，单测不依赖真实 Redis。"""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self._data[key] = value

    def exists(self, key: str) -> bool:
        return key in self._data


@pytest.fixture()
def stub_redis(monkeypatch: pytest.MonkeyPatch) -> _StubRedis:
    stub = _StubRedis()
    monkeypatch.setattr(
        "backend.services.engine.tasks.market_sync_scheduler._redis", lambda: stub
    )
    return stub


def test_hk_schedule_is_enabled_by_default_without_redis_config(stub_redis):
    # Act：Redis 里没有任何 HK 配置时读取
    cfg = get_schedule("HK")

    # Assert：港股开箱即定时同步（排在 A 股 23:30 之后），其余字段沿用全局默认
    assert cfg["enabled"] is True
    assert cfg["time"] == "23:50"
    assert cfg["days"] == DEFAULT_SCHEDULE["days"]
    assert cfg["datasets"] == []


def test_markets_in_default_table_are_enabled_with_expected_times(stub_redis):
    # Arrange / Act：Redis 里没有任何配置时逐一读取
    got = {m: get_schedule(m) for m in MARKET_DEFAULT_SCHEDULES}

    # Assert：海外市场全部开箱即定时同步，且互不错峰
    assert got["HK"]["enabled"] is True and got["HK"]["time"] == "23:50"
    assert got["US"]["enabled"] is True and got["US"]["time"] == "05:30"
    assert got["BC"]["enabled"] is True and got["BC"]["time"] == "04:15"
    assert got["FUTURES"]["enabled"] is True and got["FUTURES"]["time"] == "18:00"
    times = [cfg["time"] for cfg in got.values()]
    assert len(times) == len(set(times)), "各市场默认触发时间必须错开"


def test_ashare_has_no_market_default_and_stays_disabled_without_config(stub_redis):
    # Act：A 股不在默认表内（走独立的 daily-data-sync beat 任务）
    cfg = get_schedule("A")

    # Assert：保持关闭与全局默认时间
    assert cfg["enabled"] is False
    assert cfg["time"] == DEFAULT_SCHEDULE["time"]


def test_explicit_saved_config_overrides_market_default(stub_redis):
    # Arrange：用户在前端显式关闭港股定时
    save_schedule("HK", {"enabled": False})

    # Act
    cfg = get_schedule("HK")

    # Assert：显式关闭优先于市场默认开启；未覆盖字段沿用港股市场默认
    assert cfg["enabled"] is False
    assert cfg["time"] == "23:50"


def test_save_and_get_roundtrip_keeps_fields_not_set_by_caller(stub_redis):
    # Arrange：只传 enabled/time 的部分配置
    saved = save_schedule("HK", {"enabled": True, "time": "22:30"})

    # Act
    loaded = get_schedule("HK")

    # Assert：保存与读回一致，调用方未传字段沿用默认值
    assert saved == loaded
    assert loaded["time"] == "22:30"
    assert loaded["days"] == DEFAULT_SCHEDULE["days"]


def test_invalid_time_in_stored_config_falls_back_to_global_default(stub_redis):
    # Arrange：绕过 API 层校验，直接把坏时间写进 Redis 配置
    save_schedule("HK", {"time": "25:00"})

    # Act
    cfg = get_schedule("HK")

    # Assert：非法 HH:MM 回退到全局默认时间而不是抛错
    assert cfg["time"] == "03:00"


def test_normalize_of_missing_config_for_unknown_market_uses_global_defaults():
    from backend.services.engine.tasks.market_sync_scheduler import _normalize

    # Act / Assert：未知 market 传入时仅应用全局默认，不抛错
    assert _normalize(None, "XX") == dict(DEFAULT_SCHEDULE)


def test_ashare_schedule_persists_easy_tdx_source(stub_redis):
    saved = save_schedule(
        "A",
        {
            "enabled": True,
            "source_id": "easy_tdx",
            "publish_mode": "shadow",
            "datasets": ["daily_unadjusted", "daily_forward"],
        },
    )

    assert saved["source_id"] == "easy_tdx"
    assert get_schedule("A")["datasets"] == [
        "daily_unadjusted",
        "daily_forward",
    ]


def test_run_ashare_easy_tdx_schedule_uses_shadow_sync(monkeypatch):
    from backend.services.engine.data_platform import easy_tdx_sync
    from backend.services.engine.tasks.market_sync_scheduler import run_market_sync

    calls = []

    def _fake_sync(**kwargs):
        calls.append(kwargs)
        return {"source_id": "easy_tdx", "rows": 10}

    monkeypatch.setattr(easy_tdx_sync, "sync", _fake_sync)

    result = run_market_sync(
        "A",
        {
            "source_id": "easy_tdx",
            "publish_mode": "shadow",
            "days": 3,
            "datasets": ["daily_unadjusted"],
            "with_qlib": False,
        },
    )

    assert result["source_id"] == "easy_tdx"
    assert calls == [
        {
            "datasets": ["daily_unadjusted"],
            "days": 3,
            "publish_mode": "shadow",
        }
    ]
