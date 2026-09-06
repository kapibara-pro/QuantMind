"""管理端可选择的数据中枢目录。

适配器注册表描述运行时读取能力；本目录描述控制面可配置的数据源。即使可选
依赖暂未安装，管理端仍能看到来源及缺失原因，而不是让选项静默消失。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SourceDescriptor:
    source_id: str
    label: str
    adapter_name: str
    category: str
    transport: str
    markets: tuple[str, ...]
    delivery_modes: tuple[str, ...]
    capabilities: tuple[str, ...]
    datasets: tuple[str, ...]
    configurable: bool = True
    managed_service: bool = False
    sync_supported: bool = True
    notes: str = ""

    def to_dict(self, *, registered: bool) -> dict:
        data = asdict(self)
        data["markets"] = list(self.markets)
        data["delivery_modes"] = list(self.delivery_modes)
        data["capabilities"] = list(self.capabilities)
        data["datasets"] = list(self.datasets)
        data["registered"] = registered
        return data


SOURCE_CATALOG: dict[str, SourceDescriptor] = {
    "quantdb": SourceDescriptor(
        source_id="quantdb",
        label="QuantDB",
        adapter_name="quantdb_local",
        category="research_data",
        transport="parquet",
        markets=("A",),
        delivery_modes=("batch",),
        capabilities=(
            "daily_kline",
            "financial_report",
            "valuation",
            "ai_factors",
        ),
        datasets=(),
        managed_service=False,
        notes="正式研究数据与因子数据中枢",
    ),
    "easy_tdx": SourceDescriptor(
        source_id="easy_tdx",
        label="easy_tdx 通达信行情",
        adapter_name="easy_tdx",
        category="market_data",
        transport="tcp",
        markets=("A",),
        delivery_modes=("batch", "realtime_pull"),
        capabilities=(
            "daily_kline",
            "minute_kline",
            "realtime_quote",
            "stock_list",
        ),
        datasets=(
            "daily_unadjusted",
            "daily_forward",
            "daily_backward",
            "min5_kline",
            "min1_kline",
            "stock_list",
        ),
        managed_service=True,
        notes="行情影子数据源；不提供财务、估值及 L1/L2 因子",
    ),
}


def get_source_descriptor(source_id: str) -> SourceDescriptor:
    try:
        return SOURCE_CATALOG[source_id]
    except KeyError as exc:
        raise ValueError(f"未知数据源: {source_id}") from exc


def list_source_descriptors(registered_adapters: set[str] | None = None) -> list[dict]:
    registered = registered_adapters or set()
    return [
        descriptor.to_dict(registered=descriptor.adapter_name in registered)
        for descriptor in SOURCE_CATALOG.values()
    ]
