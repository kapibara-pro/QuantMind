"""easy_tdx A 股行情适配器。"""

from __future__ import annotations

import logging
import math
from datetime import date
from typing import Any

import pandas as pd

from backend.services.engine.data_platform.base import (
    DataUnavailable,
    InvalidFieldRequest,
    OfflineDataSourceAdapter,
)
from backend.services.engine.data_platform.easy_tdx_client import (
    EASY_TDX_AVAILABLE,
    get_easy_tdx_manager,
)
from backend.shared.stock_utils import StockCodeUtil

logger = logging.getLogger(__name__)


def _split_symbol(symbol: str) -> tuple[int, str, str]:
    prefix = StockCodeUtil.to_prefix(symbol)
    if len(prefix) != 8 or prefix[:2] not in {"SH", "SZ", "BJ"}:
        raise ValueError(f"无效 A 股代码: {symbol}")
    return {"SZ": 0, "SH": 1, "BJ": 2}[prefix[:2]], prefix[2:], prefix


def _standardize_bars(raw: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    if "datetime" not in df.columns:
        raise DataUnavailable("easy_tdx K 线响应缺少 datetime 列")
    df["trade_date"] = pd.to_datetime(df["datetime"]).dt.date
    if "vol" in df.columns:
        df.rename(columns={"vol": "volume"}, inplace=True)
    df["symbol"] = StockCodeUtil.to_prefix(symbol)
    df["adj_factor"] = 1.0
    df["source"] = source
    columns = [
        "symbol",
        "datetime",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "adj_factor",
        "source",
    ]
    result = df[[column for column in columns if column in df.columns]]
    return result.sort_values("datetime" if "datetime" in result.columns else "trade_date")


class EasyTdxAdapter(OfflineDataSourceAdapter):
    name = "easy_tdx"
    markets = ["A"]
    fields = {"daily_kline", "minute_kline", "realtime_quote", "stock_list"}
    category = "market_data"
    transport = "tcp"
    delivery_modes = {"batch", "realtime_pull"}
    configurable = True
    managed_service = True

    def __init__(self) -> None:
        self._manager = get_easy_tdx_manager()

    def fetch_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        self._ensure_available()
        market, code, prefix = _split_symbol(symbol)
        try:
            from easy_tdx import Adjust, Period

            adjust_enum = {
                "none": Adjust.NONE,
                "qfq": Adjust.QFQ,
                "hfq": Adjust.HFQ,
            }.get(adjust.lower())
            if adjust_enum is None:
                raise ValueError(f"不支持的复权类型: {adjust}")
            calendar_days = max((date.today() - start).days, (end - start).days)
            count = min(max(math.ceil(calendar_days * 0.75) + 40, 80), 10000)
            raw = self._manager.execute(
                "mac",
                lambda client: client.get_stock_kline(
                    market,
                    code,
                    period=Period.DAILY,
                    start=0,
                    count=count,
                    adjust=adjust_enum,
                ),
            )
        except Exception as exc:
            raise DataUnavailable(f"easy_tdx 日线拉取失败: {prefix}: {exc}") from exc
        df = _standardize_bars(raw, prefix, self.name)
        if df.empty:
            raise DataUnavailable(f"easy_tdx 无日线数据: {prefix}")
        mask = (pd.to_datetime(df["trade_date"]).dt.date >= start) & (
            pd.to_datetime(df["trade_date"]).dt.date <= end
        )
        df = df.loc[mask].reset_index(drop=True)
        if df.empty:
            raise DataUnavailable(f"easy_tdx 指定日期无日线数据: {prefix}")
        return df

    def fetch_minute(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        freq: str = "1min",
    ) -> pd.DataFrame:
        self._ensure_available()
        market, code, prefix = _split_symbol(symbol)
        try:
            from easy_tdx import Adjust, Period

            periods = {
                "1min": Period.MIN_1,
                "5min": Period.MIN_5,
                "15min": Period.MIN_15,
                "30min": Period.MIN_30,
                "60min": Period.MIN_60,
            }
            if freq not in periods:
                raise InvalidFieldRequest(f"easy_tdx 不支持分钟周期: {freq}")
            raw = self._manager.execute(
                "mac",
                lambda client: client.get_stock_kline(
                    market,
                    code,
                    period=periods[freq],
                    count=800,
                    adjust=Adjust.NONE,
                    bar_time="end",
                ),
            )
        except InvalidFieldRequest:
            raise
        except Exception as exc:
            raise DataUnavailable(f"easy_tdx 分钟线拉取失败: {prefix}: {exc}") from exc
        df = _standardize_bars(raw, prefix, self.name)
        if df.empty:
            raise DataUnavailable(f"easy_tdx 无分钟线数据: {prefix}")
        mask = (pd.to_datetime(df["trade_date"]).dt.date >= start) & (
            pd.to_datetime(df["trade_date"]).dt.date <= end
        )
        df = df.loc[mask].reset_index(drop=True)
        if df.empty:
            raise DataUnavailable(f"easy_tdx 指定日期无分钟线数据: {prefix}")
        return df

    def fetch_realtime(self, symbol: str) -> dict[str, Any] | None:
        self._ensure_available()
        market, code, prefix = _split_symbol(symbol)
        try:
            raw = self._manager.execute(
                "mac", lambda client: client.get_stock_quotes([(market, code)])
            )
        except Exception as exc:
            raise DataUnavailable(
                f"easy_tdx 实时报价拉取失败: {prefix}: {exc}"
            ) from exc
        if raw is None or raw.empty:
            raise DataUnavailable(f"easy_tdx 无实时报价: {prefix}")
        record = raw.iloc[0].to_dict()
        record.update({"symbol": prefix, "source": self.name})
        return record

    def fetch_meta(self, market: str) -> pd.DataFrame:
        self._ensure_available()
        if market.upper() != "A":
            raise InvalidFieldRequest(f"easy_tdx 不支持 market={market}")
        try:
            raw = self._manager.execute(
                "standard", lambda client: client.get_security_list_all()
            )
        except Exception as exc:
            raise DataUnavailable(f"easy_tdx 股票列表拉取失败: {exc}") from exc
        if raw is None or raw.empty:
            raise DataUnavailable("easy_tdx 股票列表为空")
        df = raw.copy()
        exchange = df["market"].map({0: "SZ", 1: "SH", 2: "BJ"})
        df["symbol"] = exchange.fillna("") + df["code"].astype(str).str.zfill(6)
        df["exchange"] = exchange.map({"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"})
        df["market"] = "A"
        df["is_active"] = True
        df["industry"] = df.get("industry_sw", "")
        df["sector"] = df.get("industry_tdx", "")
        df["source"] = self.name
        columns = [
            "symbol",
            "code",
            "exchange",
            "name",
            "market",
            "is_active",
            "sector",
            "industry",
            "source",
        ]
        return df[[column for column in columns if column in df.columns]]

    def fetch_field(
        self,
        field: str,
        symbol: str,
        *,
        start: date | None = None,
        end: date | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if field == "daily_kline" and start and end:
            return self.fetch_daily(
                symbol, start, end, adjust=str(kwargs.get("adjust", "qfq"))
            )
        if field == "minute_kline" and start and end:
            return self.fetch_minute(
                symbol, start, end, freq=str(kwargs.get("freq", "1min"))
            )
        if field == "stock_list":
            return self.fetch_meta("A")
        raise InvalidFieldRequest(f"easy_tdx 未实现 field={field}")

    def _ensure_available(self) -> None:
        if not EASY_TDX_AVAILABLE:
            raise DataUnavailable("easy-tdx 未安装")


def register() -> bool:
    if not EASY_TDX_AVAILABLE:
        logger.info("easy-tdx 未安装，跳过 EasyTdxAdapter 注册")
        return False
    from backend.services.engine.data_platform.registry import get_registry

    get_registry().register(EasyTdxAdapter, name=EasyTdxAdapter.name)
    return True
