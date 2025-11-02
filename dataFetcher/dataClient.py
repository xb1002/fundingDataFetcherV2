"""
Convenience exports and factory helpers for the dataFetcher package.
"""
from typing import Dict, Type, List, Optional
if __name__ == "__main__":
    import sys
    import os

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    sys.path.insert(0, parent_dir)
from dataFetcher.base_adapter import ExchangeAdapter
from dataFetcher.dto import (
    OHLCVRequestParams,
    FundingRequestParams,
    CandleType,
    FundingRecordType,
)

from dataFetcher.exchange.binance import BinanceAdapter
from dataFetcher.exchange.okx import OKXAdapter
from dataFetcher.exchange.bybit import BybitAdapter
from dataFetcher.exchange.bitget import BitgetAdapter
from dataFetcher.exchange.gate import GateAdapter

EXCHANGE_ADAPTERS: Dict[str, Type[ExchangeAdapter]] = {
    "binance": BinanceAdapter,
    "okx": OKXAdapter,
    "bybit": BybitAdapter,
    "bitget": BitgetAdapter,
    "gate": GateAdapter,
}


class DataClient:
    """统一的数据获取客户端，支持多个交易所的数据获取"""

    def __init__(self):
        self._adapters: Dict[str, ExchangeAdapter] = {}

    def _get_adapter(self, exchange: str) -> ExchangeAdapter:
        """获取或创建交易所适配器实例"""
        if exchange not in self._adapters:
            if exchange not in EXCHANGE_ADAPTERS:
                raise ValueError(f"Unsupported exchange: {exchange}")

            adapter_class = EXCHANGE_ADAPTERS[exchange]
            self._adapters[exchange] = adapter_class()

        return self._adapters[exchange]

    def fetch_markets(self, exchange: str) -> List[str]:
        """获取交易所支持的永续合约交易对列表"""
        adapter = self._get_adapter(exchange)
        return adapter.fetch_markets()

    def fetch_price_ohlcv(
        self,
        exchange: str,
        params: OHLCVRequestParams
    ) -> List[CandleType]:
        """
        获取价格历史数据(OHLCV)

        Args:
            exchange: 交易所名称 (binance, okx, bybit, bitget)
            params: OHLCV请求参数，candle_type将被设置为PRICE

        Returns:
            标准化的OHLCV数据列表
        """
        adapter = self._get_adapter(exchange)
        return adapter.fetch_price_ohlcv(params)

    def fetch_index_ohlcv(
        self,
        exchange: str,
        params: OHLCVRequestParams
    ) -> List[CandleType]:
        """
        获取价格指数历史数据(OHLCV)

        Args:
            exchange: 交易所名称
            params: OHLCV请求参数，candle_type将被设置为INDEX

        Returns:
            标准化的OHLCV数据列表
        """
        adapter = self._get_adapter(exchange)
        return adapter.fetch_index_ohlcv(params)

    def fetch_premium_index_ohlcv(
        self,
        exchange: str,
        params: OHLCVRequestParams
    ) -> List[CandleType]:
        """
        获取溢价指数历史数据(OHLCV)

        Args:
            exchange: 交易所名称
            params: OHLCV请求参数，candle_type将被设置为PREMIUM_INDEX

        Returns:
            标准化的OHLCV数据列表
        """
        adapter = self._get_adapter(exchange)
        return adapter.fetch_premium_index_ohlcv(params)

    def fetch_funding_history(
        self,
        exchange: str,
        params: FundingRequestParams
    ) -> List[FundingRecordType]:
        """
        获取资金费率历史数据

        Args:
            exchange: 交易所名称
            params: 资金费率请求参数，record_type将被设置为HISTORY

        Returns:
            标准化的资金费率数据列表
        """
        adapter = self._get_adapter(exchange)
        return adapter.fetch_funding_history(params)


# 创建全局实例供便捷使用
data_client = DataClient()

if __name__ == "__main__":
    import pandas as pd
    import datetime

    start_time = int(datetime.datetime(2025, 10, 25).timestamp() * 1000)

    ohlcv_req_params = OHLCVRequestParams(
        symbol="BTC_USDT",
        timeframe="1m",
        start_time=start_time,
        limit=5,
    )
    funding_req_params = FundingRequestParams(
        symbol="BTC_USDT",
        start_time=start_time,
        limit=5,
    )

    exchange = "binance" # options: binance, okx, bybit, bitget, gate

    print("Fetching Markets:")
    markets = data_client.fetch_markets(exchange)
    print(markets)

    print("Fetching Price OHLCV Data:")
    res = data_client.fetch_price_ohlcv(exchange, ohlcv_req_params)
    print(pd.DataFrame(res, columns=["timestamp", "open", "high", "low", "close", "volume"]))
    
    print("Fetching index OHLCV Data:")
    res = data_client.fetch_index_ohlcv(exchange, ohlcv_req_params)
    print(pd.DataFrame(res, columns=["timestamp", "open", "high", "low", "close", "volume"]))

    print("Fetching Premium Index OHLCV Data:")
    res = data_client.fetch_premium_index_ohlcv(exchange, ohlcv_req_params)
    print(pd.DataFrame(res, columns=["timestamp", "open", "high", "low", "close", "volume"]))

    print("Fetching Funding History Data:")
    res = data_client.fetch_funding_history(exchange, funding_req_params)
    print(pd.DataFrame(res, columns=["funding_time", "funding_rate"]))

