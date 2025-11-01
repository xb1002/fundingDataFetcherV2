from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
import time

if __name__ == "__main__":
    import sys
    import os

    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.insert(0, parent_dir)

from dataFetcher.base_adapter import ExchangeAdapter
from dataFetcher.dto import (
    OHLCVRequestParams,
    FundingRequestParams,
    CandleType,
    FundingRecordType,
)

KUCOIN_TIMEFRAME_MAP: Dict[str, Tuple[int, int]] = {
    "1m": (1, 60_000),
    "5m": (5, 5 * 60_000),
    "15m": (15, 15 * 60_000),
    "30m": (30, 30 * 60_000),
    "1h": (60, 60 * 60_000),
    "2h": (120, 2 * 60 * 60_000),
    "4h": (240, 4 * 60 * 60_000),
    "6h": (360, 6 * 60 * 60_000),
    "12h": (720, 12 * 60 * 60_000),
    "1d": (1440, 24 * 60 * 60_000),
    "1w": (10080, 7 * 24 * 60 * 60_000),
}

KUCOIN_ENDPOINTS = {
    "kline": "/api/v1/kline/query",
    "contracts": "/api/v1/contracts/active",
    "funding_history": "/api/v1/contract/funding-rates",
}

KUCOIN_REQ_MAX_LIMIT = {
    "ohlcv": 200,
    "funding_history": 200,
}

# Funding occurs every 8 hours on KuCoin Futures
KUCOIN_FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000


class KucoinAdapter(ExchangeAdapter):
    def __init__(self):
        super().__init__()
        self.base_url = "https://api-futures.kucoin.com"
        self.endpoint_dict = KUCOIN_ENDPOINTS
        self.req_max_limit = KUCOIN_REQ_MAX_LIMIT
        self._contract_meta_cache: Dict[str, Dict[str, Any]] = {}

    # -------- Public interface -------- #
    def fetch_price_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        contract_symbol = self._map_contract_symbol(req.symbol)
        return self._fetch_kline_series(contract_symbol, req)

    def fetch_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        contract_symbol = self._map_contract_symbol(req.symbol)
        index_symbol = self._get_contract_meta(contract_symbol)["indexSymbol"]
        return self._fetch_kline_series(index_symbol, req)

    def fetch_premium_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        contract_symbol = self._map_contract_symbol(req.symbol)
        premium_symbol = self._get_contract_meta(contract_symbol)["premiumsSymbol1M"]
        return self._fetch_kline_series(premium_symbol, req)

    def fetch_funding_history(self, req: FundingRequestParams) -> List[FundingRecordType]:
        contract_symbol = self._map_contract_symbol(req.symbol)
        desired_limit = self._resolve_limit(req.limit, "funding_history")
        params: Dict[str, Any] = {
            "symbol": contract_symbol,
            "from": 0,
            "to": int(time.time() * 1000),
        }

        if req.start_time is not None:
            params["from"] = int(req.start_time)
            params["to"] = params["from"] + desired_limit * KUCOIN_FUNDING_INTERVAL_MS

        raw = self.make_request(
            url=f"{self.base_url}{self.endpoint_dict['funding_history']}",
            params=params,
        )
        data = raw.get("data", []) if isinstance(raw, dict) else raw
        records: List[FundingRecordType] = []
        for item in data:
            funding_time = int(item["timepoint"])
            funding_rate = float(item["fundingRate"])
            records.append((funding_time, funding_rate))
        records.sort(key=lambda x: x[0])
        return records[:desired_limit]

    # -------- Internal helpers -------- #
    def _fetch_kline_series(self, symbol: str, req: OHLCVRequestParams) -> List[CandleType]:
        granularity, interval_ms = self._map_timeframe(req.timeframe)
        limit = self._resolve_limit(req.limit, "ohlcv")
        start, end = self._compute_time_window(req.start_time, interval_ms, limit)

        params: Dict[str, Any] = {
            "symbol": symbol,
            "granularity": granularity,
            "from": start,
            "to": end,
        }

        raw = self.make_request(
            url=f"{self.base_url}{self.endpoint_dict['kline']}",
            params=params,
        )
        data = raw.get("data", []) if isinstance(raw, dict) else raw
        candles: List[CandleType] = []
        for entry in data:
            open_time = int(entry[0])
            candle: CandleType = (
                open_time,
                float(entry[1]),
                float(entry[2]),
                float(entry[3]),
                float(entry[4]),
                float(entry[5]) if len(entry) > 5 and entry[5] not in ("", None) else 0.0,
            )
            candles.append(candle)
        candles.sort(key=lambda x: x[0])
        return candles[:limit]

    def _map_timeframe(self, tf: str) -> Tuple[int, int]:
        if tf not in KUCOIN_TIMEFRAME_MAP:
            raise ValueError(f"Unsupported timeframe for KuCoin Futures: {tf}")
        return KUCOIN_TIMEFRAME_MAP[tf]

    def _resolve_limit(self, limit: Optional[int], key: str) -> int:
        max_limit = self.req_max_limit[key]
        if limit is None:
            return max_limit
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if limit > max_limit:
            raise ValueError(f"Limit exceeds maximum of {max_limit}")
        return limit

    def _compute_time_window(
        self,
        start_time: Optional[int],
        interval_ms: int,
        limit: int,
    ) -> Tuple[int, int]:
        if start_time is not None:
            start = int(start_time)
            end = start + interval_ms * limit
        else:
            end = int(time.time() * 1000)
            start = end - interval_ms * limit
            if start < 0:
                start = 0
        return start, end

    def _map_contract_symbol(self, internal_symbol: str) -> str:
        base, quote = internal_symbol.split("_")
        base = self._map_base_asset(base)
        quote = quote.upper()
        return f"{base}{quote}M"

    def _map_base_asset(self, base: str) -> str:
        if base.upper() == "BTC":
            return "XBT"
        return base.upper()

    def _get_contract_meta(self, contract_symbol: str) -> Dict[str, Any]:
        if contract_symbol in self._contract_meta_cache:
            return self._contract_meta_cache[contract_symbol]
        contracts = self.make_request(
            url=f"{self.base_url}{self.endpoint_dict['contracts']}",
        )
        data = contracts.get("data", []) if isinstance(contracts, dict) else contracts
        for item in data:
            symbol = item.get("symbol")
            if symbol:
                self._contract_meta_cache[symbol] = item
        if contract_symbol not in self._contract_meta_cache:
            raise ValueError(f"Contract metadata not found for symbol {contract_symbol}")
        return self._contract_meta_cache[contract_symbol]


if __name__ == "__main__":
    import pandas as pd
    import datetime

    start_time = int(datetime.datetime(2025, 10, 25).timestamp() * 1000)

    adapter = KucoinAdapter()
    ohlcv_req = OHLCVRequestParams(
        symbol="BTC_USDT",
        timeframe="1m",
        start_time=start_time,
        limit=5,
    )
    funding_req = FundingRequestParams(
        symbol="BTC_USDT",
        start_time=start_time,
        limit=10,
    )

    print("Price OHLCV:")
    price = adapter.fetch_price_ohlcv(ohlcv_req)
    print(pd.DataFrame(price, columns=["open_time", "open", "high", "low", "close", "volume"]))

    print("\nIndex OHLCV:")
    index_price = adapter.fetch_index_ohlcv(ohlcv_req)
    print(pd.DataFrame(index_price, columns=["open_time", "open", "high", "low", "close", "volume"]))

    print("\nPremium Index OHLCV:")
    premium_price = adapter.fetch_premium_index_ohlcv(ohlcv_req)
    print(pd.DataFrame(premium_price, columns=["open_time", "open", "high", "low", "close", "volume"]))

    print("\nFunding History:")
    funding = adapter.fetch_funding_history(funding_req)
    print(pd.DataFrame(funding, columns=["fundingTime", "fundingRate"]))
