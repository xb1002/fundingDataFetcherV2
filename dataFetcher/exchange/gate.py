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

GATE_TIMEFRAME_MAP: Dict[str, Tuple[str, int]] = {
    "1m": ("1m", 60_000),
    "5m": ("5m", 5 * 60_000),
    "15m": ("15m", 15 * 60_000),
    "30m": ("30m", 30 * 60_000),
    "1h": ("1h", 60 * 60_000),
    "4h": ("4h", 4 * 60 * 60_000),
    "6h": ("6h", 6 * 60 * 60_000),
    "12h": ("12h", 12 * 60 * 60_000),
    "1d": ("1d", 24 * 60 * 60_000),
    "7d": ("7d", 7 * 24 * 60 * 60_000),
}

GATE_ENDPOINTS = {
    "price_ohlcv": "/futures/usdt/candlesticks",
    "index_ohlcv": "/futures/usdt/candlesticks",
    "premium_index_ohlcv": "/futures/usdt/premium_index",
    "funding_history": "/futures/usdt/funding_rate",
}

GATE_REQ_MAX_LIMIT = {
    "price_ohlcv": 2000,
    "index_ohlcv": 2000,
    "premium_index_ohlcv": 1000,
    "funding_history": 200,
}

GATE_FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000


class GateAdapter(ExchangeAdapter):
    def __init__(self):
        super().__init__()
        self.base_url = "https://api.gateio.ws/api/v4"
        self.endpoint_dict = GATE_ENDPOINTS
        self.req_max_limit = GATE_REQ_MAX_LIMIT

    # -------- Public methods -------- #
    def _parse_klines(self, raw) -> List[CandleType]:
        candles = []
        for entry in raw:
            open_time_ms = int(entry["t"]) * 1000
            candle: CandleType = (
                open_time_ms,
                float(entry["o"]),
                float(entry["h"]),
                float(entry["l"]),
                float(entry["c"]),
                float(entry.get("v", 0)) if entry.get("v") not in (None, "") else 0.0,
            )
            candles.append(candle)
        return candles

    def fetch_price_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["price_ohlcv"]
        contract = self._map_contract_symbol(req.symbol)

        params: Dict[str, Any] = {
            "contract": contract,
            "interval": self._map_timeframe(req.timeframe)[0],
            "limit": self._resolve_limit(req.limit, "price_ohlcv"),
        }
        if req.start_time is not None:
            params["from"] = int(req.start_time // 1000)

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )

        return self._parse_klines(raw)

    def fetch_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["index_ohlcv"]
        contract = self._map_contract_symbol(req.symbol)
        contract = "index_" + contract

        params: Dict[str, Any] = {
            "contract": contract,
            "interval": self._map_timeframe(req.timeframe)[0],
            "limit": self._resolve_limit(req.limit, "index_ohlcv"),
        }
        if req.start_time is not None:
            params["from"] = int(req.start_time // 1000)

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )

        return self._parse_klines(raw)

    def fetch_premium_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["premium_index_ohlcv"]
        contract = self._map_contract_symbol(req.symbol)

        params: Dict[str, Any] = {
            "contract": contract,
            "interval": self._map_timeframe(req.timeframe)[0],
            "limit": self._resolve_limit(req.limit, "premium_index_ohlcv"),
        }
        if req.start_time is not None:
            params["from"] = int(req.start_time // 1000)

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_klines(raw)

    def fetch_funding_history(self, req: FundingRequestParams) -> List[FundingRecordType]:
        endpoint = self.endpoint_dict["funding_history"]
        contract = self._map_contract_symbol(req.symbol)

        params: Dict[str, Any] = {
            "contract": contract,
            "limit": self._resolve_limit(req.limit, "funding_history"),
        }
        if req.start_time is not None:
            params["from"] = int(req.start_time // 1000)

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )

        funding_records: List[FundingRecordType] = []
        for entry in raw:
            funding_time_ms = int(entry["t"]) * 1000
            funding_rate = float(entry["r"])
            record: FundingRecordType = (funding_time_ms, funding_rate)
            funding_records.append(record)

        return funding_records
        

    def _map_timeframe(self, tf: str) -> Tuple[str, int]:
        if tf not in GATE_TIMEFRAME_MAP:
            raise ValueError(f"Unsupported timeframe for Gate: {tf}")
        return GATE_TIMEFRAME_MAP[tf]

    def _resolve_limit(self, limit: Optional[int], key: str) -> int:
        max_limit = self.req_max_limit[key]
        if limit is None:
            return max_limit
        if limit <= 0:
            raise ValueError("Limit must be positive")
        if limit > max_limit:
            raise ValueError(f"Limit exceeds maximum of {max_limit}")
        return limit

    def _map_contract_symbol(self, internal_symbol: str) -> str:
        return internal_symbol.replace("-", "_")


if __name__ == "__main__":
    import pandas as pd
    import datetime

    start_time = int(datetime.datetime(2025, 10, 1).timestamp() * 1000)

    adapter = GateAdapter()
    ohlcv_req = OHLCVRequestParams(
        symbol="BTC_USDT",
        timeframe="1m",
        start_time=start_time,
        limit=10,
    )
    funding_req = FundingRequestParams(
        symbol="BTC_USDT",
        start_time=start_time,
        limit=10,
    )

    print("Price OHLCV:")
    price = adapter.fetch_price_ohlcv(ohlcv_req)
    # print(pd.DataFrame(price, columns=["open_time", "open", "high", "low", "close", "volume"]))
    df = pd.DataFrame(price, columns=["open_time", "open", "high", "low", "close", "volume"])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    print(df)

    print("\nIndex OHLCV:")
    index_price = adapter.fetch_index_ohlcv(ohlcv_req)
    df = pd.DataFrame(index_price, columns=["open_time", "open", "high", "low", "close", "volume"])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    print(df)

    print("\nPremium Index OHLCV:")
    premium_price = adapter.fetch_premium_index_ohlcv(ohlcv_req)
    df = pd.DataFrame(premium_price, columns=["open_time", "open", "high", "low", "close", "volume"])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    print(df)

    print("\nFunding History:")
    funding = adapter.fetch_funding_history(funding_req)
    df = pd.DataFrame(funding, columns=["fundingTime", "fundingRate"])
    df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
    print(df)
