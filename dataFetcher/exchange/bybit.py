from typing import List, Dict, Any

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

BYBIT_TIMEFRAME_MAP: Dict[str, str] = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1M": "M",
}

BYBIT_ENDPOINTS = {
    "price_ohlcv": "/v5/market/kline",
    "index_ohlcv": "/v5/market/index-price-kline",
    "premium_index_ohlcv": "/v5/market/premium-index-price-kline",
    "funding_history": "/v5/market/funding/history",
}

BYBIT_REQ_MAX_LIMIT = {
    "price_ohlcv": 1000,
    "index_ohlcv": 1000,
    "premium_index_ohlcv": 1000,
    "funding_history": 200,
}

class BybitAdapter(ExchangeAdapter):
    def __init__(self):
        super().__init__()
        self.base_url = "https://api.bybit.com"
        self.endpoint_dict = BYBIT_ENDPOINTS
        self.req_max_limit = BYBIT_REQ_MAX_LIMIT
        self.default_category = "linear"

    def fetch_markets(self) -> List[str]:
        endpoint = "/v5/market/instruments-info"
        params = {"category": self.default_category}
        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        self._ensure_success(raw)
        result = raw.get("result", {}) if isinstance(raw, dict) else {}
        instruments = result.get("list", []) or []
        markets: List[str] = []
        for item in instruments:
            status = (item.get("status") or "").lower()
            if status and status not in ("trading", "listed"):
                continue
            base = (item.get("baseCoin") or "").upper()
            quote = (item.get("quoteCoin") or "").upper()
            if not base or not quote:
                continue
            markets.append(f"{base}_{quote}")
        return sorted(set(markets))

    def _map_timeframe(self, tf: str) -> str:
        if tf not in BYBIT_TIMEFRAME_MAP:
            raise ValueError(f"Unsupported timeframe for Bybit: {tf}")
        return BYBIT_TIMEFRAME_MAP[tf]

    def fetch_price_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["price_ohlcv"]
        params: Dict[str, Any] = {
            "category": self.default_category,
            "symbol": self._map_symbol(req.symbol),
            "interval": self._map_timeframe(req.timeframe),
        }
        self._apply_time_filters(params, req.start_time)
        self._apply_limit(params, req.limit, "price_ohlcv")

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_kline_list(raw)

    def fetch_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["index_ohlcv"]
        params: Dict[str, Any] = {
            "category": self.default_category,
            "symbol": self._map_symbol(req.symbol),
            "interval": self._map_timeframe(req.timeframe),
        }
        self._apply_time_filters(params, req.start_time)
        self._apply_limit(params, req.limit, "index_ohlcv")

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_kline_list(raw)

    def fetch_premium_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["premium_index_ohlcv"]
        params: Dict[str, Any] = {
            "category": self.default_category,
            "symbol": self._map_symbol(req.symbol),
            "interval": self._map_timeframe(req.timeframe),
        }
        self._apply_time_filters(params, req.start_time)
        self._apply_limit(params, req.limit, "premium_index_ohlcv")

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_kline_list(raw)

    def fetch_funding_history(self, req: FundingRequestParams) -> List[FundingRecordType]:
        endpoint = self.endpoint_dict["funding_history"]
        params: Dict[str, Any] = {
            "category": self.default_category,
            "symbol": self._map_symbol(req.symbol),
            "limit": 200,
        }
        self._apply_limit(params, req.limit, "funding_history")
        if req.start_time is not None:
            params["startTime"] = req.start_time
            coverage_limit = params.get("limit", self.req_max_limit["funding_history"])
            params["endTime"] = req.start_time + coverage_limit * 60 * 60 * 1000 * 1

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_funding_history(raw)

    def _apply_time_filters(self, params: Dict[str, Any], start_time: Any) -> None:
        if start_time is not None:
            params["start"] = start_time

    def _apply_limit(self, params: Dict[str, Any], limit: Any, key: str) -> None:
        if limit is not None:
            if limit <= 0:
                raise ValueError("Limit must be positive")
            if limit > self.req_max_limit[key]:
                raise ValueError(f"Limit exceeds maximum of {self.req_max_limit[key]}")
            params["limit"] = limit

    def _parse_kline_list(self, raw: Dict[str, Any]) -> List[CandleType]:
        self._ensure_success(raw)
        result = raw.get("result", {})
        data = result.get("list", [])
        candles: List[CandleType] = []
        for item in data:
            # Response can be either list or dict depending on API version
            if isinstance(item, dict):
                open_time = int(item.get("start", item.get("startTime", 0)))
                open_price = float(item.get("open", item.get("openPrice", 0)))
                high_price = float(item.get("high", item.get("highPrice", 0)))
                low_price = float(item.get("low", item.get("lowPrice", 0)))
                close_price = float(item.get("close", item.get("closePrice", 0)))
                volume = float(item.get("volume", item.get("turnover", 0)) or 0)
            else:
                # Official docs return list: [timestamp, open, high, low, close, volume, turnover]
                open_time = int(item[0])
                open_price = float(item[1])
                high_price = float(item[2])
                low_price = float(item[3])
                close_price = float(item[4])
                volume = float(item[5]) if len(item) > 5 and item[5] not in ("", None) else 0.0
            candles.append(
                (
                    open_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                )
            )
        return list(reversed(candles))

    def _parse_funding_history(self, raw: Dict[str, Any]) -> List[FundingRecordType]:
        self._ensure_success(raw)
        result = raw.get("result", {})
        data = result.get("list", [])
        records: List[FundingRecordType] = []
        for item in data:
            if isinstance(item, dict):
                funding_time = int(
                    item.get("fundingRateTimestamp", item.get("timestamp", 0))
                )
                funding_rate = float(item.get("fundingRate", item.get("rate", 0)))
            else:
                # Some responses might be simple lists like [timestamp, fundingRate, ...]
                funding_time = int(item[0])
                funding_rate = float(item[1])
            records.append((funding_time, funding_rate))
        return list(reversed(records))

    def _map_symbol(self, internal_symbol: str) -> str:
        # "BTC_USDT" -> "BTCUSDT"
        return internal_symbol.replace("_", "")

    def _ensure_success(self, raw: Dict[str, Any]) -> None:
        ret_code = raw.get("retCode")
        if ret_code is not None and ret_code != 0:
            ret_msg = raw.get("retMsg", "Unknown error from Bybit API")
            raise RuntimeError(f"Bybit API error {ret_code}: {ret_msg}")


if __name__ == "__main__":
    import pandas as pd
    import datetime

    start_time = int(datetime.datetime(2025, 10, 1).timestamp() * 1000)

    adapter = BybitAdapter()
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

    print("Markets:")
    markets = adapter.fetch_markets()
    print(markets)

    # This main block is for manual verification when network access is available.
    print("Price OHLCV:")
    price_ohlcv = adapter.fetch_price_ohlcv(ohlcv_req_params)
    # print(pd.DataFrame(price_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"]))
    df = pd.DataFrame(price_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    print(df)
    
    print("\nIndex OHLCV:")
    index_ohlcv = adapter.fetch_index_ohlcv(ohlcv_req_params)
    df = pd.DataFrame(index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    print(df)

    print("\nPremium Index OHLCV:")
    premium_index_ohlcv = adapter.fetch_premium_index_ohlcv(ohlcv_req_params)
    df = pd.DataFrame(premium_index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    print(df)

    print("\nFunding History:")
    funding_history = adapter.fetch_funding_history(funding_req_params)
    # print(pd.DataFrame(funding_history, columns=["fundingTime", "fundingRate"]))
    # 将fundingTime转换为可读时间
    df = pd.DataFrame(funding_history, columns=["fundingTime", "fundingRate"])
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")
    print(df)
