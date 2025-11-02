from typing import List, Dict, Any, Optional

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

OKX_TIMEFRAME_MAP = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "1w": "1W",
    "1M": "1M",
}

OKX_ENDPOINTS = {
    "price_ohlcv": "/api/v5/market/history-candles",
    "index_ohlcv": "/api/v5/market/history-index-candles",
    "premium_index_ohlcv": "/api/v5/public/premium-history",
    "funding_history": "/api/v5/public/funding-rate-history",
}

OKX_REQ_MAX_LIMIT = {
    "price_ohlcv": 100,
    "index_ohlcv": 100,
    "premium_index_ohlcv": 100,
    "funding_history": 400,
}

OKX_TIMEFRAME_TO_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "1H": 3_600_000,
    "2h": 7_200_000,
    "2H": 7_200_000,
    "4h": 14_400_000,
    "4H": 14_400_000,
    "6h": 21_600_000,
    "6H": 21_600_000,
    "12h": 43_200_000,
    "12H": 43_200_000,
    "1d": 86_400_000,
    "1D": 86_400_000,
    "1w": 604_800_000,
    "1W": 604_800_000,
    "1M": 2_592_000_000,
}

OKX_FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000


class OKXAdapter(ExchangeAdapter):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.okx.com"
        self.endpoint_dict = OKX_ENDPOINTS
        self.req_max_limit = OKX_REQ_MAX_LIMIT

    def _map_timeframe(self, tf: str) -> str:
        if tf not in OKX_TIMEFRAME_MAP:
            raise ValueError(f"Unsupported timeframe for OKX: {tf}")
        return OKX_TIMEFRAME_MAP[tf]

    def _get_timeframe_ms(self, tf: str) -> Optional[int]:
        if tf in OKX_TIMEFRAME_TO_MS:
            return OKX_TIMEFRAME_TO_MS[tf]
        normalized = tf.lower()
        return OKX_TIMEFRAME_TO_MS.get(normalized)

    def fetch_price_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["price_ohlcv"]
        timeframe_ms = self._get_timeframe_ms(req.timeframe)
        params: Dict[str, Any] = {
            "instId": self._map_symbol(req.symbol),
            "bar": self._map_timeframe(req.timeframe),
        }
        self._apply_limit(params, req.limit, "price_ohlcv")
        effective_limit = params.get("limit")
        self._apply_time_filters(params, req.start_time, effective_limit, timeframe_ms)

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_candles(raw)

    def fetch_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["index_ohlcv"]
        timeframe_ms = self._get_timeframe_ms(req.timeframe)
        params: Dict[str, Any] = {
            "instId": self._map_index_symbol(req.symbol),
            "bar": self._map_timeframe(req.timeframe),
        }
        self._apply_limit(params, req.limit, "index_ohlcv")
        effective_limit = params.get("limit")
        self._apply_time_filters(params, req.start_time, effective_limit, timeframe_ms)

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_candles(raw)

    def fetch_premium_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["premium_index_ohlcv"]
        timeframe_ms = self._get_timeframe_ms(req.timeframe)
        params: Dict[str, Any] = {
            "instId": self._map_symbol(req.symbol),
            "bar": self._map_timeframe(req.timeframe),
        }
        self._apply_limit(params, req.limit, "premium_index_ohlcv")
        effective_limit = params.get("limit")
        self._apply_time_filters(params, req.start_time, effective_limit, timeframe_ms)

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        
        # 由于返回格式与与其他接口不同，需单独解析
        data = raw.get("data", [])
        candles: List[CandleType] = []
        for item in data:
            # OKX returns: {"ts": , "premium":, "instId": }
            # 所以ohlcv的open, high, low, close均为premium值，volume设为0
            candle: CandleType = (
                int(item["ts"]),
                float(item["premium"]),
                float(item["premium"]),
                float(item["premium"]),
                float(item["premium"]),
                0.0,
            )
            candles.append(candle)
        return list(reversed(candles))

    def fetch_funding_history(self, req: FundingRequestParams) -> List[FundingRecordType]:
        endpoint = self.endpoint_dict["funding_history"]
        params: Dict[str, Any] = {
            "instId": self._map_symbol(req.symbol),
        }
        self._apply_limit(params, req.limit, "funding_history")
        effective_limit = params.get("limit")
        self._apply_time_filters(
            params,
            req.start_time,
            effective_limit,
            OKX_FUNDING_INTERVAL_MS,
        )

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params,
        )
        return self._parse_funding_history(raw)

    def _apply_time_filters(
        self,
        params: Dict[str, Any],
        start_time: Optional[int],
        limit: Optional[int],
        step_ms: Optional[int],
    ) -> None:
        if start_time is not None:
            # after 表示该时间戳之前的数据，before 表示该时间戳之后的数据
            before_ts = start_time - 1 if start_time > 0 else 0
            params["before"] = before_ts
            if limit is not None and step_ms:
                window = int(limit) * step_ms
                if window <= 0:
                    window = step_ms
                params["after"] = start_time + window

    def _apply_limit(self, params: Dict[str, Any], limit: Any, key: str) -> None:
        if limit is not None:
            if limit <= 0:
                raise ValueError("Limit must be positive")
            if limit > self.req_max_limit[key]:
                raise ValueError(f"Limit exceeds maximum of {self.req_max_limit[key]}")
            params["limit"] = limit

    def _parse_candles(self, raw: Dict[str, Any]) -> List[CandleType]:
        data = raw.get("data", [])
        candles: List[CandleType] = []
        for item in data:
            # OKX returns: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
            candle: CandleType = (
                int(item[0]),
                float(item[1]),
                float(item[2]),
                float(item[3]),
                float(item[4]),
                float(item[5]) if len(item) > 5 and item[5] not in ("", None) else 0.0,
            )
            candles.append(candle)
        return list(reversed(candles))

    def _parse_funding_history(self, raw: Dict[str, Any]) -> List[FundingRecordType]:
        data = raw.get("data", [])
        records: List[FundingRecordType] = []
        for item in data:
            funding_time = int(item["fundingTime"])
            funding_rate = float(item["fundingRate"])
            records.append((funding_time, funding_rate))
        return list(reversed(records))

    def _map_symbol(self, internal_symbol: str) -> str:
        # e.g. "BTC_USDT" -> "BTC-USDT-SWAP"
        return internal_symbol.replace("_", "-") + "-SWAP"

    def _map_index_symbol(self, internal_symbol: str) -> str:
        # e.g. "BTC_USDT" -> "BTC-USDT"
        return internal_symbol.replace("_", "-")


if __name__ == "__main__":
    import pandas as pd
    import datetime

    start_time = int(datetime.datetime(2025,10,1).timestamp() * 1000)

    adapter = OKXAdapter()
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
    
    print("Price OHLCV:")
    price_ohlcv = adapter.fetch_price_ohlcv(ohlcv_req_params)
    # print(pd.DataFrame(price_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"]))
    df = pd.DataFrame(price_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"])
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    print(df)

    print("\nIndex OHLCV:")
    index_ohlcv = adapter.fetch_index_ohlcv(ohlcv_req_params)
    # print(pd.DataFrame(index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"]))
    df = pd.DataFrame(index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"])
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    print(df)

    print("\nPremium Index OHLCV:")
    premium_index_ohlcv = adapter.fetch_premium_index_ohlcv(ohlcv_req_params)
    # print(pd.DataFrame(premium_index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"]))
    df = pd.DataFrame(premium_index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"])
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
    print(df)

    print("\nFunding History:")
    funding_history = adapter.fetch_funding_history(funding_req_params)
    # print(pd.DataFrame(funding_history, columns=["fundingTime", "fundingRate"]))
    df = pd.DataFrame(funding_history, columns=["fundingTime", "fundingRate"])
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit='ms')
    print(df)
