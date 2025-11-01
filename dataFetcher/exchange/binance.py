# exchanges/binance.py
from typing import List, Dict, Any
if __name__ == "__main__":
    import sys
    import os
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.insert(0, parent_dir)
from dataFetcher.base_adapter import ExchangeAdapter
from dataFetcher.dto import OHLCVRequestParams, FundingRequestParams, CandleType, FundingRecordType

# import sys
# import os
# current_dir = os.path.dirname(os.path.abspath(__file__))
# parent_dir = os.path.dirname(os.path.dirname(current_dir))
# sys.path.insert(0, parent_dir)
# from dataFetcher.base_adapter import ExchangeAdapter
# from dataFetcher.dto import OHLCVRequest, FundingRequest, CandleType, FundingRecordType

BINANCE_TIMEFRAME_MAP = {
    "1m": "1m",
    "5m": "5m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}
BINANCE_ENDPOINTS = {
    "price_ohlcv": "/fapi/v1/klines",
    "index_ohlcv": "/fapi/v1/indexPriceKlines",
    "premium_index_ohlcv": "/fapi/v1/premiumIndexKlines",
    "funding_history": "/fapi/v1/fundingRate",
}
BINANCE_REQ_MAX_LIMIT = {
    "price_ohlcv": 1500,
    "index_ohlcv": 1500,
    "premium_index_ohlcv": 1500,
    "funding_history": 1000,
}

class BinanceAdapter(ExchangeAdapter):
    def __init__(self):
        super().__init__()
        self.base_url = "https://fapi.binance.com"
        self.endpoint_dict = BINANCE_ENDPOINTS
        self.req_max_limit = BINANCE_REQ_MAX_LIMIT

    def _map_timeframe(self, tf: str) -> str:
        return BINANCE_TIMEFRAME_MAP[tf]

    def fetch_price_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["price_ohlcv"]
        params: Dict[str, Any] = {
            "symbol": self._map_symbol(req.symbol),        # e.g. "BTCUSDT"
            "interval": self._map_timeframe(req.timeframe),
            "limit": 1000, # 默认最大值1000
        }
        if req.start_time:
            params["startTime"] = req.start_time
        if req.limit:
            if req.limit > self.req_max_limit["price_ohlcv"]:
                raise ValueError(f"Limit exceeds maximum of {self.req_max_limit['price_ohlcv']}")
            params["limit"] = req.limit

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params
        )
        return self._parse_klines(raw)

    def fetch_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["index_ohlcv"]
        params: Dict[str, Any] = {
            "pair": self._map_pair(req.symbol),            # e.g. "BTCUSDT"
            "interval": self._map_timeframe(req.timeframe),
            "limit": 1000, # 默认最大值1000
        }
        if req.start_time:
            params["startTime"] = req.start_time
        if req.limit:
            if req.limit > self.req_max_limit["index_ohlcv"]:
                raise ValueError(f"Limit exceeds maximum of {self.req_max_limit['index_ohlcv']}")
            params["limit"] = req.limit

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params
        )
        return self._parse_klines(raw)

    def fetch_premium_index_ohlcv(self, req: OHLCVRequestParams) -> List[CandleType]:
        endpoint = self.endpoint_dict["premium_index_ohlcv"]
        params: Dict[str, Any] = {
            "symbol": self._map_symbol(req.symbol),
            "interval": self._map_timeframe(req.timeframe),
            "limit": 1000,  # 默认最大值1000
        }
        if req.start_time:
            params["startTime"] = req.start_time
        if req.limit:
            if req.limit > self.req_max_limit["premium_index_ohlcv"]:
                raise ValueError(f"Limit exceeds maximum of {self.req_max_limit['premium_index_ohlcv']}")
            params["limit"] = req.limit

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params
        )
        return self._parse_klines(raw)

    def fetch_funding_history(self, req: FundingRequestParams) -> List[FundingRecordType]:
        endpoint = self.endpoint_dict["funding_history"]
        params = {
            "symbol": self._map_symbol(req.symbol),
            "limit": 100,  # 默认最大值100
        }
        if req.start_time:
            params["startTime"] = req.start_time
        if req.limit:
            params["limit"] = req.limit

        raw = self.make_request(
            url=f"{self.base_url}{endpoint}",
            params=params
        )
        return self._parse_funding_records(raw)

    def _parse_funding_records(self, raw) -> List[FundingRecordType]:
        records = []
        for item in raw:
            record: FundingRecordType = (
                int(item["fundingTime"]),
                float(item["fundingRate"]),
            )
            records.append(record)
        return records

    def _parse_klines(self, raw) -> List[CandleType]:
        # 取每一行的前6个元素
        data = raw
        candles = []
        for item in data:
            # candle = Candle(
            #     open_time=int(item[0]),
            #     open=float(item[1]),
            #     high=float(item[2]),
            #     low=float(item[3]),
            #     close=float(item[4]),
            #     volume=float(item[5]) if len(item) > 5 else 0,
            # )
            candle: CandleType = (
                int(item[0]),
                float(item[1]),
                float(item[2]),
                float(item[3]),
                float(item[4]),
                float(item[5]) if len(item) > 5 else 0,
            )
            candles.append(candle)
        return candles

    def _map_symbol(self, internal_symbol: str) -> str:
        # e.g. "BTCUSDT-PERP" -> "BTCUSDT"
        return internal_symbol.replace("_", "")

    def _map_pair(self, internal_symbol: str) -> str:
        # Binance indexPriceKlines uses "pair", ex. "BTCUSDT"
        return self._map_symbol(internal_symbol)

if __name__ == "__main__":
    import pandas as pd
    import datetime

    start_time = int(datetime.datetime(2025,10,1).timestamp() * 1000)

    adapter = BinanceAdapter()
    ohlcv_req_params = OHLCVRequestParams(
        symbol="BTCUSDT",
        timeframe="1m",
        start_time=start_time,
        limit=5
    )
    funding_req_params = FundingRequestParams(
        symbol="BTCUSDT",
        start_time=start_time,
        limit=5
    )
    
    print("Price OHLCV:")
    price_ohlcv = adapter.fetch_price_ohlcv(ohlcv_req_params)
    print(pd.DataFrame(price_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"]))
    print("\nIndex OHLCV:")
    index_ohlcv = adapter.fetch_index_ohlcv(ohlcv_req_params)
    print(pd.DataFrame(index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"]))
    print("\nPremium Index OHLCV:")
    premium_index_ohlcv = adapter.fetch_premium_index_ohlcv(ohlcv_req_params)
    print(pd.DataFrame(premium_index_ohlcv, columns=["open_time", "open", "high", "low", "close", "volume"]))
    print("\nFunding History:")
    funding_history = adapter.fetch_funding_history(funding_req_params)
    print(pd.DataFrame(funding_history, columns=["fundingTime", "fundingRate"]))