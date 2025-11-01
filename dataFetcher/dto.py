from dataclasses import dataclass
from typing import Optional, Tuple, List

# ======================= 请求 DTOs =======================
@dataclass
class OHLCVRequest:
    symbol: str               # 统一内部symbol,"BTC_USDT"
    timeframe: str            # "1m", "5m", "1h", "4h", "1d", ...
    start_time: Optional[int] = None
    limit: Optional[int] = None

@dataclass
class FundingRequest:
    symbol: str               # "BTC_USDT"
    start_time: Optional[int] = None
    limit: Optional[int] = None

# ======================= 响应 DTOs =======================
# @dataclass
# class Candle:
#     open_time: int
#     open: float
#     high: float
#     low: float
#     close: float
#     volume: float  # 如果该来源没有成交量（比如某些指数价/溢价指数），填0.0或None

# @dataclass
# class FundingRecord:
#     funding_time: int
#     funding_rate: float

CandleType = Tuple[int, float, float, float, float, float]  # [open_time, open, high, low, close, volume]
FundingRecordType = Tuple[int, float]  # [funding_time, funding_rate]