"""
Convenience exports and factory helpers for the dataFetcher package.
"""
from typing import Dict, Type

from .base_adapter import ExchangeAdapter
from .dto import (
    OHLCVRequestParams,
    FundingRequestParams,
    CandleType,
    FundingRecordType,
)
from .dataClient import DataClient
from .exchange.binance import BinanceAdapter
from .exchange.okx import OKXAdapter
from .exchange.bybit import BybitAdapter
from .exchange.bitget import BitgetAdapter
from .exchange.gate import GateAdapter

EXCHANGE_ADAPTERS: Dict[str, Type[ExchangeAdapter]] = {
    "binance": BinanceAdapter,
    "okx": OKXAdapter,
    "bybit": BybitAdapter,
    "bitget": BitgetAdapter,
    "gate": GateAdapter,
}


def get_exchange_adapter(name: str) -> ExchangeAdapter:
    """
    Create an exchange adapter instance by name.

    Args:
        name: Exchange identifier, case-insensitive (e.g. ``"binance"``).

    Returns:
        ExchangeAdapter: Instantiated adapter for the requested venue.

    Raises:
        ValueError: If the exchange name is unsupported.
    """
    key = name.lower()
    if key not in EXCHANGE_ADAPTERS:
        available = ", ".join(sorted(EXCHANGE_ADAPTERS.keys()))
        raise ValueError(f"Unsupported exchange '{name}'. Available: {available}")
    return EXCHANGE_ADAPTERS[key]()


__all__ = [
    "ExchangeAdapter",
    "OHLCVRequestParams",
    "FundingRequestParams",
    "CandleType",
    "FundingRecordType",
    "BinanceAdapter",
    "OKXAdapter",
    "BybitAdapter",
    "BitgetAdapter",
    "get_exchange_adapter",
    "GateAdapter",
    "DataClient",
]
