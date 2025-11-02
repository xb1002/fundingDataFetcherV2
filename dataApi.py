from __future__ import annotations

import re
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple, Union

import pandas as pd

LOGGER = logging.getLogger(__name__)


class DataApi:
    """API for reading previously downloaded exchange datasets from local storage."""

    OHLCV_COLUMNS: Sequence[str] = ("timestamp", "open", "high", "low", "close", "volume")
    FUNDING_COLUMNS: Sequence[str] = ("funding_time", "funding_rate")

    def __init__(self, base_dir: Union[str, Path] = "./data") -> None:
        self.base_dir = Path(base_dir).resolve()

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def price_ohlcv(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str,
    ) -> pd.DataFrame:
        return self._fetch_ohlcv("price_ohlcv", exchange, symbol, timeframe, start_time, end_time)

    def index_ohlcv(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str,
    ) -> pd.DataFrame:
        return self._fetch_ohlcv("index_ohlcv", exchange, symbol, timeframe, start_time, end_time)

    def premium_index_ohlcv(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str,
    ) -> pd.DataFrame:
        return self._fetch_ohlcv("premium_index_ohlcv", exchange, symbol, timeframe, start_time, end_time)

    def funding_rate(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str = "8h",
    ) -> pd.DataFrame:
        # timeframe kept for interface parity – not used because funding files are fixed interval.
        return self._fetch_funding(exchange, symbol, start_time, end_time)

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------
    def _fetch_ohlcv(
        self,
        data_type: str,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: str,
        end_time: str,
    ) -> pd.DataFrame:
        start_ms, end_ms = self._parse_time_range(start_time, end_time)
        files = self._find_files(exchange, data_type, symbol, timeframe)
        selected = self._select_files(files, start_ms, end_ms)
        if not selected:
            LOGGER.info("No %s data found for %s:%s in requested range.", data_type, exchange, symbol)
            return pd.DataFrame(columns=self.OHLCV_COLUMNS)

        frames = self._read_csv_files(selected, expected_columns=self.OHLCV_COLUMNS)
        if not frames:
            return pd.DataFrame(columns=self.OHLCV_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        filtered = df[(df["timestamp"] >= start_ms) & (df["timestamp"] <= end_ms)]
        return filtered.sort_values("timestamp").reset_index(drop=True)

    def _fetch_funding(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
    ) -> pd.DataFrame:
        start_ms, end_ms = self._parse_time_range(start_time, end_time)
        files = self._find_funding_files(exchange, symbol)
        selected = self._select_files(files, start_ms, end_ms)
        if not selected:
            LOGGER.info("No funding_rate data found for %s:%s in requested range.", exchange, symbol)
            return pd.DataFrame(columns=self.FUNDING_COLUMNS)

        frames = self._read_csv_files(selected, expected_columns=self.FUNDING_COLUMNS)
        if not frames:
            return pd.DataFrame(columns=self.FUNDING_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        filtered = df[(df["funding_time"] >= start_ms) & (df["funding_time"] <= end_ms)]
        return filtered.sort_values("funding_time").reset_index(drop=True)

    # -------------------------------------------------------------------------
    # File system helpers
    # -------------------------------------------------------------------------
    def _parse_time_range(self, start_time: str, end_time: str) -> Tuple[int, int]:
        start_ms = self._parse_timestamp(start_time)
        end_ms = self._parse_timestamp(end_time)
        if start_ms > end_ms:
            raise ValueError("start_time must be earlier than or equal to end_time.")
        return start_ms, end_ms

    def _parse_timestamp(self, ts: str) -> int:
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d_%H:%M:%S")
        except ValueError as exc:
            raise ValueError(f"Invalid datetime format '{ts}'. Expected YYYY-MM-DD_HH:MM:SS") from exc
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    def _find_files(
        self,
        exchange: str,
        data_type: str,
        symbol: str,
        timeframe: str,
    ) -> List[Path]:
        base_path = self.base_dir / exchange / data_type / symbol / timeframe
        if not base_path.exists():
            LOGGER.warning("Directory not found for %s data: %s", data_type, base_path)
            return []
        return sorted(base_path.glob("*.csv"))

    def _find_funding_files(self, exchange: str, symbol: str) -> List[Path]:
        base_path = self.base_dir / exchange / "funding_rate" / symbol
        if not base_path.exists():
            LOGGER.warning("Directory not found for funding data: %s", base_path)
            return []
        return sorted(base_path.glob("*.csv"))

    def _select_files(
        self,
        files: Iterable[Path],
        start_ms: int,
        end_ms: int,
    ) -> List[Tuple[Path, int, int]]:
        selected: List[Tuple[Path, int, int]] = []
        for file_path in files:
            match = re.search(r"_(\d+)_(\d+)\.csv$", file_path.name)
            if not match:
                LOGGER.debug("Skipping file with unexpected name format: %s", file_path)
                continue
            file_start = int(match.group(1))
            file_end = int(match.group(2))
            if file_end < start_ms or file_start > end_ms:
                continue
            selected.append((file_path, file_start, file_end))
        return selected

    def _read_csv_files(
        self,
        files: Sequence[Tuple[Path, int, int]],
        expected_columns: Sequence[str],
    ) -> List[pd.DataFrame]:
        frames: List[pd.DataFrame] = []
        for file_path, _, _ in files:
            try:
                df = pd.read_csv(file_path)
                missing = [col for col in expected_columns if col not in df.columns]
                if missing:
                    LOGGER.warning("File %s missing expected columns %s; skipping", file_path, missing)
                    continue
                frames.append(df[list(expected_columns)])
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.error("Failed to read %s: %s", file_path, exc)
        return frames


__all__ = ["DataApi"]

if __name__ == "__main__":
    # For quick manual testing
    data_api = DataApi()
    df = data_api.price_ohlcv(
        exchange="binance",
        symbol="0G_USDT",
        start_time="2025-10-20_00:00:00",
        end_time="2025-10-21_00:00:00",
        timeframe="1m"
    )
    print(df)