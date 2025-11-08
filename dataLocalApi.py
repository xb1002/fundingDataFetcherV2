from __future__ import annotations

import re
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd

LOGGER = logging.getLogger(__name__)
DEFAULT_MIN_FILE_SIZE_BYTES = 10 * 1024 * 1024
FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000
TIMEFRAME_TO_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}


class DataLocalApi:
    """API for reading previously downloaded exchange datasets from local storage."""

    OHLCV_COLUMNS: Sequence[str] = ("timestamp", "open", "high", "low", "close", "volume")
    FUNDING_COLUMNS: Sequence[str] = ("funding_time", "funding_rate")

    def __init__(
        self,
        base_dir: Union[str, Path] = "./data",
        min_file_size_bytes: int = DEFAULT_MIN_FILE_SIZE_BYTES,
    ) -> None:
        self.base_dir = Path(base_dir).resolve()
        self.min_file_size_bytes = max(int(min_file_size_bytes), 0)

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

        gap_ms = self._resolve_timeframe_gap(timeframe)
        selected = self._maybe_consolidate_small_files(selected, sort_column="timestamp", max_gap_ms=gap_ms)
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

        selected = self._maybe_consolidate_small_files(
            selected,
            sort_column="funding_time",
            max_gap_ms=FUNDING_INTERVAL_MS,
        )
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

    def _maybe_consolidate_small_files(
        self,
        files: List[Tuple[Path, int, int]],
        sort_column: str,
        max_gap_ms: Optional[int] = None,
    ) -> List[Tuple[Path, int, int]]:
        if len(files) <= 1 or self.min_file_size_bytes <= 0:
            return files

        ordered_files = sorted(files, key=lambda item: item[1])
        consolidated: List[Tuple[Path, int, int]] = []
        buffer: List[Tuple[Path, int, int]] = []
        buffer_size = 0
        buffer_end: Optional[int] = None
        gap_allowance = max_gap_ms if max_gap_ms and max_gap_ms > 0 else 1

        def flush_buffer() -> None:
            nonlocal buffer, buffer_size, buffer_end
            if not buffer:
                return
            if len(buffer) == 1:
                consolidated.append(buffer[0])
            else:
                merged = self._merge_file_group(buffer, sort_column)
                consolidated.append(merged)
            buffer = []
            buffer_size = 0
            buffer_end = None

        for file_entry in ordered_files:
            file_path, start_ms, end_ms = file_entry
            try:
                file_size = file_path.stat().st_size
            except OSError as exc:
                LOGGER.warning("Unable to stat %s: %s", file_path, exc)
                file_size = self.min_file_size_bytes

            if buffer and buffer_end is not None and start_ms > buffer_end + gap_allowance:
                flush_buffer()

            if file_size >= self.min_file_size_bytes:
                flush_buffer()
                consolidated.append(file_entry)
                continue

            buffer.append(file_entry)
            buffer_size += file_size
            buffer_end = end_ms if buffer_end is None else max(buffer_end, end_ms)

            if buffer_size >= self.min_file_size_bytes:
                flush_buffer()

        flush_buffer()
        return consolidated

    def _merge_file_group(
        self,
        files: Sequence[Tuple[Path, int, int]],
        sort_column: str,
    ) -> Tuple[Path, int, int]:
        if len(files) == 1:
            return files[0]

        frames: List[pd.DataFrame] = []
        for file_path, _, _ in files:
            try:
                frames.append(pd.read_csv(file_path))
            except Exception as exc:  # pragma: no cover - defensive logging
                LOGGER.error("Failed to read %s while merging: %s", file_path, exc)
                return files[0]

        combined = pd.concat(frames, ignore_index=True)
        if sort_column in combined.columns:
            combined = combined.sort_values(sort_column).reset_index(drop=True)

        start_ms = files[0][1]
        end_ms = files[-1][2]
        target_path = self._build_merged_file_path(files[0][0], start_ms, end_ms)
        try:
            combined.to_csv(target_path, index=False)
        except OSError as exc:  # pragma: no cover - defensive logging
            LOGGER.error("Failed to write merged file %s: %s", target_path, exc)
            return files[0]

        for file_path, _, _ in files:
            if file_path == target_path:
                continue
            try:
                file_path.unlink(missing_ok=True)
            except OSError as exc:  # pragma: no cover - defensive logging
                LOGGER.warning("Failed to delete %s after merge: %s", file_path, exc)

        LOGGER.info("Merged %d files into %s", len(files), target_path)
        return target_path, start_ms, end_ms

    def _build_merged_file_path(self, first_path: Path, start_ms: int, end_ms: int) -> Path:
        match = re.match(r"(.+_)\d+_\d+(\.csv)$", first_path.name)
        if not match:
            LOGGER.warning("Unexpected filename format for %s; reusing original path.", first_path)
            return first_path
        prefix, suffix = match.group(1), match.group(2)
        new_name = f"{prefix}{start_ms}_{end_ms}{suffix}"
        return first_path.with_name(new_name)

    def _resolve_timeframe_gap(self, timeframe: Optional[str]) -> int:
        if not timeframe:
            return 1
        gap = TIMEFRAME_TO_MS.get(timeframe)
        if gap is None:
            LOGGER.debug("Unknown timeframe '%s'; assuming no gap allowance.", timeframe)
            return 1
        return max(gap, 1)


__all__ = ["DataLocalApi"]

if __name__ == "__main__":
    # For quick manual testing
    data_api = DataLocalApi()
    df = data_api.funding_rate(
        exchange="binance",
        symbol="0G_USDT",
        start_time="2025-11-01_00:00:00",
        end_time="2025-11-30_00:00:00",
        # timeframe="1m"
    )
    df["funding_time"] = pd.to_datetime(df["funding_time"] // 60000 * 60000, unit="ms")
    # 查看funding_time的间隔
    print(df)
    print(df["funding_time"].diff().value_counts())
