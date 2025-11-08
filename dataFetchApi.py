import argparse
import csv
import io
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from dataFetcher.dataClient import EXCHANGE_ADAPTERS, data_client
from dataFetcher.dto import FundingRequestParams, OHLCVRequestParams


DATA_TYPE_CONFIG: Dict[str, Dict[str, object]] = {
    "price_ohlcv": {
        "method": "fetch_price_ohlcv",
        "limit_key": "price_ohlcv",
        "is_ohlcv": True,
    },
    "index_ohlcv": {
        "method": "fetch_index_ohlcv",
        "limit_key": "index_ohlcv",
        "is_ohlcv": True,
    },
    "premium_index_ohlcv": {
        "method": "fetch_premium_index_ohlcv",
        "limit_key": "premium_index_ohlcv",
        "is_ohlcv": True,
    },
    "funding_rate": {
        "method": "fetch_funding_history",
        "limit_key": "funding_history",
        "is_ohlcv": False,
    },
}

TIMEFRAME_TO_MS: Dict[str, int] = {
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

DEFAULT_FLUSH_THRESHOLD_BYTES = 10 * 1024 * 1024


@dataclass
class CacheFileInfo:
    path: Path
    start_ms: int
    end_ms: int
    timeframe: Optional[str] = None


def get_output_directory(
    base_dir: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    timeframe: Optional[str],
) -> Path:
    if data_type == "funding_rate":
        directory = base_dir / exchange / "funding_rate" / symbol
    else:
        if timeframe is None:
            raise ValueError("Timeframe is required for OHLCV data.")
        directory = base_dir / exchange / data_type / symbol / timeframe
    directory.mkdir(parents=True, exist_ok=True)
    return directory


class RangeFileWriter:
    def __init__(
        self,
        base_dir: Path,
        exchange: str,
        data_type: str,
        symbol: str,
        timeframe: Optional[str],
        range_start: int,
        range_end: int,
        flush_threshold: int,
        chunk_logger: Optional[Callable[[Dict[str, object]], None]] = None,
    ) -> None:
        self.base_dir = base_dir
        self.exchange = exchange
        self.data_type = data_type
        self.symbol = symbol
        self.timeframe = timeframe
        self.range_start = range_start
        self.range_end = range_end
        self.flush_threshold = max(int(flush_threshold), 1)

        self.directory = get_output_directory(base_dir, exchange, data_type, symbol, timeframe)

        self.current_rows: List[Tuple] = []
        self.current_bytes: int = 0
        self.current_start: Optional[int] = None
        self.current_end: Optional[int] = None
        self.total_rows: int = 0
        self.chunks: List[Dict[str, object]] = []
        self.chunk_logger = chunk_logger

    def _row_byte_size(self, row: Tuple) -> int:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(row)
        return len(buf.getvalue().encode("utf-8"))

    def _write_header(self, writer: csv.writer) -> None:
        if self.data_type == "funding_rate":
            writer.writerow(["funding_time", "funding_rate"])
        else:
            writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])

    def _finalize_chunk(self) -> None:
        if not self.current_rows:
            return
        start_ts = self.current_start
        end_ts = self.current_end
        if start_ts is None or end_ts is None:
            return

        final_path = build_output_path(
            self.base_dir,
            self.exchange,
            self.data_type,
            self.symbol,
            self.timeframe,
            start_ts,
            end_ts,
        )
        if final_path.exists():
            try:
                final_path.unlink()
            except OSError:
                pass
        with final_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            self._write_header(writer)
            writer.writerows(self.current_rows)

        rows_written = len(self.current_rows)
        self.total_rows += rows_written
        chunk_index = len(self.chunks) + 1
        chunk_info = {
            "path": str(final_path),
            "rows": rows_written,
            "start": start_ts,
            "end": end_ts,
            "chunk_index": chunk_index,
        }
        self.chunks.append(chunk_info)
        if self.chunk_logger:
            try:
                self.chunk_logger(chunk_info)
            except Exception as exc:  # pragma: no cover
                logging.debug("Chunk logger raised %s", exc)

        self.current_rows = []
        self.current_bytes = 0
        self.current_start = None
        self.current_end = None

    def add_rows(self, rows: List[Tuple]) -> None:
        for row in rows:
            if self.current_start is None:
                self.current_start = row[0]
            self.current_end = row[0]
            self.current_rows.append(row)
            self.current_bytes += self._row_byte_size(row)
            if self.current_bytes >= self.flush_threshold:
                self._finalize_chunk()

    def finalize(self) -> None:
        self._finalize_chunk()

    def get_stats(self) -> Tuple[int, Optional[int], Optional[int]]:
        if not self.chunks:
            return 0, None, None
        start_candidates = [chunk.get("start") for chunk in self.chunks if chunk.get("start") is not None]
        end_candidates = [chunk.get("end") for chunk in self.chunks if chunk.get("end") is not None]
        start = min(start_candidates) if start_candidates else None
        end = max(end_candidates) if end_candidates else None
        return self.total_rows, start, end

    def get_chunks(self) -> List[Dict[str, object]]:
        return list(self.chunks)


def parse_bool(value: str) -> bool:
    if isinstance(value, bool):
        return value
    lowered = value.lower()
    if lowered in {"1", "true", "t", "yes", "y"}:
        return True
    if lowered in {"0", "false", "f", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def resolve_timezone(tz_name: Optional[str]) -> timezone:
    if not tz_name:
        tz = datetime.now().astimezone().tzinfo
        return tz or timezone.utc
    try:
        return ZoneInfo(tz_name)
    except Exception as exc:
        raise ValueError(f"Invalid timezone '{tz_name}': {exc}") from exc


def to_utc_millis(dt_str: str, tzinfo: timezone) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d_%H:%M:%S")
    dt = dt.replace(tzinfo=tzinfo)
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def parse_exchanges(arg_value: Optional[str]) -> List[str]:
    if not arg_value:
        return list(EXCHANGE_ADAPTERS.keys())
    exchanges = [item.strip().lower() for item in arg_value.split(",") if item.strip()]
    unknown = [item for item in exchanges if item not in EXCHANGE_ADAPTERS]
    if unknown:
        available = ", ".join(sorted(EXCHANGE_ADAPTERS.keys()))
        raise ValueError(f"Unsupported exchanges: {', '.join(unknown)}. Available: {available}")
    return exchanges


def parse_data_types(arg_value: Optional[str]) -> List[str]:
    supported = list(DATA_TYPE_CONFIG.keys())
    if not arg_value:
        return supported
    data_types = [item.strip() for item in arg_value.split(",") if item.strip()]
    unknown = [item for item in data_types if item not in DATA_TYPE_CONFIG]
    if unknown:
        raise ValueError(f"Unsupported data types: {', '.join(unknown)}. Available: {', '.join(supported)}")
    return data_types


def get_markets_cache_path(base_dir: Path, exchange: str) -> Path:
    return base_dir / exchange / "markets.csv"


def read_cached_markets(cache_path: Path) -> Optional[List[str]]:
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("r", newline="", encoding="utf-8") as fp:
            reader = csv.reader(fp)
            markets: List[str] = []
            for row in reader:
                if not row:
                    continue
                symbol = row[0].strip()
                if not symbol or symbol.lower() == "symbol":
                    continue
                markets.append(symbol.upper())
        if markets:
            return markets
        return None
    except Exception as exc:  # pragma: no cover
        logging.warning("Failed to read cached markets from %s: %s", cache_path, exc)
        return None


def save_markets(cache_path: Path, markets: Sequence[str]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    unique_sorted = sorted({symbol.upper() for symbol in markets})
    with cache_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(["symbol"])
        for symbol in unique_sorted:
            writer.writerow([symbol])


def ensure_exchange_markets(
    base_dir: Path,
    exchange: str,
    allow_from_cache: bool,
) -> Tuple[List[str], Path]:
    cache_path = get_markets_cache_path(base_dir, exchange)
    markets: Optional[List[str]] = None
    if allow_from_cache:
        markets = read_cached_markets(cache_path)
        if markets is not None:
            logging.info(
                "[%s] Loaded %d markets from cache %s",
                exchange,
                len(markets),
                cache_path,
            )
            return markets, cache_path

    logging.info("[%s] Fetching markets from exchange API", exchange)
    fetched = data_client.fetch_markets(exchange)
    markets = sorted({symbol.upper() for symbol in fetched})
    if not markets:
        raise ValueError(f"No markets returned for exchange '{exchange}'")
    save_markets(cache_path, markets)
    logging.info(
        "[%s] Saved %d markets to %s",
        exchange,
        len(markets),
        cache_path,
    )
    return markets, cache_path


def get_timeframe_ms(timeframe: str) -> int:
    if timeframe not in TIMEFRAME_TO_MS:
        supported = ", ".join(sorted(TIMEFRAME_TO_MS.keys()))
        raise ValueError(f"Unsupported timeframe '{timeframe}'. Supported: {supported}")
    return TIMEFRAME_TO_MS[timeframe]


def ranges_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start <= b_end and b_start <= a_end


def merge_ranges(ranges: Iterable[Tuple[int, int]]) -> List[Tuple[int, int]]:
    sorted_ranges = sorted(ranges, key=lambda item: item[0])
    merged: List[Tuple[int, int]] = []
    for start, end in sorted_ranges:
        if not merged:
            merged.append((start, end))
            continue
        last_start, last_end = merged[-1]
        if start <= last_end + 1:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def subtract_ranges(total_start: int, total_end: int, coverage: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if total_start > total_end:
        return []
    relevant = [
        (max(total_start, start), min(total_end, end))
        for start, end in coverage
        if ranges_overlap(start, end, total_start, total_end)
    ]
    merged = merge_ranges(relevant)
    missing: List[Tuple[int, int]] = []
    cursor = total_start
    for start, end in merged:
        if cursor < start:
            missing.append((cursor, start - 1))
        cursor = max(cursor, end + 1)
        if cursor > total_end:
            break
    if cursor <= total_end:
        missing.append((cursor, total_end))
    return missing


def align_up(value: int, step: int) -> int:
    if value % step == 0:
        return value
    return ((value // step) + 1) * step


def align_down(value: int, step: int) -> int:
    return (value // step) * step


def adjust_for_timeframe(ranges: Sequence[Tuple[int, int]], timeframe_ms: int) -> List[Tuple[int, int]]:
    adjusted: List[Tuple[int, int]] = []
    for start, end in ranges:
        aligned_start = align_up(start, timeframe_ms)
        aligned_end = align_down(end, timeframe_ms)
        if aligned_start <= aligned_end:
            adjusted.append((aligned_start, aligned_end))
    return adjusted


def estimate_points(ranges: Sequence[Tuple[int, int]], step_ms: int) -> int:
    total = 0
    for start, end in ranges:
        if end < start:
            continue
        total += ((end - start) // step_ms) + 1
    return total


def parse_cache_filename(filename: str, data_type: str) -> Optional[CacheFileInfo]:
    stem = filename[:-4] if filename.endswith(".csv") else filename
    if data_type == "funding_rate":
        try:
            prefix, start_str, end_str = stem.rsplit("_", 2)
            symbol_part, tag = prefix.rsplit("_", 1)
            if tag != "funding":
                return None
            return CacheFileInfo(
                path=Path(),
                start_ms=int(start_str),
                end_ms=int(end_str),
                timeframe=None,
            )
        except ValueError:
            return None
    else:
        try:
            _, timeframe, start_str, end_str = stem.rsplit("_", 3)
            return CacheFileInfo(
                path=Path(),
                start_ms=int(start_str),
                end_ms=int(end_str),
                timeframe=timeframe,
            )
        except ValueError:
            return None


def discover_cache_files(
    base_dir: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    timeframe: Optional[str],
) -> List[CacheFileInfo]:
    if data_type == "funding_rate":
        target_dir = base_dir / exchange / "funding_rate" / symbol
    else:
        if timeframe is None:
            return []
        target_dir = base_dir / exchange / data_type / symbol / timeframe

    if not target_dir.exists():
        return []

    cache_files: List[CacheFileInfo] = []
    for file_path in target_dir.iterdir():
        if not file_path.name.endswith(".csv"):
            continue
        meta = parse_cache_filename(file_path.name, data_type)
        if not meta:
            continue
        if data_type != "funding_rate" and timeframe and meta.timeframe != timeframe:
            continue
        cache_files.append(
            CacheFileInfo(
                path=file_path,
                start_ms=meta.start_ms,
                end_ms=meta.end_ms,
                timeframe=meta.timeframe,
            )
        )
    return cache_files


def collect_cached_ranges(
    base_dir: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    timeframe: Optional[str],
) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    for info in discover_cache_files(base_dir, exchange, data_type, symbol, timeframe):
        ranges.append((info.start_ms, info.end_ms))
    return ranges


def resolve_max_limit(adapter, key: str, default: int = 1000) -> int:
    limits = getattr(adapter, "req_max_limit", None)
    if isinstance(limits, dict) and key in limits:
        return int(limits[key])
    return default


def download_ohlcv_batches(
    exchange: str,
    method_name: str,
    symbol: str,
    timeframe: str,
    ranges: Sequence[Tuple[int, int]],
    max_limit: int,
    timeframe_ms: int,
    chunk_cb: Optional[Callable[[List[Tuple[int, float, float, float, float, float]]], None]] = None,
    progress_cb: Optional[Callable[[int, Optional[int], Optional[int], int], None]] = None,
    expected_total: Optional[int] = None,
) -> int:
    fetcher = getattr(data_client, method_name)
    total_downloaded = 0
    for range_start, range_end in ranges:
        cursor = range_start
        while cursor <= range_end:
            remaining = ((range_end - cursor) // timeframe_ms) + 1
            limit = min(max_limit, remaining)
            params = OHLCVRequestParams(
                symbol=symbol,
                timeframe=timeframe,
                start_time=cursor,
                limit=limit,
            )
            try:
                batch = fetcher(exchange, params)
            except Exception as exc:  # pragma: no cover
                logging.error(
                    "Failed to fetch OHLCV data for %s %s [%s]: %s",
                    exchange,
                    symbol,
                    method_name,
                    exc,
                )
                break
            if not batch:
                logging.info(
                    "No OHLCV data returned for %s %s %s starting at %s",
                    exchange,
                    symbol,
                    timeframe,
                    cursor,
                )
                break
            filtered = [row for row in batch if range_start <= row[0] <= range_end]
            if filtered:
                total_downloaded += len(filtered)
                if chunk_cb:
                    chunk_cb(filtered)
            if progress_cb:
                latest_ts: Optional[int] = None
                if filtered:
                    latest_ts = filtered[-1][0]
                elif batch:
                    latest_ts = batch[-1][0]
                progress_cb(total_downloaded, expected_total, latest_ts, len(filtered))
            time.sleep(random.uniform(0.1, 0.2))
            progress_ts = max(row[0] for row in batch)
            if progress_ts <= cursor:
                cursor += timeframe_ms * max(1, len(batch))
            else:
                cursor = progress_ts + timeframe_ms
    return total_downloaded


def download_funding_batches(
    exchange: str,
    symbol: str,
    ranges: Sequence[Tuple[int, int]],
    max_limit: int,
    chunk_cb: Optional[Callable[[List[Tuple[int, float]]], None]] = None,
    progress_cb: Optional[Callable[[int, Optional[int], Optional[int], int], None]] = None,
    expected_total: Optional[int] = None,
) -> int:
    total_downloaded = 0
    for range_start, range_end in ranges:
        cursor = range_start
        while cursor <= range_end:
            params = FundingRequestParams(
                symbol=symbol,
                start_time=cursor,
                limit=max_limit,
            )
            try:
                batch = data_client.fetch_funding_history(exchange, params)
            except Exception as exc:  # pragma: no cover
                logging.error(
                    "Failed to fetch funding data for %s %s: %s",
                    exchange,
                    symbol,
                    exc,
                )
                break
            if not batch:
                logging.info(
                    "No funding data returned for %s %s starting at %s",
                    exchange,
                    symbol,
                    cursor,
                )
                break
            filtered = [row for row in batch if range_start <= row[0] <= range_end]
            if filtered:
                total_downloaded += len(filtered)
                if chunk_cb:
                    chunk_cb(filtered)
            if progress_cb:
                latest_ts: Optional[int] = None
                if filtered:
                    latest_ts = filtered[-1][0]
                elif batch:
                    latest_ts = batch[-1][0]
                progress_cb(total_downloaded, expected_total, latest_ts, len(filtered))
            time.sleep(random.uniform(0.1, 0.2))
            progress_ts = max(row[0] for row in batch)
            if progress_ts <= cursor:
                cursor += 1
            else:
                cursor = progress_ts + 1
    return total_downloaded



def build_output_path(
    base_dir: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    timeframe: Optional[str],
    start_ms: int,
    end_ms: int,
) -> Path:
    if data_type == "funding_rate":
        directory = base_dir / exchange / "funding_rate" / symbol
        filename = f"{symbol}_funding_{start_ms}_{end_ms}.csv"
    else:
        if timeframe is None:
            raise ValueError("Timeframe is required for OHLCV data types.")
        directory = base_dir / exchange / data_type / symbol / timeframe
        filename = f"{symbol}_{timeframe}_{start_ms}_{end_ms}.csv"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / filename


def process_dataset(
    base_dir: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    allow_from_cache: bool,
    flush_threshold: int,
) -> Dict[str, object]:
    config = DATA_TYPE_CONFIG[data_type]
    adapter = data_client._get_adapter(exchange)
    max_limit = resolve_max_limit(adapter, config["limit_key"])

    cached_ranges: List[Tuple[int, int]] = []
    if allow_from_cache:
        cached_ranges = collect_cached_ranges(
            base_dir,
            exchange,
            data_type,
            symbol,
            timeframe if config["is_ohlcv"] else None,
        )

    missing_ranges = (
        subtract_ranges(start_ms, end_ms, cached_ranges)
        if allow_from_cache
        else [(start_ms, end_ms)]
    )

    total_downloaded = 0
    output_paths: List[str] = []
    output_ranges: List[Tuple[int, int]] = []
    range_details: List[Dict[str, object]] = []

    if not missing_ranges:
        logging.info(
            "[%s][%s] no download needed; requested range fully covered by cache",
            exchange,
            data_type,
        )

    total_segments = len(missing_ranges)

    for idx, (segment_start, segment_end) in enumerate(missing_ranges, start=1):
        def chunk_logger(details: Dict[str, object]) -> None:
            logging.info(
                "[%s][%s][segment %d/%d] saved chunk #%s to %s (rows=%s range=%s-%s)",
                exchange,
                data_type,
                idx,
                total_segments,
                details.get("chunk_index"),
                details.get("path"),
                details.get("rows"),
                details.get("start"),
                details.get("end"),
            )

        writer = RangeFileWriter(
            base_dir,
            exchange,
            data_type,
            symbol,
            timeframe if config["is_ohlcv"] else None,
            segment_start,
            segment_end,
            flush_threshold,
            chunk_logger=chunk_logger,
        )

        def chunk_handler(rows: List[Tuple]) -> None:
            writer.add_rows(rows)

        def format_ts(ts: Optional[int]) -> str:
            if ts is None:
                return "n/a"
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        if config["is_ohlcv"]:
            timeframe_ms = get_timeframe_ms(timeframe)
            aligned_ranges = adjust_for_timeframe([(segment_start, segment_end)], timeframe_ms)
            if not aligned_ranges:
                logging.info(
                    "[%s][%s][segment %d/%d] skipping empty aligned range %s-%s",
                    exchange,
                    data_type,
                    idx,
                    total_segments,
                    segment_start,
                    segment_end,
                )
                continue
            expected_total = estimate_points(aligned_ranges, timeframe_ms)
            if expected_total <= 0:
                expected_total = None

            def progress_logger(downloaded_count: int, expected: Optional[int], latest_ts: Optional[int], batch_size: int) -> None:
                if expected:
                    percent = min(100.0, (downloaded_count / expected) * 100) if expected else 0.0
                    logging.info(
                        "[%s][%s][segment %d/%d] progress %.1f%% (%d/%d) batch=%d latest=%s",
                        exchange,
                        data_type,
                        idx,
                        total_segments,
                        percent,
                        downloaded_count,
                        expected,
                        batch_size,
                        format_ts(latest_ts),
                    )
                else:
                    logging.info(
                        "[%s][%s][segment %d/%d] progress downloaded=%d batch=%d latest=%s",
                        exchange,
                        data_type,
                        idx,
                        total_segments,
                        downloaded_count,
                        batch_size,
                        format_ts(latest_ts),
                    )

            logging.info(
                "[%s][%s][segment %d/%d] downloading aligned range %s-%s%s",
                exchange,
                data_type,
                idx,
                total_segments,
                aligned_ranges[0][0],
                aligned_ranges[-1][1],
                f", expected ~{expected_total} rows" if expected_total else "",
            )
            segment_rows = download_ohlcv_batches(
                exchange,
                config["method"],
                symbol,
                timeframe,
                aligned_ranges,
                max_limit,
                timeframe_ms,
                chunk_cb=chunk_handler,
                progress_cb=progress_logger,
                expected_total=expected_total,
            )
        else:
            def progress_logger(downloaded_count: int, expected: Optional[int], latest_ts: Optional[int], batch_size: int) -> None:
                logging.info(
                    "[%s][%s][segment %d/%d] progress downloaded=%d batch=%d latest=%s",
                    exchange,
                    data_type,
                    idx,
                    total_segments,
                    downloaded_count,
                    batch_size,
                    format_ts(latest_ts),
                )

            logging.info(
                "[%s][%s][segment %d/%d] downloading range %s-%s",
                exchange,
                data_type,
                idx,
                total_segments,
                segment_start,
                segment_end,
            )
            segment_rows = download_funding_batches(
                exchange,
                symbol,
                [(segment_start, segment_end)],
                max_limit,
                chunk_cb=chunk_handler,
                progress_cb=progress_logger,
                expected_total=None,
            )

        writer.finalize()
        rows_written, range_start_ts, range_end_ts = writer.get_stats()
        total_downloaded += rows_written

        chunks = writer.get_chunks()
        if not chunks:
            logging.info(
                "[%s][%s][segment %d/%d] no data downloaded for range %s-%s",
                exchange,
                data_type,
                idx,
                total_segments,
                segment_start,
                segment_end,
            )
            continue

        for chunk in chunks:
            path = chunk.get("path")
            chunk_rows = chunk.get("rows", 0)
            chunk_start = chunk.get("start")
            chunk_end = chunk.get("end")
            output_paths.append(str(path))
            output_ranges.append(
                (
                    chunk_start if chunk_start is not None else segment_start,
                    chunk_end if chunk_end is not None else segment_end,
                )
            )
            range_details.append(chunk)

    result: Dict[str, object] = {
        "exchange": exchange,
        "data_type": data_type,
        "cached_ranges": len(cached_ranges),
        "downloaded": total_downloaded,
        "written": total_downloaded,
        "output_paths": output_paths,
        "output_ranges": output_ranges,
        "range_details": range_details,
    }
    return result


def process_exchange(
    base_dir: Path,
    exchange: str,
    data_types: Sequence[str],
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    allow_from_cache: bool,
    flush_threshold: int,
) -> List[Dict[str, object]]:
    normalized_symbol = symbol.replace("-", "_").upper()
    try:
        markets, markets_cache_path = ensure_exchange_markets(
            base_dir,
            exchange,
            allow_from_cache,
        )
    except Exception as exc:
        logging.exception("[%s] Failed to prepare market list: %s", exchange, exc)
        return []

    market_set = set(markets)
    if normalized_symbol not in market_set:
        logging.warning(
            "[%s] Symbol %s is not tradable on this exchange (markets cached at %s); skipping download",
            exchange,
            normalized_symbol,
            markets_cache_path,
        )
        return []

    exchange_results: List[Dict[str, object]] = []
    for data_type in data_types:
        try:
            result = process_dataset(
                base_dir,
                exchange,
                data_type,
                symbol,
                timeframe,
                start_ms,
                end_ms,
                allow_from_cache,
                flush_threshold,
            )
            exchange_results.append(result)
            logging.info(
                "[%s][%s] cached_ranges=%s downloaded_rows=%s chunks=%s",
                exchange,
                data_type,
                result.get("cached_ranges"),
                result.get("downloaded"),
                len(result.get("output_paths", [])),
            )
            for detail in result.get("range_details", []):
                logging.info(
                    "[%s][%s] chunk saved to %s (rows=%s range=%s-%s)",
                    exchange,
                    data_type,
                    detail.get("path"),
                    detail.get("rows"),
                    detail.get("start"),
                    detail.get("end"),
                )
        except Exception as exc:  # pragma: no cover
            logging.exception("Failed processing %s %s: %s", exchange, data_type, exc)
    return exchange_results


def fetch_symbol_data(
    symbol: str,
    start_time: str,
    end_time: str,
    *,
    timezone: Optional[str] = None,
    exchanges: Optional[str] = None,
    data_types: Optional[str] = None,
    timeframe: str = "1m",
    allow_from_cache: bool = True,
    output_dir: Optional[Path] = None,
    flush_threshold_bytes: Optional[int] = None,
    max_workers: Optional[int] = None,
) -> List[Dict[str, object]]:
    """Programmatic entry point mirroring the original getSingleData CLI workflow."""
    tzinfo = resolve_timezone(timezone)
    start_ms = to_utc_millis(start_time, tzinfo)
    end_ms = to_utc_millis(end_time, tzinfo)
    if start_ms > end_ms:
        raise ValueError("start_time must be earlier than or equal to end_time")

    exchange_list = parse_exchanges(exchanges)
    data_type_list = parse_data_types(data_types)
    any_ohlcv = any(DATA_TYPE_CONFIG[data_type]["is_ohlcv"] for data_type in data_type_list)
    if any_ohlcv:
        get_timeframe_ms(timeframe)

    base_dir = Path(output_dir) if output_dir else Path("./data")
    base_dir = base_dir.resolve()
    flush_threshold = (
        DEFAULT_FLUSH_THRESHOLD_BYTES
        if flush_threshold_bytes is None or flush_threshold_bytes <= 0
        else int(flush_threshold_bytes)
    )

    if not exchange_list:
        return []

    worker_count = max_workers or len(exchange_list)
    worker_count = max(1, min(worker_count, len(exchange_list)))

    logging.info(
        "Starting download: symbol=%s, exchanges=%s, data_types=%s, timeframe=%s, range=%s-%s, cache=%s, flush_threshold=%s bytes",
        symbol,
        ",".join(exchange_list),
        ",".join(data_type_list),
        timeframe,
        start_ms,
        end_ms,
        allow_from_cache,
        flush_threshold,
    )

    results: List[Dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                process_exchange,
                base_dir,
                exchange,
                data_type_list,
                symbol,
                timeframe,
                start_ms,
                end_ms,
                allow_from_cache,
                flush_threshold,
            ): exchange
            for exchange in exchange_list
        }
        for future in as_completed(future_map):
            exchange = future_map[future]
            try:
                exchange_results = future.result()
                results.extend(exchange_results)
            except Exception as exc:  # pragma: no cover
                logging.exception("Exchange %s failed: %s", exchange, exc)

    logging.info("Download finished.")
    return results


class DataFetchApi:
    """API for fetching exchange datasets from remote servers."""

    def __init__(
        self,
        base_dir: Union[Path, str] = "./data",
        *,
        timezone: Optional[str] = None,
        allow_from_cache: bool = True,
        flush_threshold_bytes: Optional[int] = None,
    ) -> None:
        self.base_dir = Path(base_dir).resolve()
        self.tzinfo = resolve_timezone(timezone)
        self.allow_from_cache = allow_from_cache
        self.flush_threshold = (
            DEFAULT_FLUSH_THRESHOLD_BYTES
            if flush_threshold_bytes is None or flush_threshold_bytes <= 0
            else int(flush_threshold_bytes)
        )

    def fetch_price_ohlcv(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str,
    ) -> None:
        """Fetch OHLCV price data from remote server."""
        self._fetch_data_type(
            data_type="price_ohlcv",
            exchange=exchange,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            timeframe=timeframe,
        )

    def fetch_index_ohlcv(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str,
    ) -> None:
        """Fetch OHLCV index data from remote server."""
        self._fetch_data_type(
            data_type="index_ohlcv",
            exchange=exchange,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            timeframe=timeframe,
        )

    def fetch_premium_index_ohlcv(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str,
    ) -> None:
        """Fetch OHLCV premium index data from remote server."""
        self._fetch_data_type(
            data_type="premium_index_ohlcv",
            exchange=exchange,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            timeframe=timeframe,
        )

    def fetch_funding_rate(
        self,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: str = "8h",
    ) -> None:
        """Fetch funding rate data from remote server."""
        self._fetch_data_type(
            data_type="funding_rate",
            exchange=exchange,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            timeframe=timeframe,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_data_type(
        self,
        *,
        data_type: str,
        exchange: str,
        symbol: str,
        start_time: str,
        end_time: str,
        timeframe: Optional[str],
    ) -> None:
        config = DATA_TYPE_CONFIG.get(data_type)
        if config is None:
            raise ValueError(f"Unsupported data_type '{data_type}'.")

        start_ms = to_utc_millis(start_time, self.tzinfo)
        end_ms = to_utc_millis(end_time, self.tzinfo)
        if start_ms > end_ms:
            raise ValueError("start_time must be earlier than or equal to end_time.")

        effective_timeframe = timeframe
        if config["is_ohlcv"]:
            if not effective_timeframe:
                raise ValueError("timeframe is required for OHLCV data types.")
            get_timeframe_ms(effective_timeframe)
        else:
            effective_timeframe = effective_timeframe or "8h"

        results = process_exchange(
            base_dir=self.base_dir,
            exchange=exchange,
            data_types=[data_type],
            symbol=symbol,
            timeframe=effective_timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
            allow_from_cache=self.allow_from_cache,
            flush_threshold=self.flush_threshold,
        )

        if not results:
            logging.warning(
                "[%s][%s] no data processed for %s between %s and %s",
                exchange,
                data_type,
                symbol,
                start_time,
                end_time,
            )
            return

        summary = results[0]
        logging.info(
            "[%s][%s] download complete: cached_ranges=%s downloaded_rows=%s files=%s",
            exchange,
            data_type,
            summary.get("cached_ranges"),
            summary.get("downloaded"),
            len(summary.get("output_paths", [])),
        )


__all__ = [
    "DATA_TYPE_CONFIG",
    "TIMEFRAME_TO_MS",
    "CacheFileInfo",
    "RangeFileWriter",
    "get_output_directory",
    "parse_bool",
    "resolve_timezone",
    "to_utc_millis",
    "parse_exchanges",
    "parse_data_types",
    "get_markets_cache_path",
    "read_cached_markets",
    "save_markets",
    "ensure_exchange_markets",
    "get_timeframe_ms",
    "ranges_overlap",
    "merge_ranges",
    "subtract_ranges",
    "align_up",
    "align_down",
    "adjust_for_timeframe",
    "estimate_points",
    "parse_cache_filename",
    "discover_cache_files",
    "collect_cached_ranges",
    "resolve_max_limit",
    "download_ohlcv_batches",
    "download_funding_batches",
    "build_output_path",
    "process_dataset",
    "process_exchange",
    "fetch_symbol_data",
    "DataFetchApi",
]
