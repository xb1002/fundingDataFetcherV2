import argparse
import csv
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

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


@dataclass
class CacheFileInfo:
    path: Path
    start_ms: int
    end_ms: int
    timeframe: Optional[str] = None


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


def read_ohlcv_csv(path: Path) -> List[Tuple[int, float, float, float, float, float]]:
    records: List[Tuple[int, float, float, float, float, float]] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        next(reader, None)  # skip header if present
        for row in reader:
            if len(row) < 6:
                continue
            try:
                records.append(
                    (
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5]),
                    )
                )
            except ValueError:
                continue
    return records


def read_funding_csv(path: Path) -> List[Tuple[int, float]]:
    records: List[Tuple[int, float]] = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for row in reader:
            if len(row) < 2:
                continue
            try:
                records.append((int(row[0]), float(row[1])))
            except ValueError:
                continue
    return records


def load_cached_dataset(
    base_dir: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    timeframe: Optional[str],
    start_ms: int,
    end_ms: int,
) -> Tuple[List[Tuple], List[Tuple[int, int]], List[Path]]:
    cached_records: List[Tuple] = []
    cached_ranges: List[Tuple[int, int]] = []
    cached_files: List[Path] = []
    for info in discover_cache_files(base_dir, exchange, data_type, symbol, timeframe):
        if not ranges_overlap(info.start_ms, info.end_ms, start_ms, end_ms):
            continue
        cached_files.append(info.path)
        records = (
            read_funding_csv(info.path)
            if data_type == "funding_rate"
            else read_ohlcv_csv(info.path)
        )
        if not records:
            continue
        cached_records.extend(records)
        cached_ranges.append((max(start_ms, info.start_ms), min(end_ms, info.end_ms)))
    return cached_records, cached_ranges, cached_files


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
) -> List[Tuple[int, float, float, float, float, float]]:
    fetcher = getattr(data_client, method_name)
    downloaded: List[Tuple[int, float, float, float, float, float]] = []
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
            downloaded.extend(filtered)
            time.sleep(random.uniform(0.1, 0.2))
            progress_ts = max(row[0] for row in batch)
            if progress_ts <= cursor:
                cursor += timeframe_ms * max(1, len(batch))
            else:
                cursor = progress_ts + timeframe_ms
    return downloaded


def download_funding_batches(
    exchange: str,
    symbol: str,
    ranges: Sequence[Tuple[int, int]],
    max_limit: int,
) -> List[Tuple[int, float]]:
    downloaded: List[Tuple[int, float]] = []
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
            downloaded.extend(filtered)
            time.sleep(random.uniform(0.1, 0.2))
            progress_ts = max(row[0] for row in batch)
            if progress_ts <= cursor:
                cursor += 1
            else:
                cursor = progress_ts + 1
    return downloaded


def merge_records(
    cached: List[Tuple],
    downloaded: List[Tuple],
) -> List[Tuple]:
    combined: Dict[int, Tuple] = {}
    for record in cached:
        ts = record[0]
        combined[ts] = record
    for record in downloaded:
        ts = record[0]
        combined[ts] = record
    return [combined[key] for key in sorted(combined.keys())]


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


def write_dataset(path: Path, data_type: str, records: List[Tuple]) -> None:
    if not records:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if data_type == "funding_rate":
            writer.writerow(["funding_time", "funding_rate"])
        else:
            writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for row in records:
            writer.writerow(row)


def process_dataset(
    base_dir: Path,
    exchange: str,
    data_type: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    allow_from_cache: bool,
) -> Dict[str, object]:
    config = DATA_TYPE_CONFIG[data_type]
    adapter = data_client._get_adapter(exchange)
    max_limit = resolve_max_limit(adapter, config["limit_key"])
    cached_records: List[Tuple] = []
    cached_ranges: List[Tuple[int, int]] = []
    cached_files: List[Path] = []
    if allow_from_cache:
        cached_records, cached_ranges, cached_files = load_cached_dataset(
            base_dir,
            exchange,
            data_type,
            symbol,
            timeframe if config["is_ohlcv"] else None,
            start_ms,
            end_ms,
        )
    missing_ranges = (
        subtract_ranges(start_ms, end_ms, cached_ranges)
        if allow_from_cache
        else [(start_ms, end_ms)]
    )

    downloaded: List[Tuple] = []
    if config["is_ohlcv"]:
        timeframe_ms = get_timeframe_ms(timeframe)
        ranges = adjust_for_timeframe(missing_ranges, timeframe_ms)
        if ranges:
            downloaded = download_ohlcv_batches(
                exchange,
                config["method"],
                symbol,
                timeframe,
                ranges,
                max_limit,
                timeframe_ms,
            )
    else:
        if missing_ranges:
            downloaded = download_funding_batches(
                exchange,
                symbol,
                missing_ranges,
                max_limit,
            )

    combined = merge_records(cached_records, downloaded)

    result: Dict[str, object] = {
        "exchange": exchange,
        "data_type": data_type,
        "used_cached": len(cached_records),
        "downloaded": len(downloaded),
        "written": len(combined),
    }

    if not combined:
        logging.warning(
            "No data available for %s %s between %s and %s",
            exchange,
            data_type,
            start_ms,
            end_ms,
        )
        return result

    combined_start = combined[0][0]
    combined_end = combined[-1][0]
    result["output_start"] = combined_start
    result["output_end"] = combined_end

    output_path = build_output_path(
        base_dir,
        exchange,
        data_type,
        symbol,
        timeframe if config["is_ohlcv"] else None,
        combined_start,
        combined_end,
    )
    write_dataset(output_path, data_type, combined)
    for old_path in cached_files:
        if old_path.resolve() == output_path.resolve():
            continue
        try:
            old_path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:  # pragma: no cover
            logging.warning("Failed to delete old cache file %s: %s", old_path, exc)
    result["output_path"] = str(output_path)
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
) -> List[Dict[str, object]]:
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
            )
            exchange_results.append(result)
            logging.info(
                "[%s][%s] cached=%s downloaded=%s written=%s",
                exchange,
                data_type,
                result.get("used_cached"),
                result.get("downloaded"),
                result.get("written"),
            )
            if "output_path" in result:
                logging.info(
                    "[%s][%s] saved to %s (range %s-%s)",
                    exchange,
                    data_type,
                    result["output_path"],
                    result.get("output_start"),
                    result.get("output_end"),
                )
        except Exception as exc:  # pragma: no cover
            logging.exception("Failed processing %s %s: %s", exchange, data_type, exc)
    return exchange_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download unified exchange data for a single symbol.")
    parser.add_argument("--symbol", required=True, help="Unified symbol, e.g. BTC_USDT")
    parser.add_argument("--start-time", "--start_time", dest="start_time", required=True, help="Start time in format YYYY-MM-DD_HH:MM:SS")
    parser.add_argument("--end-time", "--end_time", dest="end_time", required=True, help="End time in format YYYY-MM-DD_HH:MM:SS")
    parser.add_argument("--timezone", default=None, help="Timezone name, e.g. UTC or Asia/Shanghai (default: local timezone)")
    parser.add_argument("--exchanges", default=None, help="Comma separated exchange list (default: all supported)")
    parser.add_argument("--data-types", "--data_types", dest="data_types", default=None, help="Comma separated data type list (default: all supported)")
    parser.add_argument("--timeframe", default="1m", help="Timeframe for OHLCV data (default: 1m)")
    parser.add_argument("--allow-from-cache", "--allow_from_cache", dest="allow_from_cache", type=parse_bool, default=True, help="Allow reusing cached files (default: true)")
    parser.add_argument("--output-dir", "--output_dir", dest="output_dir", default=os.path.join(".", "data"), help="Output directory (default: ./data/)")
    parser.add_argument("--log-level", "--log_level", dest="log_level", default="INFO", help="Logging level (default: INFO)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")

    try:
        tzinfo = resolve_timezone(args.timezone)
    except ValueError as exc:
        raise SystemExit(str(exc))

    try:
        start_ms = to_utc_millis(args.start_time, tzinfo)
        end_ms = to_utc_millis(args.end_time, tzinfo)
    except ValueError as exc:
        raise SystemExit(f"Invalid datetime format: {exc}")

    if start_ms > end_ms:
        raise SystemExit("start_time must be earlier than or equal to end_time")

    try:
        exchanges = parse_exchanges(args.exchanges)
        data_types = parse_data_types(args.data_types)
    except ValueError as exc:
        raise SystemExit(str(exc))

    symbol = args.symbol
    base_dir = Path(args.output_dir).resolve()

    any_ohlcv = any(DATA_TYPE_CONFIG[dt]["is_ohlcv"] for dt in data_types)
    if any_ohlcv:
        try:
            get_timeframe_ms(args.timeframe)
        except ValueError as exc:
            raise SystemExit(str(exc))

    logging.info(
        "Starting download: symbol=%s, exchanges=%s, data_types=%s, timeframe=%s, range=%s-%s, cache=%s",
        symbol,
        ",".join(exchanges),
        ",".join(data_types),
        args.timeframe,
        start_ms,
        end_ms,
        args.allow_from_cache,
    )

    results: List[Dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=len(exchanges)) as executor:
        future_map = {
            executor.submit(
                process_exchange,
                base_dir,
                exchange,
                data_types,
                symbol,
                args.timeframe,
                start_ms,
                end_ms,
                args.allow_from_cache,
            ): exchange
            for exchange in exchanges
        }
        for future in as_completed(future_map):
            exchange = future_map[future]
            try:
                exchange_results = future.result()
                results.extend(exchange_results)
            except Exception as exc:  # pragma: no cover
                logging.exception("Exchange %s failed: %s", exchange, exc)

    logging.info("Download finished.")


if __name__ == "__main__":
    main()
