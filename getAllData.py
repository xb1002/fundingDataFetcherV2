import argparse
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
import random
import time
from typing import Dict, List, Sequence, Tuple

from getSingleData import (
    DATA_TYPE_CONFIG,
    ensure_exchange_markets,
    get_timeframe_ms,
    parse_bool,
    parse_data_types,
    parse_exchanges,
    process_dataset,
    resolve_timezone,
    to_utc_millis,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download exchange data for all tradable symbols."
    )
    parser.add_argument(
        "--start-time",
        "--start_time",
        dest="start_time",
        required=True,
        help="Start time in format YYYY-MM-DD_HH:MM:SS",
    )
    parser.add_argument(
        "--end-time",
        "--end_time",
        dest="end_time",
        required=True,
        help="End time in format YYYY-MM-DD_HH:MM:SS",
    )
    parser.add_argument(
        "--timezone",
        default=None,
        help="Timezone name, e.g. UTC or Asia/Shanghai (default: local timezone)",
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma separated exchange list (default: all supported)",
    )
    parser.add_argument(
        "--data-types",
        "--data_types",
        dest="data_types",
        default=None,
        help="Comma separated data type list (default: all supported)",
    )
    parser.add_argument(
        "--timeframe",
        default="1m",
        help="Timeframe for OHLCV data (default: 1m)",
    )
    parser.add_argument(
        "--allow-from-cache",
        "--allow_from_cache",
        dest="allow_from_cache",
        type=parse_bool,
        default=True,
        help="Allow reusing cached files (default: true)",
    )
    parser.add_argument(
        "--output-dir",
        "--output_dir",
        dest="output_dir",
        default=os.path.join(".", "data"),
        help="Output directory (default: ./data/)",
    )
    parser.add_argument(
        "--flush-threshold-bytes",
        "--flush_threshold_bytes",
        dest="flush_threshold_bytes",
        type=int,
        default=10 * 1024 * 1024,
        help=(
            "Flush partial downloads to disk once buffered CSV size exceeds this "
            "threshold in bytes (default: 10485760)."
        ),
    )
    parser.add_argument(
        "--log-level",
        "--log_level",
        dest="log_level",
        default="INFO",
        help="Logging level (default: INFO)",
    )
    return parser.parse_args()


def process_symbol(
    base_dir: Path,
    exchange: str,
    symbol: str,
    data_types: Sequence[str],
    timeframe: str,
    start_ms: int,
    end_ms: int,
    allow_from_cache: bool,
    flush_threshold: int,
) -> Tuple[str, str, List[Dict[str, object]]]:
    symbol_results: List[Dict[str, object]] = []
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
            symbol_results.append(result)
            logging.info(
                "[%s][%s][%s] cached_ranges=%s downloaded_rows=%s chunks=%s",
                exchange,
                symbol,
                data_type,
                result.get("cached_ranges"),
                result.get("downloaded"),
                len(result.get("output_paths", [])),
            )
        except Exception as exc:  # pragma: no cover
            logging.exception(
                "[%s][%s][%s] failed to process dataset: %s",
                exchange,
                symbol,
                data_type,
                exc,
            )
    return exchange, symbol, symbol_results


def process_exchange_symbols(
    base_dir: Path,
    exchange: str,
    markets: Sequence[str],
    data_types: Sequence[str],
    timeframe: str,
    start_ms: int,
    end_ms: int,
    allow_from_cache: bool,
    flush_threshold: int,
) -> Dict[str, object]:
    total_symbols = len(markets)
    processed_symbols = 0
    dataset_count = 0
    total_rows = 0
    total_files = 0
    failed_symbols: List[str] = []

    logging.info("[%s] Starting downloads for %d symbols", exchange, total_symbols)

    for idx, symbol in enumerate(markets, start=1):
        logging.info(
            "[%s] Processing symbol %s (%d/%d)",
            exchange,
            symbol,
            idx,
            total_symbols,
        )
        _, _, results = process_symbol(
            base_dir,
            exchange,
            symbol,
            data_types,
            timeframe,
            start_ms,
            end_ms,
            allow_from_cache,
            flush_threshold,
        )
        processed_symbols += 1

        symbol_dataset_count = 0
        for result in results:
            symbol_dataset_count += 1
            total_rows += int(result.get("downloaded", 0) or 0)
            total_files += len(result.get("output_paths", []))

        dataset_count += symbol_dataset_count
        if symbol_dataset_count == 0:
            failed_symbols.append(symbol)
            logging.warning(
                "[%s][%s] No datasets completed; marking as failed",
                exchange,
                symbol,
            )
        delay = random.uniform(0.1, 0.2)
        logging.debug("[%s][%s] Sleeping for %.3f seconds to respect rate limits", exchange, symbol, delay)
        time.sleep(delay)

    logging.info(
        "[%s] Finished: symbols=%d datasets=%d rows=%d files=%d failed=%d",
        exchange,
        processed_symbols,
        dataset_count,
        total_rows,
        total_files,
        len(failed_symbols),
    )

    return {
        "exchange": exchange,
        "processed_symbols": processed_symbols,
        "dataset_count": dataset_count,
        "rows": total_rows,
        "files": total_files,
        "failed_symbols": failed_symbols,
    }


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

    any_ohlcv = any(DATA_TYPE_CONFIG[dt]["is_ohlcv"] for dt in data_types)
    if any_ohlcv:
        try:
            get_timeframe_ms(args.timeframe)
        except ValueError as exc:
            raise SystemExit(str(exc))

    base_dir = Path(args.output_dir).resolve()
    flush_threshold = args.flush_threshold_bytes
    if flush_threshold is None or flush_threshold <= 0:
        flush_threshold = 10 * 1024 * 1024

    logging.info(
        "Starting bulk download: exchanges=%s data_types=%s timeframe=%s range=%s-%s cache=%s output_dir=%s",
        ",".join(exchanges),
        ",".join(data_types),
        args.timeframe,
        start_ms,
        end_ms,
        args.allow_from_cache,
        base_dir,
    )

    exchange_markets: Dict[str, List[str]] = {}
    total_symbols = 0
    for exchange in exchanges:
        try:
            markets, cache_path = ensure_exchange_markets(
                base_dir,
                exchange,
                args.allow_from_cache,
            )
            if not markets:
                logging.warning("[%s] No tradable markets found; skipping exchange", exchange)
                continue
            exchange_markets[exchange] = markets
            total_symbols += len(markets)
            logging.info(
                "[%s] Prepared %d markets (cache=%s)",
                exchange,
                len(markets),
                cache_path,
            )
        except Exception as exc:  # pragma: no cover
            logging.exception("[%s] Failed to prepare markets: %s", exchange, exc)

    if not exchange_markets:
        logging.warning("No exchanges with available markets; exiting")
        return

    max_workers = len(exchange_markets)
    logging.info(
        "Dispatching downloads with %d worker thread(s) (one per exchange) covering %d symbols",
        max_workers,
        total_symbols,
    )

    future_map: Dict[Future, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for exchange, markets in exchange_markets.items():
            future = executor.submit(
                process_exchange_symbols,
                base_dir,
                exchange,
                markets,
                data_types,
                args.timeframe,
                start_ms,
                end_ms,
                args.allow_from_cache,
                flush_threshold,
            )
            future_map[future] = exchange

        overall_symbols = 0
        overall_datasets = 0
        overall_rows = 0
        overall_files = 0
        failed_symbols: List[Tuple[str, str]] = []

        for future in as_completed(future_map):
            exchange = future_map[future]
            try:
                summary = future.result()
            except Exception as exc:  # pragma: no cover
                logging.exception("[%s] Worker thread failed: %s", exchange, exc)
                failed_symbols.extend(
                    (exchange, symbol) for symbol in exchange_markets.get(exchange, [])
                )
                continue

            overall_symbols += summary.get("processed_symbols", 0)
            overall_datasets += summary.get("dataset_count", 0)
            overall_rows += summary.get("rows", 0)
            overall_files += summary.get("files", 0)
            for symbol in summary.get("failed_symbols", []):
                failed_symbols.append((exchange, symbol))

    logging.info(
        "Bulk download complete: exchanges=%d symbols=%d datasets=%d rows=%d files=%d",
        len(exchange_markets),
        overall_symbols,
        overall_datasets,
        overall_rows,
        overall_files,
    )
    if failed_symbols:
        failed_str = ", ".join(f"{ex}:{sym}" for ex, sym in failed_symbols)
        logging.warning(
            "Symbols with no successful datasets (%d): %s",
            len(failed_symbols),
            failed_str,
        )


if __name__ == "__main__":
    main()
