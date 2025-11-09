import argparse
import logging
import os
from pathlib import Path

from fundingDataFetchPackage.fundingDataFetchPackage.dataFetchApi import fetch_symbol_data, parse_bool


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
    parser.add_argument(
        "--flush-threshold-bytes",
        "--flush_threshold_bytes",
        dest="flush_threshold_bytes",
        type=int,
        default=10 * 1024 * 1024,
        help="Flush partial downloads to disk once buffered CSV size exceeds this threshold in bytes (default: 10485760).",
    )
    parser.add_argument("--log-level", "--log_level", dest="log_level", default="INFO", help="Logging level (default: INFO)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")

    try:
        fetch_symbol_data(
            symbol=args.symbol,
            start_time=args.start_time,
            end_time=args.end_time,
            timezone=args.timezone,
            exchanges=args.exchanges,
            data_types=args.data_types,
            timeframe=args.timeframe,
            allow_from_cache=args.allow_from_cache,
            output_dir=Path(args.output_dir),
            flush_threshold_bytes=args.flush_threshold_bytes,
        )
    except ValueError as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
