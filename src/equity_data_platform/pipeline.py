"""Command-line entry point for historical daily-bar backfills."""

from __future__ import annotations

import argparse
import logging
import re
import uuid
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from equity_data_platform.clients import (
    create_historical_client,
    create_trading_client,
)
from equity_data_platform.config import get_settings, normalize_symbols
from equity_data_platform.daily_bars import (
    DailyBarsBatchRequest,
    execute_daily_bars_batch,
)

LOGGER = logging.getLogger(__name__)
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def parse_run_id(value: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError(
            "run ID must be 1-128 characters using letters, numbers, '.', '_', or '-'"
        )
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="equity-data-platform",
        description="Extract and publish an atomic Alpaca historical daily-bars batch.",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_date,
        help="Inclusive extraction start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_date,
        help="Inclusive extraction end date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        help=(
            "Stock symbol to extract. Repeat to replace the config.toml universe; "
            "omit to use the configured symbols."
        ),
    )
    parser.add_argument(
        "--run-id",
        type=parse_run_id,
        help="Orchestrator execution ID. A UUID is generated when omitted.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output root. Defaults to data_dir from config.toml.",
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.start_date > args.end_date:
        raise ValueError("--start-date must not be later than --end-date")

    settings = get_settings()
    symbols = normalize_symbols(args.symbols or list(settings.symbols))
    run_id = args.run_id or str(uuid.uuid4())
    output_root = (args.output_dir or settings.data_dir).resolve()

    request = DailyBarsBatchRequest(
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=symbols,
        feed=settings.feed,
    )

    LOGGER.info(
        "Starting daily-bars batch run_id=%s dates=%s..%s symbols=%s",
        run_id,
        request.start_date,
        request.end_date,
        ",".join(request.symbols),
    )
    result = execute_daily_bars_batch(
        request=request,
        run_id=run_id,
        output_root=output_root,
        historical_client=create_historical_client(settings),
        trading_client=create_trading_client(settings),
    )
    LOGGER.info(
        "Published status=%s rows=%s output=%s batch_key=%s",
        result.status,
        result.row_count,
        result.output_dir,
        result.batch_key,
    )
    return 0


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        raise SystemExit(run())
    except SystemExit:
        raise
    except Exception:
        LOGGER.exception("Daily-bars pipeline failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
