"""CLI for bounded historical Alpaca trade and quote backfills."""

from __future__ import annotations

import argparse
import logging
import uuid
from collections.abc import Sequence
from pathlib import Path

from equity_data_platform.commands.daily_bars import (
    configure_logging,
    parse_date,
    parse_run_id,
)
from equity_data_platform.config import get_settings, normalize_symbols
from equity_data_platform.sources.alpaca.clients import create_historical_client
from equity_data_platform.sources.alpaca.market_events import (
    MarketEventsBatchRequest,
    execute_market_events_batch,
)

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="equity-ingest market-events",
        description="Extract and publish bounded Alpaca trades or quotes.",
    )
    parser.add_argument("event_type", choices=("trades", "quotes"))
    parser.add_argument("--start-date", required=True, type=parse_date)
    parser.add_argument("--end-date", required=True, type=parse_date)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--run-id", type=parse_run_id)
    parser.add_argument("--output-dir", type=Path)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    request = MarketEventsBatchRequest(
        event_type=args.event_type,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=normalize_symbols([args.symbol]),
        feed=settings.feed,
    )
    run_id = args.run_id or str(uuid.uuid4())
    result = execute_market_events_batch(
        request=request,
        run_id=run_id,
        output_root=(args.output_dir or settings.data_dir).resolve(),
        historical_client=create_historical_client(settings),
    )
    LOGGER.info(
        "Published dataset=%s status=%s rows=%s output=%s",
        request.event_type,
        result.status,
        result.row_count,
        result.output_dir,
    )
    return 0


def main() -> None:
    configure_logging()
    try:
        raise SystemExit(run())
    except SystemExit:
        raise
    except Exception:
        LOGGER.exception("Historical market-event pipeline failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
