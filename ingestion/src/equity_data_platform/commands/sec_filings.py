"""CLI for SEC filing metadata batches."""

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
from equity_data_platform.sources.sec.filings import (
    SecFilingsBatchRequest,
    execute_sec_filings_batch,
)

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="equity-ingest sec-filings")
    parser.add_argument("--start-date", required=True, type=parse_date)
    parser.add_argument("--end-date", required=True, type=parse_date)
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--run-id", type=parse_run_id)
    parser.add_argument("--output-dir", type=Path)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    if not settings.sec_user_agent:
        raise ValueError(
            "SEC_USER_AGENT must identify the application owner and contact email"
        )
    request = SecFilingsBatchRequest(
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=normalize_symbols(args.symbols or list(settings.symbols)),
    )
    result = execute_sec_filings_batch(
        request=request,
        run_id=args.run_id or str(uuid.uuid4()),
        output_root=(args.output_dir or settings.data_dir).resolve(),
        user_agent=settings.sec_user_agent,
    )
    LOGGER.info(
        "Published SEC filings status=%s rows=%s", result.status, result.row_count
    )
    return 0


def main() -> None:
    configure_logging()
    try:
        raise SystemExit(run())
    except SystemExit:
        raise
    except Exception:
        LOGGER.exception("SEC filing pipeline failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
