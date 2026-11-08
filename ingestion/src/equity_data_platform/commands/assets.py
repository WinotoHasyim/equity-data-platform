"""Command-line entry point for Alpaca security-master snapshots."""

from __future__ import annotations

import argparse
import logging
import uuid
from collections.abc import Sequence
from pathlib import Path

from equity_data_platform.commands.daily_bars import configure_logging, parse_run_id
from equity_data_platform.config import get_settings, normalize_symbols
from equity_data_platform.sources.alpaca.assets import (
    AssetsBatchRequest,
    execute_assets_batch,
)
from equity_data_platform.sources.alpaca.clients import create_trading_client

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="equity-ingest assets",
        description="Extract and publish an Alpaca security-master snapshot.",
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
    settings = get_settings()
    symbols = normalize_symbols(args.symbols or list(settings.symbols))
    run_id = args.run_id or str(uuid.uuid4())
    output_root = (args.output_dir or settings.data_dir).resolve()
    request = AssetsBatchRequest(symbols=symbols)

    LOGGER.info(
        "Starting asset snapshot run_id=%s symbols=%s",
        run_id,
        ",".join(request.symbols),
    )
    result = execute_assets_batch(
        request=request,
        run_id=run_id,
        output_root=output_root,
        trading_client=create_trading_client(settings, raw_data=True),
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
    configure_logging()
    try:
        raise SystemExit(run())
    except SystemExit:
        raise
    except Exception:
        LOGGER.exception("Asset metadata pipeline failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
