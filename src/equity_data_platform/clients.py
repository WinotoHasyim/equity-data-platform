from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient

from equity_data_platform.config import Settings, get_settings


def create_historical_client(
    settings: Settings | None = None,
) -> StockHistoricalDataClient:
    settings = settings or get_settings()

    return StockHistoricalDataClient(
        api_key=settings.alpaca_api_key.get_secret_value(),
        secret_key=settings.alpaca_secret_key.get_secret_value(),
    )


def create_trading_client(settings: Settings | None = None) -> TradingClient:
    """Create the client used for the official US market calendar."""

    settings = settings or get_settings()
    return TradingClient(
        api_key=settings.alpaca_api_key.get_secret_value(),
        secret_key=settings.alpaca_secret_key.get_secret_value(),
        paper=True,
    )
