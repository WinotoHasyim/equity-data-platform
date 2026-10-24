from alpaca.data.historical import StockHistoricalDataClient

from equity_data_platform.config import Settings, get_settings


def create_historical_client(
    settings: Settings | None = None,
) -> StockHistoricalDataClient:
    settings = settings or get_settings()

    return StockHistoricalDataClient(
        api_key=settings.alpaca_api_key.get_secret_value(),
        secret_key=settings.alpaca_secret_key.get_secret_value(),
    )
