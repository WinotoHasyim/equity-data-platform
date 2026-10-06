from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    alpaca_api_key: SecretStr = Field(
        validation_alias="APCA_API_KEY_ID"
    )
    alpaca_secret_key: SecretStr = Field(
        validation_alias="APCA_API_SECRET_KEY"
    )

    symbols: tuple[str, ...] = Field(
        default=("AAPL", "MSFT", "NVDA", "AMZN", "META"),
        validation_alias="ALPACA_SYMBOLS",
    )
    feed: Literal["iex"] = Field(
        default="iex",
        validation_alias="ALPACA_FEED",
    )
    data_dir: Path = Field(
        default=Path("data"),
        validation_alias="DATA_DIR",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()