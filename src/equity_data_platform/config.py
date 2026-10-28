import re
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_CONFIG_PATH = PROJECT_ROOT / "config.toml"
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")


def normalize_symbols(symbols: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    """Normalize a symbol collection while preserving its original order."""

    normalized: list[str] = []
    seen: set[str] = set()

    for raw_symbol in symbols:
        symbol = raw_symbol.strip().upper()
        if not SYMBOL_PATTERN.fullmatch(symbol):
            raise ValueError(f"Invalid stock symbol: {raw_symbol!r}")
        if symbol not in seen:
            normalized.append(symbol)
            seen.add(symbol)

    if not normalized:
        raise ValueError("At least one stock symbol is required")

    return tuple(normalized)


class PipelineConfig(BaseModel):
    """Non-secret settings that are safe to commit to Git."""

    symbols: tuple[str, ...]
    feed: Literal["iex"]
    data_dir: Path

    @field_validator("symbols", mode="before")
    @classmethod
    def validate_symbols(cls, value: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        return normalize_symbols(value)


def load_pipeline_config() -> PipelineConfig:
    """Load non-secret pipeline configuration from the project TOML file."""

    with PROJECT_CONFIG_PATH.open("rb") as config_file:
        config = tomllib.load(config_file)
    pipeline_config = PipelineConfig.model_validate(config["pipeline"])
    if not pipeline_config.data_dir.is_absolute():
        pipeline_config.data_dir = PROJECT_ROOT / pipeline_config.data_dir
    return pipeline_config


PIPELINE_CONFIG = load_pipeline_config()


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        validate_default=True,
    )

    alpaca_api_key: SecretStr = Field(validation_alias="APCA_API_KEY_ID")
    alpaca_secret_key: SecretStr = Field(validation_alias="APCA_API_SECRET_KEY")

    symbols: tuple[str, ...] = PIPELINE_CONFIG.symbols
    feed: Literal["iex"] = PIPELINE_CONFIG.feed
    data_dir: Path = PIPELINE_CONFIG.data_dir


@lru_cache
def get_settings() -> Settings:
    return Settings()
