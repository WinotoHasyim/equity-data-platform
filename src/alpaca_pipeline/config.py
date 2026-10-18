import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_CONFIG_PATH = PROJECT_ROOT / "config.toml"


class PipelineConfig(BaseModel):
    """Non-secret settings that are safe to commit to Git."""

    symbols: tuple[str, ...]
    feed: Literal["iex"]
    data_dir: Path


def load_pipeline_config() -> PipelineConfig:
    """Load non-secret pipeline configuration from the project TOML file."""

    with PROJECT_CONFIG_PATH.open("rb") as config_file:
        config = tomllib.load(config_file)
    return PipelineConfig.model_validate(config["pipeline"])


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
