"""Central configuration. Every knob lives here and is overridable via env."""

from __future__ import annotations

from datetime import time
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

MARKET_TZ = ZoneInfo("America/New_York")
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # data provider
    massive_api_key: str = ""
    massive_rest_url: str = "https://api.polygon.io"
    massive_ws_stocks_url: str = "wss://socket.polygon.io/stocks"
    massive_ws_options_url: str = "wss://socket.polygon.io/options"
    massive_feed_mode: str = "delayed"

    # brain
    anthropic_api_key: str = ""
    anthropic_model: str = ""
    anthropic_fast_model: str = ""

    # universe
    primary_symbol: str = "QQQ"
    leader_symbols: list[str] = Field(
        default_factory=lambda: [
            "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
            "META", "AVGO", "TSLA", "NFLX", "AMD",
        ]
    )

    # operating limits — soft by design, see brain/rails.py
    max_trades_per_day: int = 2
    max_open_positions: int = 1
    daily_loss_circuit_breaker_pct: float = 25.0
    min_target_return_pct: float = 50.0
    last_entry_time_et: str = "15:15"
    max_data_age_sec: int = 120

    # attention engine (cost gate, NOT a decision gate)
    attention_threshold: float = 0.45
    attention_cooldown_sec: int = 180

    # storage
    data_dir: Path = Path("./var/data")
    journal_dir: Path = Path("./var/journal")
    playbook_path: Path = Path("./src/qqq_alpha/brain/playbook.yaml")

    # delivery — personal chat during the shadow period, subscriber bots later
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    log_level: str = "INFO"

    @field_validator("leader_symbols", mode="before")
    @classmethod
    def _split_symbols(cls, value: object) -> object:
        if isinstance(value, str):
            return [s.strip().upper() for s in value.split(",") if s.strip()]
        return value

    @property
    def last_entry_time(self) -> time:
        hour, minute = self.last_entry_time_et.split(":")
        return time(int(hour), int(minute))

    @property
    def tracked_symbols(self) -> list[str]:
        return [self.primary_symbol, *self.leader_symbols]

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.journal_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_dirs()
    return settings
