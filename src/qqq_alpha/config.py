"""Central configuration. Every knob lives here and is overridable via env."""

from __future__ import annotations

from datetime import time
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import Field
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
    # thinking is on by default on current models and shares this budget with the
    # response, so it must be generous — a tight cap truncates mid-decision
    anthropic_max_tokens: int = 8000
    # how hard the model thinks: low | medium | high | xhigh | max.
    # a trade decision is intelligence-sensitive, so the floor is high
    anthropic_effort: str = "high"

    # universe
    primary_symbol: str = "QQQ"
    # plain str, not list[str]: pydantic-settings JSON-decodes env values for
    # complex-typed fields before any validator runs, so a comma-separated
    # LEADER_SYMBOLS would crash on boot with "Expecting value: line 1 column 1"
    leader_symbols_csv: str = Field(
        default="AAPL,MSFT,NVDA,AMZN,GOOGL,META,AVGO,TSLA,NFLX,AMD",
        validation_alias="LEADER_SYMBOLS",
    )

    # shadow stock desk — single names learning in the background. Same brain,
    # same playbook, weekly contracts, simulated fills; no signals are sent.
    # Symbols must be in LEADER_SYMBOLS (that is where their bars come from).
    # Empty string disables the desk entirely.
    shadow_symbols_csv: str = Field(
        default="NVDA,TSLA,AAPL", validation_alias="SHADOW_SYMBOLS"
    )
    # per symbol per day — the desk is a learner, not a second trading floor,
    # and every wake here is a real brain call with a real cost
    shadow_max_brain_calls_per_day: int = 3

    # operating limits — soft by design, see brain/rails.py
    # three chances a day, never three obligations. The cap that actually
    # governs risk is max_open_positions: one trade at a time, so a further
    # slot only opens once the desk is flat, and the circuit breaker still
    # closes the day on real damage regardless of slots left.
    max_trades_per_day: int = 3
    max_open_positions: int = 1
    daily_loss_circuit_breaker_pct: float = 25.0
    min_target_return_pct: float = 50.0
    last_entry_time_et: str = "15:15"
    max_data_age_sec: int = 120

    # the declared-trigger lock (brain/commitments.py): once the brain names a
    # numeric level it must wait for, that number binds the next entry in that
    # direction until it expires or the brain names a new one. Off turns the
    # check into a warning only, which is how the backtest prices its cost.
    enforce_declared_trigger: bool = True
    trigger_ttl_minutes: int = 30

    # the brain's "recent trades" list used to be read from memory only at boot
    # and at the session roll, so a trade opened and closed inside one session
    # stayed invisible to it until the next morning. Off restores that older
    # behaviour, which is the switch to flip if a session ever looks wrong
    # after this landed — it changes no rule, only what the brain is told.
    recall_todays_trades: bool = True

    # ------------------------------------------------------------------
    # Real order execution (qqq_alpha/execution). OFF, and staying off until
    # a broker has confirmed API access and the wiring has been proven against
    # their sandbox. Two separate switches on purpose: naming a broker must
    # not be enough to start trading, so credentials and going live are two
    # decisions made on two different days.
    #
    # execution_max_contracts is a refusal, not a trim — an order above it is
    # dropped and announced rather than quietly resized, because a sizing bug
    # wearing a safe number is worse than a missed trade.
    execution_enabled: bool = False
    execution_broker: str = "none"

    # Sizing is in dollars, because that is what the operator decides and what
    # a broker actually spends. Every trade aims at the same figure: the
    # engine's own conviction sizing is deliberately NOT applied here, because
    # every decision on record has come in at confidence 6, so applying it
    # would halve every trade uniformly rather than distinguish between them —
    # and a record where each trade risks the same amount is the only one that
    # can later prove whether confidence means anything at all.
    execution_dollars_per_trade: float = 1000.0
    execution_size_tolerance_pct: float = 15.0

    # NOT a sizing preference — a backstop against a bad quote. A stale price
    # of $0.05 makes the budget arithmetic ask for hundreds of contracts, and
    # no budget check catches it because the arithmetic is right and the input
    # is wrong.
    execution_max_contracts: int = 40

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
    # the public channel (e.g. "@OqoodOptions"). Empty disables channel
    # publishing entirely; the bot must be a channel admin with post rights.
    telegram_channel_id: str = ""
    # the private subscribers channel (numeric id like "-1001234567890").
    # When set, live signals are posted here ONCE — reaching every subscriber
    # instantly regardless of count — instead of being fanned out one DM at a
    # time. The bot must be an admin with post + invite + ban rights: it
    # issues each trial subscriber a single-use invite link and removes them
    # at expiry. Empty keeps the per-subscriber DM delivery.
    telegram_private_channel_id: str = ""

    # subscribers — anyone who /starts the bot gets the signals for a free
    # trial, then is pointed at the follow-up channel and cut off. Zero days
    # disables sign-ups entirely (operator-only bot).
    trial_days: int = 30
    post_trial_channel_url: str = ""
    # the monthly price quoted in the welcome pitch — advertising only until
    # the payment gateway lands; changing the env var updates every message
    subscription_price_sar: int = 199
    # one Claude-written concept lesson after the bell, both channels. Gated
    # against advice, prices, and any reveal of this desk's own management —
    # a lesson that trips the gate is dropped, not edited into compliance.
    daily_lesson_enabled: bool = True

    # admin dashboard — off unless both credentials are set, since an
    # unauthenticated view of every trade thesis and lesson is not a risk
    # worth taking by default
    admin_username: str = ""
    admin_password: str = ""
    # Railway injects PORT for whatever the container should listen on
    dashboard_port: int = Field(default=8080, validation_alias="PORT")

    log_level: str = "INFO"

    @property
    def leader_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.leader_symbols_csv.split(",") if s.strip()]

    @property
    def shadow_symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.shadow_symbols_csv.split(",") if s.strip()]

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
