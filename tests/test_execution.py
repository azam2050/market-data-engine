"""The execution layer.

This is the only code in the project that can lose real money, so the tests
are weighted toward the refusals rather than the happy path: what it declines
to send, what it refuses to resend, and what it reports rather than repairs.

The single most important test in this file is the one asserting that a full
live session with execution switched off sends nothing at all.
"""

from __future__ import annotations

import pytest

from qqq_alpha.config import Settings
from qqq_alpha.execution import (
    ExecutionRouter,
    OrderRequest,
    PaperBroker,
    Side,
    build_broker,
)
from qqq_alpha.execution.types import BrokerError, BrokerOrder, BrokerPosition, OrderState
from qqq_alpha.journal import Journal


def _settings(tmp_path, **extra) -> Settings:
    settings = Settings(
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        **extra,
    )
    settings.ensure_dirs()
    return settings


def _request(**extra) -> OrderRequest:
    payload = {
        "client_order_id": "t1-entry",
        "occ_symbol": "O:QQQ260302P00711000",
        "side": Side.BUY,
        "quantity": 1,
        "limit_price": 1.25,
        "trade_id": "t1",
        "reason": "entry",
    }
    payload.update(extra)
    return OrderRequest(**payload)


class _Recorder:
    """A broker that records instead of trading, and can be told to misbehave."""

    name = "recorder"

    def __init__(self, *, fail: bool = False, state: OrderState = OrderState.FILLED):
        self.placed: list[OrderRequest] = []
        self.held: list[BrokerPosition] = []
        self._fail = fail
        self._state = state

    async def place(self, request: OrderRequest) -> BrokerOrder:
        if self._fail:
            raise BrokerError("connection reset")
        self.placed.append(request)
        return BrokerOrder(
            client_order_id=request.client_order_id,
            broker_order_id="x1",
            state=self._state,
            filled_quantity=request.quantity if self._state is OrderState.FILLED else 0,
            average_fill_price=request.limit_price,
        )

    async def cancel(self, client_order_id: str) -> BrokerOrder:
        return BrokerOrder(client_order_id=client_order_id, state=OrderState.CANCELLED)

    async def order(self, client_order_id: str) -> BrokerOrder | None:
        return None

    async def positions(self) -> list[BrokerPosition]:
        if self._fail:
            raise BrokerError("positions unavailable")
        return list(self.held)

    async def account(self):
        from qqq_alpha.execution.types import BrokerAccount

        return BrokerAccount(account_id="rec")


# ---------------------------------------------------------------- the switch
@pytest.mark.asyncio
async def test_nothing_is_sent_while_execution_is_disabled(tmp_path):
    """The default state of this engine, and the one it has always run in."""
    broker = _Recorder()
    router = ExecutionRouter(_settings(tmp_path), broker=broker)

    assert router.armed is False
    assert await router.submit(_request()) is None
    assert broker.placed == []
    assert router.withheld_count == 1


@pytest.mark.asyncio
async def test_a_configured_broker_alone_does_not_arm_execution(tmp_path):
    """Adding credentials and going live are two decisions, not one."""
    settings = _settings(tmp_path, execution_broker="paper")
    router = ExecutionRouter(settings, broker=build_broker(settings))

    assert router.broker is not None
    assert router.armed is False


@pytest.mark.asyncio
async def test_enabling_without_a_broker_still_sends_nothing(tmp_path):
    """The other half of the same guard."""
    settings = _settings(tmp_path, execution_enabled=True, execution_broker="none")
    router = ExecutionRouter(settings, broker=build_broker(settings))

    assert router.armed is False
    assert await router.submit(_request()) is None


@pytest.mark.asyncio
async def test_both_switches_on_actually_sends(tmp_path):
    """Non-vacuity: the refusals above must be the switch, not a broken path."""
    broker = _Recorder()
    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True, execution_broker="paper"),
        broker=broker,
    )

    order = await router.submit(_request())

    assert router.armed is True
    assert order is not None and order.state is OrderState.FILLED
    assert [r.client_order_id for r in broker.placed] == ["t1-entry"]


# ---------------------------------------------------------------- the guards
@pytest.mark.asyncio
async def test_an_order_above_the_cap_is_refused_not_trimmed(tmp_path):
    """A sizing bug wearing a safe number is worse than a missed trade."""
    broker = _Recorder()
    alerts: list[str] = []
    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True, execution_max_contracts=1),
        broker=broker,
        on_alert=lambda m: _collect(alerts, m),
    )

    assert await router.submit(_request(quantity=5)) is None
    assert broker.placed == []
    assert any("السقف" in a for a in alerts)


@pytest.mark.asyncio
async def test_the_same_order_id_is_never_sent_twice(tmp_path):
    """A retry loop or a restart replaying work must not double a position."""
    broker = _Recorder()
    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True), broker=broker
    )

    assert await router.submit(_request()) is not None
    assert await router.submit(_request()) is None
    assert len(broker.placed) == 1


@pytest.mark.asyncio
async def test_a_broker_failure_returns_none_and_shouts(tmp_path):
    """An order whose fate is unknown must never look like one never sent."""
    alerts: list[str] = []
    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True),
        broker=_Recorder(fail=True),
        on_alert=lambda m: _collect(alerts, m),
    )

    assert await router.submit(_request()) is None
    assert any("غير معروف" in a for a in alerts)


@pytest.mark.asyncio
async def test_a_partial_fill_is_announced(tmp_path):
    alerts: list[str] = []
    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True),
        broker=_Recorder(state=OrderState.PARTIAL),
        on_alert=lambda m: _collect(alerts, m),
    )

    await router.submit(_request(quantity=1))

    assert any("جزئي" in a for a in alerts)


@pytest.mark.asyncio
async def test_a_failing_alert_does_not_abort_execution(tmp_path):
    async def _broken(_message: str) -> None:
        raise RuntimeError("telegram is down")

    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True),
        broker=_Recorder(state=OrderState.PARTIAL),
        on_alert=_broken,
    )

    assert await router.submit(_request()) is not None


# ---------------------------------------------------------------- the journal
@pytest.mark.asyncio
async def test_a_withheld_order_is_still_written_down(tmp_path):
    """The order file fills with what would have been sent, long before
    anything is sent."""
    settings = _settings(tmp_path)
    journal = Journal(settings.journal_dir, session_tag="test")
    router = ExecutionRouter(settings, broker=_Recorder(), journal=journal)

    await router.submit(_request())

    rows = list(journal.read(journal.orders_path))
    assert [r["outcome"] for r in rows] == ["execution_disabled"]
    assert rows[0]["armed"] is False
    assert rows[0]["occ_symbol"] == "O:QQQ260302P00711000"


@pytest.mark.asyncio
async def test_a_sent_order_is_written_before_and_after_the_broker_answers(tmp_path):
    """A process that dies mid-flight leaves "submitting" on disk, which is
    the truth: we asked, and we do not know what happened."""
    settings = _settings(tmp_path, execution_enabled=True)
    journal = Journal(settings.journal_dir, session_tag="test")
    router = ExecutionRouter(settings, broker=_Recorder(), journal=journal)

    await router.submit(_request())

    assert [r["outcome"] for r in journal.read(journal.orders_path)] == [
        "submitting",
        "submitted",
    ]


# ---------------------------------------------------------------- reconciling
@pytest.mark.asyncio
async def test_matching_books_reconcile_clean(tmp_path):
    broker = _Recorder()
    broker.held = [BrokerPosition(occ_symbol="O:QQQ260302P00711000", quantity=1)]
    router = ExecutionRouter(_settings(tmp_path, execution_enabled=True), broker=broker)

    result = await router.reconcile({"O:QQQ260302P00711000": 1})

    assert result.ok
    assert "مطابقة" in result.describe()


@pytest.mark.asyncio
async def test_a_position_the_broker_does_not_have_is_reported(tmp_path):
    alerts: list[str] = []
    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True),
        broker=_Recorder(),
        on_alert=lambda m: _collect(alerts, m),
    )

    result = await router.reconcile({"O:QQQ260302P00711000": 1})

    assert not result.ok
    assert result.missing_at_broker == {"O:QQQ260302P00711000": 1}
    assert alerts and "لا يملكه" in alerts[0]


@pytest.mark.asyncio
async def test_a_position_the_engine_never_opened_is_reported_not_closed(tmp_path):
    """Closing it automatically could dump something deliberately held."""
    broker = _Recorder()
    broker.held = [BrokerPosition(occ_symbol="O:QQQ260302C00715000", quantity=2)]
    router = ExecutionRouter(_settings(tmp_path, execution_enabled=True), broker=broker)

    result = await router.reconcile({})

    assert result.unknown_to_engine == {"O:QQQ260302C00715000": 2}
    assert "لم يُصحَّح شيء تلقائيًا" in result.describe()


@pytest.mark.asyncio
async def test_a_quantity_disagreement_is_reported_with_both_numbers(tmp_path):
    broker = _Recorder()
    broker.held = [BrokerPosition(occ_symbol="O:QQQ260302P00711000", quantity=3)]
    router = ExecutionRouter(_settings(tmp_path, execution_enabled=True), broker=broker)

    result = await router.reconcile({"O:QQQ260302P00711000": 1})

    assert result.quantity_mismatch == {"O:QQQ260302P00711000": (1, 3)}


@pytest.mark.asyncio
async def test_reconciliation_reports_its_own_failure(tmp_path):
    router = ExecutionRouter(
        _settings(tmp_path, execution_enabled=True), broker=_Recorder(fail=True)
    )

    result = await router.reconcile({"O:QQQ260302P00711000": 1})

    assert not result.ok and "positions unavailable" in result.error


@pytest.mark.asyncio
async def test_a_disarmed_router_does_not_ask_the_broker_anything(tmp_path):
    router = ExecutionRouter(_settings(tmp_path), broker=_Recorder(fail=True))

    result = await router.reconcile({"O:QQQ260302P00711000": 1})

    assert result.ok  # no question asked, so nothing to disagree about


# ---------------------------------------------------------------- the registry
def test_an_unknown_broker_name_fails_loudly(tmp_path):
    """Falling back to paper would be the worse bug: the operator believes he
    is live, the engine reports fills, and none of them exist."""
    with pytest.raises(ValueError, match="unknown EXECUTION_BROKER"):
        build_broker(_settings(tmp_path, execution_broker="derayah"))


def test_no_broker_is_the_default(tmp_path):
    assert build_broker(_settings(tmp_path)) is None


# ---------------------------------------------------------------- paper broker
@pytest.mark.asyncio
async def test_the_paper_broker_tracks_what_it_holds():
    broker = PaperBroker()
    await broker.place(_request(quantity=2))

    held = await broker.positions()
    assert [(p.occ_symbol, p.quantity) for p in held] == [
        ("O:QQQ260302P00711000", 2)
    ]


@pytest.mark.asyncio
async def test_the_paper_broker_nets_a_round_trip_to_flat():
    broker = PaperBroker()
    await broker.place(_request(quantity=1))
    await broker.place(
        _request(client_order_id="t1-exit", side=Side.SELL, quantity=1, limit_price=1.60)
    )

    assert await broker.positions() == []


@pytest.mark.asyncio
async def test_the_paper_broker_refuses_to_replay_an_order_id():
    broker = PaperBroker()
    await broker.place(_request())
    await broker.place(_request())

    held = await broker.positions()
    assert held[0].quantity == 1, "a resent id must not double the position"


async def _collect(sink: list[str], message: str) -> None:
    sink.append(message)


# ---------------------------------------------------------------- end to end
@pytest.mark.asyncio
async def test_a_whole_live_session_sends_nothing_with_the_switch_off(tmp_path):
    """The test this file exists for.

    Runs the real engine over a real session, takes real trades, and asserts
    that the broker was never touched — while proving the path is live by
    checking the same trades were journalled as withheld orders.
    """
    from datetime import date

    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.data.pricing import BlackScholesPricer
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Action, Decision, OptionType, RailVerdict, Target
    from qqq_alpha.live.engine import LiveEngine
    from qqq_alpha.live.notifier import NullNotifier

    day = date(2026, 3, 2)

    class _EnterOnce:
        def __init__(self):
            self.calls = 0

        async def decide(self, snapshot, **kwargs):
            self.calls += 1
            if self.calls > 1:
                return Decision(ts=snapshot.ts, action=Action.PASS, confidence=3)
            price = snapshot.underlying.close
            return Decision(
                ts=snapshot.ts,
                action=Action.ENTER,
                direction=OptionType.CALL,
                occ_symbol=f"O:QQQ260302C{int(round(price)) * 1000:08d}",
                targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
                stop_return_pct=-40,
                confidence=7,
                thesis="اختبار",
            )

    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        max_data_age_sec=10**9,
        attention_threshold=0.0,
        attention_cooldown_sec=0,
        shadow_symbols_csv="",
        # a broker IS configured — only the switch is off
        execution_broker="paper",
    )
    settings.ensure_dirs()
    engine = LiveEngine(
        settings=settings,
        decider=_EnterOnce(),
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )
    engine.rails.post_check = lambda decision, contract: RailVerdict(allowed=True)
    engine._current_day = day

    for bar in synthetic_session("QQQ", day, seed=8, trend=0.02, volatility=0.002)[:240]:
        await engine._on_bar(bar)

    assert [*engine.manager.open_trades, *engine.manager.closed_trades], "no trade taken"
    assert engine.execution.armed is False
    assert engine.execution.broker is not None
    assert engine.execution.broker.submitted == [], "an order reached the broker"

    # ...and the path really ran: the intents are on disk, priced and sized
    rows = list(engine.journal.read(engine.journal.orders_path))
    assert rows, "the execution path was never exercised"
    assert {r["outcome"] for r in rows} == {"execution_disabled"}
    assert engine.execution.withheld_count == len(rows)

    # every intent is sized in dollars, and the entry lands inside the band
    entries = [r for r in rows if r["reason"] == "entry"]
    assert entries
    for row in entries:
        notional = row["quantity"] * row["limit_price"] * 100
        assert 850 <= notional <= 1150, notional

    # a sell never asks for more than the entry bought
    bought = {r["trade_id"]: r["quantity"] for r in entries}
    sold: dict[str, int] = {}
    for row in rows:
        if row["reason"] != "entry":
            sold[row["trade_id"]] = sold.get(row["trade_id"], 0) + row["quantity"]
    for trade_id, total in sold.items():
        assert total <= bought[trade_id], f"{trade_id} sold more than it holds"


# ---------------------------------------------------------------- sizing + scale-out
def _live_engine(tmp_path, decider, **extra):
    from qqq_alpha.brain.playbook import Playbook
    from qqq_alpha.data.pricing import BlackScholesPricer
    from qqq_alpha.live.engine import LiveEngine
    from qqq_alpha.live.notifier import NullNotifier

    settings = Settings(
        massive_api_key="k",
        journal_dir=tmp_path / "journal",
        data_dir=tmp_path / "data",
        max_data_age_sec=10**9,
        attention_threshold=0.0,
        attention_cooldown_sec=0,
        shadow_symbols_csv="",
        execution_broker="paper",
        **extra,
    )
    settings.ensure_dirs()
    return LiveEngine(
        settings=settings,
        decider=decider,
        pricer=BlackScholesPricer(),
        playbook=Playbook(),
        journal=Journal(tmp_path / "journal", session_tag="test"),
        notifier=NullNotifier(),
    )


def _sample_trade(entry: float = 1.25):
    from datetime import UTC, datetime
    from datetime import date as _date

    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.domain import Action, Decision, OptionType, Target
    from qqq_alpha.features.snapshot import SnapshotBuilder
    from qqq_alpha.trades import TradeManager

    bars = synthetic_session("QQQ", _date(2026, 3, 2), seed=15)
    snap = SnapshotBuilder("QQQ").build(bars[:80])
    decision = Decision(
        ts=snap.ts,
        action=Action.ENTER,
        direction=OptionType.CALL,
        occ_symbol="O:QQQ260302C00485000",
        targets=[Target(label="T1", price=0.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40,
        confidence=6,
        thesis="t",
    )
    trade = TradeManager().open_trade(decision, entry, snap)
    assert datetime.now(UTC)  # keep the import honest
    return trade


@pytest.mark.asyncio
async def test_the_entry_is_sized_in_dollars_not_contracts(tmp_path):
    """$1,000 against a $1.25 contract is eight, not one."""
    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)

    await engine._execute_entry(trade)

    assert engine.execution.broker.submitted[0].quantity == 8
    assert engine._executed[trade.trade_id] == 8


@pytest.mark.asyncio
async def test_conviction_sizing_does_not_shrink_the_order(tmp_path):
    """Every decision on record is confidence 6, so applying the factor would
    halve every trade uniformly rather than tell any two apart."""
    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    trade.decision.size_factor = 0.25

    await engine._execute_entry(trade)

    assert engine.execution.broker.submitted[0].quantity == 8


@pytest.mark.asyncio
async def test_banking_half_sells_half_the_contracts(tmp_path):
    from qqq_alpha.domain import TradeUpdate

    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    await engine._execute_entry(trade)

    trade.updates.append(
        TradeUpdate(ts=trade.opened_at, price=1.70, return_pct=36.0, note="scale_out: +36%")
    )
    await engine._execute_scale_out(trade)

    scale = engine.execution.broker.submitted[-1]
    assert scale.side is Side.SELL and scale.quantity == 4
    assert engine._executed[trade.trade_id] == 4, "the runner is what is left"


@pytest.mark.asyncio
async def test_the_final_exit_sells_the_remainder_not_the_original_size(tmp_path):
    """The bug this guards: asking to sell eight after banking four leaves a
    short leg behind."""
    from qqq_alpha.domain import TradeStatus, TradeUpdate

    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    await engine._execute_entry(trade)

    trade.updates.append(
        TradeUpdate(ts=trade.opened_at, price=1.70, return_pct=36.0, note="scale_out: +36%")
    )
    await engine._execute_scale_out(trade)

    trade.status = TradeStatus.CLOSED_WIN
    trade.exit_price, trade.exit_reason = 1.55, "trail_stop"
    await engine._execute_exit(trade)

    sells = [r for r in engine.execution.broker.submitted if r.side is Side.SELL]
    assert [r.quantity for r in sells] == [4, 4]
    assert sum(r.quantity for r in sells) == 8, "never more than was bought"


@pytest.mark.asyncio
async def test_a_trade_with_no_live_position_sends_no_exit(tmp_path):
    """Nothing was bought, so there is nothing to sell — and a sell here would
    open a short."""
    from qqq_alpha.domain import TradeStatus

    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    trade.status = TradeStatus.CLOSED_LOSS
    trade.exit_price, trade.exit_reason = 0.80, "stop_hit"

    await engine._execute_exit(trade)

    assert engine.execution.broker.submitted == []


@pytest.mark.asyncio
async def test_an_unsizeable_contract_is_skipped_and_announced(tmp_path):
    """A $6.00 contract is $600 for one and $1,200 for two — the band holds
    neither, so the paper trade stands and the wallet stays out."""
    notes: list[str] = []

    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    engine.notifier.note = lambda m: _collect(notes, m)  # type: ignore[method-assign]

    trade = _sample_trade(entry=6.00)
    await engine._execute_entry(trade)

    assert engine.execution.broker.submitted == []
    assert engine._executed[trade.trade_id] == 0
    assert notes and "لم يُرسَل أمر حقيقي" in notes[0]


@pytest.mark.asyncio
async def test_a_forgotten_position_reconciles_as_a_mismatch_not_a_guess(tmp_path):
    """A restart loses the contract counts. Guessing them back from a budget
    and a price that has since moved would be worse than saying so."""
    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    engine.manager.open_trades.append(trade)
    engine.execution.broker._holdings[trade.occ_symbol] = [(8, 1.25)]

    result = await engine.execution.reconcile({})

    assert result.unknown_to_engine == {trade.occ_symbol: 8}


# ---------------------------------------------------------------- surviving a restart
@pytest.mark.asyncio
async def test_the_held_size_survives_a_restart(tmp_path):
    """The engine has crash-safe state; this number belongs in it.

    A recovered position whose contract count was forgotten cannot be sold:
    the engine would either dump more than it holds or leave a leg behind.
    """
    from qqq_alpha.live.state import StateStore

    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    engine.manager.open_trades.append(trade)
    engine._current_day = trade.opened_at.date()
    await engine._execute_entry(trade)
    assert engine._executed[trade.trade_id] == 8

    # a fresh process reading the same file
    state = StateStore(engine.store.path).load()
    assert state is not None
    assert state.executed == {trade.trade_id: 8}


@pytest.mark.asyncio
async def test_a_restart_after_banking_half_remembers_the_runner(tmp_path):
    """Eight bought, four banked — a restart must resume holding four, not
    eight, or the final exit opens a short."""
    from qqq_alpha.domain import TradeUpdate
    from qqq_alpha.live.state import StateStore

    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    engine.manager.open_trades.append(trade)
    engine._current_day = trade.opened_at.date()
    await engine._execute_entry(trade)

    trade.updates.append(
        TradeUpdate(ts=trade.opened_at, price=1.70, return_pct=36.0, note="scale_out: +36%")
    )
    await engine._execute_scale_out(trade)

    state = StateStore(engine.store.path).load()
    assert state is not None and state.executed == {trade.trade_id: 4}


@pytest.mark.asyncio
async def test_a_closed_trade_leaves_no_size_behind(tmp_path):
    from qqq_alpha.domain import TradeStatus
    from qqq_alpha.live.state import StateStore

    engine = _live_engine(tmp_path, decider=None, execution_enabled=True)
    trade = _sample_trade(entry=1.25)
    engine.manager.open_trades.append(trade)
    engine._current_day = trade.opened_at.date()
    await engine._execute_entry(trade)

    trade.status = TradeStatus.CLOSED_WIN
    trade.exit_price, trade.exit_reason = 1.55, "trail_stop"
    await engine._execute_exit(trade)

    state = StateStore(engine.store.path).load()
    assert state is not None and state.executed == {}
