"""Tests for live chain pricing.

The theme here is one specific form of self-deception: pricing both sides of a
trade at the mid. It is the easiest way to build a track record that cannot be
reproduced with real money, so the spread handling is tested directly.
"""

from datetime import UTC, date, datetime, timedelta

from qqq_alpha.config import Settings
from qqq_alpha.data.chain import ChainSnapshot, LiveChainPricer
from qqq_alpha.data.pricing import BlackScholesPricer
from qqq_alpha.domain import OptionContract, OptionType

EXPIRY = date(2026, 8, 4)
NOW = datetime(2026, 8, 3, 14, 0, tzinfo=UTC)


def _contract(strike: float, option_type: OptionType, bid: float, ask: float, **kwargs):
    letter = "C" if option_type is OptionType.CALL else "P"
    return OptionContract(
        occ_symbol=f"O:QQQ260804{letter}{int(strike * 1000):08d}",
        underlying="QQQ",
        option_type=option_type,
        strike=strike,
        expiry=EXPIRY,
        bid=bid,
        ask=ask,
        volume=kwargs.get("volume", 500),
        open_interest=kwargs.get("open_interest", 2000),
        delta=kwargs.get("delta", 0.42),
        implied_volatility=kwargs.get("iv", 0.19),
    )


def _pricer(contracts, fallback=None, age_sec: float = 0.0) -> LiveChainPricer:
    pricer = LiveChainPricer(Settings(massive_api_key="k"), fallback=fallback)
    pricer.snapshot = ChainSnapshot(
        fetched_at=datetime.now(UTC) - timedelta(seconds=age_sec),
        expiry=EXPIRY,
        contracts={c.occ_symbol: c for c in contracts},
    )
    return pricer


# ------------------------------------------------------------- spread handling
def test_entry_fills_at_the_ask_and_exit_at_the_bid():
    """Mid-pricing both sides manufactures profit that will not exist."""
    contract = _contract(485, OptionType.CALL, bid=1.00, ask=1.10)
    pricer = _pricer([contract])
    symbol = contract.occ_symbol

    assert pricer.price_at(symbol, NOW, 485.0, side="entry") == 1.10
    assert pricer.price_at(symbol, NOW, 485.0, side="exit") == 1.00
    assert pricer.price_at(symbol, NOW, 485.0) == 1.05  # mark only


def test_the_spread_is_paid_twice_on_a_round_trip():
    """Buy at ask, sell at bid: a flat contract is already a loss."""
    contract = _contract(485, OptionType.CALL, bid=1.00, ask=1.10)
    pricer = _pricer([contract])
    symbol = contract.occ_symbol

    entry = pricer.entry_price(symbol, NOW, 485.0)
    exit_now = pricer.exit_price(symbol, NOW, 485.0)

    assert entry > exit_now
    round_trip_pct = (exit_now - entry) / entry * 100
    assert round_trip_pct < -8  # ~9% lost before the market moves at all


def test_missing_quote_falls_back_to_the_bid_side_of_nothing():
    """A contract quoted only on one side still returns something usable."""
    contract = _contract(485, OptionType.CALL, bid=0.0, ask=1.20)
    pricer = _pricer([contract])
    # bid is unusable, so exit falls back to the mid rather than returning zero
    assert pricer.price_at(contract.occ_symbol, NOW, 485.0, side="exit") == contract.mid


# ------------------------------------------------------------- honesty flags
def test_pricer_reports_itself_as_real_only_when_it_is():
    contract = _contract(485, OptionType.CALL, bid=1.0, ask=1.1)

    fresh = _pricer([contract])
    assert not fresh.is_approximation

    stale = _pricer([contract], age_sec=600)
    assert stale.is_approximation

    empty = LiveChainPricer(Settings(massive_api_key="k"))
    assert empty.is_approximation


def test_fallback_is_used_and_counted():
    """Silent fallbacks would hide how much of a record is modelled."""
    pricer = _pricer([], fallback=BlackScholesPricer())
    price = pricer.price_at("O:QQQ260804C00485000", NOW, 485.0)

    assert price is not None
    assert pricer.fallback_uses == 1
    assert "1 fallback prices used" in pricer.status


def test_no_fallback_means_no_invented_price():
    pricer = _pricer([])
    assert pricer.price_at("O:QQQ260804C00485000", NOW, 485.0) is None


# ------------------------------------------------------------- strike selection
def test_nearby_returns_strikes_closest_to_the_money_first():
    contracts = [
        _contract(strike, OptionType.CALL, bid=1.0, ask=1.1)
        for strike in (480, 483, 485, 487, 490)
    ]
    pricer = _pricer(contracts)

    nearby = pricer.nearby(485.2, OptionType.CALL, count=3)
    assert [c.strike for c in nearby] == [485.0, 487.0, 483.0]


def test_nearby_excludes_far_strikes_and_unquoted_contracts():
    contracts = [
        _contract(485, OptionType.CALL, bid=1.0, ask=1.1),
        _contract(600, OptionType.CALL, bid=0.01, ask=0.02),  # far out of range
        _contract(486, OptionType.CALL, bid=0.0, ask=0.0),    # no quote at all
    ]
    pricer = _pricer(contracts)

    strikes = [c.strike for c in pricer.nearby(485.0, OptionType.CALL, count=10)]
    assert strikes == [485.0]


def test_chain_context_gives_the_brain_what_it_needs_to_choose():
    contracts = [
        _contract(485, OptionType.CALL, bid=1.00, ask=1.08),
        _contract(484, OptionType.PUT, bid=0.90, ask=0.98),
    ]
    rows = _pricer(contracts).chain_context(485.0, count=2)

    assert len(rows) == 2
    for row in rows:
        # everything required to size a target and judge the cost of entry
        for field in ("symbol", "strike", "bid", "ask", "spread_pct", "delta", "open_interest"):
            assert field in row
    assert {row["type"] for row in rows} == {"CALL", "PUT"}


def test_chain_context_is_empty_without_a_chain():
    assert LiveChainPricer(Settings(massive_api_key="k")).chain_context(485.0) == []


# ------------------------------------------------------------- rails integration
def test_rails_can_now_reject_an_illiquid_contract():
    """With a real chain the rails validate the contract, not a guess."""
    from qqq_alpha.brain.rails import SafetyRails
    from qqq_alpha.domain import Action, Decision, Target

    wide = _contract(485, OptionType.CALL, bid=1.00, ask=1.60)
    decision = Decision(
        ts=NOW,
        action=Action.ENTER,
        direction=OptionType.CALL,
        occ_symbol=wide.occ_symbol,
        targets=[Target(label="T1", price=2.0, return_pct=50, take_pct=50)],
        stop_return_pct=-40,
        confidence=8,
        thesis="test",
    )

    verdict = SafetyRails(Settings()).post_check(decision, wide)
    assert not verdict.allowed
    assert any("spread_too_wide" in block for block in verdict.blocks)


# ------------------------------------------------------------- brain responses
class _Block:
    def __init__(self, type_, **kw):
        self.type = type_
        for k, v in kw.items():
            setattr(self, k, v)


class _Response:
    def __init__(self, stop_reason="tool_use", content=None, stop_details=None):
        self.stop_reason = stop_reason
        self.content = content or []
        self.stop_details = stop_details
        self.usage = None


class _FakeClient:
    def __init__(self, response):
        self._response = response
        self.messages = self
        self.kwargs = None

    async def create(self, **kwargs):
        self.kwargs = kwargs
        return self._response


class _FailThenSucceedClient:
    """Raises on the first N calls (simulating a transient 529), then returns
    a real response — proves the retry path actually recovers."""

    def __init__(self, response, fail_times: int):
        self._response = response
        self._fail_times = fail_times
        self.calls = 0
        self.messages = self

    async def create(self, **kwargs):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RuntimeError("Error code: 529 - {'type': 'overloaded_error'}")
        return self._response


class _AlwaysFailingClient:
    """Every call raises — simulates a sustained outage that outlasts retries."""

    def __init__(self):
        self.calls = 0
        self.messages = self

    async def create(self, **kwargs):
        self.calls += 1
        raise RuntimeError("Error code: 529 - {'type': 'overloaded_error'}")


def _snapshot():
    from qqq_alpha.data.synthetic import synthetic_session
    from qqq_alpha.features.snapshot import SnapshotBuilder

    bars = synthetic_session("QQQ", date(2026, 3, 2), seed=5)
    return SnapshotBuilder("QQQ").build(bars[:80])


def _decider(response):
    from qqq_alpha.brain.decider import AIDecider

    settings = Settings(anthropic_api_key="k", anthropic_model="test-model")
    client = _FakeClient(response)
    return AIDecider(settings, client=client), client


async def _decide(decider, snapshot):
    from qqq_alpha.brain.playbook import Playbook

    return await decider.decide(snapshot, Playbook(), [], [], [], "test")


async def test_refusal_becomes_a_pass_not_a_crash():
    """A declined request returns HTTP 200 with empty content — indexing it would crash."""
    from qqq_alpha.domain import Action

    response = _Response(stop_reason="refusal", content=[], stop_details=_Block("refusal", category="cyber"))
    decider, _ = _decider(response)

    decision = await _decide(decider, _snapshot())
    assert decision.action is Action.PASS
    assert "declined" in decision.thesis


async def test_truncated_response_passes_rather_than_acting_on_half_a_plan():
    from qqq_alpha.domain import Action

    response = _Response(stop_reason="max_tokens", content=[])
    decider, _ = _decider(response)

    decision = await _decide(decider, _snapshot())
    assert decision.action is Action.PASS
    assert "truncated" in decision.thesis


async def test_request_carries_effort_caching_and_a_generous_token_budget():
    response = _Response(content=[_Block("tool_use", input={"action": "PASS", "confidence": 3, "thesis": "quiet"})])
    decider, client = _decider(response)

    await _decide(decider, _snapshot())

    assert client.kwargs["output_config"] == {"effort": "high"}
    assert client.kwargs["max_tokens"] >= 8000  # thinking shares this budget
    assert client.kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert client.kwargs["tool_choice"]["name"] == "submit_decision"


# ------------------------------------------------------------- transient-outage resilience
async def test_transient_overload_recovers_on_retry(monkeypatch):
    """A 529 that clears up on retry must still produce a real decision."""
    from qqq_alpha.domain import Action

    async def _no_sleep(_seconds):
        return None

    monkeypatch.setattr("qqq_alpha.brain.resilience.asyncio.sleep", _no_sleep)

    response = _Response(content=[_Block("tool_use", input={"action": "PASS", "confidence": 3, "thesis": "quiet"})])
    client = _FailThenSucceedClient(response, fail_times=1)
    decider, _ = _decider(response)
    decider._client = client

    decision = await _decide(decider, _snapshot())

    assert decision.action is Action.PASS
    assert client.calls == 2  # one failure, one recovery — no crash in between


async def test_sustained_overload_becomes_a_safe_pass_not_a_crash(monkeypatch):
    """A 529 that never clears up must not take down the whole engine."""
    from qqq_alpha.domain import Action

    async def _no_sleep(_seconds):
        return None

    monkeypatch.setattr("qqq_alpha.brain.resilience.asyncio.sleep", _no_sleep)

    client = _AlwaysFailingClient()
    settings = Settings(anthropic_api_key="k", anthropic_model="test-model")
    from qqq_alpha.brain.decider import AIDecider

    decider = AIDecider(settings, client=client)

    decision = await _decide(decider, _snapshot())

    assert decision.action is Action.PASS
    assert "فشل تقني" in decision.thesis
    assert client.calls == 3  # 1 initial attempt + 2 retries, then gives up honestly
