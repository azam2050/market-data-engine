"""Tests for the Massive/Polygon REST client.

The theme: the options snapshot endpoint caps every response at 250
contracts. A 0DTE index chain regularly has more than 250 strikes across
both sides combined, so an unfiltered request can silently truncate one
side — this happened in production, and it looked exactly like "no PUT
available" while calls were plentiful, because the brain only ever saw
whatever survived the shared cap.
"""

from datetime import date

import httpx

from qqq_alpha.config import Settings
from qqq_alpha.data.massive import MassiveClient
from qqq_alpha.domain import OptionType

EXPIRY = date(2026, 8, 5)


def _contract_payload(ticker: str, contract_type: str, strike: float) -> dict:
    return {
        "details": {
            "ticker": ticker,
            "contract_type": contract_type,
            "strike_price": strike,
            "expiration_date": EXPIRY.isoformat(),
        },
        "last_quote": {"bid": 1.0, "ask": 1.1},
        "last_trade": {"price": 1.05},
        "greeks": {"delta": 0.4, "gamma": 0.1, "theta": -0.05},
        "day": {"volume": 500},
        "open_interest": 1000,
        "implied_volatility": 0.2,
    }


async def test_fetching_without_a_type_requests_each_side_separately():
    """The real bug: one unfiltered request shares a 250-contract cap between
    calls and puts. Two type-scoped requests each get their own budget."""
    requests_seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        requests_seen.append(params)
        contract_type = params.get("contract_type")
        if contract_type == "call":
            results = [_contract_payload("O:QQQ260805C00720000", "call", 720)]
        elif contract_type == "put":
            results = [_contract_payload("O:QQQ260805P00720000", "put", 720)]
        else:  # the old, buggy behaviour: one shared request
            results = [_contract_payload("O:QQQ260805C00720000", "call", 720)]
        return httpx.Response(200, json={"results": results})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY)

    # two separate calls, one per side — never a single shared request
    assert len(requests_seen) == 2
    assert {r.get("contract_type") for r in requests_seen} == {"call", "put"}
    assert {r.get("limit") for r in requests_seen} == {"250"}

    # both sides survive, not just whichever the API happened to favour
    types = {c.option_type for c in contracts}
    assert types == {OptionType.CALL, OptionType.PUT}


async def test_fetching_a_specific_type_makes_only_one_request():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        assert dict(request.url.params)["contract_type"] == "put"
        return httpx.Response(
            200, json={"results": [_contract_payload("O:QQQ260805P00720000", "put", 720)]}
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY, OptionType.PUT)

    assert calls["n"] == 1
    assert len(contracts) == 1
    assert contracts[0].option_type is OptionType.PUT


async def test_a_full_chain_on_each_side_is_never_truncated_by_the_other():
    """Simulates the production scenario: 250 calls and 250 puts both exist.
    With the fix, all 500 survive instead of losing one side to the cap."""

    def handler(request: httpx.Request) -> httpx.Response:
        contract_type = dict(request.url.params)["contract_type"]
        results = [
            _contract_payload(f"O:QQQ260805{'C' if contract_type == 'call' else 'P'}{i:08d}", contract_type, i)
            for i in range(250)
        ]
        return httpx.Response(200, json={"results": results})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY)

    calls = [c for c in contracts if c.option_type is OptionType.CALL]
    puts = [c for c in contracts if c.option_type is OptionType.PUT]
    assert len(calls) == 250
    assert len(puts) == 250
