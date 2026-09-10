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


# ---------------------------------------------------------------- reaching the money
# One side alone can exceed the 250 cap on a 0DTE index chain, and pages
# arrive in ascending strike order — so a single request returns the
# cheapest far-out-of-the-money strikes and stops short of the money.
def _page(strikes, contract_type: str, next_url: str | None = None) -> dict:
    body: dict = {
        "results": [
            _contract_payload(
                f"O:QQQ260805{'C' if contract_type == 'call' else 'P'}{int(s * 1000):08d}",
                contract_type, float(s),
            )
            for s in strikes
        ]
    }
    if next_url:
        body["next_url"] = next_url
    return body


async def test_one_side_longer_than_a_page_is_followed_to_the_end():
    """400 call strikes over two pages: without following ``next_url`` the
    money (720) is never seen, because page one stops at 649."""
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen.append(params)
        assert params.get("apiKey") == "k"  # the key is re-attached to page two
        if params.get("cursor") == "PAGE2":
            return httpx.Response(200, json=_page(range(650, 800), "call"))
        return httpx.Response(
            200,
            json=_page(
                range(400, 650), "call",
                next_url="https://api.polygon.io/v3/snapshot/options/QQQ?cursor=PAGE2",
            ),
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY, OptionType.CALL)

    assert len(seen) == 2
    assert len(contracts) == 400
    assert any(c.strike == 720 for c in contracts)  # the strike at the money survived


async def test_a_known_spot_asks_the_provider_for_the_strikes_around_the_money():
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen.append(params)
        lo = float(params["strike_price.gte"])
        hi = float(params["strike_price.lte"])
        return httpx.Response(
            200, json=_page([s for s in range(400, 900) if lo <= s <= hi], "call")
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY, OptionType.CALL, 720.0)

    assert len(seen) == 1
    assert float(seen[0]["strike_price.gte"]) < 720 < float(seen[0]["strike_price.lte"])
    assert len(contracts) < 250  # a band, not the whole book
    assert any(c.strike == 720 for c in contracts)


async def test_a_provider_that_rejects_the_strike_window_still_gets_its_chain():
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen.append(params)
        if "strike_price.gte" in params:
            return httpx.Response(400, json={"error": "unknown parameter strike_price.gte"})
        return httpx.Response(200, json=_page(range(700, 740), "put"))

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY, OptionType.PUT, 720.0)

    assert len(seen) == 2 and "strike_price.gte" not in seen[1]
    assert len(contracts) == 40 and all(c.option_type is OptionType.PUT for c in contracts)


async def test_paging_stops_at_a_budget_rather_than_looping_for_ever():
    """A provider whose ``next_url`` never ends must not spin the request."""
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return httpx.Response(
            200,
            json=_page(range(400, 405), "call",
                       next_url="https://api.polygon.io/v3/snapshot/options/QQQ?cursor=MORE"),
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY, OptionType.CALL)

    from qqq_alpha.data.massive import CHAIN_MAX_PAGES

    assert len(seen) == CHAIN_MAX_PAGES
    assert len(contracts) == 5 * CHAIN_MAX_PAGES


async def test_last_prices_reads_the_tape_for_several_tickers_in_one_request():
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.url.params))
        return httpx.Response(200, json={"tickers": [
            {"ticker": "QQQ", "lastTrade": {"p": 708.31, "s": 100, "t": 1789012345678900000}, "todaysChangePerc": -0.42},
            {"ticker": "SPY", "lastTrade": {"p": 0}, "day": {"c": 767.2}, "todaysChangePerc": -0.2},
            {"ticker": "IWM", "lastTrade": {"p": 0}, "day": {"c": 0}, "prevDay": {"c": 0}},
            {"ticker": "", "lastTrade": {"p": 5}},
        ]})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        out = await client.last_prices(["qqq", "SPY", "QQQ", "IWM"])

    assert len(seen) == 1 and seen[0]["tickers"] == "IWM,QQQ,SPY"
    assert out["QQQ"]["price"] == 708.31 and out["QQQ"]["ts"].startswith("2026-")
    assert out["SPY"]["price"] == 767.2  # no print yet: the day's close, not zero
    assert "IWM" not in out and "" not in out  # nothing priced at zero, nothing nameless
    assert await client.last_prices([]) == {}


async def test_a_row_without_an_expiry_is_skipped_not_guessed():
    def handler(request: httpx.Request) -> httpx.Response:
        good = _contract_payload("O:QQQ260805C00720000", "call", 720)
        broken = _contract_payload("O:QQQ260805C00721000", "call", 721)
        broken["details"].pop("expiration_date")
        nameless = _contract_payload("", "call", 722)
        nameless["details"]["ticker"] = ""
        return httpx.Response(200, json={"results": [good, broken, nameless]})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.polygon.io") as http_client:
        client = MassiveClient(Settings(massive_api_key="k"), client=http_client)
        contracts = await client.option_chain("QQQ", EXPIRY, OptionType.CALL)

    assert [c.strike for c in contracts] == [720.0]
