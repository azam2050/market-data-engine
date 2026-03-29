Yes, this file `market_data_engine.py` is the market data preparation layer, and I want it updated to match the final project logic.

Goal:
This file must decide what we take from the market and what we ignore.
I do NOT want the full raw market feed.
I want a filtered, compact, decision-ready payload sent directly to the bot webhook.

Scope:
1) Underlying:
- QQQ only

2) Leaders:
- AAPL, MSFT, NVDA, AMZN, GOOGL, META, AVGO, TSLA

3) Options:
- QQQ options only
- 0DTE and 1DTE only
- exclude illiquid contracts
- exclude contracts with missing bid/ask
- prefer near-the-money strikes
- keep only top candidate CALLs and PUTs
- do NOT send the full options chain

A) Equity candles
Monitor QQQ and the leader stocks only.
Build 30-second candles.
If no native 30s feed exists, build 30s candles internally from finer-grained events.
For each symbol output only:
- symbol
- timeframe = 30s
- open
- high
- low
- close
- vwap
- volume
- timestamp

B) Options candidates
For each selected contract output only:
- contract_symbol
- side
- strike
- expiry
- dte
- bid
- ask
- mid
- volume
- open_interest
- delta if available
- iv if available

C) Send frequency
- send one snapshot every 30 seconds
- do not send every 10 seconds
- do not send raw ticks or oversized payloads
- send only compact decision-ready data

D) Final payload format
{
  "timestamp": "...",
  "timeframe": "30s",
  "underlying": {...},
  "leaders": [...],
  "options_candidates": [...]
}

E) Important constraints
- do not change project decision logic
- do not add Telegram logic here
- do not add Claude logic here
- this file is only the market data preparation layer
- reduce noise
- reduce payload size
- reduce Claude token usage
- any field that does not help decision-making should be excluded

After the update, explain clearly:
1) exactly what is now pulled from the market
2) exactly what is excluded
3) one final realistic payload example
4) whether the 30s candles were built internally or from a native source
