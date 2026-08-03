"""Technical indicators computed from 1-minute bars.

Pure functions over lists of Bar. No state, no I/O — so the backtester and the
live engine compute identical numbers from identical inputs.
"""

from __future__ import annotations

from qqq_alpha.domain import Bar


def closes(bars: list[Bar]) -> list[float]:
    return [b.close for b in bars]


def ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    multiplier = 2.0 / (period + 1)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = (value - result) * multiplier + result
    return round(result, 4)


def sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return round(sum(values[-period:]) / period, 4)


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        change = values[i] - values[i - 1]
        gains += max(change, 0.0)
        losses += max(-change, 0.0)
    avg_gain, avg_loss = gains / period, losses / period

    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]
        avg_gain = (avg_gain * (period - 1) + max(change, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-change, 0.0)) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def atr(bars: list[Bar], period: int = 14) -> float | None:
    if len(bars) < period + 1:
        return None
    true_ranges: list[float] = []
    for i in range(1, len(bars)):
        prev_close = bars[i - 1].close
        true_ranges.append(
            max(
                bars[i].high - bars[i].low,
                abs(bars[i].high - prev_close),
                abs(bars[i].low - prev_close),
            )
        )
    recent = true_ranges[-period:]
    return round(sum(recent) / len(recent), 4)


def session_vwap(bars: list[Bar]) -> float | None:
    """Volume-weighted average price for the bars supplied (typically the session)."""
    total_volume = sum(b.volume for b in bars)
    if total_volume <= 0:
        return None
    typical = sum(((b.high + b.low + b.close) / 3.0) * b.volume for b in bars)
    return round(typical / total_volume, 4)


def vwap_deviation_pct(bars: list[Bar]) -> float | None:
    vwap = session_vwap(bars)
    if vwap is None or vwap <= 0:
        return None
    return round((bars[-1].close - vwap) / vwap * 100.0, 3)


def macd(values: list[float]) -> tuple[float | None, float | None]:
    """Returns (macd_line, histogram). Signal is EMA(9) of the macd line."""
    fast, slow = ema(values, 12), ema(values, 26)
    if fast is None or slow is None:
        return None, None
    line = round(fast - slow, 4)

    history: list[float] = []
    for end in range(26, len(values) + 1):
        window = values[:end]
        f, s = ema(window, 12), ema(window, 26)
        if f is not None and s is not None:
            history.append(f - s)
    signal = ema(history, 9) if len(history) >= 9 else None
    histogram = round(line - signal, 4) if signal is not None else None
    return line, histogram


def relative_volume(bars: list[Bar], lookback: int = 20) -> float | None:
    """Latest bar volume vs the recent average. >2 means something is happening."""
    if len(bars) < lookback + 1:
        return None
    baseline = sum(b.volume for b in bars[-lookback - 1 : -1]) / lookback
    if baseline <= 0:
        return None
    return round(bars[-1].volume / baseline, 2)


def momentum_pct(bars: list[Bar], minutes: int) -> float | None:
    if len(bars) <= minutes:
        return None
    past = bars[-minutes - 1].close
    if past <= 0:
        return None
    return round((bars[-1].close - past) / past * 100.0, 3)


def compute_all(bars: list[Bar]) -> dict[str, float | None]:
    """The full indicator pack for one moment in time."""
    values = closes(bars)
    macd_line, macd_hist = macd(values)
    return {
        "price": bars[-1].close if bars else None,
        "ema9": ema(values, 9),
        "ema21": ema(values, 21),
        "ema50": ema(values, 50),
        "rsi14": rsi(values, 14),
        "atr14": atr(bars, 14),
        "vwap": session_vwap(bars),
        "vwap_dev_pct": vwap_deviation_pct(bars),
        "macd": macd_line,
        "macd_hist": macd_hist,
        "rel_volume": relative_volume(bars),
        "mom_5m": momentum_pct(bars, 5),
        "mom_15m": momentum_pct(bars, 15),
        "mom_30m": momentum_pct(bars, 30),
    }
