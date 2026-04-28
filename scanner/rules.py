"""
Single source-of-truth for ALL trading rules.

This file intentionally centralizes rule logic so the project doesn't grow
one-file-per-rule over time.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Literal, Sequence

import pandas as pd
from ta.momentum import RSIIndicator

from scanner.config import ScannerConfig
from scanner.data_fetcher import (
    fetch_ohlcv,
    fetch_ohlcv_asof,
    fetch_ohlcv_interval,
    fetch_ohlcv_range,
)

logger = logging.getLogger(__name__)


# =========================
# TAS momentum scan rules
# =========================


@dataclass
class ScanResult:
    symbol: str
    close: float
    high20_prev: float
    volume: float
    avg_vol20: float
    volume_breakout: float
    rsi: float
    ma20: float
    ma50: float
    avg_turnover20: float
    bias_1h: str = "NEUTRAL"
    bias_15m: str = "NEUTRAL"
    avwap_signal: str = "NEUTRAL"  # BUY / SELL / NEUTRAL — informational only, not a filter

    def to_row(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "close": round(self.close, 2),
            "high20_prev": round(self.high20_prev, 2),
            "volume": int(self.volume),
            "avg_vol20": int(self.avg_vol20),
            "volume_breakout": round(self.volume_breakout, 2),
            "rsi": round(self.rsi, 2),
            "ma20": round(self.ma20, 2),
            "ma50": round(self.ma50, 2),
            "avg_turnover20_cr": round(self.avg_turnover20 / 1e7, 2),
            "bias_1h": self.bias_1h,
            "bias_15m": self.bias_15m,
            "avwap_signal": self.avwap_signal,
        }


def add_indicators(
    df: pd.DataFrame,
    *,
    ma_window: int,
    ma50_window: int,
    breakout_window: int,
    rsi_window: int,
) -> pd.DataFrame:
    out = df.copy()
    out["ma20"] = out["Close"].rolling(window=ma_window, min_periods=ma_window).mean()
    out["ma50"] = out["Close"].rolling(window=ma50_window, min_periods=ma50_window).mean()
    out["avg_vol20"] = out["Volume"].rolling(window=breakout_window, min_periods=breakout_window).mean()
    out["high20_prev"] = out["High"].rolling(window=breakout_window, min_periods=breakout_window).max().shift(1)
    out["rsi"] = RSIIndicator(close=out["Close"], window=rsi_window).rsi()
    out["turnover"] = out["Close"] * out["Volume"]
    out["avg_turnover20"] = out["turnover"].rolling(window=breakout_window, min_periods=breakout_window).mean()
    return out


def evaluate_symbol(symbol: str, df: pd.DataFrame, config: ScannerConfig) -> ScanResult | None:
    if df.empty:
        return None

    latest = df.iloc[-1]
    required_fields = ["high20_prev", "avg_vol20", "rsi", "ma20", "ma50", "avg_turnover20"]
    if latest[required_fields].isna().any():
        logger.info("Skipping %s due to insufficient indicator warm-up", symbol)
        return None

    close = float(latest["Close"])
    high20_prev = float(latest["high20_prev"])
    volume = float(latest["Volume"])
    avg_vol20 = float(latest["avg_vol20"])
    rsi = float(latest["rsi"])
    ma20 = float(latest["ma20"])
    ma50_raw = latest["ma50"]
    ma50 = float(ma50_raw) if pd.notna(ma50_raw) else None
    avg_turnover20 = float(latest["avg_turnover20"])
    volume_breakout = volume / avg_vol20 if avg_vol20 > 0 else 0.0

    rules_passed = all(
        [
            close > high20_prev,
            volume_breakout >= config.min_volume_multiplier,
            config.min_rsi <= rsi <= config.max_rsi,
            close > ma20,
            ma50 is None or close > ma50,
            avg_turnover20 >= config.min_turnover_inr,
        ]
    )
    if not rules_passed:
        return None

    return ScanResult(
        symbol=symbol,
        close=close,
        high20_prev=high20_prev,
        volume=volume,
        avg_vol20=avg_vol20,
        volume_breakout=volume_breakout,
        rsi=rsi,
        ma20=ma20,
        ma50=ma50 if ma50 is not None else 0.0,
        avg_turnover20=avg_turnover20,
    )


def rank_results(results: List[ScanResult]) -> List[ScanResult]:
    return sorted(results, key=lambda r: r.volume_breakout, reverse=True)


# =========================
# AVWAP swing signal (daily)
# =========================


def compute_avwap_signal(df: pd.DataFrame) -> str:
    """Return BUY / SELL / NEUTRAL based on 1-Day vs 2-Day Anchored VWAP crossover.

    Strategy (1-2 day swing, long-only):
    - 1-Day AVWAP anchored at the previous trading day's open  → on a daily bar series
      this equals the typical price of the previous bar (single bar cumulation).
    - 2-Day AVWAP anchored two trading days ago → cumulative TP*V over the two previous bars.
    - BUY  : 1D-AVWAP crosses ABOVE 2D-AVWAP on the latest close (previous bar was ≤).
    - SELL : 1D-AVWAP crosses BELOW 2D-AVWAP on the latest close (previous bar was ≥).
    - NEUTRAL: no fresh crossover.

    Requires at least 4 daily bars; returns NEUTRAL on insufficient data.
    """
    if len(df) < 4:
        return "NEUTRAL"

    tp = (df["High"] + df["Low"] + df["Close"]) / 3
    vol = df["Volume"]

    # avwap_1d[i] = TP of bar (i-1)  — single bar anchored at previous day
    # avwap_2d[i] = cumTP*V of bars (i-2) and (i-1) / cumVol
    tp_arr = tp.to_numpy()
    vol_arr = vol.to_numpy()

    def _avwap_1d(i: int) -> float:
        return float(tp_arr[i - 1])

    def _avwap_2d(i: int) -> float:
        cum_tpv = tp_arr[i - 2] * vol_arr[i - 2] + tp_arr[i - 1] * vol_arr[i - 1]
        cum_v = vol_arr[i - 2] + vol_arr[i - 1]
        return float(cum_tpv / cum_v) if cum_v > 0 else float(tp_arr[i - 1])

    last = len(tp_arr) - 1
    prev = last - 1

    a1_now = _avwap_1d(last)
    a2_now = _avwap_2d(last)
    a1_prev = _avwap_1d(prev)
    a2_prev = _avwap_2d(prev)

    crossed_above = a1_now > a2_now and a1_prev <= a2_prev
    crossed_below = a1_now < a2_now and a1_prev >= a2_prev

    if crossed_above:
        return "BUY"
    if crossed_below:
        return "SELL"
    return "NEUTRAL"


# =========================
# SMC / FVG rules
# =========================


Bias = Literal["BULLISH", "BEARISH", "NEUTRAL"]
Direction = Literal["bullish", "bearish"]


@dataclass(frozen=True)
class Candle:
    ts: str
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class FVG:
    direction: Direction
    timeframe: Literal["1H", "15M"]
    index_c1: int
    index_c2: int
    index_c3: int
    ts_c1: str
    ts_c3: str
    lower: float
    upper: float
    width: float


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except Exception as exc:
        raise ValueError(f"Invalid numeric value: {value!r}") from exc


def parse_candles(raw: Any) -> list[Candle]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("OHLC data must be a non-empty list.")
    out: list[Candle] = []
    for i, row in enumerate(raw):
        if not isinstance(row, dict):
            raise ValueError(f"Candle at index {i} must be an object.")
        ts = row.get("timestamp")
        if ts is None:
            raise ValueError(f"Candle at index {i} missing timestamp.")
        o = _as_float(row.get("open"))
        h = _as_float(row.get("high"))
        l = _as_float(row.get("low"))
        c = _as_float(row.get("close"))
        if h < l:
            raise ValueError(f"Candle at index {i} has high < low.")
        out.append(Candle(ts=str(ts), open=o, high=h, low=l, close=c))
    return out


def candles_from_ohlc_df(df: pd.DataFrame) -> list[Candle]:
    if df is None or df.empty:
        return []
    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(df.columns):
        return []
    out: list[Candle] = []
    for ts, row in df.iterrows():
        out.append(
            Candle(
                ts=str(ts),
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
            )
        )
    return out


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def _is_strong_displacement(c2: Candle, recent_ranges: Sequence[float]) -> bool:
    r2 = c2.high - c2.low
    if r2 <= 0:
        return False
    med = _median(list(recent_ranges)) if recent_ranges else r2
    if med <= 0:
        med = r2
    body = abs(c2.close - c2.open)
    body_ratio = body / r2 if r2 else 0.0
    close_pos = (c2.close - c2.low) / r2 if r2 else 0.5
    if len(recent_ranges) >= 5 and r2 < 1.3 * med:
        return False
    if body_ratio < 0.45:
        return False
    return close_pos < 0.2 or close_pos > 0.8


def find_fvgs(candles: list[Candle], timeframe: Literal["1H", "15M"]) -> list[FVG]:
    if len(candles) < 3:
        return []
    ranges = [c.high - c.low for c in candles]
    fvgs: list[FVG] = []
    for i in range(2, len(candles)):
        c1, c2, c3 = candles[i - 2], candles[i - 1], candles[i]
        recent = ranges[max(0, i - 22) : i - 2]
        if not _is_strong_displacement(c2, recent_ranges=recent):
            continue
        if c3.low > c1.high:
            lower, upper = c1.high, c3.low
            width = upper - lower
            if width < 0.25 * max(_median(recent) if recent else width, 1e-9):
                continue
            fvgs.append(FVG("bullish", timeframe, i - 2, i - 1, i, c1.ts, c3.ts, lower, upper, width))
            continue
        if c3.high < c1.low:
            lower, upper = c3.high, c1.low
            width = upper - lower
            if width < 0.25 * max(_median(recent) if recent else width, 1e-9):
                continue
            fvgs.append(FVG("bearish", timeframe, i - 2, i - 1, i, c1.ts, c3.ts, lower, upper, width))

    fvgs_sorted = sorted(fvgs, key=lambda f: (f.index_c3, f.width), reverse=True)
    kept: list[FVG] = []
    for f in fvgs_sorted:
        if any(k.direction == f.direction and not (f.upper < k.lower or f.lower > k.upper) for k in kept):
            continue
        kept.append(f)
    return sorted(kept, key=lambda f: f.index_c3)


def _was_respected(candles: list[Candle], fvg: FVG, lookahead: int = 30) -> bool:
    start = fvg.index_c3 + 1
    end = min(len(candles), start + lookahead)
    for c in candles[start:end]:
        if fvg.direction == "bullish":
            entered = c.low <= fvg.upper and c.high >= fvg.lower
            if entered and c.close > fvg.upper:
                return True
        else:
            entered = c.high >= fvg.lower and c.low <= fvg.upper
            if entered and c.close < fvg.lower:
                return True
    return False


def determine_bias(candles: list[Candle], fvgs: list[FVG]) -> Bias:
    if not fvgs:
        return "NEUTRAL"
    recent = sorted(fvgs, key=lambda f: f.index_c3, reverse=True)[:5]
    bull = any(f.direction == "bullish" and _was_respected(candles, f) for f in recent)
    bear = any(f.direction == "bearish" and _was_respected(candles, f) for f in recent)
    if bull and not bear:
        return "BULLISH"
    if bear and not bull:
        return "BEARISH"
    return "NEUTRAL"


def bias_from_ohlc_df(df: pd.DataFrame, timeframe: Literal["1H", "15M"]) -> Bias:
    candles = candles_from_ohlc_df(df)
    if len(candles) < 3:
        return "NEUTRAL"
    return determine_bias(candles, find_fvgs(candles, timeframe=timeframe))


def analyze_smc_fvg(payload: dict[str, Any]) -> dict[str, Any]:
    candles_1h = parse_candles(payload.get("1H"))
    candles_15m = parse_candles(payload.get("15M"))
    fvgs_1h = find_fvgs(candles_1h, timeframe="1H")
    bias = determine_bias(candles_1h, fvgs_1h)
    fvgs_15m_all = find_fvgs(candles_15m, timeframe="15M")
    if bias == "BULLISH":
        fvgs_15m = [f for f in fvgs_15m_all if f.direction == "bullish"]
    elif bias == "BEARISH":
        fvgs_15m = [f for f in fvgs_15m_all if f.direction == "bearish"]
    else:
        fvgs_15m = []
    return {
        "bias_1H": bias,
        "fvgs_1H": [
            {
                "direction": f.direction,
                "timeframe": f.timeframe,
                "zone": {"low": round(f.lower, 6), "high": round(f.upper, 6)},
                "formed": {"c1": f.ts_c1, "c3": f.ts_c3},
                "indices": {"c1": f.index_c1, "c2": f.index_c2, "c3": f.index_c3},
            }
            for f in fvgs_1h
        ],
        "fvgs_15M_aligned": [
            {
                "direction": f.direction,
                "timeframe": f.timeframe,
                "zone": {"low": round(f.lower, 6), "high": round(f.upper, 6)},
                "formed": {"c1": f.ts_c1, "c3": f.ts_c3},
                "indices": {"c1": f.index_c1, "c2": f.index_c2, "c3": f.index_c3},
            }
            for f in fvgs_15m
        ],
        "setup": None,
    }


# =========================
# Runner helper (kept here)
# =========================


def scan_symbols(symbols: Iterable[str], config: ScannerConfig, *, as_of: date | None = None) -> List[ScanResult]:
    results: List[ScanResult] = []
    symbol_list = list(symbols)
    total = len(symbol_list)

    as_of_end: datetime | None = None
    if as_of is not None:
        as_of_end = datetime(as_of.year, as_of.month, as_of.day) + timedelta(days=1)

    clean_symbols: list[str] = []
    for s in symbol_list:
        cs = s.strip().upper()
        if cs:
            clean_symbols.append(cs)

    def _scan_one(clean_symbol: str) -> ScanResult | None:
        # Fetch daily OHLCV
        if as_of is None:
            raw_df = fetch_ohlcv(clean_symbol, config.lookback_days)
        else:
            raw_df = fetch_ohlcv_asof(clean_symbol, config.lookback_days, as_of, interval="1d")
        if raw_df.empty:
            return None

        enriched_df = add_indicators(
            raw_df,
            ma_window=config.ma_window,
            ma50_window=config.ma50_window,
            breakout_window=config.breakout_window,
            rsi_window=config.rsi_window,
        )
        result = evaluate_symbol(clean_symbol, enriched_df, config)
        if not result:
            return None

        # AVWAP swing signal — informational, not used as a filter
        try:
            result.avwap_signal = compute_avwap_signal(raw_df)
        except Exception as exc:
            logger.warning("Failed to compute AVWAP signal for %s: %s", clean_symbol, exc)

        # Intraday bias only for matched symbols (still can be expensive but count is small)
        try:
            if as_of_end is None:
                df_1h = fetch_ohlcv_interval(clean_symbol, period="30d", interval="60m")
                df_15m = fetch_ohlcv_interval(clean_symbol, period="10d", interval="15m")
            else:
                df_1h = fetch_ohlcv_range(clean_symbol, start=as_of_end - timedelta(days=30), end=as_of_end, interval="60m")
                df_15m = fetch_ohlcv_range(clean_symbol, start=as_of_end - timedelta(days=10), end=as_of_end, interval="15m")
            result.bias_1h = bias_from_ohlc_df(df_1h, timeframe="1H")
            result.bias_15m = bias_from_ohlc_df(df_15m, timeframe="15M")
        except Exception as exc:
            logger.warning("Failed to compute SMC bias for %s: %s", clean_symbol, exc)

        return result

    # Parallelize daily fetches to cut wall-clock time.
    # Keep worker count conservative to reduce Yahoo throttling.
    max_workers = min(12, max(4, (os.cpu_count() or 4)))
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {ex.submit(_scan_one, cs): cs for cs in clean_symbols}
            done_count = 0
            for fut in as_completed(future_map):
                cs = future_map[fut]
                done_count += 1
                if done_count % 50 == 0 or done_count == total:
                    logger.info("Scanned %d/%d", done_count, total)
                try:
                    r = fut.result()
                    if r:
                        results.append(r)
                except Exception as exc:
                    logger.exception("Unhandled error while scanning %s: %s", cs, exc)
    except Exception:
        # Fallback to sequential in case the executor fails in some envs
        for idx, cs in enumerate(clean_symbols, start=1):
            logger.info("Scanning %d/%d: %s", idx, total, cs)
            try:
                r = _scan_one(cs)
                if r:
                    results.append(r)
            except Exception as exc:
                logger.exception("Unhandled error while scanning %s: %s", cs, exc)

    return rank_results(results)


def results_to_dataframe(results: list[ScanResult]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(
            columns=[
                "symbol",
                "close",
                "high20_prev",
                "volume",
                "avg_vol20",
                "volume_breakout",
                "rsi",
                "ma20",
                "ma50",
                "avg_turnover20_cr",
                "bias_1h",
                "bias_15m",
                "avwap_signal",
            ]
        )
    return pd.DataFrame([item.to_row() for item in results])

