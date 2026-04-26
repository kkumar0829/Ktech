import logging
import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def to_nse_symbol(symbol: str) -> str:
    symbol = symbol.strip().upper()
    return symbol if symbol.endswith(".NS") else f"{symbol}.NS"


@contextmanager
def _without_proxy_env():
    """
    Some environments set HTTP/SOCKS proxy env vars which yfinance (via curl_cffi)
    may honor, even when the local proxy doesn't allow CONNECT to Yahoo.
    """
    proxy_keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "SOCKS_PROXY",
        "SOCKS5_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "socks_proxy",
        "socks5_proxy",
    ]
    saved = {}
    try:
        for k in proxy_keys:
            if k in os.environ:
                saved[k] = os.environ.pop(k)
        yield
    finally:
        # Restore prior environment to avoid side effects elsewhere.
        for k, v in saved.items():
            os.environ[k] = v


def fetch_ohlcv(symbol: str, lookback_days: int) -> pd.DataFrame:
    return fetch_ohlcv_interval(symbol=symbol, period=f"{lookback_days}d", interval="1d")


def fetch_ohlcv_asof(
    symbol: str,
    lookback_days: int,
    as_of: date,
    *,
    interval: str = "1d",
    extra_lookback_days: int = 10,
) -> pd.DataFrame:
    """
    Fetch OHLCV up to (and including) a given as_of date.

    Notes:
    - yfinance `end` is exclusive, so we use as_of + 1 day.
    - We fetch a slightly larger window to avoid indicator warm-up gaps.
    """
    end_dt = datetime(as_of.year, as_of.month, as_of.day) + timedelta(days=1)
    start_dt = end_dt - timedelta(days=int(lookback_days) + int(extra_lookback_days))
    df = fetch_ohlcv_range(symbol=symbol, start=start_dt, end=end_dt, interval=interval)
    if df.empty:
        return df
    # Ensure we don't include candles beyond as_of (in case of timezone quirks).
    idx = pd.to_datetime(df.index)
    return df.loc[idx < end_dt].copy()


def fetch_ohlcv_range(symbol: str, start: datetime, end: datetime, interval: str) -> pd.DataFrame:
    """Fetch OHLCV for a concrete [start, end) datetime range."""
    ticker = to_nse_symbol(symbol)
    try:
        with _without_proxy_env():
            df = yf.download(
                ticker,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
    except Exception as exc:
        logger.exception("Failed to fetch data for %s (%s..%s/%s): %s", ticker, start, end, interval, exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("No data returned for %s (%s..%s/%s)", ticker, start, end, interval)
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    required_cols = {"Open", "High", "Low", "Close", "Volume"}
    if not required_cols.issubset(df.columns):
        logger.error(
            "Missing required columns for %s (%s..%s/%s). Found: %s",
            ticker,
            start,
            end,
            interval,
            df.columns,
        )
        return pd.DataFrame()

    return df.dropna().copy()


def fetch_ohlcv_interval(symbol: str, period: str, interval: str) -> pd.DataFrame:
    """
    Fetch OHLCV data via yfinance for any supported interval.

    Examples:
    - interval="1d", period="120d"
    - interval="60m", period="30d"
    - interval="15m", period="10d"
    """
    ticker = to_nse_symbol(symbol)
    try:
        # Temporarily disable proxy env vars for this external data fetch.
        with _without_proxy_env():
            df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
    except Exception as exc:
        logger.exception("Failed to fetch data for %s (%s/%s): %s", ticker, period, interval, exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("No data returned for %s (%s/%s)", ticker, period, interval)
        return pd.DataFrame()

    # yfinance may return multiindex columns depending on version/settings.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    required_cols = {"Open", "High", "Low", "Close", "Volume"}
    if not required_cols.issubset(df.columns):
        logger.error(
            "Missing required columns for %s (%s/%s). Found: %s",
            ticker,
            period,
            interval,
            df.columns,
        )
        return pd.DataFrame()

    return df.dropna().copy()
