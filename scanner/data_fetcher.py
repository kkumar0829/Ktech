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


_REQUIRED_COLS = {"Open", "High", "Low", "Close", "Volume"}
# yfinance batch size; tune via SCAN_BATCH_SIZE on the scanner side.
_DEFAULT_BATCH_SIZE = int(os.getenv("SCAN_BATCH_SIZE", "80"))


def _normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = out.columns.get_level_values(0)
    if not _REQUIRED_COLS.issubset(out.columns):
        return pd.DataFrame()
    return out.dropna().copy()


def _split_batch_download(raw: pd.DataFrame, clean_symbols: list[str]) -> dict[str, pd.DataFrame]:
    """Split a multi-ticker yfinance download into per-symbol frames."""
    empty = {s: pd.DataFrame() for s in clean_symbols}
    if raw.empty or not clean_symbols:
        return empty

    tickers = [to_nse_symbol(s) for s in clean_symbols]
    if len(clean_symbols) == 1:
        empty[clean_symbols[0]] = _normalize_ohlcv_frame(raw)
        return empty

    if not isinstance(raw.columns, pd.MultiIndex):
        # Unexpected shape — assign whole frame to first symbol only.
        empty[clean_symbols[0]] = _normalize_ohlcv_frame(raw)
        return empty

    level0 = set(raw.columns.get_level_values(0))
    for sym, tic in zip(clean_symbols, tickers):
        if tic in level0:
            empty[sym] = _normalize_ohlcv_frame(raw[tic])
        else:
            # Some yfinance builds use symbol without exchange suffix as key.
            base = sym
            if base in level0:
                empty[sym] = _normalize_ohlcv_frame(raw[base])
    return empty


def _download_batch(
    clean_symbols: list[str],
    *,
    period: str | None = None,
    interval: str = "1d",
    start: datetime | None = None,
    end: datetime | None = None,
) -> dict[str, pd.DataFrame]:
    if not clean_symbols:
        return {}
    tickers = [to_nse_symbol(s) for s in clean_symbols]
    kwargs: dict = {
        "tickers": tickers if len(tickers) > 1 else tickers[0],
        "interval": interval,
        "auto_adjust": False,
        "progress": False,
        "threads": True,
    }
    if period is not None:
        kwargs["period"] = period
    else:
        kwargs["start"] = start
        kwargs["end"] = end
    if len(tickers) > 1:
        kwargs["group_by"] = "ticker"

    try:
        with _without_proxy_env():
            raw = yf.download(**kwargs)
    except Exception as exc:
        logger.exception("Batch download failed (%s symbols, %s): %s", len(clean_symbols), interval, exc)
        return {s: pd.DataFrame() for s in clean_symbols}

    return _split_batch_download(raw, clean_symbols)


def fetch_ohlcv_batch_daily(symbols: list[str], lookback_days: int, *, chunk_size: int | None = None) -> dict[str, pd.DataFrame]:
    """Fetch daily OHLCV for many symbols in few yfinance calls."""
    size = chunk_size or _DEFAULT_BATCH_SIZE
    out: dict[str, pd.DataFrame] = {}
    for i in range(0, len(symbols), size):
        chunk = symbols[i : i + size]
        out.update(_download_batch(chunk, period=f"{lookback_days}d", interval="1d"))
    return out


def fetch_ohlcv_batch_interval(
    symbols: list[str],
    period: str,
    interval: str,
    *,
    chunk_size: int | None = None,
) -> dict[str, pd.DataFrame]:
    size = chunk_size or _DEFAULT_BATCH_SIZE
    out: dict[str, pd.DataFrame] = {}
    for i in range(0, len(symbols), size):
        chunk = symbols[i : i + size]
        out.update(_download_batch(chunk, period=period, interval=interval))
    return out


def fetch_ohlcv_batch_range(
    symbols: list[str],
    start: datetime,
    end: datetime,
    interval: str,
    *,
    chunk_size: int | None = None,
    trim_before: datetime | None = None,
) -> dict[str, pd.DataFrame]:
    size = chunk_size or _DEFAULT_BATCH_SIZE
    out: dict[str, pd.DataFrame] = {}
    for i in range(0, len(symbols), size):
        chunk = symbols[i : i + size]
        chunk_map = _download_batch(chunk, interval=interval, start=start, end=end)
        if trim_before is not None:
            for sym, df in chunk_map.items():
                if not df.empty:
                    idx = pd.to_datetime(df.index)
                    chunk_map[sym] = df.loc[idx < trim_before].copy()
        out.update(chunk_map)
    return out


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

    if not _REQUIRED_COLS.issubset(df.columns):
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

    if not _REQUIRED_COLS.issubset(df.columns):
        logger.error(
            "Missing required columns for %s (%s/%s). Found: %s",
            ticker,
            period,
            interval,
            df.columns,
        )
        return pd.DataFrame()

    return df.dropna().copy()


def fetch_last_close(symbol: str) -> float | None:
    """Latest daily close for a symbol (used for tradebook PnL)."""
    df = fetch_ohlcv_interval(symbol=symbol, period="5d", interval="1d")
    if df.empty:
        return None
    try:
        return float(df["Close"].iloc[-1])
    except Exception:
        return None
