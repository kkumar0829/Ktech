import logging

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def to_nse_symbol(symbol: str) -> str:
    symbol = symbol.strip().upper()
    return symbol if symbol.endswith(".NS") else f"{symbol}.NS"


def fetch_ohlcv(symbol: str, lookback_days: int) -> pd.DataFrame:
    ticker = to_nse_symbol(symbol)
    try:
        df = yf.download(
            ticker,
            period=f"{lookback_days}d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as exc:
        logger.exception("Failed to fetch data for %s: %s", ticker, exc)
        return pd.DataFrame()

    if df.empty:
        logger.warning("No data returned for %s", ticker)
        return pd.DataFrame()

    # yfinance may return multiindex columns depending on version/settings.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    required_cols = {"Open", "High", "Low", "Close", "Volume"}
    if not required_cols.issubset(df.columns):
        logger.error("Missing required columns for %s. Found: %s", ticker, df.columns)
        return pd.DataFrame()

    return df.dropna().copy()
