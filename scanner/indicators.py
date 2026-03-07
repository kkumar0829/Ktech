import pandas as pd
from ta.momentum import RSIIndicator


def add_indicators(
    df: pd.DataFrame,
    ma_window: int,
    ma200_window: int,
    breakout_window: int,
    rsi_window: int,
) -> pd.DataFrame:
    out = df.copy()
    out["ma20"] = out["Close"].rolling(window=ma_window, min_periods=ma_window).mean()
    out["ma200"] = out["Close"].rolling(window=ma200_window, min_periods=ma200_window).mean()
    out["avg_vol20"] = out["Volume"].rolling(window=breakout_window, min_periods=breakout_window).mean()
    out["high20_prev"] = out["High"].rolling(window=breakout_window, min_periods=breakout_window).max().shift(1)
    out["rsi"] = RSIIndicator(close=out["Close"], window=rsi_window).rsi()
    out["turnover"] = out["Close"] * out["Volume"]
    out["avg_turnover20"] = out["turnover"].rolling(window=breakout_window, min_periods=breakout_window).mean()
    return out
