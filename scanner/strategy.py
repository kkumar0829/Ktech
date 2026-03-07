import logging
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from scanner.config import ScannerConfig

logger = logging.getLogger(__name__)


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
    ma200: float
    avg_turnover20: float

    def to_row(self) -> Dict[str, float]:
        return {
            "symbol": self.symbol,
            "close": round(self.close, 2),
            "high20_prev": round(self.high20_prev, 2),
            "volume": int(self.volume),
            "avg_vol20": int(self.avg_vol20),
            "volume_breakout": round(self.volume_breakout, 2),
            "rsi": round(self.rsi, 2),
            "ma20": round(self.ma20, 2),
            "ma200": round(self.ma200, 2),
            "avg_turnover20_cr": round(self.avg_turnover20 / 1e7, 2),
        }


def evaluate_symbol(symbol: str, df: pd.DataFrame, config: ScannerConfig) -> ScanResult | None:
    if df.empty:
        return None

    latest = df.iloc[-1]

    # Guard against incomplete warm-up rows (ma200 optional when lookback < 200).
    required_fields = ["high20_prev", "avg_vol20", "rsi", "ma20", "ma200", "avg_turnover20"]
    if latest[required_fields].isna().any():
        logger.info("Skipping %s due to insufficient indicator warm-up", symbol)
        return None

    close = float(latest["Close"])
    high20_prev = float(latest["high20_prev"])
    volume = float(latest["Volume"])
    avg_vol20 = float(latest["avg_vol20"])
    rsi = float(latest["rsi"])
    ma20 = float(latest["ma20"])
    ma200_raw = latest["ma200"]
    ma200 = float(ma200_raw) if pd.notna(ma200_raw) else None
    avg_turnover20 = float(latest["avg_turnover20"])
    volume_breakout = volume / avg_vol20 if avg_vol20 > 0 else 0.0

    rules_passed = all(
        [
            close > high20_prev,
            volume_breakout >= config.min_volume_multiplier,
            config.min_rsi <= rsi <= config.max_rsi,
            close > ma20,
            ma200 is None or close > ma200,  # ma200 only when we have enough data
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
        ma200=ma200 if ma200 is not None else 0.0,  # 0 when not computed
        avg_turnover20=avg_turnover20,
    )


def rank_results(results: List[ScanResult]) -> List[ScanResult]:
    return sorted(results, key=lambda r: r.volume_breakout, reverse=True)
