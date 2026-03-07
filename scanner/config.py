from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class ScannerConfig:
    lookback_days: int = 90
    breakout_window: int = 20
    rsi_window: int = 14
    ma_window: int = 20
    ma200_window: int = 200
    min_rsi: float = 55.0
    max_rsi: float = 70.0
    min_volume_multiplier: float = 2.0
    min_turnover_inr: float = 5e7  # 5 crore INR


DEFAULT_STOCKS: List[str] = [
    "RELIANCE",
    "TCS",
    "INFY",
    "HDFCBANK",
    "ICICIBANK",
    "SBIN",
    "AXISBANK",
    "LT",
    "BHARTIARTL",
    "ITC",
]
