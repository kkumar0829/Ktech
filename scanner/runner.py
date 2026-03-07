import logging
from typing import Iterable, List

import pandas as pd

from scanner.config import ScannerConfig
from scanner.data_fetcher import fetch_ohlcv
from scanner.indicators import add_indicators
from scanner.strategy import ScanResult, evaluate_symbol, rank_results

logger = logging.getLogger(__name__)


def scan_symbols(symbols: Iterable[str], config: ScannerConfig) -> List[ScanResult]:
    results: List[ScanResult] = []
    symbol_list = list(symbols)
    total = len(symbol_list)

    for idx, symbol in enumerate(symbol_list, start=1):
        clean_symbol = symbol.strip().upper()
        if not clean_symbol:
            continue

        logger.info("Scanning %d/%d: %s", idx, total, clean_symbol)
        try:
            raw_df = fetch_ohlcv(clean_symbol, config.lookback_days)
            if raw_df.empty:
                logger.info("No usable data for %s", clean_symbol)
                continue

            enriched_df = add_indicators(
                raw_df,
                ma_window=config.ma_window,
                ma200_window=config.ma200_window,
                breakout_window=config.breakout_window,
                rsi_window=config.rsi_window,
            )
            result = evaluate_symbol(clean_symbol, enriched_df, config)
            if result:
                results.append(result)
                logger.info(
                    "Matched %s (volume breakout: %.2fx, RSI: %.2f)",
                    clean_symbol,
                    result.volume_breakout,
                    result.rsi,
                )
            else:
                logger.info("No breakout setup for %s", clean_symbol)

        except Exception as exc:
            logger.exception("Unhandled error while scanning %s: %s", clean_symbol, exc)

    return rank_results(results)


def results_to_dataframe(results: List[ScanResult]) -> pd.DataFrame:
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
                "ma200",
                "avg_turnover20_cr",
            ]
        )
    return pd.DataFrame([item.to_row() for item in results])
