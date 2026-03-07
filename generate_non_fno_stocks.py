import io
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Set
import gzip

import pandas as pd
import requests

LOGGER = logging.getLogger("nse_non_fno_generator")

EQUITY_CSV_URLS = [
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
]

FNO_CONTRACT_URL_PATTERNS = [
    "https://nsearchives.nseindia.com/content/fo/NSE_FO_contract_{date}.csv.gz",
    "https://archives.nseindia.com/content/fo/NSE_FO_contract_{date}.csv.gz",
]

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}


WARMUP_URLS = [
    "https://www.nseindia.com/",
    "https://www.nseindia.com/market-data/live-equity-market",
    "https://www.nseindia.com/api/allIndices",
]


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    # Warm-up improves NSE success rate, but should not block execution if rejected.
    warmup_ok = False
    for url in WARMUP_URLS:
        try:
            response = session.get(url, timeout=15)
            if response.status_code < 400:
                warmup_ok = True
                break
            LOGGER.warning("Warm-up URL returned %s: %s", response.status_code, url)
        except requests.RequestException as exc:
            LOGGER.warning("Warm-up request failed for %s: %s", url, exc)

    if not warmup_ok:
        LOGGER.warning(
            "NSE warm-up was blocked. Continuing with direct archive fetch attempts."
        )
    return session


def _download_text(session: requests.Session, urls: Iterable[str], name: str) -> str:
    last_error: Exception | None = None

    for url in urls:
        for attempt in range(1, 4):
            try:
                LOGGER.info("Fetching %s from %s (attempt %d)", name, url, attempt)
                response = session.get(url, timeout=20)
                response.raise_for_status()
                text = response.text.strip()
                if not text:
                    raise ValueError(f"Empty response for {name} from {url}")
                return text
            except Exception as exc:
                last_error = exc
                LOGGER.warning("Fetch failed for %s (%s): %s", name, url, exc)
                time.sleep(1.2 * attempt)

    raise RuntimeError(f"Unable to fetch {name} from NSE sources: {last_error}") from last_error


def _download_bytes(session: requests.Session, url: str, name: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            LOGGER.info("Fetching %s from %s (attempt %d)", name, url, attempt)
            response = session.get(url, timeout=25)
            response.raise_for_status()
            content = response.content
            if not content:
                raise ValueError(f"Empty response for {name} from {url}")
            return content
        except Exception as exc:
            last_error = exc
            LOGGER.warning("Fetch failed for %s (%s): %s", name, url, exc)
            time.sleep(1.2 * attempt)

    raise RuntimeError(f"Unable to fetch {name}: {last_error}") from last_error


def _get_latest_fno_contract_csv_text(session: requests.Session, lookback_days: int = 10) -> str:
    today = datetime.utcnow().date()
    last_error: Exception | None = None

    for offset in range(lookback_days + 1):
        candidate_date = (today - timedelta(days=offset)).strftime("%d%m%Y")
        for pattern in FNO_CONTRACT_URL_PATTERNS:
            url = pattern.format(date=candidate_date)
            try:
                raw_gz = _download_bytes(session, url, f"F&O contract file {candidate_date}")
                csv_text = gzip.decompress(raw_gz).decode("utf-8", errors="ignore")
                if csv_text.strip():
                    LOGGER.info("Using F&O contract file date: %s", candidate_date)
                    return csv_text
                raise ValueError("Decompressed F&O file is empty.")
            except Exception as exc:
                last_error = exc

    raise RuntimeError(
        f"Could not fetch recent F&O contract CSV in last {lookback_days} days: {last_error}"
    ) from last_error


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(col).strip().upper() for col in normalized.columns]
    return normalized


def _load_fno_contract_csv(text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text), dtype=str)
    df = _normalize_columns(df)
    required_columns = {"FININSTRMNM", "TCKRSYMB"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"F&O contract CSV missing required columns: {missing}")
    return df


def fetch_equity_symbols(session: requests.Session) -> Set[str]:
    text = _download_text(session, EQUITY_CSV_URLS, "equity list")
    df = pd.read_csv(io.StringIO(text), dtype=str)
    df = _normalize_columns(df)

    required_columns = {"SYMBOL", "SERIES"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Equity CSV missing required columns: {missing}")

    equities = df[df["SERIES"].astype(str).str.strip().str.upper() == "EQ"]
    symbols = {
        symbol.strip().upper()
        for symbol in equities["SYMBOL"].dropna().astype(str)
        if symbol.strip()
    }
    LOGGER.info("Fetched %d NSE EQ symbols", len(symbols))
    return symbols


def fetch_fno_symbols(session: requests.Session) -> Set[str]:
    text = _get_latest_fno_contract_csv_text(session)
    df = _load_fno_contract_csv(text)

    stock_derivative_rows = df[
        df["FININSTRMNM"].astype(str).str.strip().str.upper().isin({"FUTSTK", "OPTSTK"})
    ]

    symbols = {
        symbol.strip().upper()
        for symbol in stock_derivative_rows["TCKRSYMB"].dropna().astype(str)
        if symbol.strip()
    }
    LOGGER.info("Fetched %d current F&O stock symbols", len(symbols))
    return symbols


def build_non_fno_symbols() -> List[str]:
    session = _build_session()
    equity_symbols = fetch_equity_symbols(session)
    fno_symbols = fetch_fno_symbols(session)

    # Intersect with equity universe to exclude anything that's not a cash EQ symbol.
    fno_equity_symbols = fno_symbols.intersection(equity_symbols)
    return sorted(equity_symbols - fno_equity_symbols)


def generate_non_fno_list(output_file: str = "non_fno_stocks.txt") -> int:
    non_fno_stocks = build_non_fno_symbols()

    output_path = Path(output_file)
    output_path.write_text("\n".join(non_fno_stocks) + "\n", encoding="utf-8")

    LOGGER.info("Generated %s with %d symbols", output_path, len(non_fno_stocks))
    print(f"Total non-F&O NSE equity symbols: {len(non_fno_stocks)}")
    return len(non_fno_stocks)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    try:
        generate_non_fno_list()
    except Exception as exc:
        LOGGER.exception("Failed to generate non-F&O stock list: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
