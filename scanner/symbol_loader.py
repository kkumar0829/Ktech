from pathlib import Path
from typing import List

from scanner.config import DEFAULT_STOCKS


def read_symbols_from_file(file_path: str) -> List[str]:
    path = Path(file_path)
    if not path.exists():
        return DEFAULT_STOCKS

    symbols = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    deduped = list(dict.fromkeys(symbols))
    return deduped if deduped else DEFAULT_STOCKS


def resolve_symbols(
    symbols_file: str = "non_fno_stocks.txt",
    limit: int = 200,
) -> List[str]:
    file_symbols = read_symbols_from_file(symbols_file)
    if limit > 0:
        return file_symbols[:limit]
    return file_symbols
