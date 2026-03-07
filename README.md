# Indian Stock Momentum Scanner (NSE Cash)

Simple, modular Python scanner to detect short-term breakout candidates using:

- Close > previous 20-day high
- Volume > 2x 20-day average volume
- RSI between 55 and 70
- Close above 20-day moving average
- 20-day average turnover >= 5 crore INR

## Project Structure

```text
.
├── main.py
├── requirements.txt
└── scanner/
    ├── __init__.py
    ├── config.py
    ├── data_fetcher.py
    ├── indicators.py
    ├── logger.py
    ├── runner.py
    └── strategy.py
```

## Setup

1. Activate your virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run Flask API

Start server:

```bash
python3 main.py
```

Base URL:

```text
http://localhost:5000
```

## API Endpoints

### 1) Health Check

```http
GET /api/v1/health
```

### 2) Generate non-F&O symbol file

```http
POST /api/v1/symbols/non-fno/generate
Content-Type: application/json
```

Request body (optional):

```json
{
  "output_file": "non_fno_stocks.txt",
  "include_symbols": false,
  "force_refresh": false
}
```

Notes:
- If file already exists and `force_refresh` is `false`, API reuses existing symbols file.
- Set `force_refresh: true` only when you want to fetch fresh NSE symbols again.

### 3) Run momentum scan

```http
POST /api/v1/scan
Content-Type: application/json
```

Sample request:

```json
{
  "symbols_file": "non_fno_stocks.txt",
  "limit": 200,
  "lookback_days": 90,
  "filters": {
    "min_rsi": 55,
    "max_rsi": 70,
    "min_volume_multiplier": 2.0,
    "min_turnover_inr": 50000000
  }
}
```

You can also pass symbols directly:

```json
{
  "symbols": ["RELIANCE", "TCS", "INFY"],
  "lookback_days": 90
}
```

Response shape:

```json
{
  "success": true,
  "message": "Scan completed.",
  "data": {
    "scanned_symbols": 200,
    "matched_symbols": 3,
    "results": [],
    "applied_config": {}
  }
}
```

## Notes

- Use NSE symbols without `.NS`; code appends it internally.
- Results are sorted by strongest volume breakout (`volume / avg_vol20`).
- `scan_symbols()` and `ScanResult` are intentionally separated to keep it easy to plug into a future backtest module.
