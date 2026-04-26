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

### 2) Run momentum scan

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
  "symbols_file": "non_fno_stocks.txt",
  "limit": 200,
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

### 3) SMC Fair Value Gap (FVG) analysis (1H + 15M)

```http
POST /api/v1/smc/fvg
Content-Type: application/json
```

Request body:

```json
{
  "1H": [
    { "timestamp": "2026-01-01T10:00:00Z", "open": 0, "high": 0, "low": 0, "close": 0 }
  ],
  "15M": [
    { "timestamp": "2026-01-01T10:00:00Z", "open": 0, "high": 0, "low": 0, "close": 0 }
  ]
}
```

Response:
- Always returns JSON with `bias_1H`, `fvgs_1H`, `fvgs_15M_aligned`, and `setup`.
- `setup` is `null` when no strict setup is detected.
