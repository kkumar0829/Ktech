# Indian Stock Momentum Scanner (NSE Cash)

Modular Python scanner to detect short-term breakout candidates using daily momentum + SMC/FVG bias + Anchored VWAP swing signal.

## Signals in every matched result

| Key | Values | Purpose |
|---|---|---|
| `bias_1h` | BULLISH / BEARISH / NEUTRAL | SMC FVG bias on 1H timeframe |
| `bias_15m` | BULLISH / BEARISH / NEUTRAL | SMC FVG bias on 15M timeframe |
| `avwap_signal` | BUY / SELL / NEUTRAL | 1-day vs 2-day Anchored VWAP crossover (swing, info only) |

## Project Structure

```text
.
├── main.py
├── render.yaml
├── requirements.txt
├── tas_rules.md
├── non_fno_stocks.txt
└── scanner/
    ├── __init__.py
    ├── config.py
    ├── data_fetcher.py
    ├── logger.py
    ├── rules.py          ← all rule logic lives here
    └── symbol_loader.py
```

## Local Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
PORT=5001 python main.py
```

## Deploy on Render

1. Push this repo to GitHub.
2. Go to [render.com](https://render.com) → **New → Web Service** → connect your repo.
3. Render auto-detects `render.yaml` — just click **Deploy**.
4. Your service URL will be something like `https://indian-stock-scanner.onrender.com`.

> **Free plan note**: Render free instances spin down after inactivity. Use the **Starter** plan ($7/mo) to keep it always-on.

## API Endpoints

### 1. Health Check

```http
GET /api/v1/health
```

Response: `{ "success": true, "message": "Service is healthy" }`

---

### 2. Start a Scan (async — returns immediately)

The full scan takes ~5 minutes. The API returns a `job_id` right away so Render's HTTP timeout is never hit.

```http
POST /api/v1/scan
Content-Type: application/json
```

Minimal payload (scan today's data, all defaults):

```json
{}
```

Historical scan:

```json
{ "as_of": "2026-04-16" }
```

Full payload:

```json
{
  "as_of": "2026-04-16",
  "limit": 0,
  "lookback_days": 90,
  "filters": {
    "min_rsi": 55,
    "max_rsi": 70,
    "min_volume_multiplier": 2.0,
    "min_turnover_inr": 50000000
  }
}
```

Response (202 Accepted — scan is running in background):

```json
{
  "success": true,
  "message": "Scan started. Poll the status endpoint for results.",
  "job_id": "a1b2c3d4-...",
  "status_url": "/api/v1/scan/a1b2c3d4-..."
}
```

---

### 3. Poll Scan Results

```http
GET /api/v1/scan/{job_id}
```

**While running** (202):
```json
{ "success": true, "status": "running", "job_id": "..." }
```

**When done** (200):
```json
{
  "success": true,
  "status": "done",
  "job_id": "...",
  "data": {
    "scanned_symbols": 1900,
    "matched_symbols": 14,
    "results": [
      {
        "symbol": "FSL",
        "close": 244.22,
        "rsi": 60.07,
        "volume_breakout": 14.63,
        "bias_1h": "BULLISH",
        "bias_15m": "NEUTRAL",
        "avwap_signal": "BUY"
      }
    ],
    "applied_config": {},
    "as_of": "2026-04-16"
  }
}
```

**Typical polling interval**: every 10–15 seconds until `status == "done"`.

---

### 4. SMC Fair Value Gap (FVG) Analysis

```http
POST /api/v1/smc/fvg
Content-Type: application/json
```

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

Returns `bias_1H`, detected `fvgs_1H`, and aligned `fvgs_15M_aligned`.

## Notes

- NSE symbols are passed **without** `.NS` — the code appends it internally.
- Results are sorted by strongest volume breakout (`volume / avg_vol20`).
- `avwap_signal` and bias keys are **informational only** — they do not filter stocks in/out.
- See `tas_rules.md` for the exact rule definitions.
