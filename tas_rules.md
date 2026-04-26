# TAS Scan Rules (Current Implementation)

This document describes the **exact rules currently used by the scanner** to decide whether an NSE symbol is a match.

Source of truth:
- Rule evaluation: `scanner/strategy.py` (`evaluate_symbol`)
- Indicator calculations: `scanner/indicators.py` (`add_indicators`)
- Default thresholds/windows: `scanner/config.py` (`ScannerConfig`)

## 1) Data universe and timeframe

- **Universe**: NSE cash symbols (the code appends `.NS` internally).
- **Candles**: daily (`interval="1d"`).
- **Lookback**: controlled by `lookback_days` (default **90**).

## 2) Indicators computed (per symbol)

All indicators are calculated from the downloaded OHLCV DataFrame.

### 2.1 Moving averages

- **MA20** (`ma20`):
  - \(ma20 = SMA(Close, 20)\)
  - Implemented as `Close.rolling(window=ma_window, min_periods=ma_window).mean()`
  - Default `ma_window = 20`

- **MA50** (`ma50`):
  - \(ma50 = SMA(Close, 50)\)
  - Implemented as `Close.rolling(window=ma50_window, min_periods=ma50_window).mean()`
  - Default `ma50_window = 50`

### 2.2 Breakout / volume baselines (20-day window)

Default `breakout_window = 20`.

- **Previous 20-day high** (`high20_prev`):
  - \(high20\_prev = \max(High_{t-20..t-1})\)
  - Implemented as `High.rolling(20).max().shift(1)`

- **20-day average volume** (`avg_vol20`):
  - \(avg\_vol20 = SMA(Volume, 20)\)
  - Implemented as `Volume.rolling(20).mean()`

### 2.3 RSI

- **RSI(14)** (`rsi`):
  - Computed using `ta.momentum.RSIIndicator(close=Close, window=rsi_window).rsi()`
  - Default `rsi_window = 14`

### 2.4 Turnover / liquidity

- **Turnover** (`turnover`):
  - \(turnover = Close \times Volume\)

- **20-day average turnover** (`avg_turnover20`):
  - \(avg\_turnover20 = SMA(turnover, 20)\)
  - Implemented as `turnover.rolling(20).mean()`

## 3) Warm-up / data readiness rule

The scanner **skips** a symbol if the latest row does not have fully computed indicator values.

Specifically, the latest candle must have **non-null** values for:
- `high20_prev`
- `avg_vol20`
- `rsi`
- `ma20`
- `ma50`
- `avg_turnover20`

If any are missing, the symbol is skipped as **insufficient indicator warm-up**.

## 4) Match rules (all must pass)

On the latest candle, a symbol is considered a match only if **all** rules below pass:

1. **20-day breakout**
   - `close > high20_prev`

2. **Volume breakout**
   - `volume_breakout = volume / avg_vol20`
   - Rule: `volume_breakout >= min_volume_multiplier`
   - Default: `min_volume_multiplier = 2.0`

3. **RSI band**
   - Rule: `min_rsi <= rsi <= max_rsi`
   - Defaults: `min_rsi = 55.0`, `max_rsi = 70.0`

4. **Above MA20**
   - `close > ma20`

5. **Above MA50 (when available)**
   - Rule: `close > ma50`
   - Note: this rule is **only enforced** when MA50 is computed (i.e., sufficient history exists).

6. **Liquidity / turnover filter**
   - Rule: `avg_turnover20 >= min_turnover_inr`
   - Default: `min_turnover_inr = 5e7` (5 crore INR)

## 5) Ranking of results

Matched symbols are sorted by **strongest volume breakout** (descending):
- `volume_breakout` high → ranked higher

## 6) Default parameters (ScannerConfig)

Defaults used if you don’t override them via the API:

- `lookback_days`: 90
- `breakout_window`: 20
- `rsi_window`: 14
- `ma_window`: 20
- `ma50_window`: 50
- `min_rsi`: 55.0
- `max_rsi`: 70.0
- `min_volume_multiplier`: 2.0
- `min_turnover_inr`: 5e7

## 7) SMC / Fair Value Gap (FVG) bias rule (intraday)

In addition to the daily momentum rules above, the scanner also computes **SMC/FVG bias** on two intraday timeframes and attaches them to each **matched** result row:

- `bias_1h`: `BULLISH` / `BEARISH` / `NEUTRAL`
- `bias_15m`: `BULLISH` / `BEARISH` / `NEUTRAL`

Source of truth:
- FVG + bias logic: `scanner/smc_fvg.py`
- Bias is computed from OHLC on:
  - **1H** candles (`interval="60m"`, ~30D window)
  - **15M** candles (`interval="15m"`, ~10D window)

### 7.1 FVG definitions (3-candle structure)

- **Bullish FVG**:
  - `low(candle3) > high(candle1)`
  - Zone = \([high(candle1), low(candle3)]\)

- **Bearish FVG**:
  - `high(candle3) < low(candle1)`
  - Zone = \([high(candle3), low(candle1)]\)

### 7.2 Strict filters (quality control)

Only “clean” FVGs are kept:
- Candle2 (the middle candle) must be a **strong displacement** candle (body + impulse heuristic).
- Very small/weak gaps are ignored (minimum width threshold).
- Overlapping FVGs in the same direction are de-duplicated (prefers the most recent).

### 7.3 Bias rules (per timeframe)

For a given timeframe:
- If recent price is **respecting bullish FVGs** → `BULLISH`
- If recent price is **respecting bearish FVGs** → `BEARISH`
- Else → `NEUTRAL`

“Respected” (strict heuristic):
- After the FVG forms, price trades into the zone and then closes away in the expected direction.

### 7.4 API-only SMC setup (optional endpoint)

There is also a dedicated endpoint for full 1H + 15M FVG analysis:
- `POST /api/v1/smc/fvg`

It returns:
- `bias_1H`
- all detected `fvgs_1H`
- `fvgs_15M_aligned` (15M FVGs aligned with 1H bias)
- `setup` (or `null` if no strict setup)

## Anchored VWAP (AVWAP) Swing Signal — `avwap_signal`

**Timeframe**: Daily  
**Purpose**: 1-2 day swing indicator — informational only, not used to filter stocks.  
**Key in scan response**: `avwap_signal` (`"BUY"` / `"SELL"` / `"NEUTRAL"`)

### AVWAP Definitions
- **Typical Price (TP)** = (H + L + C) / 3
- **1-Day AVWAP**: Anchored at the start of the **previous trading day** → on a daily bar series this equals TP of the previous bar (single-bar cumulation).
- **2-Day AVWAP**: Anchored at the start of **two trading days ago** → `cum(TP × V) / cum(V)` over the two previous bars.

### Signal Rules (long-only)
| Signal | Condition |
|--------|-----------|
| `BUY`  | 1D-AVWAP crosses **above** 2D-AVWAP on the latest close (was ≤ on the previous bar). Enter at next day open. |
| `SELL` | 1D-AVWAP crosses **below** 2D-AVWAP (was ≥ on the previous bar). Exit signal; max hold = 2 trading days. |
| `NEUTRAL` | No fresh crossover on the latest close. |

No short selling. Purely a swing duration of 1-2 trading days.

### Where it appears
Every stock that passes the momentum scan filter receives an `avwap_signal` key alongside `bias_1h` and `bias_15m`. It is **not** used to accept or reject a stock — it is purely an informational signal for the trader to consider.

