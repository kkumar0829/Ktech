import os
import logging
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

from scanner.config import ScannerConfig
from scanner.logger import setup_logging
from scanner.symbol_loader import resolve_symbols
from scanner.rules import analyze_smc_fvg, results_to_dataframe, scan_symbols

app = Flask(__name__)

setup_logging(level=logging.INFO)


def _json_payload() -> dict[str, Any]:
    payload = request.get_json(silent=True)
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError("JSON request body must be an object.")
    return payload


def _build_config(payload: dict[str, Any]) -> ScannerConfig:
    config_data = asdict(ScannerConfig())
    if "lookback_days" in payload:
        config_data["lookback_days"] = int(payload["lookback_days"])

    filters = payload.get("filters", {})
    if filters and not isinstance(filters, dict):
        raise ValueError("filters must be an object.")

    for key in [
        "breakout_window",
        "rsi_window",
        "ma_window",
        "ma50_window",
        "min_rsi",
        "max_rsi",
        "min_volume_multiplier",
        "min_turnover_inr",
    ]:
        if key in filters:
            value = filters[key]
            if key in {"breakout_window", "rsi_window", "ma_window", "ma50_window"}:
                config_data[key] = int(value)
            else:
                config_data[key] = float(value)

    return ScannerConfig(**config_data)


SCAN_RESULTS_LOG = Path(__file__).parent / "scan_results.log"


def _log_scan_results(
    scanned: int,
    matched: int,
    results: list[dict[str, Any]],
) -> None:
    """Append scan result to log file with datetime."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "",
        "=" * 70,
        f"{ts}  |  scanned: {scanned}  |  matched: {matched}",
        "-" * 70,
    ]
    if results:
        lines.append(f"{'symbol':<12} {'close':>10} {'ma50':>10} {'vol_breakout':>12} {'rsi':>8} {'avg_turnover20_cr':>18}")
        for r in results:
            lines.append(
                f"{str(r.get('symbol','')):<12} "
                f"{r.get('close',0):>10.2f} "
                f"{r.get('ma50',0):>10.2f} "
                f"{r.get('volume_breakout',0):>12.2f} "
                f"{r.get('rsi',0):>8.2f} "
                f"{r.get('avg_turnover20_cr',0):>18.2f}"
            )
    else:
        lines.append("(no matches)")
    lines.append("=" * 70)
    existing = SCAN_RESULTS_LOG.read_text(encoding="utf-8") if SCAN_RESULTS_LOG.exists() else ""
    SCAN_RESULTS_LOG.write_text(existing + "\n".join(lines) + "\n", encoding="utf-8")


def _resolve_symbols_from_payload(payload: dict[str, Any]) -> list[str]:
    symbols_file = str(payload.get("symbols_file", "non_fno_stocks.txt"))
    limit = int(payload.get("limit", 200))
    return resolve_symbols(symbols_file=symbols_file, limit=limit)


def _as_of_from_payload(payload: dict[str, Any]) -> date | None:
    raw = str(payload.get("as_of", "")).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception as exc:
        raise ValueError("as_of must be YYYY-MM-DD") from exc


@app.get("/api/v1/health")
def health() -> Any:
    return jsonify({"success": True, "message": "Service is healthy"}), 200


@app.post("/api/v1/scan")
def scan() -> Any:
    try:
        payload = _json_payload()
        config = _build_config(payload)
        symbols = _resolve_symbols_from_payload(payload)
        as_of = _as_of_from_payload(payload)

        logging.info("Starting API scan for %d symbols", len(symbols))
        results = scan_symbols(symbols, config, as_of=as_of)
        results_df = results_to_dataframe(results)
        result_rows = results_df.to_dict(orient="records")

        try:
            _log_scan_results(len(symbols), len(result_rows), result_rows)
        except Exception as exc:
            logging.warning("Failed to write scan results log: %s", exc)

        return (
            jsonify(
                {
                    "success": True,
                    "message": "Scan completed.",
                    "data": {
                        "scanned_symbols": len(symbols),
                        "matched_symbols": len(result_rows),
                        "results": result_rows,
                        "applied_config": asdict(config),
                        "as_of": as_of.isoformat() if as_of else None,
                    },
                }
            ),
            200,
        )
    except ValueError as exc:
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Invalid request payload.",
                    "error": str(exc),
                }
            ),
            400,
        )
    except Exception as exc:
        logging.exception("Scan failed: %s", exc)
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Scan failed due to internal error.",
                    "error": str(exc),
                }
            ),
            500,
        )


@app.post("/api/v1/smc/fvg")
def smc_fvg() -> Any:
    try:
        payload = _json_payload()
        result = analyze_smc_fvg(payload)
        return jsonify(result), 200
    except ValueError as exc:
        return (
            jsonify(
                {
                    "error": "INVALID_INPUT",
                    "message": str(exc),
                }
            ),
            400,
        )
    except Exception as exc:
        logging.exception("SMC FVG analysis failed: %s", exc)
        return (
            jsonify(
                {
                    "error": "INTERNAL_ERROR",
                    "message": str(exc),
                }
            ),
            500,
        )


if __name__ == "__main__":
    import socket

    def _free_port(start: int = 5001) -> int:
        for p in range(start, start + 10):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("", p))
                    return p
            except OSError:
                continue
        return start

    port = int(os.getenv("PORT", "0")) or _free_port()
    print(f"Starting server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
