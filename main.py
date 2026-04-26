import os
import logging
import threading
import uuid
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


# ---------------------------------------------------------------------------
# In-memory async job store  (lives as long as the process, single-worker)
# ---------------------------------------------------------------------------

_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = threading.Lock()


def _new_job() -> str:
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {"status": "pending", "created_at": datetime.utcnow().isoformat()}
    return job_id


def _update_job(job_id: str, **kwargs: Any) -> None:
    with _jobs_lock:
        _jobs[job_id].update(kwargs)


def _get_job(job_id: str) -> dict[str, Any] | None:
    with _jobs_lock:
        return dict(_jobs.get(job_id, {}))


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------


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


def _log_scan_results(scanned: int, matched: int, results: list[dict[str, Any]]) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "",
        "=" * 70,
        f"{ts}  |  scanned: {scanned}  |  matched: {matched}",
        "-" * 70,
    ]
    if results:
        lines.append(
            f"{'symbol':<12} {'close':>10} {'ma50':>10} {'vol_breakout':>12} {'rsi':>8} {'avg_turnover20_cr':>18}"
        )
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
    offset = int(payload.get("offset", 0))
    return resolve_symbols(symbols_file=symbols_file, limit=limit, offset=offset)


def _as_of_from_payload(payload: dict[str, Any]) -> date | None:
    raw = str(payload.get("as_of", "")).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception as exc:
        raise ValueError("as_of must be YYYY-MM-DD") from exc


# ---------------------------------------------------------------------------
# Background worker for the long-running scan
# ---------------------------------------------------------------------------


def _run_scan_job(job_id: str, symbols: list[str], config: ScannerConfig, as_of: date | None) -> None:
    _update_job(job_id, status="running", started_at=datetime.utcnow().isoformat())
    try:
        logging.info("[job:%s] Scanning %d symbols", job_id, len(symbols))
        results = scan_symbols(symbols, config, as_of=as_of)
        result_rows = results_to_dataframe(results).to_dict(orient="records")

        try:
            _log_scan_results(len(symbols), len(result_rows), result_rows)
        except Exception as exc:
            logging.warning("[job:%s] Failed to write scan log: %s", job_id, exc)

        _update_job(
            job_id,
            status="done",
            finished_at=datetime.utcnow().isoformat(),
            data={
                "scanned_symbols": len(symbols),
                "matched_symbols": len(result_rows),
                "results": result_rows,
                "applied_config": asdict(config),
                "as_of": as_of.isoformat() if as_of else None,
            },
        )
        logging.info("[job:%s] Done — matched %d/%d", job_id, len(result_rows), len(symbols))
    except Exception as exc:
        logging.exception("[job:%s] Scan failed: %s", job_id, exc)
        _update_job(
            job_id,
            status="error",
            finished_at=datetime.utcnow().isoformat(),
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/api/v1/health")
def health() -> Any:
    return jsonify({"success": True, "message": "Service is healthy"}), 200


@app.post("/api/v1/scan")
def scan_start() -> Any:
    """Start an async scan.  Returns a job_id immediately.
    Poll GET /api/v1/scan/<job_id> for progress / results.
    """
    try:
        payload = _json_payload()
        config = _build_config(payload)
        symbols = _resolve_symbols_from_payload(payload)
        as_of = _as_of_from_payload(payload)
        offset = int(payload.get("offset", 0))
    except ValueError as exc:
        return jsonify({"success": False, "message": "Invalid request payload.", "error": str(exc)}), 400

    job_id = _new_job()
    _update_job(job_id, offset=offset, total_in_chunk=len(symbols))
    t = threading.Thread(target=_run_scan_job, args=(job_id, symbols, config, as_of), daemon=True)
    t.start()

    return (
        jsonify(
            {
                "success": True,
                "message": "Scan started. Poll the status endpoint for results.",
                "job_id": job_id,
                "status_url": f"/api/v1/scan/{job_id}",
            }
        ),
        202,
    )


@app.get("/api/v1/scan/<job_id>")
def scan_status(job_id: str) -> Any:
    """Poll the result of an async scan job."""
    job = _get_job(job_id)
    if not job:
        return jsonify({"success": False, "message": "Job not found."}), 404

    status = job.get("status")

    if status == "done":
        return (
            jsonify(
                {
                    "success": True,
                    "status": "done",
                    "message": "Scan completed.",
                    "job_id": job_id,
                    "data": job.get("data"),
                }
            ),
            200,
        )

    if status == "error":
        return (
            jsonify(
                {
                    "success": False,
                    "status": "error",
                    "message": "Scan failed.",
                    "job_id": job_id,
                    "error": job.get("error"),
                }
            ),
            500,
        )

    # pending / running
    return (
        jsonify(
            {
                "success": True,
                "status": status,
                "message": "Scan is still running. Try again in a few seconds.",
                "job_id": job_id,
            }
        ),
        202,
    )


@app.post("/api/v1/smc/fvg")
def smc_fvg() -> Any:
    try:
        payload = _json_payload()
        result = analyze_smc_fvg(payload)
        return jsonify(result), 200
    except ValueError as exc:
        return jsonify({"error": "INVALID_INPUT", "message": str(exc)}), 400
    except Exception as exc:
        logging.exception("SMC FVG analysis failed: %s", exc)
        return jsonify({"error": "INTERNAL_ERROR", "message": str(exc)}), 500


# ---------------------------------------------------------------------------
# Dev entrypoint (not used by Gunicorn on Render)
# ---------------------------------------------------------------------------

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
