import os
import logging
import threading
import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request, render_template, redirect, url_for
import requests

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
_job_counter = 0


_STATE_DIR = Path(os.getenv("STATE_DIR", Path(__file__).parent))
_COUNTER_FILE = _STATE_DIR / "job_counter.txt"
_RESULTS_STORE_FILE = _STATE_DIR / "scan_results_store.jsonl"  # newest-first JSONL, keep last 50
_MAX_STORED_JOBS = int(os.getenv("MAX_STORED_JOBS", "200"))

# Supabase persistence (free-tier friendly if you use an external DB)
_SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
# Only enable Supabase when the server has the service_role key.
# Avoid accidentally enabling with a publishable/anon key (would fail with RLS and cause 500s).
_SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()


def _sb_enabled() -> bool:
    return bool(_SUPABASE_URL and _SUPABASE_KEY)


def _sb_headers() -> dict[str, str]:
    return {
        "apikey": _SUPABASE_KEY,
        "Authorization": f"Bearer {_SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def _sb_table_url() -> str:
    return f"{_SUPABASE_URL.rstrip('/')}/rest/v1/scan_jobs"


def _sb_create_job(*, batch: int, as_of: str | None) -> str:
    """Insert a job row and return numeric job_id as string."""
    payload: dict[str, Any] = {"status": "pending", "batch": batch}
    if as_of:
        payload["as_of"] = as_of
    r = requests.post(
        _sb_table_url(),
        headers={**_sb_headers(), "Prefer": "return=representation"},
        data=json.dumps(payload),
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    # PostgREST returns list of rows
    job_id = data[0]["job_id"]
    return str(job_id)


def _sb_update_job(job_id: str, patch: dict[str, Any]) -> None:
    r = requests.patch(
        _sb_table_url(),
        headers=_sb_headers(),
        params={"job_id": f"eq.{job_id}"},
        data=json.dumps(patch),
        timeout=20,
    )
    r.raise_for_status()


def _sb_get_job(job_id: str) -> dict[str, Any] | None:
    r = requests.get(
        _sb_table_url(),
        headers=_sb_headers(),
        params={"job_id": f"eq.{job_id}", "select": "*"},
        timeout=20,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else None


def _sb_list_jobs(limit: int = 50) -> list[dict[str, Any]]:
    r = requests.get(
        _sb_table_url(),
        headers=_sb_headers(),
        params={"select": "job_id,batch,created_at,finished_at,status", "order": "job_id.desc", "limit": str(limit)},
        timeout=20,
    )
    r.raise_for_status()
    return r.json() or []


def _ensure_state_dir() -> None:
    try:
        _STATE_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        # best-effort; Render's filesystem is writable for the service runtime
        pass


def _load_counter_from_file() -> int:
    _ensure_state_dir()
    try:
        raw = _COUNTER_FILE.read_text(encoding="utf-8").strip()
        return int(raw) if raw else 0
    except Exception:
        return 0


def _persist_counter_to_file(value: int) -> None:
    _ensure_state_dir()
    tmp = _COUNTER_FILE.with_suffix(".tmp")
    tmp.write_text(str(value), encoding="utf-8")
    tmp.replace(_COUNTER_FILE)


def _store_prepend_record(record: dict[str, Any], *, keep: int | None = None) -> None:
    """Store newest-first JSONL, keep only N records (LIFO)."""
    _ensure_state_dir()
    keep_n = int(keep) if keep is not None else _MAX_STORED_JOBS
    line = json.dumps(record, ensure_ascii=False)
    existing: list[str] = []
    if _RESULTS_STORE_FILE.exists():
        try:
            existing = [ln for ln in _RESULTS_STORE_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()]
        except Exception:
            existing = []
    new_lines = [line] + existing[: max(0, keep_n - 1)]
    tmp = _RESULTS_STORE_FILE.with_suffix(".tmp")
    tmp.write_text("\n".join(new_lines) + ("\n" if new_lines else ""), encoding="utf-8")
    tmp.replace(_RESULTS_STORE_FILE)


def _read_record_from_store(job_id: str) -> dict[str, Any] | None:
    """Read a job record from the results store file (newest-first)."""
    if not _RESULTS_STORE_FILE.exists():
        return None
    try:
        for ln in _RESULTS_STORE_FILE.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if not ln:
                continue
            obj = json.loads(ln)
            if str(obj.get("job_id")) == str(job_id):
                return obj
    except Exception:
        return None
    return None


def _format_job_label(job_id: str, batch: str | None, ts_iso: str | None) -> str:
    """Format exactly: 4 (2 - 24-04 2:07 pm)."""
    if not ts_iso:
        return f"{job_id} ({batch})" if batch else str(job_id)
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        # dd-mm h:mm am/pm
        label = dt.strftime("%d-%m %I:%M %p").lower()
        # strip leading zero from hour (e.g. 02:07 -> 2:07)
        label = label.replace(" 0", " ")
        b = str(batch) if batch else "?"
        return f"{job_id} ({b} - {label})"
    except Exception:
        return str(job_id)


def _list_jobs_from_store(*, limit: int | None = None) -> list[dict[str, str]]:
    """Return newest-first jobs from the store file for UI dropdown."""
    if _sb_enabled():
        out: list[dict[str, str]] = []
        try:
            rows = _sb_list_jobs(limit=int(limit) if limit is not None else _MAX_STORED_JOBS)
            for row in rows:
                jid_s = str(row.get("job_id"))
                batch = str(row.get("batch")) if row.get("batch") is not None else None
                ts = row.get("finished_at") or row.get("created_at")
                out.append({"id": jid_s, "label": _format_job_label(jid_s, batch, ts)})
        except Exception:
            return out
        return out

    if not _RESULTS_STORE_FILE.exists():
        return []
    out: list[dict[str, str]] = []
    try:
        lim = int(limit) if limit is not None else _MAX_STORED_JOBS
        for ln in _RESULTS_STORE_FILE.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if not ln:
                continue
            obj = json.loads(ln)
            jid = obj.get("job_id")
            if jid is None:
                continue
            jid_s = str(jid)
            ts = obj.get("finished_at") or obj.get("created_at")
            batch = None
            try:
                # stored records put batch under data.batch (done) or batch (error)
                if isinstance(obj.get("data"), dict) and obj["data"].get("batch") is not None:
                    batch = str(obj["data"].get("batch"))
                elif obj.get("batch") is not None:
                    batch = str(obj.get("batch"))
            except Exception:
                batch = None
            out.append({"id": jid_s, "label": _format_job_label(jid_s, batch, ts)})
            if len(out) >= lim:
                break
    except Exception:
        return out
    return out


def _new_job() -> str:
    global _job_counter
    if _sb_enabled():
        # Supabase identity column gives us short numeric ids; no local counter needed.
        # as_of/batch are attached later in scan_start/ui_start.
        raise RuntimeError("Use _create_job_with_batch() when Supabase is enabled")

    with _jobs_lock:
        if _job_counter <= 0:
            _job_counter = _load_counter_from_file()
        _job_counter += 1
        job_id = str(_job_counter)
        _persist_counter_to_file(_job_counter)
        _jobs[job_id] = {"status": "pending", "created_at": datetime.utcnow().isoformat()}
    return job_id


def _create_job_with_batch(*, batch: int, as_of: str | None) -> str:
    if _sb_enabled():
        return _sb_create_job(batch=batch, as_of=as_of)
    # fallback to local counter
    return _new_job()


def _update_job(job_id: str, **kwargs: Any) -> None:
    with _jobs_lock:
        _jobs[job_id].update(kwargs)


def _get_job(job_id: str) -> dict[str, Any] | None:
    with _jobs_lock:
        return dict(_jobs.get(job_id, {}))


def _payload_from_request() -> dict[str, Any]:
    """Support both GET query params and POST JSON body.

    Chrome-friendly:
    - GET /api/v1/scan?as_of=YYYY-MM-DD&b=2
    """
    if request.method == "GET":
        payload: dict[str, Any] = {}
        for key in request.args:
            payload[key] = request.args.get(key)
        return payload
    return _json_payload()


def _batch_params(payload: dict[str, Any]) -> tuple[int, int, int]:
    """Return (batch, limit, offset) for 2-batch scanning."""
    raw_b = str(payload.get("b", "")).strip()
    batch = int(raw_b) if raw_b else 1
    if batch not in (1, 2):
        raise ValueError("b must be 1 or 2")

    limit = 950
    offset = 0 if batch == 1 else 950
    return batch, limit, offset


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
    # Only support 2 batches (Chrome friendly): b=1 or b=2
    _, limit, offset = _batch_params(payload)
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
    if _sb_enabled():
        try:
            _sb_update_job(job_id, {"status": "running", "started_at": datetime.utcnow().isoformat()})
        except Exception as exc:
            logging.warning("[job:%s] Supabase update failed (running): %s", job_id, exc)
    try:
        job_snapshot = _get_job(job_id) or {}
        logging.info("[job:%s] Scanning %d symbols", job_id, len(symbols))
        results = scan_symbols(symbols, config, as_of=as_of)
        result_rows = results_to_dataframe(results).to_dict(orient="records")

        try:
            _log_scan_results(len(symbols), len(result_rows), result_rows)
        except Exception as exc:
            logging.warning("[job:%s] Failed to write scan log: %s", job_id, exc)

        data = {
            "scanned_symbols": len(symbols),
            "matched_symbols": len(result_rows),
            "results": result_rows,
            "applied_config": asdict(config),
            "as_of": as_of.isoformat() if as_of else None,
            "batch": job_snapshot.get("batch"),
            "limit": job_snapshot.get("limit"),
            "offset": job_snapshot.get("offset"),
        }
        _update_job(job_id, status="done", finished_at=datetime.utcnow().isoformat(), data=data)

        if _sb_enabled():
            try:
                _sb_update_job(
                    job_id,
                    {
                        "status": "done",
                        "finished_at": datetime.utcnow().isoformat(),
                        "result": data,
                        "error": None,
                    },
                )
                # keep last N in DB
                # (do best-effort cleanup; not critical if it fails)
                rows = _sb_list_jobs(limit=5000)
                if len(rows) > _MAX_STORED_JOBS:
                    cutoff = sorted([int(r["job_id"]) for r in rows], reverse=True)[_MAX_STORED_JOBS - 1]
                    requests.delete(
                        _sb_table_url(),
                        headers=_sb_headers(),
                        params={"job_id": f"lt.{cutoff}"},
                        timeout=20,
                    )
            except Exception as exc:
                logging.warning("[job:%s] Supabase update failed (done): %s", job_id, exc)
        else:
            _store_prepend_record(
                {
                    "job_id": job_id,
                    "status": "done",
                    "created_at": job_snapshot.get("created_at"),
                    "started_at": job_snapshot.get("started_at"),
                    "finished_at": datetime.utcnow().isoformat(),
                    "data": data,
                }
            )
        logging.info("[job:%s] Done — matched %d/%d", job_id, len(result_rows), len(symbols))
    except Exception as exc:
        logging.exception("[job:%s] Scan failed: %s", job_id, exc)
        _update_job(job_id, status="error", finished_at=datetime.utcnow().isoformat(), error=str(exc))
        job_snapshot = _get_job(job_id) or {}
        if _sb_enabled():
            try:
                _sb_update_job(
                    job_id,
                    {
                        "status": "error",
                        "finished_at": datetime.utcnow().isoformat(),
                        "error": str(exc),
                    },
                )
            except Exception as sb_exc:
                logging.warning("[job:%s] Supabase update failed (error): %s", job_id, sb_exc)
        else:
            _store_prepend_record(
                {
                    "job_id": job_id,
                    "status": "error",
                    "created_at": job_snapshot.get("created_at"),
                    "started_at": job_snapshot.get("started_at"),
                    "finished_at": datetime.utcnow().isoformat(),
                    "error": str(exc),
                    "batch": job_snapshot.get("batch"),
                    "limit": job_snapshot.get("limit"),
                    "offset": job_snapshot.get("offset"),
                }
            )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/api/v1/health")
def health() -> Any:
    return jsonify({"success": True, "message": "Service is healthy"}), 200


@app.get("/api/v1/scan")
@app.post("/api/v1/scan")
def scan_start() -> Any:
    """Start an async scan.  Returns a job_id immediately.
    Poll GET /api/v1/scan/<job_id> for progress / results.
    """
    try:
        payload = _payload_from_request()
        config = _build_config({})
        symbols = _resolve_symbols_from_payload(payload)
        as_of = _as_of_from_payload(payload)
        batch, limit, offset = _batch_params(payload)
    except ValueError as exc:
        return jsonify({"success": False, "message": "Invalid request payload.", "error": str(exc)}), 400

    try:
        job_id = _create_job_with_batch(batch=batch, as_of=as_of.isoformat() if as_of else None)
    except Exception as exc:
        logging.exception("Failed to create job id (Supabase/local): %s", exc)
        return jsonify({"success": False, "message": "Failed to start scan.", "error": str(exc)}), 500
    _update_job(job_id, batch=batch, limit=limit, offset=offset, total_in_chunk=len(symbols))
    t = threading.Thread(target=_run_scan_job, args=(job_id, symbols, config, as_of), daemon=True)
    t.start()

    return (
        jsonify(
            {
                "success": True,
                "message": "Scan started. Poll the status endpoint for results.",
                "job_id": job_id,
                "status_url": f"/api/v1/scan/{job_id}",
                "batch": batch,
                "limit": limit,
                "offset": offset,
            }
        ),
        202,
    )


# ---------------------------------------------------------------------------
# Simple UI (server-rendered HTML)
# ---------------------------------------------------------------------------


@app.get("/")
@app.get("/ui")
def ui_home() -> Any:
    as_of = request.args.get("as_of", "").strip()
    b = request.args.get("b", "").strip() or "1"
    job_id = request.args.get("job_id", "").strip()

    job: dict[str, Any] | None = None
    if job_id:
        job = _read_record_from_store(job_id) or _get_job(job_id)

    # UI dropdown: show up to the stored limit (default 200)
    recent_jobs = _list_jobs_from_store(limit=_MAX_STORED_JOBS)
    return render_template(
        "ui.html",
        as_of=as_of,
        b=b,
        job_id=job_id,
        job=job,
        recent_jobs=recent_jobs,
    )


@app.get("/ui/start")
def ui_start() -> Any:
    # Start scan with GET-friendly args (as_of, b)
    args_payload: dict[str, Any] = {}
    if request.args.get("as_of"):
        args_payload["as_of"] = request.args.get("as_of")
    if request.args.get("b"):
        args_payload["b"] = request.args.get("b")

    # Reuse the same logic as API start (but avoid calling the route function directly)
    try:
        config = _build_config({})
        symbols = _resolve_symbols_from_payload(args_payload)
        as_of = _as_of_from_payload(args_payload)
        batch, limit, offset = _batch_params(args_payload)
    except ValueError as exc:
        return render_template("ui.html", as_of=args_payload.get("as_of", ""), b=args_payload.get("b", "1"), job=None, job_id="", error=str(exc)), 400

    try:
        job_id = _create_job_with_batch(batch=batch, as_of=as_of.isoformat() if as_of else None)
    except Exception as exc:
        return (
            render_template(
                "ui.html",
                as_of=args_payload.get("as_of", ""),
                b=args_payload.get("b", "1"),
                job=None,
                job_id="",
                recent_jobs=_list_jobs_from_store(limit=_MAX_STORED_JOBS),
                error=f"Failed to start scan (persistence not configured): {exc}",
            ),
            500,
        )
    _update_job(job_id, batch=batch, limit=limit, offset=offset, total_in_chunk=len(symbols))
    t = threading.Thread(target=_run_scan_job, args=(job_id, symbols, config, as_of), daemon=True)
    t.start()

    return redirect(url_for("ui_job", job_id=job_id))


@app.get("/ui/job")
def ui_job_picker() -> Any:
    job_id = request.args.get("job_id", "").strip()
    if not job_id:
        return redirect(url_for("ui_home"))
    return redirect(url_for("ui_job", job_id=job_id))


@app.get("/ui/<job_id>")
@app.get("/ui/job/<job_id>")
def ui_job(job_id: str) -> Any:
    job = _read_record_from_store(job_id) or _get_job(job_id)
    if not job:
        return render_template("ui_job.html", job_id=job_id, status="not_found", data=None), 404

    # Normalize shape to match the stored record format when reading from memory
    status = job.get("status", "pending")
    if "data" in job:
        data = job.get("data")
    else:
        data = job.get("data")

    return render_template("ui_job.html", job_id=job_id, status=status, job=job, data=data)


@app.get("/api/v1/scan/<job_id>")
def scan_status(job_id: str) -> Any:
    """Poll the result of an async scan job."""
    if _sb_enabled():
        row = _sb_get_job(job_id)
        if not row:
            return jsonify({"success": False, "message": "Job not found."}), 404
        status = row.get("status")
        if status == "done":
            return (
                jsonify(
                    {
                        "success": True,
                        "status": "done",
                        "message": "Scan completed.",
                        "job_id": str(row.get("job_id")),
                        "data": row.get("result"),
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
                        "job_id": str(row.get("job_id")),
                        "error": row.get("error"),
                    }
                ),
                500,
            )
        return jsonify({"success": True, "status": status, "job_id": str(row.get("job_id"))}), 202

    # First check persistent store (survives restarts)
    stored = _read_record_from_store(job_id)
    if stored:
        status = stored.get("status")
        if status == "done":
            return (
                jsonify(
                    {
                        "success": True,
                        "status": "done",
                        "message": "Scan completed.",
                        "job_id": str(stored.get("job_id")),
                        "data": stored.get("data"),
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
                        "job_id": str(stored.get("job_id")),
                        "error": stored.get("error"),
                    }
                ),
                500,
            )
        return jsonify({"success": True, "status": status, "job_id": str(stored.get("job_id"))}), 202

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
