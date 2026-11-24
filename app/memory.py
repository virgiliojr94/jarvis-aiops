"""Persistent SQLite + sqlite-vec memory, Keep-style job queue, and feedback loop."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import uuid
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("JARVIS_DB_PATH", "/app/data/jarvis.db")
EMBED_PROVIDER = os.getenv("JARVIS_EMBED_PROVIDER", "auto").strip().lower()
EMBED_MODEL = os.getenv("JARVIS_EMBED_MODEL", "gemini-embedding-001")
OPENAI_EMBED_MODEL = os.getenv("JARVIS_OPENAI_EMBED_MODEL", "text-embedding-3-small")
EMBED_DIM = int(os.getenv("JARVIS_EMBED_DIM", "768"))

RECALL_K = int(os.getenv("JARVIS_RECALL_K", "5"))
RECALL_MAX_COSINE_DIST = float(os.getenv("JARVIS_RECALL_MAX_DIST", "0.25"))

MIN_RESOLUTION_CHARS = 100
MIN_INCIDENT_SECONDS = 120
MAX_ROWS = int(os.getenv("JARVIS_MAX_INCIDENTS", "5000"))

DIAGNOSIS_JOB_STATES = (
    "queued", "running", "done", "sent", "failed",
    "skipped_circuit_breaker", "suppressed_flap", "grouped",
)

KNOWN_ABSENT = frozenset(
    x.strip().lower()
    for x in os.getenv("JARVIS_REJECTED_PATTERNS", "").split(",")
    if x.strip()
)


def _connect() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH)
    try:
        import sqlite_vec
        db.enable_load_extension(True)
        sqlite_vec.load(db)
        db.enable_load_extension(False)
    except Exception as exc:
        logger.debug("sqlite_vec load skipped: %s", exc)
    db.row_factory = sqlite3.Row
    return db


def init_db() -> None:
    """Idempotently initialize incidents, vec0 index, diagnosis_jobs, conversation_turns, and feedback."""
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with _connect() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id       TEXT,
                clock          TEXT DEFAULT (datetime('now')),
                host           TEXT,
                host_id        TEXT,
                trigger_name   TEXT,
                severity       TEXT,
                diagnosis      TEXT,
                resolution     TEXT,
                feedback_score INTEGER NOT NULL DEFAULT 0
            )
        """)
        db.execute("CREATE INDEX IF NOT EXISTS idx_incidents_host ON incidents(host)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_incidents_event ON incidents(event_id)")

        cols = {r[1] for r in db.execute("PRAGMA table_info(incidents)").fetchall()}
        if "feedback_score" not in cols:
            db.execute("ALTER TABLE incidents ADD COLUMN feedback_score INTEGER NOT NULL DEFAULT 0")
        db.execute("CREATE INDEX IF NOT EXISTS idx_incidents_score ON incidents(feedback_score)")

        try:
            db.execute(
                f"CREATE VIRTUAL TABLE IF NOT EXISTS vec_incidents "
                f"USING vec0(embedding float[{EMBED_DIM}] distance=cosine)"
            )
        except Exception as exc:
            logger.debug("vec0 table creation skipped: %s", exc)
        _init_diagnosis_queue(db)
        db.commit()


def _init_diagnosis_queue(db: sqlite3.Connection) -> None:
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='diagnosis_jobs'"
    ).fetchone()

    if row:
        current_sql = row[0] or ""
        cols = {r[1] for r in db.execute("PRAGMA table_info(diagnosis_jobs)").fetchall()}
        needs_migration = (
            "suppressed_flap" not in current_sql
            or "grouped" not in current_sql
            or "skipped_circuit_breaker" not in current_sql
            or "run_after" not in cols
            or "group_id" not in cols
            or "correlated_events" not in cols
        )
        if needs_migration:
            logger.info("Migrating diagnosis_jobs table to Keep schema...")
            db.execute("""
                CREATE TABLE diagnosis_jobs_v3 (
                    job_id            TEXT PRIMARY KEY,
                    event_id          TEXT NOT NULL UNIQUE,
                    payload           TEXT NOT NULL,
                    host              TEXT,
                    trigger_name      TEXT,
                    state             TEXT NOT NULL CHECK(state IN ('queued','running','done','sent','failed','skipped_circuit_breaker','suppressed_flap','grouped')),
                    created_at        REAL NOT NULL,
                    updated_at        REAL NOT NULL,
                    deadline_at       REAL NOT NULL,
                    run_after         REAL NOT NULL DEFAULT 0,
                    group_id          TEXT,
                    correlated_events TEXT,
                    result            TEXT,
                    last_error        TEXT
                )
            """)
            run_after_expr = "run_after" if "run_after" in cols else "created_at"
            group_id_expr = "group_id" if "group_id" in cols else "NULL"
            corr_expr = "correlated_events" if "correlated_events" in cols else "NULL"
            host_expr = "host" if "host" in cols else ("host_key" if "host_key" in cols else "NULL")
            trigger_expr = "trigger_name" if "trigger_name" in cols else ("trigger_key" if "trigger_key" in cols else "NULL")
            result_expr = "result" if "result" in cols else "NULL"
            last_err_expr = "last_error" if "last_error" in cols else "NULL"

            db.execute(f"""
                INSERT INTO diagnosis_jobs_v3 (
                    job_id, event_id, payload, host, trigger_name, state,
                    created_at, updated_at, deadline_at, run_after, group_id, correlated_events,
                    result, last_error
                )
                SELECT
                    job_id, event_id, payload, {host_expr}, {trigger_expr}, state,
                    created_at, updated_at, deadline_at, {run_after_expr}, {group_id_expr}, {corr_expr},
                    {result_expr}, {last_err_expr}
                FROM diagnosis_jobs
            """)
            db.execute("DROP TABLE diagnosis_jobs")
            db.execute("ALTER TABLE diagnosis_jobs_v3 RENAME TO diagnosis_jobs")
    else:
        db.execute("""
            CREATE TABLE IF NOT EXISTS diagnosis_jobs (
                job_id            TEXT PRIMARY KEY,
                event_id          TEXT NOT NULL UNIQUE,
                payload           TEXT NOT NULL,
                host              TEXT,
                trigger_name      TEXT,
                state             TEXT NOT NULL CHECK(state IN ('queued','running','done','sent','failed','skipped_circuit_breaker','suppressed_flap','grouped')),
                created_at        REAL NOT NULL,
                updated_at        REAL NOT NULL,
                deadline_at       REAL NOT NULL,
                run_after         REAL NOT NULL DEFAULT 0,
                group_id          TEXT,
                correlated_events TEXT,
                result            TEXT,
                last_error        TEXT
            )
        """)

    db.execute("CREATE INDEX IF NOT EXISTS idx_diagnosis_jobs_next ON diagnosis_jobs(state, run_after, created_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_diagnosis_jobs_cooldown ON diagnosis_jobs(host, trigger_name, created_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_diagnosis_jobs_group ON diagnosis_jobs(group_id)")

    db.execute("""
        CREATE TABLE IF NOT EXISTS conversation_turns (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role       TEXT NOT NULL CHECK(role IN ('user','model')),
            text       TEXT NOT NULL,
            created_at REAL NOT NULL
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_conv_turns_session ON conversation_turns(session_id, created_at)")

    db.execute("""
        CREATE TABLE IF NOT EXISTS feedback (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            ts             REAL NOT NULL,
            correlation_id TEXT,
            event_id       TEXT,
            host           TEXT,
            rating         TEXT NOT NULL CHECK(rating IN ('up','down')),
            comment        TEXT
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_feedback_cid ON feedback(correlation_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_feedback_event ON feedback(event_id)")


def init_diagnosis_queue() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with _connect() as db:
        _init_diagnosis_queue(db)
        db.commit()


def recover_diagnosis_jobs() -> int:
    now = time.time()
    with _connect() as db:
        cur = db.execute("UPDATE diagnosis_jobs SET state='queued', updated_at=? WHERE state='running'", (now,))
        db.commit()
        return cur.rowcount


def enqueue_diagnosis_job(
    event: dict,
    deadline_at: float,
    max_pending: int = 20,
    cooldown_s: int = 0,
    job_id: Optional[str] = None,
    debounce_s: int = 0,
    exempt_patterns: tuple[str, ...] = (),
) -> dict:
    event_id = str(event.get("event_id") or "").strip()
    if not event_id:
        raise ValueError("event_id required")
    now = time.time()
    host = str(event.get("host") or event.get("host_id") or "")
    trigger_name = str(event.get("trigger_name") or "")
    job_id = job_id or uuid.uuid4().hex
    payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))

    is_exempt = bool(debounce_s > 0 and exempt_patterns and any(p in f"{host} {trigger_name}" for p in exempt_patterns))
    run_after = now if (debounce_s <= 0 or is_exempt) else (now + debounce_s)
    effective_deadline = deadline_at
    if run_after > now and deadline_at > now:
        effective_deadline = max(deadline_at, run_after + (deadline_at - now))

    with _connect() as db:
        db.execute("BEGIN IMMEDIATE")
        existing = db.execute("SELECT job_id, state FROM diagnosis_jobs WHERE event_id=?", (event_id,)).fetchone()
        if existing:
            db.commit()
            return {"status": "duplicate", "job_id": existing["job_id"], "state": existing["state"]}

        if cooldown_s > 0 and host and trigger_name:
            recent = db.execute(
                "SELECT job_id, created_at FROM diagnosis_jobs "
                "WHERE host=? AND trigger_name=? AND state IN ('queued','running','done','sent') "
                "AND created_at>=? ORDER BY created_at DESC LIMIT 1",
                (host, trigger_name, now - cooldown_s),
            ).fetchone()
            if recent:
                db.commit()
                return {"status": "cooldown", "job_id": recent["job_id"], "retry_after_s": max(0, int(cooldown_s - (now - recent["created_at"])))}

        pending = db.execute("SELECT COUNT(*) FROM diagnosis_jobs WHERE state IN ('queued','running','done')").fetchone()[0]
        if pending >= max_pending:
            db.commit()
            return {"status": "full", "pending": pending}

        db.execute(
            "INSERT INTO diagnosis_jobs (job_id,event_id,payload,host,trigger_name,state,created_at,updated_at,deadline_at,run_after) "
            "VALUES (?,?,?,?,?,'queued',?,?,?,?)",
            (job_id, event_id, payload, host, trigger_name, now, now, effective_deadline, run_after),
        )
        db.commit()
        res = {"status": "queued", "job_id": job_id, "state": "queued"}
        if debounce_s > 0:
            res["run_after"] = run_after
            res["debounced"] = bool(run_after > now)
            res["deadline_at"] = effective_deadline
        return res


def claim_next_diagnosis_job() -> Optional[dict]:
    now = time.time()
    with _connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT * FROM diagnosis_jobs WHERE state='queued' AND run_after <= ? ORDER BY created_at LIMIT 1",
            (now,),
        ).fetchone()
        if not row:
            db.commit()
            return None
        db.execute("UPDATE diagnosis_jobs SET state='running', updated_at=? WHERE job_id=?", (now, row["job_id"]))
        db.commit()
        job = dict(row)
        job["state"] = "running"
        job["payload"] = json.loads(job["payload"])
        if job.get("correlated_events"):
            try:
                job["correlated_events"] = json.loads(job["correlated_events"])
            except Exception:
                pass
        return job


def cancel_diagnosis_job_on_recovery(event_id: str = "", host: str = "", trigger_name: str = "") -> dict:
    now = time.time()
    with _connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = None
        if event_id:
            row = db.execute(
                "SELECT job_id, event_id, host FROM diagnosis_jobs WHERE event_id=? AND state='queued' LIMIT 1",
                (str(event_id),),
            ).fetchone()
        if not row and host and trigger_name:
            row = db.execute(
                "SELECT job_id, event_id, host FROM diagnosis_jobs WHERE host=? AND trigger_name=? AND state='queued' ORDER BY created_at DESC LIMIT 1",
                (host, trigger_name),
            ).fetchone()
        if not row:
            db.commit()
            return {"cancelled": False, "job_id": None}

        job_id = row["job_id"]
        db.execute(
            "UPDATE diagnosis_jobs SET state='suppressed_flap', updated_at=?, last_error='resolved_within_debounce' WHERE job_id=?",
            (now, job_id),
        )
        db.commit()
        return {"cancelled": True, "job_id": job_id, "event_id": row["event_id"], "host": row["host"]}


def claim_correlated_queued_jobs(lead_job: dict, window_s: int = 180) -> list[dict]:
    lead_id = lead_job.get("job_id")
    lead_created = lead_job.get("created_at") or time.time()
    lead_host = str(lead_job.get("host") or "")
    lead_trigger = str(lead_job.get("trigger_name") or "").strip()
    lead_payload = lead_job.get("payload") or {}
    lead_service = ""
    if isinstance(lead_payload, dict):
        lead_service = str(lead_payload.get("service") or "")
        if not lead_service and lead_payload.get("tags"):
            tags = {t.get("tag"): t.get("value") for t in lead_payload.get("tags", []) if isinstance(t, dict)}
            lead_service = str(tags.get("service") or "")

    now = time.time()
    correlated_payloads = []
    with _connect() as db:
        db.execute("BEGIN IMMEDIATE")
        queued_rows = db.execute(
            "SELECT * FROM diagnosis_jobs WHERE state='queued' AND job_id != ? AND created_at BETWEEN ? AND ?",
            (lead_id, lead_created - window_s, lead_created + window_s),
        ).fetchall()
        is_large_cascade = (len(queued_rows) + 1) >= 3
        to_group_ids = []
        for r in queued_rows:
            sib_host = str(r["host"] or "")
            sib_payload = {}
            try:
                sib_payload = json.loads(r["payload"])
            except Exception:
                pass
            sib_service = ""
            if isinstance(sib_payload, dict):
                sib_service = str(sib_payload.get("service") or "")
                if not sib_service and sib_payload.get("tags"):
                    tags = {t.get("tag"): t.get("value") for t in sib_payload.get("tags", []) if isinstance(t, dict)}
                    sib_service = str(tags.get("service") or "")

            same_host = bool(lead_host and sib_host and lead_host.lower() == sib_host.lower())
            same_svc = bool(lead_service and sib_service and lead_service.lower() == sib_service.lower())
            sib_trigger = str(r["trigger_name"] or "").strip()
            same_trigger = bool(lead_trigger and sib_trigger and lead_trigger.lower() == sib_trigger.lower())

            if same_host or same_svc or same_trigger or is_large_cascade:
                to_group_ids.append(r["job_id"])
                correlated_payloads.append(sib_payload or {"event_id": r["event_id"], "host": sib_host, "trigger_name": r["trigger_name"]})

        if to_group_ids:
            placeholders = ",".join("?" for _ in to_group_ids)
            db.execute(
                f"UPDATE diagnosis_jobs SET state='grouped', updated_at=?, group_id=? WHERE job_id IN ({placeholders})",
                [now, lead_id] + to_group_ids,
            )
            db.execute(
                "UPDATE diagnosis_jobs SET correlated_events=?, updated_at=? WHERE job_id=?",
                (json.dumps(correlated_payloads, ensure_ascii=False, separators=(",", ":")), now, lead_id),
            )
        db.commit()
    return correlated_payloads


def finish_diagnosis_job(job_id: str, state: str, result: Optional[dict] = None, last_error: Optional[str] = None) -> None:
    if state not in DIAGNOSIS_JOB_STATES:
        raise ValueError("invalid diagnosis job state")
    with _connect() as db:
        db.execute(
            "UPDATE diagnosis_jobs SET state=?, updated_at=?, result=COALESCE(?, result), last_error=? WHERE job_id=?",
            (state, time.time(), json.dumps(result, ensure_ascii=False, separators=(",", ":")) if result is not None else None, last_error, job_id),
        )
        db.commit()


def find_recent_sent_diagnosis(host: str, trigger_name: str, within_s: int = 3600) -> Optional[dict]:
    if within_s <= 0 or not host or not trigger_name:
        return None
    cutoff = time.time() - within_s
    with _connect() as db:
        row = db.execute(
            "SELECT job_id, event_id, host, trigger_name, updated_at, result FROM diagnosis_jobs "
            "WHERE host=? AND trigger_name=? AND state='sent' AND updated_at>=? ORDER BY updated_at DESC LIMIT 1",
            (host, trigger_name, cutoff),
        ).fetchone()
        if not row or not row["result"]:
            return None
        try:
            parsed = json.loads(row["result"])
        except Exception:
            return None
        diag = parsed.get("diagnosis") or {}
        if not isinstance(diag, dict) or diag.get("partial_reason") or not diag.get("hipotese"):
            return None
        return {
            "job_id": row["job_id"], "event_id": row["event_id"], "host": row["host"],
            "trigger_name": row["trigger_name"], "updated_at": row["updated_at"], "diagnosis": diag,
        }


def get_recent_sent_diagnoses(within_s: int = 1800, limit: int = 3) -> list[dict]:
    if within_s <= 0 or limit <= 0:
        return []
    cutoff = time.time() - within_s
    with _connect() as db:
        rows = db.execute(
            "SELECT event_id, host, trigger_name, updated_at, result FROM diagnosis_jobs "
            "WHERE state='sent' AND updated_at>=? ORDER BY updated_at DESC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
    out = []
    for row in rows:
        try:
            diag = (json.loads(row["result"] or "{}")).get("diagnosis") or {}
        except Exception:
            continue
        if isinstance(diag, dict) and diag.get("constat"):
            out.append({
                "event_id": row["event_id"], "host": row["host"], "trigger_name": row["trigger_name"],
                "updated_at": row["updated_at"], "constat": str(diag.get("constat", ""))[:220],
                "hipotese": str(diag.get("hipotese", ""))[:220],
            })
    return out


def count_recent_auto_diagnoses(within_s: int) -> int:
    if within_s <= 0:
        return 0
    cutoff = time.time() - within_s
    with _connect() as db:
        row = db.execute(
            "SELECT COUNT(*) as cnt FROM diagnosis_jobs "
            "WHERE state IN ('running', 'done', 'sent') AND (updated_at >= ? OR created_at >= ?)",
            (cutoff, cutoff),
        ).fetchone()
        return int(row["cnt"]) if row else 0


def check_auto_circuit_breaker(hourly_cap: int = 12, daily_cap: int = 60, soft_downgrade_hourly: int = 6) -> dict:
    hourly_count = count_recent_auto_diagnoses(within_s=3600)
    daily_count = count_recent_auto_diagnoses(within_s=86400)
    if daily_count >= daily_cap:
        action, reason = "open", f"daily_cap_exceeded ({daily_count}/{daily_cap})"
    elif hourly_count >= hourly_cap:
        action, reason = "open", f"hourly_cap_exceeded ({hourly_count}/{hourly_cap})"
    elif hourly_count >= soft_downgrade_hourly:
        action, reason = "downgrade_cheap", f"soft_downgrade_active ({hourly_count}/{soft_downgrade_hourly})"
    else:
        action, reason = "allow", "within_limits"
    return {
        "action": action, "hourly_count": hourly_count, "daily_count": daily_count,
        "hourly_cap": hourly_cap, "daily_cap": daily_cap, "soft_downgrade_hourly": soft_downgrade_hourly,
        "reason": reason,
    }


def record_feedback(correlation_id: str = "", rating: str = "", comment: str = "", event_id: str = "", host: str = "") -> dict:
    rating_clean = rating.strip().lower()
    if rating_clean not in ("up", "down"):
        raise ValueError("rating must be 'up' or 'down'")
    delta = 1 if rating_clean == "up" else -1
    now = time.time()
    with _connect() as db:
        db.execute(
            "INSERT INTO feedback (ts, correlation_id, event_id, host, rating, comment) VALUES (?, ?, ?, ?, ?, ?)",
            (now, correlation_id or None, event_id or None, host or None, rating_clean, comment or None),
        )
        matched_id = None
        if event_id:
            row = db.execute("SELECT id FROM incidents WHERE event_id=? ORDER BY id DESC LIMIT 1", (str(event_id),)).fetchone()
            if row:
                matched_id = row[0]
        if not matched_id and correlation_id:
            c_clean = correlation_id.replace("job:", "").strip()
            row = db.execute("SELECT event_id FROM diagnosis_jobs WHERE job_id=? OR event_id=? LIMIT 1", (c_clean, c_clean)).fetchone()
            if row and row["event_id"]:
                inc_row = db.execute("SELECT id FROM incidents WHERE event_id=? ORDER BY id DESC LIMIT 1", (str(row["event_id"]),)).fetchone()
                if inc_row:
                    matched_id = inc_row[0]
        if not matched_id and host:
            inc_row = db.execute("SELECT id FROM incidents WHERE host=? ORDER BY id DESC LIMIT 1", (host,)).fetchone()
            if inc_row:
                matched_id = inc_row[0]
        if matched_id:
            db.execute("UPDATE incidents SET feedback_score = feedback_score + ? WHERE id = ?", (delta, matched_id))
        db.commit()
    return {"ok": True, "rating": rating_clean, "delta": delta, "matched_incident_id": matched_id}


def get_operational_stats() -> dict:
    with _connect() as db:
        job_counts = {}
        try:
            for r in db.execute("SELECT state, COUNT(*) as cnt FROM diagnosis_jobs GROUP BY state").fetchall():
                job_counts[r["state"]] = int(r["cnt"])
        except Exception:
            pass
        inc_count = fb_up = fb_down = h_runs = d_runs = 0
        try:
            inc_count = db.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
            fb_up = db.execute("SELECT COUNT(*) FROM feedback WHERE rating='up'").fetchone()[0]
            fb_down = db.execute("SELECT COUNT(*) FROM feedback WHERE rating='down'").fetchone()[0]
            now = time.time()
            h_runs = db.execute(
                "SELECT COUNT(*) FROM diagnosis_jobs WHERE state IN ('running','done','sent') AND (updated_at>=? OR created_at>=?)",
                (now - 3600, now - 3600),
            ).fetchone()[0]
            d_runs = db.execute(
                "SELECT COUNT(*) FROM diagnosis_jobs WHERE state IN ('running','done','sent') AND (updated_at>=? OR created_at>=?)",
                (now - 86400, now - 86400),
            ).fetchone()[0]
        except Exception:
            pass
        return {
            "diagnosis_jobs": job_counts,
            "incidents_total": int(inc_count),
            "feedback": {"up": int(fb_up), "down": int(fb_down), "total": int(fb_up + fb_down)},
            "recent_runs": {"last_1h": int(h_runs), "last_24h": int(d_runs)},
        }


def get_conversation_turns(session_id: str, max_turns: int = 4, ttl_s: int = 1800) -> list[dict]:
    if not session_id or max_turns <= 0 or ttl_s <= 0:
        return []
    cutoff = time.time() - ttl_s
    with _connect() as db:
        rows = db.execute(
            "SELECT role, text FROM conversation_turns WHERE session_id=? AND created_at>=? ORDER BY id DESC LIMIT ?",
            (session_id, cutoff, max_turns * 2),
        ).fetchall()
    turns = [{"role": r["role"], "text": r["text"]} for r in reversed(rows)]
    while turns and turns[0]["role"] != "user":
        turns.pop(0)
    while turns and turns[-1]["role"] != "model":
        turns.pop()
    return turns


def append_conversation_turn(session_id: str, user_text: str, model_text: str, max_keep: int = 10) -> None:
    if not session_id or not user_text or not model_text:
        return
    now = time.time()
    with _connect() as db:
        db.execute("INSERT INTO conversation_turns (session_id, role, text, created_at) VALUES (?,?,?,?)", (session_id, "user", user_text.strip()[:800], now))
        db.execute("INSERT INTO conversation_turns (session_id, role, text, created_at) VALUES (?,?,?,?)", (session_id, "model", model_text.strip()[:1800], now + 0.001))
        db.execute(
            "DELETE FROM conversation_turns WHERE session_id=? AND id NOT IN (SELECT id FROM conversation_turns WHERE session_id=? ORDER BY id DESC LIMIT ?)",
            (session_id, session_id, max_keep * 2),
        )
        db.execute("DELETE FROM conversation_turns WHERE created_at < ?", (now - 86400,))
        db.commit()


def clear_conversation_turns(session_id: str) -> int:
    if not session_id:
        return 0
    with _connect() as db:
        cur = db.execute("DELETE FROM conversation_turns WHERE session_id=?", (session_id,))
        db.commit()
        return cur.rowcount


def _active_embed_provider() -> str:
    if EMBED_PROVIDER != "auto":
        return EMBED_PROVIDER
    if os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"):
        return "gemini"
    if os.getenv("OPENAI_API_KEY") or os.getenv("EMBEDDING_BASE_URL"):
        return "openai"
    return "disabled"


def embed(text: str) -> Optional[list[float]]:
    provider = _active_embed_provider()
    if provider == "disabled":
        return None
    if provider == "gemini":
        from google import genai
        client = genai.Client(api_key=(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")))
        resp = client.models.embed_content(model=EMBED_MODEL, contents=text, config={"output_dimensionality": EMBED_DIM})
        return list(resp.embeddings[0].values)
    if provider == "openai":
        base = (os.getenv("EMBEDDING_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/")
        headers = {"Content-Type": "application/json"}
        key = os.getenv("OPENAI_API_KEY", "").strip()
        if key:
            headers["Authorization"] = f"Bearer {key}"
        resp = httpx.post(
            f"{base}/embeddings",
            headers=headers,
            json={"model": OPENAI_EMBED_MODEL, "input": text, "dimensions": EMBED_DIM},
            timeout=15.0,
        )
        resp.raise_for_status()
        return list(resp.json()["data"][0]["embedding"])
    return None


def _serialize(vec: list[float]) -> bytes:
    try:
        import sqlite_vec
        return sqlite_vec.serialize_float32(vec)
    except Exception:
        return json.dumps(vec).encode()


def recall_similar(query_text: str, host: Optional[str] = None,
                   k: int = RECALL_K, max_dist: float = RECALL_MAX_COSINE_DIST) -> list[dict]:
    try:
        with _connect() as db:
            if db.execute("SELECT COUNT(*) FROM incidents").fetchone()[0] == 0:
                return []
            vec = embed(query_text)
            if not vec:
                # Non-vector fallback when embeddings are disabled: exact/recent host incidents
                if not host:
                    return []
                rows = db.execute(
                    "SELECT event_id, host, trigger_name, diagnosis, resolution, feedback_score "
                    "FROM incidents WHERE host=? AND feedback_score>=0 ORDER BY feedback_score DESC, id DESC LIMIT ?",
                    (host, k),
                ).fetchall()
                return [{**dict(r), "cosine": 1.0} for r in rows]

            rows = db.execute(
                "SELECT v.rowid AS id, v.distance AS dist FROM vec_incidents v WHERE v.embedding MATCH ? AND k = ? ORDER BY v.distance",
                (_serialize(vec), k * 6 if host else k * 3),
            ).fetchall()
            out = []
            for r in rows:
                if r["dist"] > max_dist:
                    continue
                meta = db.execute(
                    "SELECT event_id, host, trigger_name, diagnosis, resolution, feedback_score FROM incidents WHERE id = ?",
                    (r["id"],),
                ).fetchone()
                if not meta:
                    continue
                score = meta["feedback_score"] if "feedback_score" in meta.keys() else 0
                if score < 0 or (host and meta["host"] != host):
                    continue
                d = dict(meta)
                d["cosine"] = round(1.0 - r["dist"], 3)
                d["feedback_score"] = score
                out.append(d)
            out.sort(key=lambda item: (-item.get("feedback_score", 0), -item.get("cosine", 0)))
            return out[:k]
    except Exception as exc:
        logger.warning("recall_similar skipped: %s", exc)
        return []


def format_recall_block(rows: list[dict]) -> str:
    if not rows:
        return ""
    lines = ["<recall source='jarvis.db (sqlite-vec)'>"]
    for r in rows:
        diag = r.get("diagnosis") or {}
        if isinstance(diag, str):
            try:
                diag = json.loads(diag)
            except Exception:
                diag = {}
        lines.append(
            f"  <incident cosine='{r.get('cosine', 0)}' host='{r.get('host')}'>"
            f"{r.get('trigger_name')}: {diag.get('hipotese', '')} -> resolution: {(r.get('resolution') or '')[:200]}</incident>"
        )
    lines.append("</recall>")
    return "\n".join(lines)


def should_index(resolution: str, duration_seconds: int, has_tech: bool = True) -> bool:
    return bool(resolution) and len(resolution.strip()) >= MIN_RESOLUTION_CHARS and duration_seconds >= MIN_INCIDENT_SECONDS and has_tech


def _mentions_absent_entity(text: str) -> str:
    t = str(text or "").lower()
    for entity in KNOWN_ABSENT:
        if entity and entity in t:
            return entity
    return ""


def should_index_diagnosis(diagnosis: dict) -> bool:
    if not isinstance(diagnosis, dict):
        return False
    conf = str(diagnosis.get("confianca", "")).lower()
    hip = str(diagnosis.get("hipotese", "")).strip()
    if not (conf in ("alta", "high", "media", "medium") and len(hip) >= 20):
        return False
    return not _mentions_absent_entity(hip + " " + str(diagnosis.get("constat", "")))


def prune(db: sqlite3.Connection, max_rows: int = MAX_ROWS) -> int:
    n = db.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    if n <= max_rows:
        return 0
    old = [r[0] for r in db.execute("SELECT id FROM incidents ORDER BY clock ASC LIMIT ?", (n - max_rows,)).fetchall()]
    qmarks = ",".join("?" * len(old))
    try:
        db.execute(f"DELETE FROM vec_incidents WHERE rowid IN ({qmarks})", old)
    except Exception:
        pass
    db.execute(f"DELETE FROM incidents WHERE id IN ({qmarks})", old)
    return len(old)


def upsert_incident(event_id: str, host: str, host_id: str, trigger_name: str,
                    severity: str, diagnosis: dict, resolution: str) -> Optional[int]:
    text = f"{trigger_name} | {json.dumps(diagnosis, ensure_ascii=False)} | {resolution}"
    vec = embed(text)
    diag_json = json.dumps(diagnosis, ensure_ascii=False)
    with _connect() as db:
        existing = db.execute(
            "SELECT id FROM incidents WHERE host = ? AND trigger_name = ? LIMIT 1",
            (host, trigger_name),
        ).fetchone()
        if existing:
            rowid = existing["id"]
            db.execute(
                "UPDATE incidents SET event_id=?, clock=datetime('now'), severity=?, diagnosis=?, resolution=? WHERE id=?",
                (event_id, severity, diag_json, resolution, rowid),
            )
            if vec:
                try:
                    db.execute("DELETE FROM vec_incidents WHERE rowid=?", (rowid,))
                    db.execute("INSERT INTO vec_incidents(rowid, embedding) VALUES (?,?)", (rowid, _serialize(vec)))
                except Exception:
                    pass
            db.commit()
            return rowid

        cur = db.execute(
            "INSERT INTO incidents (event_id, host, host_id, trigger_name, severity, diagnosis, resolution) VALUES (?,?,?,?,?,?,?)",
            (event_id, host, host_id, trigger_name, severity, diag_json, resolution),
        )
        rowid = cur.lastrowid
        if vec:
            try:
                db.execute("INSERT INTO vec_incidents(rowid, embedding) VALUES (?, ?)", (rowid, _serialize(vec)))
            except Exception:
                pass
        prune(db)
        db.commit()
        return rowid
