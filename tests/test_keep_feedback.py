"""Tests for Keep debounce, post-debounce deadline, r_eventid suppression, grouping, and feedback loop."""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import time
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import memory  # noqa: E402


def test_sqlite_legacy_migration():
    with tempfile.NamedTemporaryFile(suffix=".db") as tf:
        db = sqlite3.connect(tf.name)
        db.execute("""
            CREATE TABLE diagnosis_jobs (
                job_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                host TEXT,
                trigger_name TEXT,
                state TEXT NOT NULL CHECK(state IN ('queued','running','done','sent','failed')),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                deadline_at REAL NOT NULL,
                result TEXT,
                last_error TEXT
            )
        """)
        db.execute("INSERT INTO diagnosis_jobs VALUES ('j1','101','{}','web-01','high cpu','sent',1.0,2.0,10.0,NULL,NULL)")
        db.commit()
        db.close()

        orig = memory.DB_PATH
        memory.DB_PATH = tf.name
        try:
            memory.init_db()
            c = sqlite3.connect(tf.name)
            cols = {r[1] for r in c.execute("PRAGMA table_info(diagnosis_jobs)")}
            assert {"run_after", "group_id", "correlated_events"} <= cols
            assert c.execute("SELECT event_id FROM diagnosis_jobs").fetchone()[0] == "101"
        finally:
            memory.DB_PATH = orig


def test_debounce_post_deadline_and_grouping():
    with tempfile.NamedTemporaryFile(suffix=".db") as tf:
        orig = memory.DB_PATH
        memory.DB_PATH = tf.name
        try:
            memory.init_db()
            now = time.time()
            res = memory.enqueue_diagnosis_job(
                {"event_id": "201", "host": "sw-01", "trigger_name": "Port 25 down"},
                deadline_at=now + 210, debounce_s=60,
            )
            assert res["debounced"] is True
            assert res["deadline_at"] >= now + 269
            assert memory.claim_next_diagnosis_job() is None

            # Force run_after ready and test same_trigger grouping
            c = sqlite3.connect(tf.name)
            c.execute("UPDATE diagnosis_jobs SET run_after = ? WHERE event_id='201'", (now - 1,))
            c.commit()
            c.close()

            memory.enqueue_diagnosis_job(
                {"event_id": "202", "host": "sw-02", "trigger_name": "Port 25 down"},
                deadline_at=now + 210, debounce_s=0,
            )
            lead = memory.claim_next_diagnosis_job()
            assert lead["event_id"] == "201"
            corr = memory.claim_correlated_queued_jobs(lead, window_s=180)
            assert len(corr) == 1 and corr[0]["event_id"] == "202"
        finally:
            memory.DB_PATH = orig


def test_recovery_flap_cancellation():
    with tempfile.NamedTemporaryFile(suffix=".db") as tf:
        orig = memory.DB_PATH
        memory.DB_PATH = tf.name
        try:
            memory.init_db()
            memory.enqueue_diagnosis_job(
                {"event_id": "301", "host": "api-01", "trigger_name": "Timeout"},
                deadline_at=time.time() + 200, debounce_s=60,
            )
            cancel = memory.cancel_diagnosis_job_on_recovery(event_id="301")
            assert cancel["cancelled"] is True
            assert memory.get_operational_stats()["diagnosis_jobs"].get("suppressed_flap") == 1
        finally:
            memory.DB_PATH = orig


def test_feedback_scoring_and_recall():
    with tempfile.NamedTemporaryFile(suffix=".db") as tf:
        orig = memory.DB_PATH
        memory.DB_PATH = tf.name
        try:
            memory.init_db()
            with patch.dict(os.environ, {"JARVIS_EMBED_PROVIDER": "disabled"}), \
                 patch.object(memory, "EMBED_PROVIDER", "disabled"):
                memory.upsert_incident(
                    event_id="401", host="app-01", host_id="10",
                    trigger_name="5xx burst", severity="High",
                    diagnosis={"constat": "5xx", "hipotese": "Pool exhausted on app-01", "confianca": "alta"},
                    resolution="Increased pool size",
                )
                fb = memory.record_feedback(correlation_id="401", event_id="401", rating="down", comment="wrong")
                assert fb["matched_incident_id"] is not None
                # Negative score suppresses recall
                assert memory.recall_similar("5xx", host="app-01") == []

                memory.record_feedback(correlation_id="401", event_id="401", rating="up")
                memory.record_feedback(correlation_id="401", event_id="401", rating="up")
                hits = memory.recall_similar("5xx", host="app-01")
                assert len(hits) == 1 and hits[0]["feedback_score"] == 1
        finally:
            memory.DB_PATH = orig


if __name__ == "__main__":
    test_sqlite_legacy_migration()
    test_debounce_post_deadline_and_grouping()
    test_recovery_flap_cancellation()
    test_feedback_scoring_and_recall()
    print("ok: test_keep_feedback")
