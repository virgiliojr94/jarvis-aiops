"""Tests for Robusta-style dynamic SigNoz catalog, pre-fetch, and Quota Circuit Breaker."""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import enrichment  # noqa: E402
import memory  # noqa: E402


def test_dynamic_catalog_and_prefetch():
    class _FakeSigNoz:
        async def call_tool(self, name, args):
            if name == "signoz_list_services":
                return [{"text": json.dumps({"data": [{"serviceName": "checkout-api"}, {"serviceName": "traefik"}]})}]
            if name == "signoz_aggregate_logs":
                if args.get("groupBy") == "service.name":
                    return [{"text": json.dumps({"data": {"data": {"results": [{"data": [["checkout-api", 120]]}]}}})}]
                return [{"text": json.dumps({"data": {"data": {"results": [{"data": [[12]]}]}}})}]
            if name == "signoz_search_logs":
                return [{"text": json.dumps({"data": {"data": {"results": [{"rows": [{"data": {"timestamp": "10:00Z", "body": "ERROR pool timeout"}}]}]}}})}]
            if name == "signoz_aggregate_traces":
                return [{"text": json.dumps({"data": {"data": {"results": [{"data": [[3]]}]}}})}]
            return []

    cat = asyncio.run(enrichment.fetch_signoz_catalog(_FakeSigNoz()))
    assert cat["traces_only"] == ["traefik"]
    assert "checkout-api" in cat["log_services"]

    svcs = enrichment.resolve_signoz_services(
        host_context={"role": "app-server", "service": "checkout-api", "ingress": "traefik"}
    )
    assert svcs == ["checkout-api", "traefik"]

    pref = asyncio.run(enrichment.fetch_signoz_prefetch(svcs, _FakeSigNoz(), timeout_s=3.0))
    assert pref["checkout-api"]["error_logs_count"] == 12
    assert pref["traefik"]["traces_only"] is True
    assert pref["traefik"]["error_logs_count"] is None


def test_circuit_breaker_states():
    with tempfile.NamedTemporaryFile(suffix=".db") as tf:
        orig_db = memory.DB_PATH
        memory.DB_PATH = tf.name
        try:
            memory.init_db()
            cb0 = memory.check_auto_circuit_breaker(hourly_cap=12, daily_cap=60, soft_downgrade_hourly=6)
            assert cb0["action"] == "allow"

            for i in range(6):
                memory.enqueue_diagnosis_job({"event_id": f"e{i}", "host": "h1", "trigger_name": "t1"}, deadline_at=9999999999)
                job = memory.claim_next_diagnosis_job()
                memory.finish_diagnosis_job(job["job_id"], "sent")

            cb1 = memory.check_auto_circuit_breaker(hourly_cap=12, daily_cap=60, soft_downgrade_hourly=6)
            assert cb1["action"] == "downgrade_cheap"

            for i in range(6, 12):
                memory.enqueue_diagnosis_job({"event_id": f"e{i}", "host": "h1", "trigger_name": "t1"}, deadline_at=9999999999)
                job = memory.claim_next_diagnosis_job()
                memory.finish_diagnosis_job(job["job_id"], "sent")

            cb2 = memory.check_auto_circuit_breaker(hourly_cap=12, daily_cap=60, soft_downgrade_hourly=6)
            assert cb2["action"] == "open"
        finally:
            memory.DB_PATH = orig_db


if __name__ == "__main__":
    test_dynamic_catalog_and_prefetch()
    test_circuit_breaker_states()
    print("ok: test_robusta_prefetch")
