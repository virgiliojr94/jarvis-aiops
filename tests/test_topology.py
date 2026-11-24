"""Tests for live Tier 1 & Tier 2 Zabbix tag-based topology discovery."""

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import enrichment  # noqa: E402


def test_fetch_topology_from_tags():
    sample_hosts = [
        {"hostid": "1", "host": "hv-node-01", "tags": [{"tag": "tier", "value": "1"}, {"tag": "role", "value": "hypervisor"}]},
        {"hostid": "2", "host": "app-host-01", "tags": [{"tag": "tier", "value": "2"}, {"tag": "role", "value": "docker-host"}, {"tag": "service", "value": "checkout-api"}]},
        {"hostid": "3", "host": "endpoint-01", "tags": [{"tag": "tier", "value": "3"}, {"tag": "service", "value": "checkout-api"}]},
    ]

    class _FakeZbx:
        async def call_tool(self, name, args):
            return [{"text": json.dumps(sample_hosts)}]

    lines = asyncio.run(enrichment.fetch_topology(_FakeZbx()))
    assert len(lines) == 2
    assert "hv-node-01 tier=1 role=hypervisor" in lines
    assert "app-host-01 tier=2 role=docker-host service=checkout-api" in lines


if __name__ == "__main__":
    test_fetch_topology_from_tags()
    print("ok: test_topology")
