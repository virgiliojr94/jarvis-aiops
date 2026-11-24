"""Tests for MCP strictly read-only guardrails and tool filtering."""

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from classifier import QuestionClass  # noqa: E402
from mcp_loader import MCPToolLoader, is_readonly_tool, resolve_tools_key  # noqa: E402


def test_readonly_guardrails():
    assert is_readonly_tool("zabbix__host_get") is True
    assert is_readonly_tool("zabbix__event_get") is True
    assert is_readonly_tool("zabbix__maintenance_create") is False
    assert is_readonly_tool("zabbix__host_delete") is False
    assert is_readonly_tool("zabbix_rw__event_acknowledge") is False
    assert is_readonly_tool("signoz__signoz_search_logs") is True
    assert is_readonly_tool("signoz__signoz_create_dashboard") is False
    assert is_readonly_tool("custom__node_update") is False
    assert resolve_tools_key(QuestionClass.GENERICA, {"found_in_zabbix": True}) == "investigacao_host"


def test_loader_blocks_write_execution():
    class _FakeSession:
        async def list_tools(self):
            return []
        async def call_tool(self, name, args):
            raise AssertionError("should never execute write tool")

    loader = MCPToolLoader({"zabbix": _FakeSession()}, readonly_only=True)
    res = asyncio.run(loader.execute_tool(SimpleNamespace(name="zabbix__host_delete", args={"hostid": "1"})))
    assert "error" in res and "READ-ONLY" in res["error"]


if __name__ == "__main__":
    test_readonly_guardrails()
    test_loader_blocks_write_execution()
    print("ok: test_mcp_guardrails")
