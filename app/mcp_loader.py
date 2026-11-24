"""MCP session manager and strictly read-only tool loader."""

from __future__ import annotations

import logging
import os
import re
from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from typing import Dict, List, Optional

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    from mcp.client.streamable_http import streamablehttp_client
except ImportError:  # pragma: no cover - allow unit-testing guardrails without mcp package
    ClientSession = None  # type: ignore[assignment]
    StdioServerParameters = None  # type: ignore[assignment]
    stdio_client = None  # type: ignore[assignment]
    streamablehttp_client = None  # type: ignore[assignment]

from enrichment import MCP_TRUNCATION_LIMIT

try:
    from google.genai import types
except Exception:  # pragma: no cover
    from types import SimpleNamespace
    types = SimpleNamespace(Tool=dict, FunctionDeclaration=dict)

logger = logging.getLogger(__name__)

# Curated strictly read-only Zabbix tools (out of 231+ exposed by zabbix-mcp-server).
BASE_ZABBIX_RO_TOOLS: frozenset[str] = frozenset({
    "zabbix__host_get", "zabbix__hostgroup_get", "zabbix__hostinterface_get",
    "zabbix__proxy_get", "zabbix__proxygroup_get", "zabbix__hanode_get",
    "zabbix__template_get", "zabbix__templategroup_get", "zabbix__usermacro_get",
    "zabbix__valuemap_get", "zabbix__problem_get", "zabbix__event_get",
    "zabbix__history_get", "zabbix__trend_get", "zabbix__item_get",
    "zabbix__trigger_get", "zabbix__alert_get", "zabbix__action_get",
    "zabbix__maintenance_get", "zabbix__correlation_get", "zabbix__auditlog_get",
    "zabbix__httptest_get", "zabbix__sla_get", "zabbix__sla_getsli",
    "zabbix__map_get", "zabbix__graph_get", "zabbix__graphitem_get",
    "zabbix__dashboard_get", "zabbix__discoveryrule_get", "zabbix__itemprototype_get",
    "zabbix__triggerprototype_get", "zabbix__graphprototype_get", "zabbix__hostprototype_get",
    "zabbix__graph_render", "zabbix__anomaly_detect", "zabbix__capacity_forecast",
    "zabbix__health_check",
})

_extra_ro = {
    t.strip() for t in os.getenv("JARVIS_ZABBIX_EXTRA_RO_TOOLS", "").split(",") if t.strip()
}
ALLOWED_ZABBIX_RO_TOOLS: frozenset[str] = BASE_ZABBIX_RO_TOOLS | frozenset(_extra_ro)

DENIED_SIGNOZ_WRITE_TOOLS: frozenset[str] = frozenset({
    "signoz__signoz_create_dashboard",
    "signoz__signoz_update_dashboard",
})

_WRITE_VERB_RE = re.compile(
    r"(?:^|_)(?:create|update|delete|patch|put|post|add|remove|set|import|"
    r"provision|execute|clear|push|acknowledge|manage|massadd|massremove|"
    r"massupdate|propagate|login|logout|unblock|resettotp|prepare|confirm)(?:$|_)",
    re.IGNORECASE,
)

TOOL_DESCRIPTION_HINTS: Dict[str, str] = {
    "zabbix__host_get": "TIP: pass selectTags/selectInterfaces inside 'extra_params'; keep 'output' compact.",
    "zabbix__problem_get": "TIP: pass hostids=[...] (list, NOT singular hostid); put selectTags inside 'extra_params'.",
    "zabbix__event_get": "TIP: pass eventids and selectHosts inside 'extra_params'; r_eventid != '0' means already recovered.",
    "zabbix__trigger_get": "TIP: use extra_params={'selectDependencies':'extend','monitored':True} to inspect dependency trees.",
    "zabbix__item_get": "TIP: check value_type (0=float,3=uint) before history_get.",
    "zabbix__history_get": "TIP: match history=value_type; for monotonic counters fetch start+end and compute delta.",
    "signoz__signoz_list_services": "TIP: pass limit='200' to avoid truncation.",
    "signoz__signoz_search_logs": "TIP: always filter by service.name and short window (<=6h).",
    "signoz__signoz_search_traces": "TIP: filter by service.name or isRoot=true within a short window (<=6h).",
}


def is_readonly_tool(full_name: str) -> bool:
    """Hard guardrail: return True only for verified read-only MCP tools."""
    mcp_name, sep, tool_name = full_name.partition("__")
    if not sep or not tool_name:
        return False
    if mcp_name == "zabbix_rw":
        return False
    if mcp_name == "zabbix":
        return full_name in ALLOWED_ZABBIX_RO_TOOLS
    if mcp_name == "signoz":
        return full_name not in DENIED_SIGNOZ_WRITE_TOOLS and not _WRITE_VERB_RE.search(tool_name)
    return not _WRITE_VERB_RE.search(tool_name)


TOOLS_BY_CLASS: Dict[str, frozenset] = {
    "status_geral": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__alert_get", "zabbix__trigger_get", "zabbix__hostgroup_get",
        "zabbix__health_check", "zabbix__graph_render",
    }),
    "investigacao_host": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__alert_get", "zabbix__trigger_get", "zabbix__item_get",
        "zabbix__hostgroup_get", "zabbix__history_get", "zabbix__trend_get",
        "zabbix__graph_render", "zabbix__maintenance_get", "zabbix__health_check",
        "signoz__signoz_search_logs", "signoz__signoz_aggregate_logs",
    }),
    "por_que_alertando": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__alert_get", "zabbix__trigger_get", "zabbix__item_get",
        "zabbix__history_get", "zabbix__graph_render",
        "signoz__signoz_search_logs", "signoz__signoz_aggregate_logs",
    }),
    "anomalia": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__trigger_get", "zabbix__item_get", "zabbix__history_get",
        "zabbix__trend_get", "zabbix__anomaly_detect", "zabbix__graph_render",
    }),
    "top_consumidores": frozenset({
        "zabbix__host_get", "zabbix__hostgroup_get", "zabbix__item_get",
        "zabbix__history_get", "zabbix__trend_get", "zabbix__problem_get",
        "zabbix__graph_render",
    }),
    "manutencao": frozenset({"zabbix__maintenance_get", "zabbix__host_get", "zabbix__hostgroup_get"}),
    "logs": frozenset({
        "signoz__signoz_list_services", "signoz__signoz_get_field_keys",
        "signoz__signoz_get_field_values", "signoz__signoz_search_logs",
        "signoz__signoz_aggregate_logs",
    }),
    "latencia": frozenset({
        "signoz__signoz_list_services", "signoz__signoz_get_service_top_operations",
        "signoz__signoz_aggregate_traces", "signoz__signoz_search_traces",
        "signoz__signoz_get_trace_details", "zabbix__host_get", "zabbix__item_get", "zabbix__history_get",
    }),
    "erros": frozenset({
        "signoz__signoz_list_services", "signoz__signoz_aggregate_traces",
        "signoz__signoz_search_traces", "signoz__signoz_get_trace_details",
        "signoz__signoz_search_logs", "signoz__signoz_aggregate_logs",
    }),
    "trace": frozenset({
        "signoz__signoz_search_traces", "signoz__signoz_get_trace_details",
        "signoz__signoz_search_logs",
    }),
    "metricas": frozenset({
        "signoz__signoz_list_metrics", "signoz__signoz_query_metrics",
        "zabbix__host_get", "zabbix__item_get", "zabbix__history_get", "zabbix__trend_get",
    }),
    "post_mortem": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__trigger_get", "zabbix__item_get", "zabbix__history_get",
        "zabbix__trend_get", "zabbix__auditlog_get",
        "signoz__signoz_search_traces", "signoz__signoz_aggregate_traces",
        "signoz__signoz_get_trace_details", "signoz__signoz_search_logs",
        "signoz__signoz_aggregate_logs",
    }),
    "capacity": frozenset({
        "zabbix__host_get", "zabbix__hostgroup_get", "zabbix__item_get",
        "zabbix__trend_get", "zabbix__capacity_forecast", "zabbix__graph_render",
    }),
    "correlacao_cross_host": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__trigger_get", "zabbix__item_get", "zabbix__history_get",
        "signoz__signoz_aggregate_traces", "signoz__signoz_aggregate_logs",
        "signoz__signoz_search_traces",
    }),
    "seguranca": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__trigger_get", "zabbix__item_get", "zabbix__history_get",
        "zabbix__auditlog_get", "signoz__signoz_search_logs",
        "signoz__signoz_aggregate_logs", "signoz__signoz_search_traces",
    }),
    "generica": frozenset({
        "zabbix__host_get", "zabbix__problem_get", "zabbix__event_get",
        "zabbix__trigger_get", "zabbix__item_get", "zabbix__history_get",
        "zabbix__trend_get", "zabbix__maintenance_get",
        "signoz__signoz_list_services", "signoz__signoz_search_logs",
        "signoz__signoz_aggregate_logs", "signoz__signoz_search_traces",
        "signoz__signoz_aggregate_traces",
    }),
}


def resolve_tools_key(question_class, host_context: Optional[dict]) -> str:
    key = question_class.value if hasattr(question_class, "value") else str(question_class)
    if key == "generica" and isinstance(host_context, dict) and host_context.get("found_in_zabbix"):
        return "investigacao_host"
    return key


class MCPSessionBase(ABC):
    def __init__(self, name: str):
        self.name = name
        self._stack: Optional[AsyncExitStack] = None
        self._session: Optional[ClientSession] = None

    @abstractmethod
    async def _open_transport(self):
        ...

    async def connect(self):
        self._stack = AsyncExitStack()
        read, write = await self._open_transport()
        self._session = await self._stack.enter_async_context(ClientSession(read, write))
        await self._session.initialize()

    async def list_tools(self) -> list:
        res = await self._session.list_tools()
        return res.tools

    async def call_tool(self, name: str, arguments: dict) -> list:
        try:
            result = await self._session.call_tool(name, arguments)
        except Exception:
            logger.warning("MCP '%s' session dropped, reconnecting", self.name)
            try:
                await self.close()
            except Exception:
                pass
            await self.connect()
            result = await self._session.call_tool(name, arguments)
        return [
            item.model_dump() if hasattr(item, "model_dump") else {"text": str(item)}
            for item in result.content
        ]

    async def close(self):
        if self._stack:
            await self._stack.aclose()


class MCPHttpSession(MCPSessionBase):
    def __init__(self, name: str, url: str):
        super().__init__(name)
        self.url = url

    async def _open_transport(self):
        read, write, _ = await self._stack.enter_async_context(streamablehttp_client(self.url))
        return read, write


class MCPStdioSession(MCPSessionBase):
    def __init__(self, name: str, command: str, args: Optional[list] = None, env: Optional[dict] = None):
        super().__init__(name)
        self.command = command
        self.args = args or []
        self.env = env or {}

    async def _open_transport(self):
        full_env = dict(os.environ)
        for k, v in self.env.items():
            if v is None:
                full_env.pop(k, None)
            else:
                full_env[k] = str(v)
        params = StdioServerParameters(command=self.command, args=self.args, env=full_env)
        read, write = await self._stack.enter_async_context(stdio_client(params))
        return read, write


_SCHEMA_STRIP_KEYS = frozenset([
    "additionalProperties", "$schema", "$defs", "definitions",
    "examples", "default", "$id", "$ref", "$anchor",
    "unevaluatedProperties", "if", "then", "else", "not",
    "contentEncoding", "contentMediaType",
])


def _sanitize_schema(schema: object) -> object:
    if not isinstance(schema, dict):
        return schema
    cleaned = {}
    for k, v in schema.items():
        if k in _SCHEMA_STRIP_KEYS:
            continue
        out_key = "anyOf" if k == "oneOf" else k
        if out_key == "type" and isinstance(v, list):
            non_null = [t for t in v if t != "null"]
            cleaned["type"] = non_null[0] if non_null else "string"
            if "null" in v:
                cleaned["nullable"] = True
            continue
        if isinstance(v, bool):
            if v is True:
                cleaned[out_key] = {}
        elif isinstance(v, dict):
            cleaned[out_key] = _sanitize_schema(v)
        elif isinstance(v, list):
            cleaned[out_key] = [_sanitize_schema(i) if isinstance(i, dict) else i for i in v]
        else:
            cleaned[out_key] = v
    return cleaned


class MCPToolLoader:
    def __init__(self, sessions: Dict[str, MCPSessionBase], readonly_only: bool = False):
        self.sessions = sessions
        self.readonly_only = readonly_only
        self._tool_cache: Optional[List] = None

    async def load_tools(self) -> List:
        if self._tool_cache:
            return self._tool_cache

        function_declarations = []
        for mcp_name, session in self.sessions.items():
            try:
                tools = await session.list_tools()
                for tool in tools:
                    full_name = f"{mcp_name}__{tool.name}"
                    if self.readonly_only and not is_readonly_tool(full_name):
                        continue
                    schema = _sanitize_schema(tool.inputSchema) if tool.inputSchema else None
                    desc = tool.description or ""
                    hint = TOOL_DESCRIPTION_HINTS.get(full_name)
                    if hint and hint not in desc:
                        desc = f"{desc}\n{hint}".strip()
                    try:
                        fd = types.FunctionDeclaration(name=full_name, description=desc, parameters=schema)
                        function_declarations.append(fd)
                    except Exception as exc:
                        logger.warning("Skipping incompatible tool schema %s: %s", full_name, exc)
            except Exception as exc:
                logger.error("Failed to load tools from MCP %s: %s", mcp_name, exc)

        logger.info("MCPToolLoader loaded %d tools (readonly_only=%s)", len(function_declarations), self.readonly_only)
        self._tool_cache = [types.Tool(function_declarations=function_declarations)]
        return self._tool_cache

    async def get_tools_for_class(self, question_class) -> List:
        if not self._tool_cache:
            await self.load_tools()
        key = question_class.value if hasattr(question_class, "value") else str(question_class)
        allowed = TOOLS_BY_CLASS.get(key, TOOLS_BY_CLASS["generica"])
        if self.readonly_only:
            allowed = frozenset(n for n in allowed if is_readonly_tool(n))
        all_fds = self._tool_cache[0].function_declarations if self._tool_cache else []
        filtered = [fd for fd in all_fds if fd.name in allowed]
        return [types.Tool(function_declarations=filtered)]

    async def execute_tool(self, function_call) -> Dict:
        name = function_call.name
        if self.readonly_only and not is_readonly_tool(name):
            logger.warning("Blocked non-readonly tool call on readonly loader: %s", name)
            return {"error": f"Tool '{name}' blocked: strictly READ-ONLY environment."}

        mcp_name, tool_name = name.split("__", 1)
        session = self.sessions.get(mcp_name)
        if not session:
            raise ValueError(f"MCP session '{mcp_name}' not available")

        try:
            result = await session.call_tool(tool_name, dict(function_call.args))
            size = sum(len(str(item.get("text", ""))) for item in result)
            if size >= MCP_TRUNCATION_LIMIT:
                logger.warning("tool %s truncated by gateway: %d chars", name, size)
                result = list(result) + [{
                    "text": f"[System: WARNING — response truncated at {size} chars. Narrow 'output' or filter by tag/group before claiming totals.]"
                }]
            return {"result": result}
        except Exception as exc:
            logger.exception("Tool %s failed", name)
            return {"error": str(exc)}


async def init_mcp_loader(mcp_configs: Dict[str, dict]) -> MCPToolLoader:
    sessions: Dict[str, MCPSessionBase] = {}
    for name, cfg in mcp_configs.items():
        transport = cfg.get("transport", "http")
        if transport == "http":
            s = MCPHttpSession(name, cfg["url"])
        elif transport == "stdio":
            s = MCPStdioSession(name, cfg["command"], args=cfg.get("args"), env=cfg.get("env"))
        else:
            raise ValueError(f"Unknown MCP transport: {transport}")
        try:
            await s.connect()
            sessions[name] = s
            logger.info("MCP '%s' connected via %s", name, transport)
        except Exception as exc:
            logger.warning("MCP '%s' unavailable (%s): %s", name, transport, exc)
    return MCPToolLoader(sessions)
