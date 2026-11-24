"""Deterministic context enrichment & SigNoz/Zabbix pre-fetch (Robusta pattern)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

TZ_OFFSET_HOURS = float(os.getenv("JARVIS_TZ_OFFSET_HOURS", "0"))
TZ_LABEL = os.getenv("JARVIS_TZ_LABEL", "UTC").strip() or "UTC"

WITH_TAGS = {"output": "extend", "extra_params": {"selectTags": "extend"}}
UNAVAILABLE = "UNAVAILABLE"
MCP_TRUNCATION_LIMIT = int(os.getenv("JARVIS_MCP_TRUNCATION_LIMIT", "49000"))

TAGS_TIER_1_2 = [{"tag": "tier", "value": "1"}, {"tag": "tier", "value": "2"}]
TAGS_TIER_3 = [{"tag": "tier", "value": "3"}]
EVALTYPE_OR = 2

SIGNOZ_SKIP_ROLES_OR_HOSTS = (
    "hypervisor", "firewall", "fortigate", "bind", "san-switch", "nfs-storage", "switch", "router"
)


def _csv_env(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default).strip()
    return [x.strip() for x in raw.split(",") if x.strip()]


def _json_dict_env(name: str) -> dict[str, str]:
    raw = os.getenv(name, "{}").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items() if k and v}
    except Exception as exc:
        logger.warning("Invalid JSON in %s: %s", name, exc)
    return {}


DEFAULT_TRACES_ONLY_SERVICES = frozenset(
    s.lower() for s in _csv_env("JARVIS_TRACES_ONLY_SERVICES", "traefik")
)
SIGNOZ_KNOWN_SERVICES = _csv_env("JARVIS_KNOWN_SIGNOZ_SERVICES", "traefik")
SERVICE_TO_HOST = _json_dict_env("JARVIS_SERVICE_HOST_MAP")

_SIGNOZ_CATALOG_CACHE: Dict[str, Any] = {
    "trace_services": [],
    "log_services": [],
    "traces_only": set(),
    "all_services": [],
}


class RespostaTruncada(RuntimeError):
    """Raised when an MCP gateway response hits the truncation ceiling."""


def format_local_ts(ts_utc: int) -> str:
    """Format epoch seconds using configured JARVIS_TZ_OFFSET_HOURS / JARVIS_TZ_LABEL."""
    dt_utc = datetime.fromtimestamp(ts_utc, tz=timezone.utc)
    dt_local = dt_utc.astimezone(timezone(timedelta(hours=TZ_OFFSET_HOURS)))
    return f"{dt_local.strftime('%Y-%m-%d %H:%M:%S')} {TZ_LABEL}"


# Backwards-compatible alias
utc_to_brt_str = format_local_ts


def _normalize_svc_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").strip().lower())


def update_signoz_catalog(catalog: Dict[str, Any]) -> None:
    if not isinstance(catalog, dict):
        return
    traces = sorted({str(s).strip() for s in (catalog.get("trace_services") or []) if str(s).strip()})
    logs = sorted({str(s).strip() for s in (catalog.get("log_services") or []) if str(s).strip()})
    all_svcs = sorted(set(traces) | set(logs))
    traces_only = set(traces) - set(logs)
    _SIGNOZ_CATALOG_CACHE["trace_services"] = traces
    _SIGNOZ_CATALOG_CACHE["log_services"] = logs
    _SIGNOZ_CATALOG_CACHE["traces_only"] = traces_only
    _SIGNOZ_CATALOG_CACHE["all_services"] = all_svcs


def get_signoz_catalog() -> Dict[str, Any]:
    return {
        "trace_services": list(_SIGNOZ_CATALOG_CACHE["trace_services"]),
        "log_services": list(_SIGNOZ_CATALOG_CACHE["log_services"]),
        "traces_only": sorted(_SIGNOZ_CATALOG_CACHE["traces_only"]),
        "all_services": list(_SIGNOZ_CATALOG_CACHE["all_services"]),
    }


def _canonical_signoz_service(candidate: str) -> Optional[str]:
    """Resolve service name to exact SigNoz casing from live catalog or configured fallback."""
    cand = str(candidate or "").strip()
    if not cand:
        return None
    pool = _SIGNOZ_CATALOG_CACHE["all_services"] or SIGNOZ_KNOWN_SERVICES
    cand_low = cand.lower()
    cand_norm = _normalize_svc_key(cand)
    for svc in pool:
        if svc == cand:
            return svc
    for svc in pool:
        if svc.lower() == cand_low:
            return svc
    if cand_norm:
        for svc in pool:
            if _normalize_svc_key(svc) == cand_norm:
                return svc
    return None


def _is_signoz_traces_only(svc: str) -> bool:
    if _SIGNOZ_CATALOG_CACHE["all_services"]:
        return svc in _SIGNOZ_CATALOG_CACHE["traces_only"]
    return svc.lower() in DEFAULT_TRACES_ONLY_SERVICES


def _parse_mcp_json(raw: list) -> list:
    if not raw:
        return []
    first = raw[0]
    text = first if isinstance(first, str) else (first.get("text") if isinstance(first, dict) else "")
    if not text:
        return []
    lines = [l for l in text.splitlines() if not l.startswith("[System:")]
    clean = "\n".join(lines).strip()
    if not clean:
        return []
    if len(clean) >= MCP_TRUNCATION_LIMIT:
        raise RespostaTruncada(
            f"response size {len(clean)} chars reached gateway limit; narrow output or filter by tag"
        )
    result = json.loads(clean)
    if isinstance(result, dict):
        if result.get("error"):
            raise ValueError(f"MCP error: {result.get('message', result)}")
        return result.get("result", [])
    return result


def count_tier3_simultaneous(problems: List[Dict]) -> Dict:
    """Detect >=3 simultaneous Tier-3 alerts sharing the same `service` tag."""
    tier3_by_service: Dict[str, int] = {}
    for p in problems:
        tags = {t["tag"]: t["value"] for t in p.get("tags", []) if isinstance(t, dict)}
        svc = tags.get("service")
        if tags.get("tier") == "3" and svc:
            tier3_by_service[svc] = tier3_by_service.get(svc, 0) + 1
    return {svc: {"count": cnt, "alert_tier2": cnt >= 3} for svc, cnt in tier3_by_service.items()}


async def fetch_active_problems(zabbix_session) -> List[Dict]:
    """Return active non-suppressed problems backed by enabled triggers on monitored hosts."""
    raw = await zabbix_session.call_tool(
        "problem_get", {"output": "eventid,objectid,name,severity", "suppressed": False}
    )
    problems = _parse_mcp_json(raw)
    raw_trig = await zabbix_session.call_tool(
        "trigger_get", {"filter": {"value": 1}, "monitored": True, "output": "triggerid"}
    )
    live = {t["triggerid"] for t in _parse_mcp_json(raw_trig)}
    return [p for p in problems if p.get("objectid") in live]


async def fetch_tier3_cascade(zabbix_session) -> Dict:
    raw = await zabbix_session.call_tool(
        "problem_get",
        {
            "output": "eventid,objectid,name",
            "suppressed": False,
            "extra_params": {"selectTags": "extend", "tags": TAGS_TIER_3},
        },
    )
    return count_tier3_simultaneous(_parse_mcp_json(raw))


async def fetch_topology(zabbix_session) -> List[str]:
    """Read live Tier 1 & Tier 2 inventory from Zabbix host tags."""
    raw = await zabbix_session.call_tool(
        "host_get",
        {
            "output": "hostid,host",
            "extra_params": {
                "selectTags": "extend",
                "monitored_hosts": True,
                "tags": TAGS_TIER_1_2,
                "evaltype": EVALTYPE_OR,
            },
        },
    )
    lines = []
    for h in _parse_mcp_json(raw):
        tags = {t["tag"]: t["value"] for t in h.get("tags", []) if isinstance(t, dict)}
        if tags.get("tier") not in ("1", "2"):
            continue
        line = f"{h['host']} tier={tags['tier']} role={tags.get('role', '?')}"
        if tags.get("service"):
            line += f" service={tags['service']}"
        lines.append(line)
    return sorted(lines)


_SECRET_PATTERNS = [
    (re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]{12,}"), "Bearer <redacted>"),
    (re.compile(r"\bAIza[0-9A-Za-z_\-]{20,}\b"), "<google-api-key-redacted>"),
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}\b"), "<api-key-redacted>"),
    (re.compile(r"\b[0-9a-f]{64}\b"), "<token64-redacted>"),
    (re.compile(r"\$6\$[./0-9A-Za-z]{1,16}\$[./0-9A-Za-z]{20,}"), "<sha512-hash-redacted>"),
    (re.compile(r"\$2[aby]\$[0-9]{2}\$[./0-9A-Za-z]{53}"), "<bcrypt-hash-redacted>"),
    (re.compile(r"(?i)(password|passwd|senha|secret|api[_-]?key)\s*[:=]\s*\S+"), r"\1=<redacted>"),
]


def strip_secrets(text: str) -> str:
    if not text:
        return text
    for pat, repl in _SECRET_PATTERNS:
        text = pat.sub(repl, text)
    return text


def truncate_tool_output(text: str, max_chars: int = 4000) -> str:
    if not text or len(text) <= max_chars:
        return text
    head = text[: max_chars - 200]
    return f"{head}\n...[truncated {len(text) - max_chars + 200} chars of {len(text)} total]"


def _extract_host_context(host_data: dict) -> Dict:
    tags = {t["tag"]: t["value"] for t in host_data.get("tags", []) if isinstance(t, dict)}
    desc = strip_secrets((host_data.get("description") or "").strip())
    ctx = {
        "host": host_data["host"],
        "name": host_data.get("name"),
        "hostid": str(host_data["hostid"]),
        "tier": tags.get("tier"),
        "role": tags.get("role"),
        "team": tags.get("team"),
        "owner": tags.get("owner"),
        "service": tags.get("service"),
        "ingress": tags.get("ingress"),
        "depends_on": tags.get("depends_on"),
        "app_host": tags.get("app_host"),
        "description": desc[:500] if desc else None,
        "found_in_zabbix": True,
    }
    return {k: v for k, v in ctx.items() if v is not None}


def _extract_host_candidate_from_question(question: str) -> tuple[Optional[str], Optional[str]]:
    host_regex = os.getenv("JARVIS_HOST_REGEX", "").strip()
    if host_regex:
        m = re.search(host_regex, question, re.IGNORECASE)
        if m:
            return m.group(1 if m.groups() else 0), None

    q_lower = question.lower()
    svc_map = _json_dict_env("JARVIS_SERVICE_HOST_MAP") or SERVICE_TO_HOST
    for svc_name, mapped_host in svc_map.items():
        if svc_name.lower() in q_lower:
            return mapped_host, svc_name

    # Generic hostname-like token fallback (e.g. web-prod-01, srv-db01.local)
    m_gen = re.search(r"\b([a-z][a-z0-9]*(?:-[a-z0-9]+)+)\b", question, re.IGNORECASE)
    if m_gen:
        return m_gen.group(1), None
    return None, None


async def enrich_context(question: str, zabbix_session) -> Dict:
    enrichment: Dict = {
        "mode": "UNKNOWN",
        "active_problems_count": 0,
        "tier3_simultaneous": {},
        "host_context": None,
        "timestamp_local": format_local_ts(int(datetime.now(timezone.utc).timestamp())),
    }

    if not zabbix_session:
        return enrichment

    host_name, svc_mentioned = _extract_host_candidate_from_question(question)
    if svc_mentioned:
        enrichment["service_mentioned"] = svc_mentioned

    if host_name:
        enrichment["host_mentioned"] = host_name
        host_data = None
        for strategy in [
            {"filter": {"host": host_name}},
            {"search": {"name": host_name}},
            {"search": {"host": host_name[:8]}},
        ]:
            try:
                raw = await zabbix_session.call_tool("host_get", {**strategy, **WITH_TAGS})
                result = _parse_mcp_json(raw)
                if result:
                    host_data = result[0]
                    break
            except Exception as exc:
                logger.debug("host_get strategy %s failed: %s", strategy, exc)
        if host_data:
            enrichment["host_context"] = _extract_host_context(host_data)
        else:
            enrichment["host_context"] = {
                "host": host_name,
                "found_in_zabbix": False,
                "tier": UNAVAILABLE,
                "role": UNAVAILABLE,
                "service": UNAVAILABLE,
            }

    try:
        problems = await fetch_active_problems(zabbix_session)
        enrichment["active_problems_count"] = len(problems)
        enrichment["mode"] = "ACTIVE" if problems else "HISTORICAL"
        enrichment["tier3_simultaneous"] = await fetch_tier3_cascade(zabbix_session)
    except Exception as exc:
        logger.warning("problem_get for mode detection failed: %s", exc)

    return enrichment


def resolve_signoz_services(
    host_context: Optional[dict] = None,
    host: Optional[str] = None,
    trigger_name: Optional[str] = None,
    service_mentioned: Optional[str] = None,
) -> list[str]:
    """Resolve SigNoz services to prefetch from Zabbix tags and the live SigNoz catalog."""
    role = hc_host = hc_service = hc_ingress = hc_depends_on = ""
    if isinstance(host_context, dict):
        role = str(host_context.get("role") or "").strip().lower()
        hc_host = str(host_context.get("host") or "").strip().lower()
        hc_service = str(host_context.get("service") or "").strip()
        hc_ingress = str(host_context.get("ingress") or "").strip().lower()
        hc_depends_on = str(host_context.get("depends_on") or "").strip()

    h = str(host or "").strip().lower()
    t = str(trigger_name or "").strip().lower()

    for skip in SIGNOZ_SKIP_ROLES_OR_HOSTS:
        if skip in role or skip in h or skip in hc_host:
            return []

    services: list[str] = []

    def _add_candidate(cand: str) -> None:
        for part in str(cand or "").split(","):
            canon = _canonical_signoz_service(part.strip())
            if canon and canon not in services:
                services.append(canon)

    if hc_service and hc_service.lower() != UNAVAILABLE.lower():
        _add_candidate(hc_service)
    if hc_depends_on:
        _add_candidate(hc_depends_on)
    if hc_ingress:
        _add_candidate(hc_ingress)
    elif role in ("web-frontend", "ingress", "reverse-proxy") or "traefik" in f"{h} {hc_host} {t}":
        _add_candidate("traefik")
    if service_mentioned:
        _add_candidate(service_mentioned)

    pool = _SIGNOZ_CATALOG_CACHE["all_services"] or SIGNOZ_KNOWN_SERVICES
    combined = f"{h} {hc_host} {t}"
    combined_norm = _normalize_svc_key(combined)
    for ks in pool:
        k_low = ks.lower()
        k_norm = _normalize_svc_key(ks)
        if len(k_norm) < 3:
            continue
        if (k_low in combined or (combined_norm and k_norm in combined_norm)) and ks not in services:
            services.append(ks)

    # For database hosts without explicit tags, fall back to top catalog app services if configured
    if not services and role in ("database", "db-oracle", "db-postgres", "db-mysql"):
        for fallback_svc in _csv_env("JARVIS_DB_DEPENDENT_SERVICES"):
            _add_candidate(fallback_svc)

    return services[:3]


def _parse_signoz_json(raw: Any) -> Any:
    if not raw:
        return None
    if isinstance(raw, (dict, int, float)):
        return raw
    if isinstance(raw, list):
        first = raw[0]
        if isinstance(first, dict) and "text" not in first:
            return raw
        text = first if isinstance(first, str) else (first.get("text") if isinstance(first, dict) else "")
    elif isinstance(raw, str):
        text = raw
    else:
        return None
    lines = [l for l in str(text).splitlines() if not l.startswith("[System:")]
    clean = "\n".join(lines).strip()
    return json.loads(clean) if clean else None


def _extract_signoz_scalar_count(parsed: Any) -> Any:
    if parsed is None:
        return None
    if isinstance(parsed, (int, float, str)):
        return parsed
    if isinstance(parsed, list):
        if not parsed:
            return 0
        first = parsed[0]
        if isinstance(first, dict):
            return first.get("value") if "value" in first else (first.get("count") if "count" in first else len(parsed))
        return first
    if isinstance(parsed, dict):
        if "value" in parsed or "count" in parsed:
            return parsed.get("value") if "value" in parsed else parsed.get("count")
        results = ((parsed.get("data") or {}).get("data") or {}).get("results")
        if isinstance(results, list):
            total = 0
            saw_num = False
            for res in results:
                for row in (res.get("data") or []):
                    if isinstance(row, list) and row and isinstance(row[-1], (int, float)):
                        total += row[-1]
                        saw_num = True
            return total if saw_num else 0
        if "data" in parsed and isinstance(parsed["data"], (int, float, str)):
            return parsed["data"]
    return None


def _extract_signoz_log_rows(parsed: Any, limit: int = 3) -> list[str]:
    items: list[Any] = []
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        results = ((parsed.get("data") or {}).get("data") or {}).get("results")
        if isinstance(results, list):
            for res in results:
                for r in (res.get("rows") or []):
                    items.append(r.get("data") if isinstance(r, dict) and isinstance(r.get("data"), dict) else r)
    logs_list: list[str] = []
    for item in items[:limit]:
        if isinstance(item, dict):
            body = item.get("body") or item.get("message") or item.get("text") or str(item)
            ts = item.get("timestamp") or item.get("time") or ""
            logs_list.append(f"{ts} {body}".strip()[:200])
        elif item is not None:
            logs_list.append(str(item)[:200])
    return logs_list


async def _call_signoz(session, name: str, args: dict):
    try:
        return await session.call_tool(name, args)
    except Exception as e:
        if name.startswith("signoz_"):
            try:
                return await session.call_tool(name.replace("signoz_", "", 1), args)
            except Exception:
                pass
        raise e


async def fetch_signoz_catalog(signoz_session, timeout_s: float = 8.0) -> Dict[str, Any]:
    if not signoz_session:
        return {}

    async def _query() -> Dict[str, Any]:
        trace_svcs: list[str] = []
        log_svcs: list[str] = []
        try:
            raw_svcs = await _call_signoz(
                signoz_session, "signoz_list_services", {"timeRange": "24h", "limit": "200"}
            )
            parsed = _parse_signoz_json(raw_svcs)
            rows = parsed.get("data", []) if isinstance(parsed, dict) else (parsed if isinstance(parsed, list) else [])
            for d in rows:
                if isinstance(d, dict) and d.get("serviceName"):
                    trace_svcs.append(str(d["serviceName"]).strip())
        except Exception as exc:
            logger.debug("fetch_signoz_catalog list_services failed: %s", exc)

        try:
            raw_logs = await _call_signoz(
                signoz_session,
                "signoz_aggregate_logs",
                {
                    "aggregation": "count",
                    "groupBy": "service.name",
                    "timeRange": "24h",
                    "limit": "100",
                    "requestType": "scalar",
                },
            )
            parsed = _parse_signoz_json(raw_logs)
            results = (
                ((parsed.get("data") or {}).get("data") or {}).get("results") or []
                if isinstance(parsed, dict) else []
            )
            for res in results:
                for row in (res.get("data") or []):
                    if isinstance(row, list) and row and row[0]:
                        log_svcs.append(str(row[0]).strip())
        except Exception as exc:
            logger.debug("fetch_signoz_catalog aggregate_logs failed: %s", exc)

        if trace_svcs or log_svcs:
            update_signoz_catalog({"trace_services": trace_svcs, "log_services": log_svcs})
        return get_signoz_catalog()

    try:
        return await asyncio.wait_for(_query(), timeout=timeout_s)
    except Exception as exc:
        logger.debug("fetch_signoz_catalog failed/timed out: %s", exc)
        return {}


async def fetch_signoz_prefetch(
    service_names: list[str],
    signoz_session,
    timeout_s: float = 5.0,
) -> dict:
    if not service_names or not signoz_session:
        return {}

    async def _fetch_one(svc: str) -> dict:
        traces_only = _is_signoz_traces_only(svc)
        svc_data = {
            "service": svc,
            "error_logs_count": None,
            "recent_error_logs": [],
            "error_traces_count": None,
            "traces_only": traces_only,
        }
        if not traces_only:
            try:
                raw_agg = await _call_signoz(
                    signoz_session,
                    "signoz_aggregate_logs",
                    {"aggregation": "count", "service": svc, "severity": "ERROR", "timeRange": "30m", "requestType": "scalar"},
                )
                svc_data["error_logs_count"] = _extract_signoz_scalar_count(_parse_signoz_json(raw_agg))
            except Exception:
                pass
            try:
                raw_search = await _call_signoz(
                    signoz_session,
                    "signoz_search_logs",
                    {"service": svc, "severity": "ERROR", "timeRange": "30m", "limit": "3"},
                )
                svc_data["recent_error_logs"] = _extract_signoz_log_rows(_parse_signoz_json(raw_search), limit=3)
            except Exception:
                pass
        try:
            raw_traces = await _call_signoz(
                signoz_session,
                "signoz_aggregate_traces",
                {
                    "aggregation": "count", "service": svc, "error": "true",
                    "timeRange": "30m", "groupBy": "name", "limit": "5", "requestType": "scalar",
                },
            )
            svc_data["error_traces_count"] = _extract_signoz_scalar_count(_parse_signoz_json(raw_traces))
        except Exception:
            pass
        return svc_data

    async def _all():
        results = await asyncio.gather(*[_fetch_one(s) for s in service_names[:3]], return_exceptions=True)
        return {r["service"]: r for r in results if isinstance(r, dict) and r.get("service")}

    try:
        return await asyncio.wait_for(_all(), timeout=timeout_s)
    except asyncio.TimeoutError:
        logger.warning("fetch_signoz_prefetch timed out after %.1fs", timeout_s)
        return {}
    except Exception as exc:
        logger.warning("fetch_signoz_prefetch failed: %s", exc)
        return {}


async def enrich_for_event(
    host: Optional[str],
    host_id: Optional[Union[str, int]],
    trigger_clock: Optional[Union[str, int]],
    zabbix_session,
    window_pre_s: int = 3600,
    trigger_name: Optional[str] = None,
    signoz_session=None,
) -> Dict:
    ts_now = int(datetime.now(timezone.utc).timestamp())
    clock = int(trigger_clock) if trigger_clock else ts_now
    since = clock - window_pre_s

    enrichment: Dict = {
        "mode": "ACTIVE",
        "event_driven": True,
        "active_problems_count": 0,
        "tier3_simultaneous": {},
        "host_context": None,
        "trigger_dependencies": [],
        "timestamp_local": format_local_ts(ts_now),
        "incident_window": {
            "since": since,
            "until": ts_now,
            "since_local": format_local_ts(since),
            "until_local": format_local_ts(ts_now),
            "trigger_clock_local": format_local_ts(clock),
        },
        "provenance": [],
    }

    host_data = None
    if zabbix_session and host_id:
        try:
            raw = await zabbix_session.call_tool(
                "host_get",
                {"hostids": [str(host_id)], "output": "extend",
                 "extra_params": {"selectTags": "extend", "selectInterfaces": "extend"}},
            )
            res = _parse_mcp_json(raw)
            if res:
                host_data = res[0]
        except Exception as exc:
            logger.warning("enrich_for_event host_get by hostid %s failed: %s", host_id, exc)

    if not host_data and zabbix_session and host:
        for strategy in [{"filter": {"host": host}}, {"search": {"name": host}}]:
            try:
                raw = await zabbix_session.call_tool("host_get", {**strategy, **WITH_TAGS})
                res = _parse_mcp_json(raw)
                if res:
                    host_data = res[0]
                    break
            except Exception:
                pass

    resolved_hostid = None
    if host_data:
        resolved_hostid = str(host_data["hostid"])
        enrichment["host_context"] = _extract_host_context(host_data)
        enrichment["provenance"].append(
            {"fact": "host_context", "source": "zabbix.host_get", "ts_local": enrichment["timestamp_local"]}
        )
    elif host:
        enrichment["host_context"] = {
            "host": host, "found_in_zabbix": False,
            "tier": UNAVAILABLE, "role": UNAVAILABLE, "service": UNAVAILABLE,
        }

    target_hid = resolved_hostid or (str(host_id) if host_id else None)
    if zabbix_session and target_hid:
        try:
            raw_deps = await zabbix_session.call_tool(
                "trigger_get",
                {
                    "hostids": [target_hid],
                    "output": "triggerid,description,priority,value",
                    "extra_params": {
                        "monitored": True,
                        "selectDependencies": ["triggerid", "description", "priority", "value"],
                    },
                },
            )
            deps_out = []
            tname_low = (trigger_name or "").strip().lower()
            for trig in _parse_mcp_json(raw_deps):
                deps = trig.get("dependencies") or []
                if not deps:
                    continue
                tdesc = trig.get("description") or ""
                if (tname_low and tname_low in tdesc.lower()) or str(trig.get("value")) == "1":
                    deps_out.append({
                        "trigger": tdesc,
                        "active": str(trig.get("value")) == "1",
                        "depends_on": [
                            {"triggerid": str(d.get("triggerid", "")), "description": d.get("description", ""), "active": str(d.get("value")) == "1"}
                            for d in deps[:5]
                        ],
                    })
            if deps_out:
                enrichment["trigger_dependencies"] = deps_out[:10]
                enrichment["provenance"].append(
                    {"fact": "trigger_dependencies", "source": "zabbix.trigger_get(selectDependencies)", "ts_local": enrichment["timestamp_local"]}
                )
        except Exception as exc:
            logger.debug("trigger dependencies failed: %s", exc)

    if zabbix_session:
        try:
            problems = await fetch_active_problems(zabbix_session)
            enrichment["active_problems_count"] = len(problems)
            enrichment["tier3_simultaneous"] = await fetch_tier3_cascade(zabbix_session)
            enrichment["provenance"].append(
                {"fact": "active_problems", "source": "zabbix.problem_get", "ts_local": enrichment["timestamp_local"]}
            )
        except Exception as exc:
            logger.warning("enrich_for_event problem_get failed: %s", exc)

    if signoz_session:
        try:
            services = resolve_signoz_services(
                enrichment.get("host_context"), host=host, trigger_name=trigger_name
            )
            if services:
                prefetch = await fetch_signoz_prefetch(services, signoz_session, timeout_s=5.0)
                if prefetch:
                    enrichment["signoz_prefetch"] = prefetch
                    enrichment["provenance"].append(
                        {"fact": "signoz_prefetch", "source": "signoz.mcp_prefetch", "ts_local": enrichment["timestamp_local"]}
                    )
        except Exception as exc:
            logger.warning("enrich_for_event signoz_prefetch failed: %s", exc)

    return enrichment


def _append_common_context_blocks(lines: list, enrichment: Dict) -> None:
    if enrichment.get("host_context"):
        lines.append("  <host_context>")
        for k, v in enrichment["host_context"].items():
            if v is not None:
                lines.append(f"    <{k}>{v}</{k}>")
        lines.append("  </host_context>")

    if enrichment.get("trigger_dependencies"):
        lines.append("  <trigger_dependencies hint='Verify parent triggers before assuming local failure'>")
        for td in enrichment["trigger_dependencies"]:
            lines.append(f"    <trigger name='{strip_secrets(td['trigger'])}' active='{td['active']}'>")
            for dep in td["depends_on"]:
                lines.append(
                    f"      <depends_on triggerid='{dep['triggerid']}' active='{dep['active']}'>"
                    f"{strip_secrets(dep['description'])}</depends_on>"
                )
            lines.append("    </trigger>")
        lines.append("  </trigger_dependencies>")

    if enrichment.get("tier3_simultaneous"):
        alerts = [(svc, d) for svc, d in enrichment["tier3_simultaneous"].items() if d["alert_tier2"]]
        if alerts:
            lines.append("  <tier3_cascade_warning>")
            for svc, d in alerts:
                lines.append(f"    <service name='{svc}' count='{d['count']}'>Check app_host for service={svc} FIRST</service>")
            lines.append("  </tier3_cascade_warning>")

    if enrichment.get("signoz_prefetch"):
        for svc, data in enrichment["signoz_prefetch"].items():
            lines.append(f"  <signoz_prefetch service='{svc}' window='30m'>")
            if data.get("error_logs_count") is not None:
                lines.append(f"    <error_logs_count>{data['error_logs_count']}</error_logs_count>")
            if data.get("error_traces_count") is not None:
                lines.append(f"    <error_traces_count>{data['error_traces_count']}</error_traces_count>")
            recent = data.get("recent_error_logs") or []
            if recent:
                lines.append("    <recent_error_logs>")
                for r in recent:
                    lines.append(f"      <log>{strip_secrets(r)}</log>")
                lines.append("    </recent_error_logs>")
            lines.append("  </signoz_prefetch>")


def format_event_context_block(enrichment: Dict, event: Optional[Dict] = None) -> str:
    lines = [
        "<context_block>",
        f"  <timestamp_local>{enrichment['timestamp_local']}</timestamp_local>",
        f"  <mode>{enrichment['mode']}</mode>",
        "  <event_driven>true</event_driven>",
        f"  <active_problems_count>{enrichment['active_problems_count']}</active_problems_count>",
    ]
    if event:
        lines.append("  <trigger_event>")
        for k in ("event_id", "trigger_name", "severity"):
            if event.get(k) is not None:
                lines.append(f"    <{k}>{strip_secrets(str(event[k]))}</{k}>")
        lines.append("  </trigger_event>")
    corr = (event.get("correlated_events") if event else None) or enrichment.get("correlated_events")
    if corr:
        lines.append(f"  <correlated_events count='{len(corr)}'>")
        for c in corr:
            if isinstance(c, dict):
                lines.append(
                    f"    <event host='{strip_secrets(str(c.get('host', '')))}' "
                    f"trigger='{strip_secrets(str(c.get('trigger_name', '')))}' "
                    f"event_id='{c.get('event_id', '')}'/>"
                )
            else:
                lines.append(f"    <event>{strip_secrets(str(c))}</event>")
        lines.append("  </correlated_events>")
    iw = enrichment.get("incident_window")
    if iw:
        lines.append("  <incident_window>")
        lines.append(f"    <since_local>{iw['since_local']}</since_local>")
        lines.append(f"    <until_local>{iw['until_local']}</until_local>")
        lines.append(f"    <since_epoch>{iw['since']}</since_epoch>")
        lines.append(f"    <until_epoch>{iw['until']}</until_epoch>")
        lines.append("  </incident_window>")
    _append_common_context_blocks(lines, enrichment)
    if enrichment.get("provenance"):
        lines.append("  <provenance>")
        for p in enrichment["provenance"]:
            lines.append(f"    <fact name='{p['fact']}' source='{p['source']}' ts='{p['ts_local']}'/>")
        lines.append("  </provenance>")
    lines.append("</context_block>")
    return "\n".join(lines)


def format_context_block(enrichment: Dict) -> str:
    lines = [
        "<context_block>",
        f"  <timestamp_local>{enrichment['timestamp_local']}</timestamp_local>",
        f"  <mode>{enrichment['mode']}</mode>",
        f"  <active_problems_count>{enrichment['active_problems_count']}</active_problems_count>",
    ]
    _append_common_context_blocks(lines, enrichment)
    lines.append("</context_block>")
    return "\n".join(lines)


def format_recent_diagnoses_block(rows: List[Dict]) -> str:
    if not rows:
        return ""
    lines = ["<recent_auto_diagnoses hint='Use if operator asks about a recent alert'>"]
    for r in rows:
        ts_local = format_local_ts(int(r.get("updated_at") or 0))
        lines.append(
            f"  <auto_diag event_id='{r.get('event_id')}' host='{strip_secrets(str(r.get('host', '')))}' "
            f"ts='{ts_local}'>trigger={strip_secrets(str(r.get('trigger_name', '')))} | "
            f"constat={strip_secrets(str(r.get('constat', '')))} | "
            f"hipotese={strip_secrets(str(r.get('hipotese', '')))}</auto_diag>"
        )
    lines.append("</recent_auto_diagnoses>")
    return "\n".join(lines)
