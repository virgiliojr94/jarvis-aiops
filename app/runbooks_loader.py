"""Modular JSON runbook loader and context matcher (HolmesGPT pattern)."""

import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_RUNBOOKS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runbooks")


def load_runbooks(directory: Optional[str] = None) -> list[dict]:
    """Load all JSON runbooks from directory sorted by priority (ascending)."""
    dir_path = directory if directory is not None else DEFAULT_RUNBOOKS_DIR
    if not os.path.isdir(dir_path):
        return []

    runbooks = []
    for fname in sorted(os.listdir(dir_path)):
        if not fname.endswith(".json"):
            continue
        full_path = os.path.join(dir_path, fname)
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and (data.get("id") or data.get("title")):
                runbooks.append(data)
        except Exception as e:
            logger.warning("Failed to load runbook %s: %s", full_path, e)

    runbooks.sort(key=lambda rb: int(rb.get("priority", 100)))
    return runbooks


def match_runbooks(
    host: Optional[str] = None,
    trigger_name: Optional[str] = None,
    enrichment: Optional[dict] = None,
    question: Optional[str] = None,
    top_k: int = 2,
    directory: Optional[str] = None,
) -> list[dict]:
    """Select up to top_k runbooks matching host, role, service, groups, or keywords."""
    runbooks = load_runbooks(directory=directory)
    if not runbooks:
        return []

    target_hosts: set[str] = set()
    target_roles: set[str] = set()
    target_services: set[str] = set()
    target_groups: set[str] = set()
    text_blobs: list[str] = []

    if host:
        h_clean = str(host).strip().lower()
        if h_clean:
            target_hosts.add(h_clean)
            text_blobs.append(h_clean)
    if trigger_name:
        t_clean = str(trigger_name).strip().lower()
        if t_clean:
            text_blobs.append(t_clean)
    if question:
        q_clean = str(question).strip().lower()
        if q_clean:
            text_blobs.append(q_clean)

    if enrichment and isinstance(enrichment, dict):
        hc = enrichment.get("host_context")
        if isinstance(hc, dict):
            for key, bucket in (("host", target_hosts), ("name", target_hosts),
                                ("role", target_roles), ("service", target_services)):
                if hc.get(key):
                    val = str(hc[key]).strip().lower()
                    bucket.add(val)
                    if bucket is target_hosts:
                        text_blobs.append(val)
            if hc.get("description"):
                text_blobs.append(str(hc["description"]).lower())
            for g in hc.get("groups") or []:
                gname = g if isinstance(g, str) else (g.get("name") if isinstance(g, dict) else "")
                if gname:
                    target_groups.add(str(gname).strip().lower())

        if enrichment.get("host_mentioned"):
            hm = str(enrichment["host_mentioned"]).strip().lower()
            target_hosts.add(hm)
            text_blobs.append(hm)
        if enrichment.get("service_mentioned"):
            sm = str(enrichment["service_mentioned"]).strip().lower()
            target_services.add(sm)
            text_blobs.append(sm)
        if enrichment.get("role"):
            target_roles.add(str(enrichment["role"]).strip().lower())
        if enrichment.get("service"):
            target_services.add(str(enrichment["service"]).strip().lower())

    prompt_text = " ".join(
        str(x).strip().lower() for x in (trigger_name, question) if x
    )
    full_blob = " ".join(text_blobs)

    matched = []
    for rb in runbooks:
        m = rb.get("match") or {}
        if not isinstance(m, dict):
            continue
        rb_hosts = [str(x).lower() for x in (m.get("hosts") or []) if x]
        rb_roles = [str(x).lower() for x in (m.get("roles") or []) if x]
        rb_groups = [str(x).lower() for x in (m.get("groups") or []) if x]
        rb_services = [str(x).lower() for x in (m.get("services") or []) if x]
        rb_keywords = [str(x).lower() for x in (m.get("keywords") or []) if x]

        hit = False
        if any(rh in target_hosts or rh in full_blob for rh in rb_hosts):
            hit = True
        elif any(rr in target_roles for rr in rb_roles):
            hit = True
        elif any(rg in target_groups for rg in rb_groups):
            hit = True
        elif any(rs in target_services or rs in prompt_text for rs in rb_services):
            hit = True
        elif any(rk in full_blob for rk in rb_keywords):
            hit = True

        if hit:
            matched.append(rb)
            if len(matched) >= top_k:
                break

    return matched


def format_runbooks_block(runbooks: list[dict], max_chars: int = 2500) -> str:
    """Format matched runbooks into an XML <runbooks> block bounded by max_chars."""
    if not runbooks:
        return ""
    lines = ["<runbooks>"]
    used = len("<runbooks>\n</runbooks>")
    for rb in runbooks:
        rb_id = rb.get("id", "unknown")
        title = rb.get("title", rb_id)
        block_lines = [f"  <runbook id='{rb_id}' title='{title}'>"]
        for s in rb.get("steps") or []:
            block_lines.append(f"    <step>{s}</step>")
        for a in rb.get("avoid") or []:
            block_lines.append(f"    <avoid>{a}</avoid>")
        block_lines.append("  </runbook>")
        block_str = "\n".join(block_lines) + "\n"
        if used + len(block_str) > max_chars:
            break
        lines.append("\n".join(block_lines))
        used += len(block_str)
    if len(lines) == 1:
        return ""
    lines.append("</runbooks>")
    return "\n".join(lines)
