"""Jarvis AIOps — Flask gateway, async diagnosis worker, and Keep/Robusta/HolmesGPT orchestrator."""

from __future__ import annotations

import asyncio
import hashlib
import html as _html
import json as _json
import logging
import os
import re
import socket
import sys
import threading
import time
import uuid
from urllib.parse import urlparse

import httpx
from flask import Flask, jsonify, request

from classifier import classify
from enrichment import (
    enrich_context,
    enrich_for_event,
    fetch_signoz_catalog,
    fetch_topology,
    format_context_block,
    format_event_context_block,
    format_local_ts,
    format_recent_diagnoses_block,
)
from llm_router import build_default_model_chain, get_client
from mcp_loader import MCPToolLoader, init_mcp_loader
from react_loop import run_react_loop
from runbooks_loader import format_runbooks_block, match_runbooks

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("jarvis")
logging.getLogger("httpx").setLevel(logging.WARNING)

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_LANG = os.getenv("JARVIS_LANGUAGE", "en").strip().lower()
_LANG_TAG = "pt" if _LANG.startswith("pt") else "en"

SYSTEM_PROMPT_FILE = os.getenv(
    "JARVIS_SYSTEM_PROMPT_FILE",
    os.path.join(_APP_DIR, "prompts", f"system-prompt.{_LANG_TAG}.txt"),
)
ASK_PROMPT_FILE = os.getenv(
    "JARVIS_ASK_PROMPT_FILE",
    os.path.join(_APP_DIR, "prompts", f"ask-prompt.{_LANG_TAG}.txt"),
)

_auth_token = os.environ.get("JARVIS_AUTH_TOKEN", "").strip()
if not _auth_token:
    logging.error("JARVIS_AUTH_TOKEN not set — refusing to start")
    sys.exit(1)

LLM_CONCURRENCY = int(os.getenv("JARVIS_LLM_CONCURRENCY", "2"))
ASYNC_DIAGNOSE_DEADLINE_S = int(os.getenv("JARVIS_ASYNC_DIAGNOSE_DEADLINE_S", "210"))
ASYNC_DIAGNOSE_MAX_QUEUE = int(os.getenv("JARVIS_ASYNC_DIAGNOSE_MAX_QUEUE", "20"))
ASYNC_DIAGNOSE_COOLDOWN_S = int(os.getenv("JARVIS_ASYNC_DIAGNOSE_COOLDOWN_S", "720"))
ASYNC_DIAGNOSE_DEDUP_S = int(os.getenv("JARVIS_ASYNC_DIAGNOSE_DEDUP_S", "3600"))
ASYNC_DIAGNOSE_POLL_S = float(os.getenv("JARVIS_ASYNC_DIAGNOSE_POLL_S", "1"))
ASYNC_DEBOUNCE_S = int(os.getenv("JARVIS_ASYNC_DEBOUNCE_S", "60"))
ASYNC_GROUP_WINDOW_S = int(os.getenv("JARVIS_ASYNC_GROUP_WINDOW_S", "180"))
DEBOUNCE_EXEMPT_PATTERNS = tuple(
    p.strip() for p in os.getenv("JARVIS_DEBOUNCE_EXEMPT_PATTERNS", "").split(",") if p.strip()
)

ASK_MAX_ITERATIONS = int(os.getenv("JARVIS_ASK_MAX_ITERATIONS", "10"))
AUTO_MAX_ITERATIONS = int(os.getenv("JARVIS_AUTO_MAX_ITERATIONS", "6"))
ASK_MIN_THINKING_BUDGET = int(os.getenv("JARVIS_ASK_MIN_THINKING_BUDGET", "2048"))
ASK_CONV_TTL_S = int(os.getenv("JARVIS_ASK_CONV_TTL_S", "1800"))
ASK_CONV_MAX_TURNS = int(os.getenv("JARVIS_ASK_CONV_MAX_TURNS", "4"))
TG_DELIVERY_RESERVE_S = float(os.getenv("JARVIS_TG_DELIVERY_RESERVE_S", "20"))
TG_MIN_SEND_BUDGET_S = float(os.getenv("JARVIS_TG_MIN_SEND_BUDGET_S", "15"))
TG_SEND_ATTEMPTS = int(os.getenv("JARVIS_TG_SEND_ATTEMPTS", "3"))

_tg_token = os.getenv("JARVIS_TG_TOKEN", "").strip()
_tg_chat_id = os.getenv("JARVIS_TG_CHAT_ID", "").strip()


def _build_mcp_configs() -> dict[str, dict]:
    cfgs: dict[str, dict] = {}
    zbx_url = os.getenv("MCP_ZABBIX_URL", "").strip()
    if zbx_url:
        cfgs["zabbix"] = {"transport": "http", "url": zbx_url}

    signoz_cmd = os.getenv("MCP_SIGNOZ_COMMAND", "/usr/local/bin/signoz-mcp-server").strip()
    signoz_url = os.getenv("SIGNOZ_URL", "").strip()
    if signoz_cmd and os.path.exists(signoz_cmd) and signoz_url:
        cfgs["signoz"] = {
            "transport": "stdio",
            "command": signoz_cmd,
            "args": [],
            "env": {
                "SIGNOZ_URL": signoz_url,
                "SIGNOZ_API_KEY": os.getenv("SIGNOZ_API_KEY", ""),
                "TRANSPORT_MODE": "stdio",
                "OTEL_EXPORTER_OTLP_ENDPOINT": None,
                "OTEL_EXPORTER_OTLP_PROTOCOL": None,
                "OTEL_SDK_DISABLED": "true",
            },
        }
    return cfgs


MCP_CONFIGS = _build_mcp_configs()


def _load_prompt_file(path: str, label: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read().strip()
        log.info("%s loaded (%d chars): %s", label, len(text), path)
        return text
    except FileNotFoundError:
        log.warning("%s (%s) not found", label, path)
        return ""


_system_prompt = _load_prompt_file(SYSTEM_PROMPT_FILE, "system prompt")
_ask_prompt = _load_prompt_file(ASK_PROMPT_FILE, "ask prompt")
_prompt_lock = threading.Lock()

TOPOLOGY_REFRESH_S = int(os.getenv("JARVIS_TOPOLOGY_REFRESH_S", "300"))
_topology_block = "(topology not loaded yet)"
_signoz_services_block = "(SigNoz catalog not loaded yet — call signoz_list_services with limit='200')"
_topology_lock = threading.Lock()


def _prompt_for(limit: int, ask_mode: bool = False) -> str:
    with _prompt_lock:
        base = (_ask_prompt or _system_prompt) if ask_mode else _system_prompt
    with _topology_lock:
        topo = _topology_block
        signoz_svcs = _signoz_services_block
    return (
        base.replace("{MAX_ITER}", str(limit))
        .replace("{TOPOLOGY}", topo)
        .replace("{SIGNOZ_SERVICES}", signoz_svcs)
    )


async def _topology_refresher():
    global _topology_block, _signoz_services_block
    while True:
        zbx_sess = _mcp_sessions.get("zabbix")
        if zbx_sess:
            try:
                lines = await fetch_topology(zbx_sess)
                if lines:
                    ts = format_local_ts(int(time.time()))
                    with _topology_lock:
                        _topology_block = f"(fetched from Zabbix at {ts} — {len(lines)} Tier 1/2 hosts)\n" + "\n".join(lines)
                    log.info("topology refreshed hosts=%d", len(lines))
            except Exception as exc:
                log.warning("topology refresh failed: %s", exc)

        signoz_sess = _mcp_sessions.get("signoz")
        if signoz_sess:
            try:
                cat = await fetch_signoz_catalog(signoz_sess, timeout_s=8.0)
                if cat and cat.get("all_services"):
                    ts = format_local_ts(int(time.time()))
                    traces_list = cat.get("trace_services") or []
                    logs_list = cat.get("log_services") or []
                    traces_only = cat.get("traces_only") or []
                    block = (
                        f"(fetched from SigNoz at {ts} — {len(traces_list)} in traces, {len(logs_list)} in logs)\n"
                        f"  traces+logs: {', '.join(logs_list)}\n"
                        f"  traces-only: {', '.join(traces_only)}"
                    )
                    with _topology_lock:
                        _signoz_services_block = block
            except Exception as exc:
                log.warning("signoz catalog refresh failed: %s", exc)

        await asyncio.sleep(TOPOLOGY_REFRESH_S)


# OpenTelemetry (optional)
_otel_enabled = False
_hist_duration = _hist_iterations = _hist_thinking_tokens = None
_hist_input_tokens = _hist_output_tokens = _counter_errors = _gauge_queue = None

try:
    _otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if _otlp_endpoint:
        from opentelemetry import metrics as _ot_metrics, trace as _ot_trace
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.flask import FlaskInstrumentor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        _svc = os.environ.get("OTEL_SERVICE_NAME", "jarvis-aiops")
        _tp = TracerProvider()
        _tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{_otlp_endpoint}/v1/traces")))
        _ot_trace.set_tracer_provider(_tp)
        _mr = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{_otlp_endpoint}/v1/metrics"))
        _mp = MeterProvider(metric_readers=[_mr])
        _ot_metrics.set_meter_provider(_mp)
        _m = _ot_metrics.get_meter(_svc)
        _hist_duration = _m.create_histogram("jarvis.request.duration", unit="ms")
        _hist_iterations = _m.create_histogram("jarvis.request.iterations", unit="1")
        _hist_thinking_tokens = _m.create_histogram("jarvis.request.thinking_tokens", unit="tokens")
        _hist_input_tokens = _m.create_histogram("jarvis.request.input_tokens", unit="tokens")
        _hist_output_tokens = _m.create_histogram("jarvis.request.output_tokens", unit="tokens")
        _counter_errors = _m.create_counter("jarvis.errors")
        _gauge_queue = _m.create_up_down_counter("jarvis.queue.depth")
        FlaskInstrumentor().instrument_app(app)
        _otel_enabled = True
        log.info("OTel enabled -> %s", _otlp_endpoint)
except Exception as exc:
    log.warning("OTel init skipped: %s", exc)


# Asyncio event loop running in dedicated daemon thread
_loop = asyncio.new_event_loop()


def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


_loop_thread = threading.Thread(target=_start_loop, args=(_loop,), daemon=True)
_loop_thread.start()


async def _init_globals():
    sem = asyncio.Semaphore(LLM_CONCURRENCY)
    full_loader = await init_mcp_loader(MCP_CONFIGS)
    s = full_loader.sessions
    ro_sessions = {k: s[k] for k in ("zabbix", "signoz") if k in s}
    ro_loader = MCPToolLoader(ro_sessions, readonly_only=True)
    return sem, s, ro_loader


_init_future = asyncio.run_coroutine_threadsafe(_init_globals(), _loop)
_sem, _mcp_sessions, _ro_loader = _init_future.result(timeout=30)
log.info("MCP loader initialized (sessions=%s)", list(_mcp_sessions.keys()))

try:
    import memory as _mem
    _mem.init_db()
    _MEM_OK = True
    log.info("memory initialized at %s", _mem.DB_PATH)
except Exception as exc:
    _MEM_OK = False
    log.warning("memory init failed — recall/index disabled: %s", exc)


def _check_auth():
    auth = request.headers.get("Authorization", "")
    if not (auth.startswith("Bearer ") and auth[7:] == _auth_token):
        return jsonify({"error": "unauthorized"}), 401
    return None


def _get_correlation_id() -> str:
    try:
        cid = request.headers.get("X-Correlation-ID", "").strip()
        return cid or str(uuid.uuid4())
    except RuntimeError:
        return str(uuid.uuid4())


def _url_tcp_ok(url: str, timeout: float = 3.0) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


asyncio.run_coroutine_threadsafe(_topology_refresher(), _loop)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/readiness", methods=["GET"])
def readiness():
    checks = {
        "llm_chain": bool(build_default_model_chain()),
        "mcp_loader": _ro_loader is not None,
        "memory": _MEM_OK,
    }
    zbx_url = os.getenv("MCP_ZABBIX_URL", "").strip()
    if zbx_url:
        checks["zabbix_mcp"] = _url_tcp_ok(zbx_url)
    signoz_cmd = os.getenv("MCP_SIGNOZ_COMMAND", "").strip()
    if signoz_cmd and os.getenv("SIGNOZ_URL", "").strip():
        checks["signoz_mcp"] = os.path.exists(signoz_cmd)
    all_ok = all(checks.values())
    return jsonify({"ok": all_ok, **checks}), (200 if all_ok else 503)


@app.route("/admin/reload-prompt", methods=["POST"])
def reload_prompt():
    err = _check_auth()
    if err:
        return err
    global _system_prompt, _ask_prompt
    with _prompt_lock:
        _system_prompt = _load_prompt_file(SYSTEM_PROMPT_FILE, "system prompt")
        _ask_prompt = _load_prompt_file(ASK_PROMPT_FILE, "ask prompt")
    return jsonify({"ok": True, "chars": len(_system_prompt), "ask_chars": len(_ask_prompt)})


@app.route("/admin/stats", methods=["GET"])
def admin_stats():
    err = _check_auth()
    if err:
        return err
    if not _MEM_OK:
        return jsonify({"ok": False, "error": "memory not available"}), 503
    try:
        return jsonify({"ok": True, "stats": _mem.get_operational_stats()})
    except Exception as exc:
        log.exception("admin_stats error")
        return jsonify({"ok": False, "error": str(exc)}), 500


# ===== /ask (interactive READ-ONLY mode) =====

_RESET_COMMANDS = frozenset({"limpar", "/limpar", "nova conversa", "reset", "clear"})


async def _handle_ask(question: str, correlation_id: str, session_id: str | None = None):
    if session_id and question.strip().lower() in _RESET_COMMANDS:
        removed = 0
        if _MEM_OK:
            try:
                removed = await asyncio.to_thread(_mem.clear_conversation_turns, session_id)
            except Exception as exc:
                log.warning("clear session failed sid=%s: %s", session_id, exc)
        msg = (
            f"Contexto da conversa limpo ({removed} mensagens removidas)."
            if _LANG_TAG == "pt"
            else f"Conversation context cleared ({removed} messages removed)."
        )
        return jsonify({
            "ok": True,
            "response": msg,
            "correlation_id": correlation_id,
            "metadata": {"class": "session_reset", "iterations": 0, "tokens_thinking": 0, "session_id": session_id},
        })

    async with _sem:
        enrichment = await enrich_context(question, _mcp_sessions.get("zabbix"))
        has_active = enrichment["mode"] == "ACTIVE"
        question_class, thinking_budget = classify(question, has_active)
        thinking_budget = max(thinking_budget, ASK_MIN_THINKING_BUDGET)
        context_block = format_context_block(enrichment)

        prior_turns = []
        if _MEM_OK:
            try:
                recent_auto = await asyncio.to_thread(_mem.get_recent_sent_diagnoses, 1800, 3)
                auto_block = format_recent_diagnoses_block(recent_auto)
                if auto_block:
                    context_block = context_block + "\n" + auto_block
            except Exception:
                pass
            if session_id:
                try:
                    prior_turns = await asyncio.to_thread(
                        _mem.get_conversation_turns, session_id, ASK_CONV_MAX_TURNS, ASK_CONV_TTL_S
                    )
                except Exception:
                    pass

        try:
            hc = enrichment.get("host_context") if isinstance(enrichment, dict) else None
            target_h = hc.get("host") if isinstance(hc, dict) else enrichment.get("host_mentioned")
            rb_matched = match_runbooks(host=target_h, trigger_name=None, enrichment=enrichment, question=question, top_k=2)
            rb_block = format_runbooks_block(rb_matched)
            if rb_block:
                context_block = context_block + "\n\n" + rb_block
        except Exception:
            pass

        system_prompt = _prompt_for(ASK_MAX_ITERATIONS, ask_mode=True)
        response_text, metadata = await run_react_loop(
            system_prompt=system_prompt,
            user_question=question,
            enrichment_block=context_block,
            mcp_loader=_ro_loader,
            thinking_budget=thinking_budget,
            question_class=None,
            max_iterations=ASK_MAX_ITERATIONS,
            prior_turns=prior_turns,
        )

        if (_MEM_OK and session_id
                and metadata.get("stopped_reason") in ("completed", "max_iterations_synthesized")
                and response_text and response_text.strip()):
            try:
                await asyncio.to_thread(_mem.append_conversation_turn, session_id, question, response_text)
            except Exception:
                pass

        log.info(
            "ask_completed cid=%s class=%s budget=%d iters=%d reason=%s",
            correlation_id, question_class.value, thinking_budget,
            metadata["iterations"], metadata["stopped_reason"],
        )

        if _otel_enabled and _hist_duration:
            attrs = {"category": question_class.value, "stopped_reason": metadata["stopped_reason"], "ok": str(metadata["stopped_reason"] == "completed")}
            _hist_duration.record(metadata.get("duration_ms", 0), attrs)
            if _hist_iterations:
                _hist_iterations.record(metadata.get("iterations", 0), attrs)

        return jsonify({
            "ok": True,
            "response": response_text,
            "correlation_id": correlation_id,
            "metadata": {
                "class": question_class.value,
                "iterations": metadata["iterations"],
                "tokens_thinking": metadata["tokens_thinking"],
            },
        })


@app.route("/ask", methods=["POST"])
def ask():
    err = _check_auth()
    if err:
        return err
    cid = _get_correlation_id()
    data = request.get_json(silent=True) or {}
    question = data.get("question", "").strip()
    session_id = (data.get("session_id") or "").strip() or None
    if not question:
        return jsonify({"error": "question required"}), 400

    log.info("ask cid=%s sid=%s question_len=%d", cid, session_id or "-", len(question))
    if _otel_enabled and _gauge_queue:
        _gauge_queue.add(1)
    try:
        future = asyncio.run_coroutine_threadsafe(_handle_ask(question, cid, session_id=session_id), _loop)
        return future.result(timeout=580)
    except asyncio.TimeoutError:
        future.cancel()
        return jsonify({"error": "timeout", "ok": False}), 504
    except Exception as exc:
        log.exception("ask error cid=%s", cid)
        return jsonify({"error": str(exc), "ok": False}), 500
    finally:
        if _otel_enabled and _gauge_queue:
            _gauge_queue.add(-1)


# ===== /diagnose (event-driven, read-only) =====

DIAGNOSE_SCHEMA = {
    "type": "object",
    "properties": {
        "constat": {"type": "string", "description": "Objective statement of current state"},
        "evidencia": {"type": "array", "items": {"type": "string"}, "description": "Observed facts with source"},
        "hipotese": {"type": "string", "description": "Most likely root cause hypothesis"},
        "hipoteses_descartadas": {"type": "array", "items": {"type": "string"}, "description": "Ruled out hypotheses and why"},
        "acao": {"type": "string", "description": "Recommended read-only next action or escalation target"},
        "confianca": {"type": "string", "enum": ["alta", "media", "baixa"]},
    },
    "required": ["constat", "evidencia", "hipotese", "acao", "confianca"],
}

_DIAGNOSE_EVENT_KEYS = ("event_id", "host", "host_id", "severity", "trigger_name", "trigger_clock")


def _build_diagnose_question(event: dict) -> str:
    return (
        "An alert trigger fired. Investigate root cause in READ-ONLY mode and explain what is happening.\n"
        f"Trigger: {event.get('trigger_name', '?')}\n"
        f"Severity: {event.get('severity', '?')}\n"
        f"Host: {event.get('host', '?')} (hostid={event.get('host_id', '?')})\n"
        "Scope log/history queries to <incident_window>. Do not execute write actions."
    )


async def _diagnose_structured(
    system_prompt: str,
    context_block: str,
    event: dict,
    investigation: str,
    deadline_at: float | None = None,
    model_chain_override: list[str] | None = None,
) -> dict:
    client = get_client()
    prompt = (
        "Produce the structured incident diagnosis strictly matching the JSON schema.\n"
        "- 'evidencia': cite verifiable facts and their <provenance> source.\n"
        "- 'confianca': alta|media|baixa.\n\n"
        f"{context_block}\n\n"
        f"<event>{_json.dumps(event, ensure_ascii=False)}</event>\n\n"
        f"<investigation>\n{investigation}\n</investigation>"
    )
    contents = [{"role": "user", "parts": [{"text": prompt}]}]
    kwargs = {"model_chain_override": model_chain_override} if model_chain_override is not None else {}

    call = asyncio.get_running_loop().run_in_executor(
        None,
        lambda: client.call(
            system_prompt=system_prompt,
            contents=contents,
            tools=None,
            thinking_budget=1024,
            response_schema=DIAGNOSE_SCHEMA,
            **kwargs,
        ),
    )
    timeout = None if deadline_at is None else max(0, deadline_at - time.time())
    if timeout is not None and timeout <= 0:
        raise asyncio.TimeoutError()
    response = await asyncio.wait_for(call, timeout=timeout)
    text = "\n".join(p.text for p in response.candidates[0].content.parts if getattr(p, "text", None))
    try:
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
        return _json.loads(clean)
    except Exception as exc:
        log.warning("diagnose structured parse failed err=%s", exc)
        return {"parse_error": str(exc), "raw": text}


def _partial_diagnosis(investigation: str, metadata: dict, reason: str) -> dict:
    evidence = list(metadata.get("evidence") or [])
    if not evidence and investigation:
        evidence.append(investigation[:800])
    if not evidence:
        evidence.append("No evidence collected before deadline.")
    return {
        "constat": "Automatic investigation stopped before completion.",
        "evidencia": evidence[:6],
        "hipotese": "Root cause unconfirmed within time budget.",
        "hipoteses_descartadas": [],
        "acao": "Escalate to the owning team with collected evidence.",
        "confianca": "baixa",
        "partial_reason": reason,
    }


async def _handle_diagnose(
    event: dict,
    correlation_id: str,
    deadline_at: float | None = None,
    automatic: bool = False,
) -> dict:
    def remaining() -> float | None:
        return None if deadline_at is None else deadline_at - time.time()

    def partial(investigation: str, metadata: dict, reason: str) -> dict:
        return {
            "ok": True,
            "event_id": event.get("event_id"),
            "diagnosis": _partial_diagnosis(investigation, metadata, reason),
            "investigation": investigation,
            "correlation_id": correlation_id,
            "metadata": {
                "class": "automatic_partial",
                "iterations": metadata.get("iterations", 0),
                "tokens_thinking": metadata.get("tokens_thinking", 0),
                "stopped_reason": reason,
            },
        }

    if automatic and (remaining() is not None and remaining() <= 0):
        return partial("", {}, "absolute_deadline_before_start")

    model_chain_override = None
    if automatic and _MEM_OK:
        hourly_cap = int(os.getenv("JARVIS_AUTO_HOURLY_CAP", "12"))
        daily_cap = int(os.getenv("JARVIS_AUTO_DAILY_CAP", "60"))
        soft_downgrade = int(os.getenv("JARVIS_AUTO_SOFT_DOWNGRADE_HOURLY", "6"))
        cb = _mem.check_auto_circuit_breaker(hourly_cap, daily_cap, soft_downgrade)
        if cb["action"] == "open":
            log.warning("Circuit breaker OPEN cid=%s reason=%s", correlation_id, cb["reason"])
            diag_suppressed = {
                "constat": f"Automatic diagnosis suppressed by quota Circuit Breaker ({cb['reason']}).",
                "evidencia": [f"Hourly: {cb['hourly_count']}/{hourly_cap}", f"Daily: {cb['daily_count']}/{daily_cap}"],
                "hipotese": "Quota threshold reached for autonomous investigations.",
                "hipoteses_descartadas": [],
                "acao": "Wait for quota window or query manually via /ask.",
                "confianca": "alta",
                "partial_reason": "circuit_breaker_open",
            }
            return {
                "ok": True, "event_id": event.get("event_id"), "diagnosis": diag_suppressed,
                "investigation": f"Suppressed by Circuit Breaker ({cb['reason']}).",
                "correlation_id": correlation_id,
                "metadata": {"class": "automatic_circuit_breaker_open", "iterations": 0, "tokens_thinking": 0, "stopped_reason": "circuit_breaker_open", "circuit_breaker": cb},
            }
        if cb["action"] == "downgrade_cheap":
            cheap_raw = os.getenv("JARVIS_CHEAP_MODEL_CHAIN", "").strip()
            if cheap_raw:
                model_chain_override = [m.strip() for m in cheap_raw.split(",") if m.strip()]

    async with _sem:
        try:
            enrichment_call = enrich_for_event(
                host=event.get("host"),
                host_id=event.get("host_id"),
                trigger_clock=event.get("trigger_clock"),
                zabbix_session=_mcp_sessions.get("zabbix"),
                trigger_name=event.get("trigger_name"),
                signoz_session=_mcp_sessions.get("signoz"),
            )
            timeout = remaining()
            enrichment = await (
                asyncio.wait_for(enrichment_call, timeout=max(0, timeout))
                if automatic and timeout is not None else enrichment_call
            )
        except asyncio.TimeoutError:
            return partial("", {}, "absolute_deadline_during_enrichment")

        question_class, thinking_budget = classify(event.get("trigger_name", ""), True)
        context_block = format_event_context_block(enrichment, event)

        if _MEM_OK:
            try:
                recall_fn = lambda: _mem.recall_similar(f"{event.get('trigger_name', '')} {event.get('host', '')}", host=event.get("host"))
                if automatic:
                    timeout = remaining()
                    if timeout is None or timeout <= 0:
                        return partial("", {}, "absolute_deadline_before_recall")
                    recall = await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(None, recall_fn), timeout=timeout)
                else:
                    recall = recall_fn()
                block = _mem.format_recall_block(recall)
                if block:
                    context_block = context_block + "\n" + block
            except Exception as exc:
                log.warning("recall skipped cid=%s: %s", correlation_id, exc)

        try:
            rb_matched = match_runbooks(host=event.get("host"), trigger_name=event.get("trigger_name"), enrichment=enrichment, question=event.get("trigger_name"), top_k=2)
            rb_block = format_runbooks_block(rb_matched)
            if rb_block:
                context_block = context_block + "\n\n" + rb_block
        except Exception:
            pass

        iter_limit = AUTO_MAX_ITERATIONS if automatic else ASK_MAX_ITERATIONS
        system_prompt = _prompt_for(iter_limit)

        investigation, metadata = await run_react_loop(
            system_prompt=system_prompt,
            user_question=_build_diagnose_question(event),
            enrichment_block=context_block,
            mcp_loader=_ro_loader,
            thinking_budget=thinking_budget,
            question_class=None,
            sanitize=True,
            max_iterations=iter_limit if automatic else None,
            deadline_at=deadline_at if automatic else None,
            model_chain_override=model_chain_override,
        )

        if automatic and metadata.get("stopped_reason") == "absolute_deadline":
            return partial(investigation, metadata, "absolute_deadline_react")
        try:
            diagnosis = await _diagnose_structured(
                system_prompt, context_block, event, investigation,
                deadline_at=deadline_at if automatic else None,
                model_chain_override=model_chain_override,
            )
        except asyncio.TimeoutError:
            return partial(investigation, metadata, "absolute_deadline_structured")

        if _MEM_OK and _mem.should_index_diagnosis(diagnosis):
            try:
                index_fn = lambda: _mem.upsert_incident(
                    event_id=str(event.get("event_id", "")),
                    host=event.get("host") or "",
                    host_id=str(event.get("host_id", "")),
                    trigger_name=event.get("trigger_name") or "",
                    severity=str(event.get("severity", "")),
                    diagnosis=diagnosis,
                    resolution="",
                )
                if automatic:
                    timeout = remaining()
                    if timeout is not None and timeout > 0:
                        await asyncio.wait_for(asyncio.get_running_loop().run_in_executor(None, index_fn), timeout=timeout)
                else:
                    index_fn()
            except Exception as exc:
                log.warning("auto-index skipped cid=%s: %s", correlation_id, exc)

        log.info("diagnose_completed cid=%s event_id=%s class=%s iters=%d reason=%s",
                 correlation_id, event.get("event_id"), question_class.value, metadata["iterations"], metadata["stopped_reason"])
        return {
            "ok": True,
            "event_id": event.get("event_id"),
            "diagnosis": diagnosis,
            "investigation": investigation,
            "correlation_id": correlation_id,
            "metadata": {
                "class": question_class.value,
                "iterations": metadata["iterations"],
                "tokens_thinking": metadata["tokens_thinking"],
                "stopped_reason": metadata["stopped_reason"],
            },
        }


_CONF_BADGE = {"alta": "🟢 alta", "high": "🟢 high", "media": "🟡 media", "medium": "🟡 medium", "baixa": "🔴 baixa", "low": "🔴 low"}


def _telegram_text(event: dict, diagnosis: dict) -> str:
    esc = lambda v, cap=400: _html.escape(str(v or "?")[:cap], quote=False)
    conf_raw = str(diagnosis.get("confianca", "low")).lower().strip()
    conf_label = _CONF_BADGE.get(conf_raw, esc(conf_raw, 40))
    evidence = diagnosis.get("evidencia") or []
    evidence_text = "\n".join(f"• {esc(item, 320)}" for item in evidence[:6]) or "• none"

    if _LANG_TAG == "pt":
        return (
            "🤖 <b>Diagnóstico automático Jarvis</b>\n\n"
            f"<b>Evento:</b> <code>{esc(event.get('event_id', '?'), 60)}</code>\n"
            f"<b>Host:</b> <code>{esc(event.get('host') or event.get('host_id') or '?', 80)}</code>\n"
            f"<b>Trigger:</b> {esc(event.get('trigger_name', '?'), 200)}\n"
            f"<b>Severidade:</b> {esc(event.get('severity', '?'), 40)}\n\n"
            f"<b>Constatação</b>\n{esc(diagnosis.get('constat', '?'), 700)}\n\n"
            f"<b>Hipótese</b>\n{esc(diagnosis.get('hipotese', '?'), 600)}\n\n"
            f"<b>Confiança:</b> {conf_label}\n"
            f"<b>Ação:</b> {esc(diagnosis.get('acao', 'Escalar para equipe responsável.'), 500)}\n\n"
            f"<b>Evidências</b>\n{evidence_text}"
        )
    return (
        "🤖 <b>Jarvis Autonomous Diagnosis</b>\n\n"
        f"<b>Event:</b> <code>{esc(event.get('event_id', '?'), 60)}</code>\n"
        f"<b>Host:</b> <code>{esc(event.get('host') or event.get('host_id') or '?', 80)}</code>\n"
        f"<b>Trigger:</b> {esc(event.get('trigger_name', '?'), 200)}\n"
        f"<b>Severity:</b> {esc(event.get('severity', '?'), 40)}\n\n"
        f"<b>Finding</b>\n{esc(diagnosis.get('constat', '?'), 700)}\n\n"
        f"<b>Hypothesis</b>\n{esc(diagnosis.get('hipotese', '?'), 600)}\n\n"
        f"<b>Confidence:</b> {conf_label}\n"
        f"<b>Action:</b> {esc(diagnosis.get('acao', 'Escalate to owning team.'), 500)}\n\n"
        f"<b>Evidence</b>\n{evidence_text}"
    )


def _build_telegram_payload(event: dict, diagnosis: dict, correlation_id: str | None = None) -> dict:
    payload: dict = {
        "chat_id": _tg_chat_id,
        "text": _telegram_text(event, diagnosis),
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    ref_id = str(event.get("event_id") or correlation_id or "").strip()[:48]
    if ref_id:
        up_lbl = "👍 Útil" if _LANG_TAG == "pt" else "👍 Helpful"
        down_lbl = "👎 Errou" if _LANG_TAG == "pt" else "👎 Wrong"
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": up_lbl, "callback_data": f"jfb:up:{ref_id}"},
                {"text": down_lbl, "callback_data": f"jfb:down:{ref_id}"},
            ]]
        }
    return payload


async def _send_telegram(event: dict, diagnosis: dict, deadline_at: float, correlation_id: str | None = None) -> None:
    if not _tg_token or not _tg_chat_id:
        raise RuntimeError("JARVIS_TG_TOKEN and JARVIS_TG_CHAT_ID are required for Telegram delivery")
    url = f"https://api.telegram.org/bot{_tg_token}/sendMessage"
    payload = _build_telegram_payload(event, diagnosis, correlation_id=correlation_id)

    send_deadline = max(deadline_at, time.time() + TG_MIN_SEND_BUDGET_S)
    response = None
    last_exc = None
    for attempt in range(TG_SEND_ATTEMPTS):
        restante = send_deadline - time.time()
        if attempt > 0 and restante <= 1.0:
            break
        try:
            async with httpx.AsyncClient(timeout=min(20.0, max(10.0, restante))) as client:
                response = await client.post(url, json=payload)
            break
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            last_exc = exc
            wait = min(2.0 * (attempt + 1), max(0.0, send_deadline - time.time() - 1.0))
            if wait <= 0:
                break
            await asyncio.sleep(wait)

    if response is None:
        raise RuntimeError(f"Telegram unreachable after {TG_SEND_ATTEMPTS} attempts: {last_exc}")

    if response.status_code == 400 and payload.get("parse_mode") == "HTML":
        plain = dict(payload)
        plain.pop("parse_mode", None)
        plain["text"] = re.sub(r"<[^>]+>", "", str(payload["text"]))[:4000]
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(url, json=plain)
        except Exception:
            pass

    if response.is_error:
        raise RuntimeError(f"Telegram HTTP {response.status_code}: {response.text}")


async def _check_events_still_open(events: list[dict], zabbix_session) -> tuple[bool, list[str]]:
    eids = [str(e.get("event_id")).strip() for e in (events or []) if e and e.get("event_id")]
    if not eids or not zabbix_session:
        return True, []
    try:
        raw = await asyncio.wait_for(
            zabbix_session.call_tool("event_get", {"extra_params": {"eventids": eids}, "output": "eventid,r_eventid"}),
            timeout=5.0,
        )
        from enrichment import _parse_mcp_json
        rows = _parse_mcp_json(raw)
        if not rows:
            return True, []
        by_id = {str(r.get("eventid")): str(r.get("r_eventid") or "0") for r in rows if isinstance(r, dict)}
        recovered = []
        for eid in eids:
            r_id = by_id.get(eid, "0")
            if not r_id or r_id == "0":
                return True, []
            recovered.append(f"{eid}->{r_id}")
        return False, recovered
    except Exception:
        return True, []


async def _diagnosis_worker():
    log.info("diagnosis worker started")
    while True:
        try:
            job = await asyncio.to_thread(_mem.claim_next_diagnosis_job)
            if not job:
                await asyncio.sleep(ASYNC_DIAGNOSE_POLL_S)
                continue

            event = job["payload"]
            correlated = []
            if _MEM_OK:
                try:
                    correlated = await asyncio.to_thread(_mem.claim_correlated_queued_jobs, job, ASYNC_GROUP_WINDOW_S)
                except Exception:
                    pass
            if correlated:
                event["correlated_events"] = correlated

            host_key = str(event.get("host") or event.get("host_id") or "")
            trig_key = str(event.get("trigger_name") or "")
            is_exempt = bool(DEBOUNCE_EXEMPT_PATTERNS and any(p in f"{host_key} {trig_key}" for p in DEBOUNCE_EXEMPT_PATTERNS))
            z_sess = _mcp_sessions.get("zabbix")
            if not is_exempt and z_sess and event.get("event_id"):
                still_open, recovered_ids = await _check_events_still_open([event] + correlated, z_sess)
                if not still_open:
                    sup_res = {"ok": True, "event_id": event.get("event_id"), "recovered_events": recovered_ids, "metadata": {"stopped_reason": "resolved_before_worker", "iterations": 0, "tokens_thinking": 0}}
                    await asyncio.to_thread(_mem.finish_diagnosis_job, job["job_id"], "suppressed_flap", sup_res, "resolved_before_investigation")
                    continue

            result = None
            prior = None
            if _MEM_OK and ASYNC_DIAGNOSE_DEDUP_S > 0:
                try:
                    prior = await asyncio.to_thread(_mem.find_recent_sent_diagnosis, host_key, trig_key, ASYNC_DIAGNOSE_DEDUP_S)
                except Exception:
                    pass

            if prior and str(prior.get("event_id")) != str(event.get("event_id")):
                prev_diag = prior["diagnosis"]
                prev_ts = format_local_ts(int(prior["updated_at"]))
                result = {
                    "ok": True,
                    "event_id": event.get("event_id"),
                    "diagnosis": {
                        "constat": f"Recurring alert within {ASYNC_DIAGNOSE_DEDUP_S // 60}m (previous event {prior['event_id']} at {prev_ts}).",
                        "evidencia": [f"Previous diagnosis {prior['job_id'][:8]} ({prev_ts}): {prev_diag.get('constat', '')[:220]}"],
                        "hipotese": prev_diag.get("hipotese", ""),
                        "hipoteses_descartadas": prev_diag.get("hipoteses_descartadas") or [],
                        "acao": prev_diag.get("acao", "Follow previous diagnosis or escalate."),
                        "confianca": prev_diag.get("confianca", "media"),
                    },
                    "metadata": {"class": "dedup_window", "iterations": 0, "tokens_thinking": 0, "stopped_reason": "dedup_recent_sent"},
                }
            else:
                investigate_deadline = max(time.time() + 15.0, job["deadline_at"] - TG_DELIVERY_RESERVE_S)
                try:
                    result = await _handle_diagnose(event, f"job:{job['job_id']}", deadline_at=investigate_deadline, automatic=True)
                except Exception as exc:
                    log.exception("automatic diagnose failed job=%s", job["job_id"])
                    result = {"ok": False, "event_id": event.get("event_id"), "diagnosis": _partial_diagnosis("", {}, f"investigation_error: {exc}"), "metadata": {"stopped_reason": "investigation_error"}}

            if result.get("metadata", {}).get("stopped_reason") == "circuit_breaker_open":
                await asyncio.to_thread(_mem.finish_diagnosis_job, job["job_id"], "skipped_circuit_breaker", result)
            else:
                await asyncio.to_thread(_mem.finish_diagnosis_job, job["job_id"], "done", result)
                try:
                    await _send_telegram(event, result["diagnosis"], job["deadline_at"], correlation_id=job["job_id"])
                    await asyncio.to_thread(_mem.finish_diagnosis_job, job["job_id"], "sent", result)
                except Exception as exc:
                    log.exception("telegram delivery failed job=%s", job["job_id"])
                    await asyncio.to_thread(_mem.finish_diagnosis_job, job["job_id"], "failed", result, str(exc))
        except Exception:
            log.exception("diagnosis worker loop failed")
            await asyncio.sleep(ASYNC_DIAGNOSE_POLL_S)


_diagnosis_worker_future = None
if _MEM_OK:
    try:
        _mem.recover_diagnosis_jobs()
        _diagnosis_worker_future = asyncio.run_coroutine_threadsafe(_diagnosis_worker(), _loop)
    except Exception as exc:
        log.exception("diagnosis queue unavailable: %s", exc)


@app.route("/diagnose", methods=["POST"])
def diagnose():
    err = _check_auth()
    if err:
        return err
    cid = _get_correlation_id()
    data = request.get_json(silent=True) or {}
    if not (data.get("host") or data.get("host_id")):
        return jsonify({"error": "host or host_id required", "ok": False}), 400

    event = {k: data.get(k) for k in _DIAGNOSE_EVENT_KEYS}
    status_raw = str(data.get("event_status") or data.get("status") or "").strip().upper()
    value_raw = str(data.get("value") or "").strip()
    if status_raw in ("RESOLVED", "OK", "0") or value_raw == "0":
        cancelled_info = {"cancelled": False}
        if _MEM_OK:
            try:
                cancelled_info = _mem.cancel_diagnosis_job_on_recovery(
                    event_id=str(event.get("event_id") or ""),
                    host=str(event.get("host") or ""),
                    trigger_name=str(event.get("trigger_name") or ""),
                )
            except Exception:
                pass
        return jsonify({"ok": True, "status": "suppressed_flap", "recovery": True, "event_id": event.get("event_id"), **cancelled_info}), 200

    if data.get("async") is True:
        if not event.get("event_id"):
            return jsonify({"error": "event_id required for async diagnosis", "ok": False}), 400
        if not _MEM_OK or _diagnosis_worker_future is None or _diagnosis_worker_future.done():
            return jsonify({"error": "diagnosis queue unavailable", "ok": False}), 503
        queued = _mem.enqueue_diagnosis_job(
            event,
            deadline_at=time.time() + ASYNC_DIAGNOSE_DEADLINE_S,
            max_pending=ASYNC_DIAGNOSE_MAX_QUEUE,
            cooldown_s=ASYNC_DIAGNOSE_COOLDOWN_S,
            debounce_s=ASYNC_DEBOUNCE_S,
            exempt_patterns=DEBOUNCE_EXEMPT_PATTERNS,
        )
        if queued["status"] == "full":
            return jsonify({"error": "diagnosis queue full", "ok": False, "pending": queued["pending"]}), 429
        return jsonify({"ok": True, "async": True, "event_id": event["event_id"], **queued}), 202

    try:
        future = asyncio.run_coroutine_threadsafe(_handle_diagnose(event, cid), _loop)
        return jsonify(future.result(timeout=580))
    except asyncio.TimeoutError:
        future.cancel()
        return jsonify({"error": "timeout", "ok": False}), 504
    except Exception as exc:
        log.exception("diagnose error cid=%s", cid)
        return jsonify({"error": str(exc), "ok": False}), 500


@app.route("/index", methods=["POST"])
def index_incident():
    err = _check_auth()
    if err:
        return err
    if not _MEM_OK:
        return jsonify({"error": "memory not available", "ok": False}), 503
    data = request.get_json(silent=True) or {}
    host = data.get("host", "").strip()
    resolution = (data.get("resolution") or "").strip()
    duration = int(data.get("duration_seconds", 0) or 0)
    if not host or not resolution:
        return jsonify({"error": "host and resolution required", "ok": False}), 400
    if not _mem.should_index(resolution, duration, bool(data.get("has_tech", True))):
        return jsonify({"ok": True, "indexed": False, "reason": "below minimum resolution/duration threshold"})
    rid = _mem.upsert_incident(
        event_id=str(data.get("event_id", "")),
        host=host,
        host_id=str(data.get("host_id", "")),
        trigger_name=data.get("trigger_name", ""),
        severity=data.get("severity", ""),
        diagnosis=data.get("diagnosis") or {},
        resolution=resolution,
    )
    return jsonify({"ok": True, "indexed": True, "id": rid})


@app.route("/feedback", methods=["POST"])
def feedback():
    err = _check_auth()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    correlation_id = data.get("correlation_id", "").strip()
    rating = data.get("rating", "")
    comment = data.get("comment", "").strip()
    if not correlation_id:
        return jsonify({"error": "correlation_id required"}), 400
    if rating not in ("up", "down"):
        return jsonify({"error": 'rating must be "up" or "down"'}), 400

    fb_record = None
    if _MEM_OK:
        fb_record = _mem.record_feedback(
            correlation_id=correlation_id,
            rating=rating,
            comment=comment,
            event_id=str(data.get("event_id") or "").strip(),
            host=str(data.get("host") or "").strip(),
        )
    log.info("feedback cid=%s rating=%s", correlation_id, rating)
    res = {"ok": True}
    if fb_record:
        res["feedback_record"] = fb_record
    return jsonify(res)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8765")), debug=False)
