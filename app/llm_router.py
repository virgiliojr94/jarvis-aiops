"""Multi-provider open-source LLM router for Jarvis AIOps.

Supports:
  - Google Gemini Developer API (GEMINI_API_KEY / GOOGLE_API_KEY)
  - Anthropic Messages API (ANTHROPIC_API_KEY)
  - OpenAI-compatible & Ollama APIs (OPENAI_API_KEY + OPENAI_BASE_URL / OLLAMA_BASE_URL)

Provides automatic model fallback on 429/500/502/503 with progressive per-model cooldowns,
returning a unified response shape compatible with react_loop.py.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, Optional

import httpx

try:
    from google.genai import types as genai_types
except Exception:  # pragma: no cover
    genai_types = None

try:
    from opentelemetry import trace as _ot_trace
except Exception:  # pragma: no cover
    _ot_trace = None

logger = logging.getLogger(__name__)

GEMINI_API_BASE = os.getenv(
    "GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta"
).rstrip("/")
ANTHROPIC_API_URL = os.getenv(
    "ANTHROPIC_API_URL", "https://api.anthropic.com/v1/messages"
)
DEFAULT_THINKING_BUDGET = int(os.getenv("JARVIS_THINKING_BUDGET_DEFAULT", "2048"))
HTTP_TIMEOUT_S = float(os.getenv("JARVIS_LLM_TIMEOUT_S", "120"))
DEFAULT_COOLDOWN_S = float(os.getenv("JARVIS_COOLDOWN_BASE_S", "60"))
MAX_COOLDOWN_S = float(os.getenv("JARVIS_COOLDOWN_MAX_S", "300"))
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 529})

_SCHEMA_STRIP_KEYS = frozenset({
    "$schema", "$defs", "definitions", "$id", "$ref", "$anchor",
    "additionalProperties", "unevaluatedProperties", "examples", "default",
    "if", "then", "else", "not", "contentEncoding", "contentMediaType",
    "nullable", "title", "propertyOrdering",
})


def _llm_tracer():
    if _ot_trace is None:
        return None
    try:
        return _ot_trace.get_tracer("jarvis-aiops")
    except Exception:
        return None


def parse_provider_model(spec: str) -> tuple[str, str]:
    """Resolve '<provider>:<model>' or infer provider from model prefix."""
    s = (spec or "").strip()
    if ":" in s:
        prefix, rest = s.split(":", 1)
        p_low = prefix.strip().lower()
        if p_low in ("gemini", "google"):
            return "gemini", rest.strip()
        if p_low in ("anthropic", "claude"):
            return "anthropic", rest.strip()
        if p_low == "openai":
            return "openai", rest.strip()
        if p_low == "ollama":
            return "ollama", rest.strip()
    low = s.lower()
    if low.startswith("claude"):
        return "anthropic", s
    if low.startswith(("gpt-", "o1", "o3", "o4")):
        return "openai", s
    return "gemini", s


def _gemini_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()


def _anthropic_key() -> str:
    return os.getenv("ANTHROPIC_API_KEY", "").strip()


def _openai_key() -> str:
    return os.getenv("OPENAI_API_KEY", "").strip()


def _openai_base_url() -> str:
    return os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip().rstrip("/")


def _ollama_base_url() -> str:
    return os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1").strip().rstrip("/")


def build_default_model_chain() -> list[str]:
    """Build an ordered model chain from JARVIS_MODEL_CHAIN or active credentials."""
    raw = os.getenv("JARVIS_MODEL_CHAIN", "").strip()
    if raw:
        chain = [m.strip() for m in raw.split(",") if m.strip()]
        if chain:
            return chain

    chain: list[str] = []
    if _gemini_key():
        chain.extend(["gemini:gemini-2.5-flash", "gemini:gemini-2.5-pro", "gemini:gemini-2.5-flash-lite"])
    if _anthropic_key():
        chain.append("anthropic:claude-sonnet-4-5")
    if _openai_key():
        chain.append("openai:gpt-4o-mini")
    if os.getenv("OLLAMA_BASE_URL", "").strip():
        chain.append(f"ollama:{os.getenv('JARVIS_OLLAMA_MODEL', 'qwen2.5:14b').strip()}")
    if not chain:
        chain = ["gemini:gemini-2.5-flash", "gemini:gemini-2.5-flash-lite"]
    return chain


def _provider_has_credentials(provider: str) -> bool:
    if provider == "gemini":
        return bool(_gemini_key())
    if provider == "anthropic":
        return bool(_anthropic_key())
    if provider == "openai":
        return bool(_openai_key()) or _openai_base_url() != "https://api.openai.com/v1"
    if provider == "ollama":
        return True
    return False


def _clean_schema_for_gemini(schema: Any) -> Any:
    """Recursively normalize JSON schema for Gemini Developer API."""
    if not isinstance(schema, dict):
        return schema
    cleaned: dict[str, Any] = {}
    for k, v in schema.items():
        if k in _SCHEMA_STRIP_KEYS:
            continue
        out_key = "anyOf" if k == "oneOf" else k
        if out_key == "type" and isinstance(v, list):
            non_null = [t for t in v if t != "null"]
            cleaned["type"] = non_null[0] if non_null else "string"
            continue
        if isinstance(v, dict):
            cleaned[out_key] = _clean_schema_for_gemini(v)
        elif isinstance(v, list):
            cleaned[out_key] = [
                _clean_schema_for_gemini(i) if isinstance(i, dict) else i for i in v
            ]
        else:
            cleaned[out_key] = v
    return cleaned


def _schema_to_dict(schema: Any) -> Optional[dict]:
    if schema is None:
        return None
    if isinstance(schema, dict):
        return dict(schema)
    if hasattr(schema, "model_dump"):
        return schema.model_dump(exclude_none=True, by_alias=True)
    return dict(schema)


def _extract_tool_declarations(tools: Any) -> list[dict]:
    """Normalize google.genai Tool objects or plain dicts into [{name, description, parameters}]."""
    decls: list[dict] = []
    for t in tools or []:
        fds = getattr(t, "function_declarations", None)
        if fds is None and isinstance(t, dict):
            fds = t.get("function_declarations") or ([t] if "name" in t else [])
        for fd in fds or []:
            if isinstance(fd, dict):
                name = fd.get("name")
                desc = fd.get("description") or ""
                params = fd.get("parameters")
            else:
                name = getattr(fd, "name", None)
                desc = getattr(fd, "description", "") or ""
                params = getattr(fd, "parameters", None)
            if not name:
                continue
            params_dict = _schema_to_dict(params) or {"type": "object", "properties": {}}
            decls.append({
                "name": str(name),
                "description": str(desc),
                "parameters": params_dict,
            })
    return decls


def _serialize_part(part: Any) -> Optional[dict]:
    if isinstance(part, dict):
        return dict(part)
    out: dict[str, Any] = {}
    if getattr(part, "thought", False):
        out["thought"] = True
    text = getattr(part, "text", None)
    if text is not None:
        out["text"] = text
    fc = getattr(part, "function_call", None)
    if fc is not None:
        fc_dict: dict[str, Any] = {
            "name": getattr(fc, "name", ""),
            "args": dict(getattr(fc, "args", None) or {}),
        }
        fc_id = getattr(fc, "id", None)
        if fc_id:
            fc_dict["id"] = fc_id
        out["functionCall"] = fc_dict
    fr = getattr(part, "function_response", None)
    if fr is not None:
        fr_dict: dict[str, Any] = {
            "name": getattr(fr, "name", ""),
            "response": dict(getattr(fr, "response", None) or {}),
        }
        fr_id = getattr(fr, "id", None)
        if fr_id:
            fr_dict["id"] = fr_id
        out["functionResponse"] = fr_dict
    sig = getattr(part, "thought_signature", None)
    if sig is not None:
        out["thoughtSignature"] = sig
    return out or None


def _normalize_contents(contents: Any) -> list[dict]:
    """Convert Content objects or plain dicts into normalized [{role, parts}] list."""
    out: list[dict] = []
    if not contents:
        return out
    for item in contents:
        if isinstance(item, dict):
            role = item.get("role", "user")
            raw_parts = item.get("parts") or []
        else:
            role = getattr(item, "role", "user") or "user"
            raw_parts = getattr(item, "parts", None) or []
        parts: list[dict] = []
        for p in raw_parts:
            sp = _serialize_part(p)
            if sp is not None:
                parts.append(sp)
        if parts:
            out.append({"role": role, "parts": parts})
    return out


def _make_response(parts: list[Any], usage: dict[str, int]) -> SimpleNamespace:
    content = SimpleNamespace(role="model", parts=parts)
    candidate = SimpleNamespace(content=content)
    usage_meta = SimpleNamespace(
        prompt_token_count=int(usage.get("input", 0)),
        candidates_token_count=int(usage.get("output", 0)),
        thoughts_token_count=int(usage.get("thinking", 0)),
    )
    return SimpleNamespace(candidates=[candidate], usage_metadata=usage_meta)


def _part_from_fc(name: str, args: dict, call_id: Optional[str] = None, thought_sig: Any = None) -> SimpleNamespace:
    fc = SimpleNamespace(name=name, args=args, id=call_id or f"call_{uuid.uuid4().hex[:12]}")
    return SimpleNamespace(text=None, function_call=fc, thought=False, thought_signature=thought_sig)


def _part_from_text(text: str, thought: bool = False) -> SimpleNamespace:
    return SimpleNamespace(text=text, function_call=None, thought=thought, thought_signature=None)


class UpstreamHttpError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class MultiProviderLLMRouter:
    """Open-source multi-provider router with automatic fallback and cooldowns."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cooldowns: dict[str, tuple[float, int]] = {}
        self._http = httpx.Client(timeout=HTTP_TIMEOUT_S)

    def _in_cooldown(self, model_spec: str) -> bool:
        with self._lock:
            entry = self._cooldowns.get(model_spec)
            if not entry:
                return False
            until, _ = entry
            if time.time() >= until:
                return False
            return True

    def _mark_cooldown(self, model_spec: str) -> float:
        with self._lock:
            _, prev_streak = self._cooldowns.get(model_spec, (0.0, 0))
            streak = prev_streak + 1
            duration = min(MAX_COOLDOWN_S, DEFAULT_COOLDOWN_S * (2 ** (streak - 1)))
            self._cooldowns[model_spec] = (time.time() + duration, streak)
            return duration

    def _clear_cooldown(self, model_spec: str) -> None:
        with self._lock:
            self._cooldowns.pop(model_spec, None)

    def _call_gemini(
        self,
        model: str,
        system_prompt: str,
        norm_contents: list[dict],
        tool_decls: list[dict],
        thinking_budget: int,
        temperature: float,
        response_schema: Any,
    ) -> SimpleNamespace:
        api_key = _gemini_key()
        if not api_key:
            raise UpstreamHttpError(401, "GEMINI_API_KEY / GOOGLE_API_KEY not configured")

        gemini_contents = []
        for turn in norm_contents:
            parts_out = []
            for p in turn["parts"]:
                p_clean = {k: v for k, v in p.items() if k != "thought"}
                if "functionCall" in p_clean:
                    p_clean["functionCall"] = {
                        "name": p_clean["functionCall"]["name"],
                        "args": p_clean["functionCall"].get("args") or {},
                    }
                if "functionResponse" in p_clean:
                    p_clean["functionResponse"] = {
                        "name": p_clean["functionResponse"]["name"],
                        "response": p_clean["functionResponse"].get("response") or {},
                    }
                if p_clean:
                    parts_out.append(p_clean)
            if parts_out:
                role = "model" if turn["role"] in ("model", "assistant") else "user"
                gemini_contents.append({"role": role, "parts": parts_out})

        gen_cfg: dict[str, Any] = {"temperature": temperature}
        if thinking_budget > 0 and "lite" not in model.lower():
            gen_cfg["thinkingConfig"] = {"thinkingBudget": int(thinking_budget)}
        if response_schema is not None:
            gen_cfg["responseMimeType"] = "application/json"
            gen_cfg["responseSchema"] = _clean_schema_for_gemini(_schema_to_dict(response_schema))

        body: dict[str, Any] = {
            "contents": gemini_contents,
            "generationConfig": gen_cfg,
        }
        if system_prompt:
            body["systemInstruction"] = {"parts": [{"text": system_prompt}]}
        if tool_decls:
            body["tools"] = [{
                "functionDeclarations": [
                    {
                        "name": td["name"],
                        "description": td["description"],
                        "parameters": _clean_schema_for_gemini(td["parameters"]),
                    }
                    for td in tool_decls
                ]
            }]

        url = f"{GEMINI_API_BASE}/models/{model}:generateContent"
        resp = self._http.post(
            url,
            headers={"x-goog-api-key": api_key, "Content-Type": "application/json"},
            json=body,
        )
        if resp.status_code >= 400:
            raise UpstreamHttpError(resp.status_code, resp.text[:500])

        data = resp.json()
        cands = data.get("candidates") or []
        raw_parts = ((cands[0].get("content") or {}).get("parts") or []) if cands else []
        parts_out: list[Any] = []
        for rp in raw_parts:
            if "functionCall" in rp:
                fc = rp["functionCall"]
                parts_out.append(_part_from_fc(
                    fc.get("name", ""),
                    fc.get("args") or {},
                    thought_sig=rp.get("thoughtSignature"),
                ))
            elif "text" in rp:
                parts_out.append(_part_from_text(rp["text"], thought=bool(rp.get("thought", False))))

        um = data.get("usageMetadata") or {}
        usage = {
            "input": um.get("promptTokenCount", 0),
            "output": um.get("candidatesTokenCount", 0),
            "thinking": um.get("thoughtsTokenCount", 0),
        }
        return _make_response(parts_out, usage)

    def _call_anthropic(
        self,
        model: str,
        system_prompt: str,
        norm_contents: list[dict],
        tool_decls: list[dict],
        thinking_budget: int,
        temperature: float,
        response_schema: Any,
    ) -> SimpleNamespace:
        api_key = _anthropic_key()
        if not api_key:
            raise UpstreamHttpError(401, "ANTHROPIC_API_KEY not configured")

        sys_text = system_prompt or ""
        if response_schema is not None:
            sys_text = (
                f"{sys_text}\n\nRespond STRICTLY with valid JSON matching this schema:\n"
                f"{json.dumps(_schema_to_dict(response_schema), ensure_ascii=False)}"
            ).strip()

        messages: list[dict] = []
        call_id_queue: list[tuple[str, str]] = []
        for turn in norm_contents:
            role = "assistant" if turn["role"] in ("model", "assistant") else "user"
            blocks: list[dict] = []
            for p in turn["parts"]:
                if p.get("thought"):
                    continue
                if "text" in p and p["text"] is not None:
                    blocks.append({"type": "text", "text": str(p["text"])})
                elif "functionCall" in p:
                    fc = p["functionCall"]
                    cid = str(fc.get("id") or f"toolu_{uuid.uuid4().hex[:16]}")
                    call_id_queue.append((fc.get("name", ""), cid))
                    blocks.append({
                        "type": "tool_use",
                        "id": cid,
                        "name": fc.get("name", ""),
                        "input": fc.get("args") or {},
                    })
                elif "functionResponse" in p:
                    fr = p["functionResponse"]
                    fr_id = fr.get("id")
                    if not fr_id:
                        for idx, (cname, cid) in enumerate(call_id_queue):
                            if cname == fr.get("name"):
                                fr_id = cid
                                call_id_queue.pop(idx)
                                break
                    fr_id = str(fr_id or f"toolu_{uuid.uuid4().hex[:16]}")
                    blocks.append({
                        "type": "tool_result",
                        "tool_use_id": fr_id,
                        "content": json.dumps(fr.get("response") or {}, ensure_ascii=False, default=str),
                    })
            if blocks:
                messages.append({"role": role, "content": blocks})

        body: dict[str, Any] = {
            "model": model,
            "max_tokens": 4096,
            "messages": messages,
            "temperature": temperature if thinking_budget <= 0 else 1.0,
        }
        if sys_text:
            body["system"] = sys_text
        if tool_decls:
            body["tools"] = [
                {
                    "name": td["name"],
                    "description": td["description"] or td["name"],
                    "input_schema": td["parameters"] or {"type": "object", "properties": {}},
                }
                for td in tool_decls
            ]

        resp = self._http.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json=body,
        )
        if resp.status_code >= 400:
            raise UpstreamHttpError(resp.status_code, resp.text[:500])

        data = resp.json()
        parts_out: list[Any] = []
        for blk in data.get("content") or []:
            btype = blk.get("type")
            if btype == "text":
                parts_out.append(_part_from_text(blk.get("text", "")))
            elif btype == "tool_use":
                parts_out.append(_part_from_fc(
                    blk.get("name", ""),
                    blk.get("input") or {},
                    call_id=blk.get("id"),
                ))

        um = data.get("usage") or {}
        usage = {
            "input": um.get("input_tokens", 0),
            "output": um.get("output_tokens", 0),
            "thinking": 0,
        }
        return _make_response(parts_out, usage)

    def _call_openai_compat(
        self,
        provider: str,
        model: str,
        system_prompt: str,
        norm_contents: list[dict],
        tool_decls: list[dict],
        temperature: float,
        response_schema: Any,
    ) -> SimpleNamespace:
        if provider == "ollama":
            base_url = _ollama_base_url()
            api_key = os.getenv("OLLAMA_API_KEY", "ollama")
        else:
            base_url = _openai_base_url()
            api_key = _openai_key()
            if not api_key and base_url == "https://api.openai.com/v1":
                raise UpstreamHttpError(401, "OPENAI_API_KEY not configured")

        messages: list[dict] = []
        sys_text = system_prompt or ""
        if response_schema is not None:
            sys_text = (
                f"{sys_text}\n\nRespond strictly with valid JSON matching this schema:\n"
                f"{json.dumps(_schema_to_dict(response_schema), ensure_ascii=False)}"
            ).strip()
        if sys_text:
            messages.append({"role": "system", "content": sys_text})

        call_id_queue: list[tuple[str, str]] = []
        for turn in norm_contents:
            role = "assistant" if turn["role"] in ("model", "assistant") else "user"
            texts = []
            tool_calls = []
            tool_results = []
            for p in turn["parts"]:
                if p.get("thought"):
                    continue
                if "text" in p and p["text"] is not None:
                    texts.append(str(p["text"]))
                elif "functionCall" in p:
                    fc = p["functionCall"]
                    cid = str(fc.get("id") or f"call_{uuid.uuid4().hex[:12]}")
                    call_id_queue.append((fc.get("name", ""), cid))
                    tool_calls.append({
                        "id": cid,
                        "type": "function",
                        "function": {
                            "name": fc.get("name", ""),
                            "arguments": json.dumps(fc.get("args") or {}, ensure_ascii=False),
                        },
                    })
                elif "functionResponse" in p:
                    fr = p["functionResponse"]
                    fr_id = fr.get("id")
                    if not fr_id:
                        for idx, (cname, cid) in enumerate(call_id_queue):
                            if cname == fr.get("name"):
                                fr_id = cid
                                call_id_queue.pop(idx)
                                break
                    fr_id = str(fr_id or f"call_{uuid.uuid4().hex[:12]}")
                    tool_results.append({
                        "role": "tool",
                        "tool_call_id": fr_id,
                        "content": json.dumps(fr.get("response") or {}, ensure_ascii=False, default=str),
                    })

            if tool_results:
                messages.extend(tool_results)
            else:
                msg: dict[str, Any] = {"role": role, "content": "\n".join(texts) if texts else None}
                if tool_calls:
                    msg["tool_calls"] = tool_calls
                if msg["content"] is not None or tool_calls:
                    messages.append(msg)

        body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        if response_schema is not None and provider == "openai":
            body["response_format"] = {"type": "json_object"}
        if tool_decls:
            body["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": td["name"],
                        "description": td["description"] or td["name"],
                        "parameters": td["parameters"] or {"type": "object", "properties": {}},
                    },
                }
                for td in tool_decls
            ]

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        resp = self._http.post(f"{base_url}/chat/completions", headers=headers, json=body)
        if resp.status_code >= 400:
            raise UpstreamHttpError(resp.status_code, resp.text[:500])

        data = resp.json()
        choices = data.get("choices") or []
        message = choices[0].get("message", {}) if choices else {}
        parts_out: list[Any] = []
        if message.get("content"):
            parts_out.append(_part_from_text(str(message["content"])))
        for tc in message.get("tool_calls") or []:
            fn = tc.get("function") or {}
            raw_args = fn.get("arguments") or "{}"
            try:
                args_dict = json.loads(raw_args) if isinstance(raw_args, str) else dict(raw_args)
            except Exception:
                args_dict = {}
            parts_out.append(_part_from_fc(fn.get("name", ""), args_dict, call_id=tc.get("id")))

        um = data.get("usage") or {}
        usage = {
            "input": um.get("prompt_tokens", 0),
            "output": um.get("completion_tokens", 0),
            "thinking": 0,
        }
        return _make_response(parts_out, usage)

    def _dispatch(
        self,
        spec: str,
        system_prompt: str,
        norm_contents: list[dict],
        tool_decls: list[dict],
        thinking_budget: int,
        temperature: float,
        response_schema: Any,
    ) -> SimpleNamespace:
        provider, model = parse_provider_model(spec)
        if provider == "gemini":
            return self._call_gemini(
                model, system_prompt, norm_contents, tool_decls,
                thinking_budget, temperature, response_schema,
            )
        if provider == "anthropic":
            return self._call_anthropic(
                model, system_prompt, norm_contents, tool_decls,
                thinking_budget, temperature, response_schema,
            )
        if provider in ("openai", "ollama"):
            return self._call_openai_compat(
                provider, model, system_prompt, norm_contents, tool_decls,
                temperature, response_schema,
            )
        raise ValueError(f"Unsupported LLM provider: {provider}")

    def call(
        self,
        system_prompt: str,
        contents: Any,
        tools: Any = None,
        thinking_budget: Optional[int] = None,
        temperature: float = 0.2,
        response_schema: Any = None,
        model_chain_override: Optional[list[str]] = None,
    ) -> SimpleNamespace:
        """Execute LLM request across the fallback chain until one succeeds."""
        budget = DEFAULT_THINKING_BUDGET if thinking_budget is None else int(thinking_budget)
        chain = list(model_chain_override) if model_chain_override else build_default_model_chain()
        norm_contents = _normalize_contents(contents)
        tool_decls = _extract_tool_declarations(tools)

        tracer = _llm_tracer()
        last_err: Optional[Exception] = None

        candidates = [m for m in chain if not self._in_cooldown(m)]
        if not candidates:
            candidates = list(chain)

        for idx, spec in enumerate(candidates, start=1):
            provider, _ = parse_provider_model(spec)
            if len(candidates) > 1 and not _provider_has_credentials(provider):
                continue

            span_ctx = tracer.start_as_current_span("jarvis.llm.call") if tracer else nullcontext(None)
            with span_ctx as span:
                if span is not None:
                    span.set_attribute("llm.model", spec)
                    span.set_attribute("llm.provider", provider)
                try:
                    resp = self._dispatch(
                        spec, system_prompt, norm_contents, tool_decls,
                        budget, temperature, response_schema,
                    )
                    self._clear_cooldown(spec)
                    return resp
                except UpstreamHttpError as exc:
                    last_err = exc
                    if exc.status_code in RETRYABLE_STATUS_CODES:
                        cd = self._mark_cooldown(spec)
                        logger.warning(
                            "LLM attempt %d/%d failed on %s (%s); cooldown %.0fs",
                            idx, len(candidates), spec, exc.status_code, cd,
                        )
                        continue
                    logger.warning("LLM non-retryable error on %s: %s", spec, exc)
                    continue
                except httpx.RequestError as exc:
                    last_err = exc
                    cd = self._mark_cooldown(spec)
                    logger.warning(
                        "LLM network error on %s (%s); cooldown %.0fs",
                        spec, exc, cd,
                    )
                    continue

        if last_err is not None:
            raise last_err
        raise RuntimeError("No configured LLM providers succeeded (check API keys / JARVIS_MODEL_CHAIN)")


_client_singleton: Optional[MultiProviderLLMRouter] = None
_client_lock = threading.Lock()


def get_client() -> MultiProviderLLMRouter:
    global _client_singleton
    with _client_lock:
        if _client_singleton is None:
            _client_singleton = MultiProviderLLMRouter()
        return _client_singleton
