"""ReAct investigation loop with HolmesGPT-style tool summarization and secret scrubbing."""

from __future__ import annotations

import asyncio
import concurrent.futures
import json as _json
import logging
import os
import time
from contextlib import nullcontext
from typing import Any, Optional, Tuple

from enrichment import strip_secrets, truncate_tool_output
from llm_router import get_client
from mcp_loader import MCPToolLoader

try:
    from opentelemetry import trace as _ot_trace
except Exception:  # pragma: no cover
    _ot_trace = None

logger = logging.getLogger(__name__)

MAX_TOOL_ITERATIONS = int(os.getenv("JARVIS_MAX_TOOL_ITERATIONS", "10"))
TOTAL_TIMEOUT_S = int(os.getenv("JARVIS_TOTAL_TIMEOUT_S", "540"))

_summarize_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="jarvis-summarizer"
)

SUMMARIZE_SYSTEM_PROMPT = (
    "You are a technical summarizer of observability tool outputs. "
    "Compact the output while strictly preserving exact errors, exceptions, stack traces, "
    "timestamps, IP addresses, hostnames, trace_ids, HTTP status codes, and key counts/metrics. "
    "Remove duplicate noise and verbosity."
)


def _loop_tracer():
    if _ot_trace is None:
        return None
    try:
        return _ot_trace.get_tracer("jarvis-aiops")
    except Exception:
        return None


def summarize_tool_output(text: str, client: Optional[Any] = None, model: Optional[str] = None) -> str:
    if client is None:
        client = get_client()
    summarize_model = (model or os.getenv("JARVIS_SUMMARIZE_MODEL", "")).strip()
    kwargs: dict[str, Any] = {
        "system_prompt": SUMMARIZE_SYSTEM_PROMPT,
        "contents": [{"role": "user", "parts": [{"text": text}]}],
        "tools": None,
        "thinking_budget": 0,
        "temperature": 0.1,
    }
    if summarize_model:
        kwargs["model_chain_override"] = [summarize_model]
    resp = client.call(**kwargs)
    if not hasattr(resp, "candidates") or not resp.candidates:
        return ""
    content = getattr(resp.candidates[0], "content", None)
    parts = getattr(content, "parts", []) if content else []
    return "\n".join(
        getattr(p, "text", "") for p in parts
        if getattr(p, "text", None) and not getattr(p, "thought", False)
    ).strip()


def _maybe_summarize_tool_text(
    text: str,
    client: Optional[Any] = None,
    remaining_seconds: Optional[float] = None,
    max_chars: int = 4000,
    threshold_chars: Optional[int] = None,
) -> str:
    enabled = os.getenv("JARVIS_LLM_SUMMARIZE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
    threshold = (
        threshold_chars
        if threshold_chars is not None
        else int(os.getenv("JARVIS_SUMMARIZE_THRESHOLD_CHARS", "6000"))
    )
    if not enabled or len(text) <= threshold:
        return truncate_tool_output(text, max_chars=max_chars)
    if remaining_seconds is not None and remaining_seconds <= 15:
        return truncate_tool_output(text, max_chars=max_chars)

    timeout = 12.0 if remaining_seconds is None else min(12.0, max(1.0, remaining_seconds - 3.0))
    try:
        future = _summarize_executor.submit(summarize_tool_output, text, client)
        summary = future.result(timeout=timeout)
        if summary and len(summary) < len(text):
            return summary
    except Exception as exc:
        logger.warning("Tool summarization failed (%s: %s), falling back to truncate", type(exc).__name__, exc)
    return truncate_tool_output(text, max_chars=max_chars)


def _unwrap_mcp_text(result: Any) -> str:
    if not isinstance(result, dict):
        return str(result or "")
    if isinstance(result.get("content"), str):
        txt = result["content"]
    elif result.get("error"):
        txt = str(result.get("error"))
    elif isinstance(result.get("result"), list):
        parts = []
        for item in result["result"]:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif item is not None:
                parts.append(str(item))
        txt = "\n".join(parts)
    else:
        try:
            txt = _json.dumps(result, ensure_ascii=False, default=str)
        except Exception:
            txt = str(result)
    lines = [l for l in txt.splitlines() if not l.startswith("[System:")]
    return " ".join("\n".join(lines).split())


def _sanitize_tool_result(result: dict, client: Optional[Any] = None, remaining_seconds: Optional[float] = None) -> dict:
    if isinstance(result, dict) and isinstance(result.get("result"), list):
        raw_text = _unwrap_mcp_text(result)
    else:
        try:
            raw_text = _json.dumps(result, ensure_ascii=False, default=str)
        except Exception:
            raw_text = str(result)
    cleaned = strip_secrets(raw_text)
    return {
        "content": _maybe_summarize_tool_text(
            cleaned, client=client, remaining_seconds=remaining_seconds, max_chars=4000
        )
    }


def _scrub_ask_tool_result(
    result: dict,
    max_chars: int = 12000,
    client: Optional[Any] = None,
    remaining_seconds: Optional[float] = None,
) -> dict:
    if not isinstance(result, dict):
        return result
    out = dict(result)
    if "error" in out and isinstance(out["error"], str):
        out["error"] = truncate_tool_output(strip_secrets(out["error"]), max_chars=2000)
    items = out.get("result")
    if isinstance(items, list):
        cleaned = []
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                copy_item = dict(item)
                txt = strip_secrets(copy_item["text"])
                copy_item["text"] = _maybe_summarize_tool_text(
                    txt, client=client, remaining_seconds=remaining_seconds, max_chars=max_chars
                )
                cleaned.append(copy_item)
            else:
                cleaned.append(item)
        out["result"] = cleaned
    return out


async def run_react_loop(
    system_prompt: str,
    user_question: str,
    enrichment_block: str,
    mcp_loader: MCPToolLoader,
    thinking_budget: int,
    question_class=None,
    sanitize: bool = False,
    max_iterations: int | None = None,
    deadline_at: float | None = None,
    prior_turns: list[dict] | None = None,
    model_chain_override: Optional[list[str]] = None,
) -> Tuple[str, dict]:
    client = get_client()
    tools = await mcp_loader.get_tools_for_class(question_class) if question_class else await mcp_loader.load_tools()

    history: list[dict] = []
    for turn in prior_turns or []:
        role = turn.get("role")
        text = turn.get("text")
        if role in ("user", "model") and text:
            history.append({"role": role, "parts": [{"text": text}]})

    user_content = f"{enrichment_block}\n\n<question>{user_question}</question>"
    history.append({"role": "user", "parts": [{"text": user_content}]})

    metadata = {
        "iterations": 0,
        "tool_calls": [],
        "tokens_input": 0,
        "tokens_output": 0,
        "tokens_thinking": 0,
        "stopped_reason": None,
        "duration_ms": 0,
        "evidence": [],
    }

    start = time.time()
    limit = max_iterations if max_iterations is not None else MAX_TOOL_ITERATIONS

    def remaining_seconds() -> float:
        ceiling = deadline_at if deadline_at is not None else start + TOTAL_TIMEOUT_S
        return ceiling - time.time()

    def timeout_result() -> Tuple[str, dict]:
        metadata["stopped_reason"] = "absolute_deadline" if deadline_at is not None else "total_timeout"
        metadata["duration_ms"] = (time.time() - start) * 1000
        text = (
            "Investigation deadline reached; use collected evidence and escalate."
            if deadline_at is not None
            else "Loop timeout reached before completion."
        )
        return text, metadata

    tracer = _loop_tracer()

    for iter_idx in range(limit):
        if remaining_seconds() <= 0:
            return timeout_result()

        iter_ctx = tracer.start_as_current_span("jarvis.react.iteration") if tracer else nullcontext(None)
        with iter_ctx as iter_span:
            if iter_span is not None:
                iter_span.set_attribute("react.iteration", iter_idx + 1)

            try:
                call_kwargs = {"model_chain_override": model_chain_override} if model_chain_override is not None else {}
                call = asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: client.call(
                        system_prompt=system_prompt,
                        contents=history,
                        tools=tools,
                        thinking_budget=thinking_budget,
                        **call_kwargs,
                    ),
                )
                response = await asyncio.wait_for(call, timeout=remaining_seconds())
            except asyncio.TimeoutError:
                return timeout_result()
            except Exception as exc:
                logger.exception("Model call failed")
                metadata["stopped_reason"] = f"api_error: {exc}"
                metadata["duration_ms"] = (time.time() - start) * 1000
                return f"LLM call error: {exc}", metadata

            metadata["iterations"] += 1
            if hasattr(response, "usage_metadata") and response.usage_metadata:
                um = response.usage_metadata
                metadata["tokens_input"] += getattr(um, "prompt_token_count", 0) or 0
                metadata["tokens_output"] += getattr(um, "candidates_token_count", 0) or 0
                metadata["tokens_thinking"] += getattr(um, "thoughts_token_count", 0) or 0

            model_content = response.candidates[0].content
            history.append(model_content)

            function_calls = [
                p.function_call for p in model_content.parts if getattr(p, "function_call", None)
            ]
            if not function_calls:
                final_text = "\n".join(
                    p.text for p in model_content.parts
                    if getattr(p, "text", None) and not getattr(p, "thought", False)
                )
                metadata["stopped_reason"] = "completed"
                metadata["duration_ms"] = (time.time() - start) * 1000
                return final_text, metadata

            response_parts = []
            for fc in function_calls:
                metadata["tool_calls"].append({"name": fc.name, "args": dict(fc.args or {})})
                try:
                    result = await asyncio.wait_for(mcp_loader.execute_tool(fc), timeout=remaining_seconds())
                    rem = remaining_seconds()
                    if sanitize:
                        result = await asyncio.get_running_loop().run_in_executor(
                            None, lambda: _sanitize_tool_result(result, client=client, remaining_seconds=rem)
                        )
                    else:
                        result = await asyncio.get_running_loop().run_in_executor(
                            None, lambda: _scrub_ask_tool_result(result, client=client, remaining_seconds=rem)
                        )
                    metadata["evidence"].append(f"{fc.name}: {_unwrap_mcp_text(result)[:500]}")
                    response_parts.append({
                        "functionResponse": {
                            "name": fc.name,
                            "id": getattr(fc, "id", None),
                            "response": result,
                        }
                    })
                except asyncio.TimeoutError:
                    return timeout_result()
                except Exception as exc:
                    logger.exception("Tool execution failed")
                    response_parts.append({
                        "functionResponse": {
                            "name": fc.name,
                            "id": getattr(fc, "id", None),
                            "response": {"error": str(exc)},
                        }
                    })
            history.append({"role": "user", "parts": response_parts})

    # Synthesize final text after hitting max_iterations
    metadata["stopped_reason"] = "max_iterations_synthesized"
    try:
        rem = remaining_seconds()
        if rem > 5:
            history.append({
                "role": "user",
                "parts": [{"text": "Iteration limit reached. Synthesize the final response from the facts collected so far."}],
            })
            call_kwargs = {"model_chain_override": model_chain_override} if model_chain_override is not None else {}
            resp = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: client.call(
                        system_prompt=system_prompt, contents=history, tools=None,
                        thinking_budget=0, **call_kwargs,
                    ),
                ),
                timeout=rem,
            )
            txt = "\n".join(
                p.text for p in resp.candidates[0].content.parts
                if getattr(p, "text", None) and not getattr(p, "thought", False)
            ).strip()
            if txt:
                metadata["duration_ms"] = (time.time() - start) * 1000
                return txt, metadata
    except Exception as exc:
        logger.warning("Synthesis after max_iterations failed: %s", exc)

    metadata["duration_ms"] = (time.time() - start) * 1000
    ev_txt = "\n".join(f"- {e}" for e in metadata["evidence"][:5])
    return f"Iteration limit ({limit}) reached.\n{ev_txt}".strip(), metadata
