"""Tests for multi-provider LLM router (Gemini, Anthropic, OpenAI/Ollama, fallback, cooldown)."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import llm_router  # noqa: E402


def _mock_http_response(status_code: int, body: dict):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.text = json.dumps(body)
    return resp


def test_parse_provider_and_default_chain():
    assert llm_router.parse_provider_model("gemini:gemini-2.5-flash") == ("gemini", "gemini-2.5-flash")
    assert llm_router.parse_provider_model("claude-sonnet-4-5") == ("anthropic", "claude-sonnet-4-5")
    assert llm_router.parse_provider_model("gpt-4o-mini") == ("openai", "gpt-4o-mini")
    assert llm_router.parse_provider_model("ollama:qwen2.5:14b") == ("ollama", "qwen2.5:14b")

    with patch.dict(os.environ, {"GEMINI_API_KEY": "g", "ANTHROPIC_API_KEY": "a", "JARVIS_MODEL_CHAIN": ""}, clear=False):
        chain = llm_router.build_default_model_chain()
        assert any(c.startswith("gemini:") for c in chain)
        assert "anthropic:claude-sonnet-4-5" in chain


def test_fallback_gemini_to_anthropic_to_ollama_and_cooldown():
    router = llm_router.MultiProviderLLMRouter()
    calls = []

    def fake_post(url, headers=None, json=None):
        calls.append(url)
        if "generativelanguage" in url:
            return _mock_http_response(429, {"error": {"message": "rate limit"}})
        if "anthropic" in url:
            return _mock_http_response(503, {"error": {"message": "overloaded"}})
        return _mock_http_response(200, {
            "choices": [{"message": {"content": "ok from ollama", "tool_calls": [{"id": "tc1", "function": {"name": "zabbix__host_get", "arguments": "{\"host\":\"web-01\"}"}}]}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5},
        })

    with patch.dict(os.environ, {"GEMINI_API_KEY": "g-key", "ANTHROPIC_API_KEY": "a-key", "OLLAMA_BASE_URL": "http://localhost:11434/v1"}, clear=False), \
         patch.object(router._http, "post", side_effect=fake_post):
        resp = router.call(
            system_prompt="sys",
            contents=[{"role": "user", "parts": [{"text": "hello"}]}],
            tools=[{"function_declarations": [{"name": "zabbix__host_get", "description": "get host", "parameters": {"type": "object", "properties": {}}}]}],
            model_chain_override=["gemini:gemini-2.5-flash", "anthropic:claude-sonnet-4-5", "ollama:qwen2.5:14b"],
        )
        parts = resp.candidates[0].content.parts
        assert parts[0].text == "ok from ollama"
        assert parts[1].function_call.name == "zabbix__host_get"
        assert parts[1].function_call.args == {"host": "web-01"}
        assert len(calls) == 3

        # Subsequent call skips cooled-down Gemini and Anthropic directly to Ollama
        calls.clear()
        router.call(
            system_prompt="sys",
            contents=[{"role": "user", "parts": [{"text": "second"}]}],
            model_chain_override=["gemini:gemini-2.5-flash", "anthropic:claude-sonnet-4-5", "ollama:qwen2.5:14b"],
        )
        assert len(calls) == 1 and "localhost:11434" in calls[0]


def test_anthropic_tool_use_normalization():
    router = llm_router.MultiProviderLLMRouter()

    def fake_post(url, headers=None, json=None):
        assert json["tools"][0]["input_schema"]["type"] == "object"
        return _mock_http_response(200, {
            "content": [{"type": "tool_use", "id": "toolu_123", "name": "zabbix__problem_get", "input": {"suppressed": False}}],
            "usage": {"input_tokens": 20, "output_tokens": 10},
        })

    with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "a-key"}, clear=False), \
         patch.object(router._http, "post", side_effect=fake_post):
        resp = router.call(
            system_prompt="sys",
            contents=[{"role": "user", "parts": [{"text": "problems?"}]}],
            tools=[{"function_declarations": [{"name": "zabbix__problem_get", "description": "list", "parameters": {"type": "object", "properties": {"suppressed": {"type": "boolean"}}}}]}],
            model_chain_override=["anthropic:claude-sonnet-4-5"],
        )
        fc = resp.candidates[0].content.parts[0].function_call
        assert fc.name == "zabbix__problem_get"
        assert fc.id == "toolu_123"


if __name__ == "__main__":
    test_parse_provider_and_default_chain()
    test_fallback_gemini_to_anthropic_to_ollama_and_cooldown()
    test_anthropic_tool_use_normalization()
    print("ok: test_llm_router")
