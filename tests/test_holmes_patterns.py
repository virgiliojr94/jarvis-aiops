"""Tests for HolmesGPT patterns: tool summarization, secret scrubbing, and modular JSON runbooks."""

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import react_loop  # noqa: E402
from enrichment import strip_secrets  # noqa: E402
from runbooks_loader import format_runbooks_block, load_runbooks, match_runbooks  # noqa: E402


def test_llm_summarize_and_fallback():
    mock_client = MagicMock()
    mock_client.call.return_value = SimpleNamespace(
        candidates=[SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(text="Compact: 10.0.0.5 timeout", thought=False)]))]
    )
    long_text = "2026-11-24 ERROR 10.0.0.5 timeout " * 400
    res = react_loop._maybe_summarize_tool_text(long_text, client=mock_client, remaining_seconds=60.0)
    assert "Compact: 10.0.0.5 timeout" in res

    # Tight deadline skips summarization and truncates
    mock_client.call.reset_mock()
    res_tight = react_loop._maybe_summarize_tool_text(long_text, client=mock_client, remaining_seconds=10.0, max_chars=500)
    assert mock_client.call.call_count == 0 and len(res_tight) <= 500


def test_secret_scrubbing():
    fake_gkey = "AIza" + "FakeTestKey1234567890abcd"
    raw = f"token=Bearer superSecretToken987654 key={fake_gkey} password=mysecret"
    clean = strip_secrets(raw)
    assert "superSecretToken987654" not in clean
    assert fake_gkey not in clean
    assert "mysecret" not in clean


def test_runbooks_load_and_match():
    rbs = load_runbooks()
    assert len(rbs) == 6
    matched = match_runbooks(host="proxy-edge-01", enrichment={"host_context": {"role": "web-frontend"}}, top_k=2)
    assert matched and matched[0]["id"] == "web_reverse_proxy"
    block = format_runbooks_block(matched)
    assert "<runbooks>" in block and "web_reverse_proxy" in block


if __name__ == "__main__":
    test_llm_summarize_and_fallback()
    test_secret_scrubbing()
    test_runbooks_load_and_match()
    print("ok: test_holmes_patterns")
