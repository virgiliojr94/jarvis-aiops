"""Tests for Telegram inline feedback buttons, callback handler, and callback_query stripping."""

import http.server
import importlib.util
import json
import os
import sys
import threading
import urllib.parse
from unittest.mock import patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _load_mod(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, rel_path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_feedback_callback_and_strip():
    cb = _load_mod("feedback_callback", "integrations/telegram/feedback_callback.py")
    only_cb = json.dumps({
        "ok": True,
        "result": [{
            "update_id": 10,
            "callback_query": {
                "id": "cq-1",
                "message": {"message_id": 50, "reply_to_message": {"text": "/jarvis check status"}},
                "data": "jfb:up:cid-1",
            },
        }],
    }, separators=(",", ":"))
    stripped = cb.strip_callback_updates(only_cb)
    assert "/jarvis" not in stripped
    assert json.loads(stripped) == {"ok": True, "result": []}

    calls = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            ln = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(ln).decode("utf-8"))
            calls.append((self.path, body))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        def log_message(self, *_):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        port = srv.server_address[1]
        cb.FEEDBACK_URL = f"http://127.0.0.1:{port}/feedback"
        orig_tg = cb._tg_post
        cb._tg_post = lambda tok, method, payload: calls.append((method, payload)) or {"ok": True}
        with patch.dict(os.environ, {"JARVIS_AUTH_TOKEN": "tok-1"}):
            res = cb.process_callback_query({
                "id": "cq-9",
                "from": {"id": 1, "username": "alice"},
                "message": {"chat": {"id": -1001}, "message_id": 77},
                "data": "jfb:up:9001",
            }, "123:TG")
        cb._tg_post = orig_tg
        assert res["ok"] is True
        assert calls[0][0] == "/feedback" and calls[0][1]["event_id"] == "9001"
        assert calls[2][0] == "editMessageReplyMarkup"
    finally:
        srv.shutdown()


def test_batbot_worker_chunk_buttons():
    worker = _load_mod("batbot_worker", "integrations/telegram/batbot_worker.py")
    sent = []

    class _Resp:
        def __init__(self, body=b"{}"):
            self._b = body
        def read(self):
            return self._b
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass

    def fake_urlopen(req, timeout=20):
        if "/ask" in req.full_url:
            return _Resp(json.dumps({"ok": True, "response": "Healthy!", "correlation_id": "cid-abc"}).encode())
        parsed = urllib.parse.parse_qs(req.data.decode("utf-8"))
        sent.append({k: v[0] for k, v in parsed.items()})
        return _Resp(b'{"ok":true}')

    with patch.dict(os.environ, {"JARVIS_AUTH_TOKEN": "tok"}), \
         patch.object(worker.urllib.request, "urlopen", side_effect=fake_urlopen):
        ans, cid = worker.ask_jarvis("status", "chat:1")
        assert ans == "Healthy!" and cid == "cid-abc"
        worker.send_telegram_chunks("123:TG", "-1001", "9", ("Long test diagnostic line.\n\n" * 220), correlation_id=cid)
        assert len(sent) >= 2
        assert "reply_markup" not in sent[0]
        kb = json.loads(sent[-1]["reply_markup"])["inline_keyboard"][0]
        assert kb[0]["callback_data"] == "jfb:up:cid-abc"


if __name__ == "__main__":
    test_feedback_callback_and_strip()
    test_batbot_worker_chunk_buttons()
    print("ok: test_telegram_feedback")
