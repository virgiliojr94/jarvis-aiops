#!/usr/bin/env python3
"""Telegram inline feedback handler (`jfb:up:<ref>` / `jfb:down:<ref>`) + bash loop safeguard (`--strip-callbacks`)."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.request

FEEDBACK_URL = os.environ.get("JARVIS_FEEDBACK_URL", "http://127.0.0.1:8765/feedback")
AUTH_TOKEN_FILE = os.environ.get("JARVIS_AUTH_TOKEN_FILE", "/etc/jarvis/auth_token")
LANG = os.environ.get("JARVIS_LANGUAGE", "en").strip().lower()


def read_auth_token() -> str:
    env_tok = os.environ.get("JARVIS_AUTH_TOKEN", "").strip()
    if env_tok:
        return env_tok
    try:
        with open(AUTH_TOKEN_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


def _tg_post(bot_token: str, method: str, payload: dict) -> dict:
    url = f"https://api.telegram.org/bot{bot_token}/{method}"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _post_feedback(ref_id: str, rating: str, comment: str) -> dict:
    token = read_auth_token()
    if not token:
        return {"ok": False, "error": "auth_token missing"}
    body: dict = {"correlation_id": ref_id, "rating": rating, "comment": comment}
    if ref_id.isdigit():
        body["event_id"] = ref_id
    req = urllib.request.Request(
        FEEDBACK_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def process_callback_query(cq: dict, bot_token: str) -> dict:
    data = str(cq.get("data") or "")
    parts = data.split(":", 2)
    if len(parts) != 3 or parts[0] != "jfb" or parts[1] not in ("up", "down") or not parts[2]:
        return {"ok": False, "reason": "ignored"}

    rating, ref_id = parts[1], parts[2]
    cq_id = cq.get("id", "")
    user = cq.get("from") or {}
    uname = user.get("username") or user.get("first_name") or str(user.get("id") or "operator")
    label = f"@{uname}" if user.get("username") else uname

    fb_res = _post_feedback(ref_id, rating, f"Telegram inline by {label}")
    ok = bool(fb_res.get("ok"))

    if bot_token and cq_id:
        try:
            _tg_post(bot_token, "answerCallbackQuery", {
                "callback_query_id": cq_id,
                "text": "Feedback recorded!" if ok else "Failed to record feedback",
                "show_alert": False,
            })
        except Exception as exc:
            print(f"[feedback_callback] answerCallbackQuery error: {exc}", file=sys.stderr)

    msg = cq.get("message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    message_id = msg.get("message_id")
    if bot_token and ok and chat_id is not None and message_id is not None:
        up_lbl = "👍 Útil" if LANG.startswith("pt") else "👍 Helpful"
        down_lbl = "👎 Errou" if LANG.startswith("pt") else "👎 Wrong"
        up_txt = f"✅ {up_lbl} ({label})" if rating == "up" else up_lbl
        down_txt = f"✅ {down_lbl} ({label})" if rating == "down" else down_lbl
        try:
            _tg_post(bot_token, "editMessageReplyMarkup", {
                "chat_id": chat_id,
                "message_id": message_id,
                "reply_markup": {"inline_keyboard": [[
                    {"text": up_txt, "callback_data": f"jfb:up:{ref_id}"},
                    {"text": down_txt, "callback_data": f"jfb:down:{ref_id}"},
                ]]},
            })
        except Exception as exc:
            print(f"[feedback_callback] editMessageReplyMarkup error: {exc}", file=sys.stderr)

    return {"ok": ok, "rating": rating, "ref_id": ref_id}


def extract_and_dispatch_updates(raw_stdin: str) -> str:
    try:
        payload = json.loads(raw_stdin)
    except Exception:
        return ""
    updates = payload.get("result") or []
    if not isinstance(updates, list) or not updates:
        return ""
    ids = [u["update_id"] for u in updates if isinstance(u, dict) and "update_id" in u]

    for u in updates:
        if not isinstance(u, dict):
            continue
        cq = u.get("callback_query")
        if isinstance(cq, dict) and str(cq.get("data") or "").startswith("jfb:"):
            try:
                subprocess.Popen(
                    [sys.executable, os.path.abspath(__file__), "--handle-json", json.dumps(cq, ensure_ascii=False)],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                    close_fds=True,
                )
            except Exception as exc:
                print(f"[feedback_callback] spawn failed: {exc}", file=sys.stderr)
    return str(max(ids)) if ids else ""


def strip_callback_updates(raw_stdin: str) -> str:
    """Strip callback_query entries so legacy getUpdates bash loops never re-run reply_to_message.text."""
    try:
        payload = json.loads(raw_stdin)
    except Exception:
        return raw_stdin
    if not isinstance(payload, dict) or not isinstance(payload.get("result"), list):
        return raw_stdin
    out = dict(payload)
    out["result"] = [u for u in payload["result"] if not (isinstance(u, dict) and "callback_query" in u)]
    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-updates", action="store_true")
    ap.add_argument("--strip-callbacks", action="store_true")
    ap.add_argument("--handle-json", default="")
    args = ap.parse_args()

    if args.from_updates:
        max_id = extract_and_dispatch_updates(sys.stdin.read())
        if max_id:
            print(max_id)
        return 0

    if args.strip_callbacks:
        print(strip_callback_updates(sys.stdin.read()), end="")
        return 0

    if args.handle_json:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        res = process_callback_query(json.loads(args.handle_json), bot_token)
        return 0 if res.get("ok") else 1

    ap.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
