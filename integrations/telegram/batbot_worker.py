#!/usr/bin/env python3
"""Telegram /ask worker: forwards operator queries to Jarvis and attaches 👍/👎 feedback buttons."""

from __future__ import annotations

import html
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

JARVIS_URL = os.environ.get("JARVIS_URL", "http://127.0.0.1:8765/ask")
AUTH_TOKEN_FILE = os.environ.get("JARVIS_AUTH_TOKEN_FILE", "/etc/jarvis/auth_token")
LANG = os.environ.get("JARVIS_LANGUAGE", "en").strip().lower()
TG_CHUNK_LIMIT = 3900


def read_auth_token() -> str:
    env_tok = os.environ.get("JARVIS_AUTH_TOKEN", "").strip()
    if env_tok:
        return env_tok
    try:
        with open(AUTH_TOKEN_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


def decode_bot_text(s: str) -> str:
    return (
        s.replace("\\n", "\n")
        .replace("\\/", "/")
        .replace('\\"', '"')
        .replace("\\\\", "\\")
        .strip()
    )


def format_for_telegram_html(text: str) -> str:
    lines = text.splitlines()
    out = []
    in_code = False
    code_buf: list[str] = []

    def _inline(line: str) -> str:
        tokens: list[str] = []

        def _hold(raw_html: str) -> str:
            tokens.append(raw_html)
            return f"\x00T{len(tokens) - 1}\x00"

        line = re.sub(r"`([^`\n]+)`", lambda m: _hold(f"<code>{html.escape(m.group(1), quote=False)}</code>"), line)
        line = re.sub(
            r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)",
            lambda m: _hold(f'<a href="{html.escape(m.group(2), quote=True)}">{html.escape(m.group(1), quote=False)}</a>'),
            line,
        )
        line = html.escape(line, quote=False)
        line = re.sub(r"\*\*([^\*\n]+)\*\*", r"<b>\1</b>", line)
        line = re.sub(r"__([^_\n]+)__", r"<b>\1</b>", line)
        line = re.sub(r"(?<!\*)\*([^\*\n]+)\*(?!\*)", r"<i>\1</i>", line)
        for idx, tok in enumerate(tokens):
            line = line.replace(f"\x00T{idx}\x00", tok)
        return line

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            if not in_code:
                in_code = True
                code_buf = []
            else:
                out.append(f"<pre>{html.escape(chr(10).join(code_buf), quote=False)}</pre>")
                in_code = False
            continue
        if in_code:
            code_buf.append(line)
            continue
        if stripped in ("---", "***", "___"):
            out.append("────────────")
            continue
        m_hdr = re.match(r"^(#{1,4})\s+(.+)$", stripped)
        if m_hdr:
            out.append(f"<b>{_inline(m_hdr.group(2))}</b>")
            continue
        m_bul = re.match(r"^(\s*)[*\-]\s+(.+)$", line)
        if m_bul:
            indent = "  " if len(m_bul.group(1)) >= 2 else ""
            out.append(f"{indent}• {_inline(m_bul.group(2))}")
            continue
        out.append(_inline(line))

    if in_code and code_buf:
        out.append(f"<pre>{html.escape(chr(10).join(code_buf), quote=False)}</pre>")
    return "\n".join(out)


def ask_jarvis(question: str, session_id: str) -> tuple[str, str]:
    token = read_auth_token()
    if not token:
        return "❌ Error: JARVIS_AUTH_TOKEN not configured.", ""
    payload = {"question": question}
    if session_id:
        payload["session_id"] = session_id

    req = urllib.request.Request(
        JARVIS_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=590) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("ok"):
            ans = (data.get("response") or "").strip() or "⚠️ Empty response."
            return ans, str(data.get("correlation_id") or "").strip()
        return f"❌ Jarvis: {data.get('error', 'unknown error')}", ""
    except urllib.error.HTTPError as exc:
        return f"❌ Jarvis HTTP {exc.code}: {exc.read().decode('utf-8', 'ignore')[:300]}", ""
    except Exception as exc:
        return f"❌ Jarvis error: {exc}", ""


def send_telegram_chunks(bot_token: str, chat_id: str, reply_to_msg_id: str, text: str, correlation_id: str = "") -> None:
    chunks = []
    rem = text
    while len(rem) > TG_CHUNK_LIMIT:
        cut = rem.rfind("\n\n", 0, TG_CHUNK_LIMIT)
        if cut < 1500:
            cut = rem.rfind("\n", 0, TG_CHUNK_LIMIT)
        if cut < 1500:
            cut = TG_CHUNK_LIMIT
        chunks.append(rem[:cut])
        rem = rem[cut:].lstrip()
    chunks.append(rem)

    is_error = text.lstrip().startswith("❌")
    ref_id = str(correlation_id or "").strip()[:48]
    up_lbl = "👍 Útil" if LANG.startswith("pt") else "👍 Helpful"
    down_lbl = "👎 Errou" if LANG.startswith("pt") else "👎 Wrong"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    for idx, chunk in enumerate(chunks):
        form = {
            "chat_id": chat_id,
            "text": format_for_telegram_html(chunk),
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
        if idx == 0 and reply_to_msg_id and reply_to_msg_id != "0":
            form["reply_to_message_id"] = reply_to_msg_id
        if idx == len(chunks) - 1 and ref_id and not is_error:
            form["reply_markup"] = json.dumps({
                "inline_keyboard": [[
                    {"text": up_lbl, "callback_data": f"jfb:up:{ref_id}"},
                    {"text": down_lbl, "callback_data": f"jfb:down:{ref_id}"},
                ]]
            }, separators=(",", ":"))
        req = urllib.request.Request(url, data=urllib.parse.urlencode(form).encode("utf-8"), method="POST")
        try:
            with urllib.request.urlopen(req, timeout=20):
                pass
        except urllib.error.HTTPError as exc:
            if exc.code == 400:
                form.pop("parse_mode", None)
                form["text"] = chunk[:4000]
                req2 = urllib.request.Request(url, data=urllib.parse.urlencode(form).encode("utf-8"), method="POST")
                with urllib.request.urlopen(req2, timeout=20):
                    pass
            else:
                raise


def main() -> int:
    if len(sys.argv) < 5:
        print("usage: batbot_worker.py <chat_id> <from_id> <msg_id> <question...>", file=sys.stderr)
        return 1
    chat_id, from_id, msg_id = sys.argv[1], sys.argv[2], sys.argv[3]
    question = decode_bot_text(" ".join(sys.argv[4:]))
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token or not question:
        return 1
    answer, cid = ask_jarvis(question, f"{chat_id}:{from_id}" if from_id else chat_id)
    send_telegram_chunks(bot_token, chat_id, msg_id, answer, correlation_id=cid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
