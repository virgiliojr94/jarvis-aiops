#!/usr/bin/env bash
# Generic CLI / Zabbix alertscript wrapper for Jarvis AIOps
# Usage: ./zabbix_call.sh "What is causing high latency on api-payments?"

set -euo pipefail

JARVIS_URL="${JARVIS_URL:-http://127.0.0.1:8765/ask}"
TOKEN_FILE="${JARVIS_AUTH_TOKEN_FILE:-/etc/jarvis/auth_token}"
TIMEOUT="${JARVIS_CLI_TIMEOUT:-590}"

TOKEN="${JARVIS_AUTH_TOKEN:-}"
if [[ -z "$TOKEN" && -r "$TOKEN_FILE" ]]; then
  TOKEN="$(<"$TOKEN_FILE")"
fi
if [[ -z "$TOKEN" ]]; then
  echo "❌ JARVIS_AUTH_TOKEN not set and $TOKEN_FILE unreadable." >&2
  exit 1
fi

QUESTION="${*:-}"
if [[ -z "$QUESTION" ]]; then
  echo "Usage: $0 <question>" >&2
  exit 1
fi

PAYLOAD="$(python3 -c 'import json,sys; print(json.dumps({"question": sys.argv[1]}))' "$QUESTION")"

curl -sf --max-time "$TIMEOUT" \
  -X POST "$JARVIS_URL" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" | python3 -c '
import sys, json
d = json.load(sys.stdin)
print(d.get("response", json.dumps(d, ensure_ascii=False)) if d.get("ok") else "❌ Error: " + str(d))
'
