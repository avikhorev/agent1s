#!/usr/bin/env bash
# Refreshes ANTHROPIC_API_KEY in .env from the Claude Code OAuth token stored in macOS Keychain.
# Run this when the token expires (typically after Claude Code session ends).
set -euo pipefail

TOKEN=$(python3 - <<'EOF'
import json, ast, subprocess
raw = subprocess.check_output(
    ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
    stderr=subprocess.DEVNULL
).decode().strip()
d = json.loads(raw)
oauth = d.get("claudeAiOauth", {})
if isinstance(oauth, str):
    oauth = ast.literal_eval(oauth)
print(oauth["accessToken"])
EOF
)

ENV_FILE="$(dirname "$0")/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    cp "$(dirname "$0")/.env.example" "$ENV_FILE"
fi

if grep -q "^ANTHROPIC_API_KEY=" "$ENV_FILE"; then
    sed -i '' "s|^ANTHROPIC_API_KEY=.*|ANTHROPIC_API_KEY=$TOKEN|" "$ENV_FILE"
else
    echo "ANTHROPIC_API_KEY=$TOKEN" >> "$ENV_FILE"
fi

echo "✅ ANTHROPIC_API_KEY updated in .env (token: ${TOKEN:0:20}...)"
echo "   Run: docker compose up -d --build"
