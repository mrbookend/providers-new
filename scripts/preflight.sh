#!/usr/bin/env bash
set -euo pipefail
git fetch origin --prune
LOCAL="$(git rev-parse HEAD)"; REMOTE="$(git rev-parse origin/main || true)"
echo "Local:  $LOCAL"
echo "Remote: ${REMOTE:-<none>}"
python3 -m py_compile app_admin.py || true
command -v ruff >/dev/null || { echo "ruff not found in PATH"; exit 1; }
ruff format --check . || true
ruff check --output-format=github . || true
