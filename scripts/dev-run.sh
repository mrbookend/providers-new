#!/usr/bin/env bash
set -euo pipefail

# dev-run.sh — ensure venv, install deps, compile check, run Streamlit locally.

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

echo "=== DEV RUN ==="
echo "repo: $here"

# 1) venv
if [[ ! -d .venv ]]; then
  echo "[1/5] creating venv (.venv)…"
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

# 2) deps
echo "[2/5] pip bootstrap…"
python -m pip install -U pip >/dev/null
echo "[3/5] install -r requirements.txt…"
python -m pip install -r requirements.txt

# 3) compile
echo "[4/5] compile check…"
python -m py_compile app_admin.py

# 4) optional lint (non-fatal)
if command -v ruff >/dev/null 2>&1; then
  echo "[4b] ruff check (non-fatal)…"
  ruff check . || true
  echo "[4c] ruff format --check (non-fatal)…"
  ruff format --check . || true
fi

# 5) run
echo "[5/5] streamlit run… (Ctrl+C to stop)"
exec python -m streamlit run app_admin.py
