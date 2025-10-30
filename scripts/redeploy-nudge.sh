#!/usr/bin/env bash
set -euo pipefail

# redeploy-nudge.sh — create a harmless no-op change to trigger Cloud redeploy.

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

branch="$(git rev-parse --abbrev-ref HEAD)"
echo "=== REDEPLOY NUDGE ==="
echo "repo:   $here"
echo "branch: $branch"

# Safety: ensure clean or staged tree (allow staged == clean).
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "ERROR: Working tree is dirty. Commit or stash first."
  exit 1
fi

# Add a harmless comment near the top to change file hash
sed -i '1a # noop: nudge redeploy' app_admin.py

python -m py_compile app_admin.py

# Optional: auto-format if Ruff exists (non-fatal)
if command -v ruff >/dev/null 2>&1; then
  ruff format app_admin.py || true
  ruff check --fix app_admin.py || true
fi

git add app_admin.py
git commit -m "chore(deploy): nudge Streamlit Cloud redeploy (no-op)"
git push
echo "Pushed. Cloud should redeploy shortly."
