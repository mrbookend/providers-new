#!/usr/bin/env bash
set -euo pipefail

# merge: post-commit sanity chain that works without user aliases.

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

# Try to enable aliases if available (harmless if not)
if [ -f "$HOME/.bashrc" ]; then
  shopt -s expand_aliases || true
  # shellcheck disable=SC1090
  source "$HOME/.bashrc" || true
fi

# 1) Fast-forward sync (safe)
if [ -x scripts/sync-main.sh ]; then
  scripts/sync-main.sh
else
  git fetch --prune
  git switch main
  git pull --ff-only
fi

# 2) Status facts (prefer hcrcheck if present)
if command -v hcrcheck >/dev/null 2>&1; then
  hcrcheck
else
  echo "=== BRANCH & COMMITS ==="
  echo "branch:            $(git rev-parse --abbrev-ref HEAD)"
  echo "local  HEAD:       $(git rev-parse HEAD)"
  echo "origin/main HEAD:  $(git rev-parse origin/main)"
fi

# 3) Preflight hygiene
if [ -x scripts/preflight.sh ]; then
  scripts/preflight.sh
else
  echo "WARN: scripts/preflight.sh not found; skipping."
fi

