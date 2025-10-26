#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || echo ".")"
cd "$repo_root"

# Always up-to-date remote refs
git fetch -q origin || true

HEAD_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo "UNKNOWN")"
MAIN_COMMIT="$(git rev-parse origin/main 2>/dev/null || echo "UNKNOWN")"

LOCAL_HASH="$(sha256sum app_admin.py 2>/dev/null | awk '{print $1}')"
REMOTE_HASH="$(curl -sL https://raw.githubusercontent.com/mrbookend/providers-new/main/app_admin.py | sha256sum | awk '{print $1}')"

echo "HCR SYNC CHECK"
echo "HEAD:        $HEAD_COMMIT"
echo "origin/main: $MAIN_COMMIT"
echo "app_admin.py local : $LOCAL_HASH"
echo "app_admin.py remote: $REMOTE_HASH"

if [[ "$LOCAL_HASH" != "$REMOTE_HASH" ]]; then
  echo "!! WARNING: local app_admin.py differs from remote main."
  echo "   If you just pushed, run: git pull --ff-only origin main"
fi

if [[ "$HEAD_COMMIT" != "$MAIN_COMMIT" ]]; then
  echo "!! NOTICE: HEAD != origin/main (may be normal before pushing)."
fi
