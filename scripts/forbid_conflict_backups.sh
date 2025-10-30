#!/usr/bin/env bash
set -euo pipefail
PATTERN='(\.LOCAL\.|\.REMOTE\.|\.BACKUP\.|\.BAK\.|\.orig$|\.rej$)'
bad=$(git diff --cached --name-only | grep -E "$PATTERN" || true)
if [ -n "$bad" ]; then
  echo "ERROR: conflict/backup files staged:"
  echo "$bad" | sed 's/^/  - /'
  exit 1
fi
