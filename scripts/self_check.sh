#!/usr/bin/env bash
set -euo pipefail

echo "== self_check start =="
# Full read-only chain (external script you already use)
~/bin/zzz

# Strong SQLite check (fails unless 'ok')
make -s sqlite-integrity

# Guard audits (non-interactive safe)
fff
ggg

# CKW dry-run + TEST smoke
make -s test-ckw

echo "== self_check OK =="
