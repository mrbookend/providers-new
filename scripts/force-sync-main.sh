#!/usr/bin/env bash
set -euo pipefail
git fetch --prune
git switch main
git reset --hard origin/main
git clean -fd
hcrcheck
scripts/preflight.sh
