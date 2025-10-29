#!/usr/bin/env bash
set -euo pipefail
git fetch --prune
git switch main
git pull --ff-only
hcrcheck
scripts/preflight.sh
