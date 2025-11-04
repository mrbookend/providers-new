#!/usr/bin/env bash
set -euo pipefail
echo "== self_check start =="
make bbb >/dev/null
make ccc
make sqlite-sanity
make sqlite-integrity
fff
ggg
make test-ckw
echo "== self_check OK =="
