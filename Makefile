# ===============================
# providers-new — Makefile (full)
# ===============================

PY ?= python3

.PHONY: help status bbb ccc bc zzz \
        rowcount-check rowcount-show rowcount-guard rowcount-refresh rowcount-accept \
        guard-debug

help:
	@echo "status         - git sync status (bbb)"
	@echo "bbb            - fetch/prune + show HEAD vs origin/main"
	@echo "ccc            - code checks (py_compile + ruff fix/format + debug guard)"
	@echo "bc             - fast loop: bbb + ccc"
	@echo "zzz            - full read-only health chain (bbb + ccc + rowcount-guard)"
	@echo "rowcount-check - compute current rowcounts to /tmp/_row.json"
	@echo "rowcount-show  - diff baseline vs current (canonicalized JSON)"
	@echo "rowcount-guard - fail if baseline != current"
	@echo "rowcount-refresh - recompute counts and stage temp canonical files"
	@echo "rowcount-accept  - accept current counts as new baseline (.rowcounts.json)"
	@echo "guard-debug    - run debug-panel guard script"

status: bbb

bbb:
	@echo "=== git sync (bbb) ==="
	@git fetch --prune > /dev/null 2>&1 || true
	@echo "HEAD:        $$(git rev-parse --short HEAD)"
	@echo "origin/main: $$(git rev-parse --short origin/main)"

ccc:
	@echo "=== code checks (ccc) ==="
	@$(PY) -m py_compile app_readonly.py app_admin.py
	@ruff check --fix
	@ruff format
	@$(PY) scripts/check_debug_panel.py

bc: bbb ccc

# Full chain
zzz: bbb ccc rowcount-guard

# ---- Rowcount baseline / guard -----------------------------------------

rowcount-check:
	@echo "=== rowcount-check ==="
	@$(PY) scripts/rowcount_guard.py > /tmp/_row.json

rowcount-show: rowcount-check
	@echo "=== rowcount-show (diff baseline vs current) ==="
	@$(PY) -c 'import json,sys;print(json.dumps(json.load(open(".rowcounts.json")),sort_keys=True,separators=(",",":")))' > /tmp/_row.base.canon.json
	@$(PY) -c 'import json,sys;print(json.dumps(json.load(open("/tmp/_row.json")),sort_keys=True,separators=(",",":")))' > /tmp/_row.curr.canon.json
	@diff -u /tmp/_row.base.canon.json /tmp/_row.curr.canon.json || true

rowcount-guard: rowcount-check
	@$(PY) -c 'import json,sys;print(json.dumps(json.load(open(".rowcounts.json")),sort_keys=True,separators=(",",":")))' > /tmp/_row.base.canon.json
	@$(PY) -c 'import json,sys;print(json.dumps(json.load(open("/tmp/_row.json")),sort_keys=True,separators=(",",":")))' > /tmp/_row.curr.canon.json
	@sh -c 'diff -u /tmp/_row.base.canon.json /tmp/_row.curr.canon.json >/dev/null && echo "rowcounts: OK" || (echo "rowcounts: DRIFT (see /tmp/_row.json)"; exit 1)'

rowcount-refresh:
	@echo "=== rowcount-refresh ==="
	@$(PY) scripts/rowcount_guard.py > /tmp/_row.json
	@$(PY) -c 'import json,sys;print(json.dumps(json.load(open("/tmp/_row.json")),sort_keys=True,separators=(",",":")))' > /tmp/_row.curr.canon.json
	@echo "Current counts recomputed -> /tmp/_row.json (and canonical at /tmp/_row.curr.canon.json)"

rowcount-accept: rowcount-refresh
	@echo "=== rowcount-accept ==="
	@cp /tmp/_row.json .rowcounts.json.NEW
	@mv .rowcounts.json.NEW .rowcounts.json
	@echo "Updated baseline: .rowcounts.json"
	@echo "Tip: git add .rowcounts.json && git commit -m \"chore: update rowcount baseline\" && git push"

# ---- Misc guards --------------------------------------------------------

guard-debug:
	@$(PY) scripts/check_debug_panel.py
