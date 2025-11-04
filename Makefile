SHELL := /bin/bash
.PHONY: default help status bc zzz guard-debug
	@echo "status     - git sync status (bbb)"
	@echo "bc         - fast loop: sync + code checks (bbb+ccc)"
	@echo "zzz        - full read-only health chain"
	@echo "guard-debug- run debug-panel guard script"
# Default: quick sync snapshot
default: status

help::
	@echo "status     - git sync status (bbb)"
	@echo "bc         - fast loop: sync + code checks (bbb+ccc)"
	@echo "zzz        - full read-only health chain"
	@echo "guard-debug- run debug-panel guard script"
status:
	~/bin/bbb

bc:
	~/bin/bc

zzz:
	~/bin/zzz

guard-debug:
	python3 scripts/check_debug_panel.py
.PHONY: test-ckw
test-ckw:
	@echo "Dry-run on prod DB"
	@python3 scripts/ckw_recompute.py --dry-run
	@echo "Smoke on TEST DB (50 rows)"
	@SQLITE_PATH=providers.TEST.db python3 scripts/ckw_recompute.py --limit 50
.PHONY: sqlite-integrity
sqlite-integrity:
	@echo "=== sqlite integrity_check ==="
	@sqlite3 $${SQLITE_PATH:-providers.db} "PRAGMA integrity_check" | grep -qx "ok"

.PHONY: zzz
# If you redefine, ensure sqlite-integrity is included
# zzz: bc guard-debug sqlite-sanity sqlite-integrity test-ckw
.PHONY: db-backup
db-backup:
	@cp -p $${SQLITE_PATH:-providers.db} backups/providers.$(date +%Y%m%d-%H%M%S).db
	@echo "Backup -> backups/"
help::
	@echo "test-ckw         - dry-run prod + 50-row smoke on TEST DB"
	@echo "sqlite-integrity - PRAGMA integrity_check must be ok"
	@echo "db-backup        - copy providers.db to backups/providers.YYYYMMDD-HHMMSS.db"
