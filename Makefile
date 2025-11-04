# === Providers Read-Only health chain Makefile ===

# Phony targets
.PHONY: help zzz test-ckw sqlite-integrity db-backup db-restore-test schema-check

# Help (double-colon so we can append later if needed)
help::
	@echo "status          - git sync status (bbb)"
	@echo "bc              - fast loop: sync + code checks (bbb+ccc)"
	@echo "zzz             - full read-only health chain"
	@echo "guard-debug     - run debug-panel guard script"
	@echo "test-ckw        - dry-run prod + 50-row smoke on TEST DB"
	@echo "sqlite-integrity- PRAGMA integrity_check must be ok"
	@echo "db-backup       - copy providers.db to backups/providers.YYYYMMDD-HHMMSS.db"
	@echo "db-restore-test - overwrite providers.TEST.db from providers.db"
	@echo "schema-check    - compare schema to baseline (set SCHEMA_GUARD=1 to enforce)"
	@echo

# Full chain wrapper: run your external script, then (env-gated) schema check
zzz:
	@~/bin/zzz
	@$(MAKE) schema-check

# CKW smoke tests
test-ckw:
	@echo "Dry-run on prod DB"
	@python3 scripts/ckw_recompute.py --dry-run
	@echo "Smoke on TEST DB (50 rows)"
	@SQLITE_PATH=providers.TEST.db python3 scripts/ckw_recompute.py --limit 50

# Strong SQLite integrity check (separate from quick_check inside zzz)
sqlite-integrity:
	@echo "=== sqlite integrity_check ==="
	@sqlite3 $${SQLITE_PATH:-providers.db} "PRAGMA integrity_check" | grep -qx "ok"

# Backup/restore helpers
db-backup:
	@mkdir -p backups
	@cp -p $${SQLITE_PATH:-providers.db} backups/providers.$(date +%Y%m%d-%H%M%S).db
	@echo "Backup -> backups/"

db-restore-test:
	@cp -p $${SQLITE_PATH:-providers.db} providers.TEST.db
	@echo "TEST DB restored from prod copy"

# Schema checksum guard (env-gated)
schema-check:
	@[ -n "$$SCHEMA_GUARD" ] || { echo "(schema-check skipped — set SCHEMA_GUARD=1 to enforce)"; exit 0; }
	@echo "=== schema checksum ==="
	@[ "$$(python3 scripts/schema_checksum.py)" = "$$(cat .schema.sha256)" ] && echo "schema: OK" || (echo "schema: DRIFT"; exit 1)
# Convenience targets referenced in help
.PHONY: self-check status bc guard-debug
self-check:
	@./scripts/self_check.sh

status:
	@verify_admin || true
	@hcrsync || true

# "bc" = fast loop: sync + code checks
bc:
	@git fetch --prune
	@python3 -m py_compile app_readonly.py app_admin.py || exit 1
	@ruff check --fix
	@ruff format --check

guard-debug:
	@python3 scripts/check_debug_panel.py
