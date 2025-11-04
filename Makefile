# === Providers Makefile (no-heredoc; canonicalized rowcount guard) ===
# NOTE: Recipe lines (those starting with @) must begin with a literal TAB.

.PHONY: help rowcount-refresh rowcount-check rowcount-guard rowcount-show rowcount-accept

help:
	@echo "Targets:"
	@echo "  rowcount-refresh  - write current counts to .rowcounts.json (tolerant)"
	@echo "  rowcount-check    - print current counts to stdout (tolerant)"
	@echo "  rowcount-guard    - fail if current counts drift (canonical compare)"
	@echo "  rowcount-show     - show baseline vs fresh (canonicalized)"
	@echo "  rowcount-accept   - accept current counts as new baseline"

# Write the current counts atomically to .rowcounts.json (tolerant of non-zero rc)
rowcount-refresh:
	@python3 scripts/rowcount_guard.py --write || true
	@echo "Refreshed .rowcounts.json"

# Just run the script and print counts (tolerant of non-zero rc)
rowcount-check:
	@python3 scripts/rowcount_guard.py || true

# Fail if fresh counts differ from committed baseline (canonical compare)
rowcount-guard:
	@python3 scripts/rowcount_guard.py > /tmp/_row.json || true
	@python3 -c 'import json;print(json.dumps(json.load(open(".rowcounts.json")),sort_keys=True,separators=(",",":")))' > /tmp/_row.base.canon.json
	@python3 -c 'import json;print(json.dumps(json.load(open("/tmp/_row.json")),sort_keys=True,separators=(",",":")))' > /tmp/_row.curr.canon.json
	@diff -u /tmp/_row.base.canon.json /tmp/_row.curr.canon.json >/dev/null && \
	  echo "rowcounts: OK" || (echo "rowcounts: DRIFT (see /tmp/_row.json)"; exit 1)

# Show baseline vs fresh counts (canonicalized; no failing)
rowcount-show:
	@echo "--- baseline (.rowcounts.json) canonical ---"
	@python3 -c 'import json;print(json.dumps(json.load(open(".rowcounts.json")),sort_keys=True,indent=2))'
	@echo "--- current (/tmp/_row.json) canonical ---"
	@python3 -c 'import json;print(json.dumps(json.load(open("/tmp/_row.json")),sort_keys=True,indent=2))'

# Accept current counts as the new baseline (tolerant)
rowcount-accept:
	@python3 scripts/rowcount_guard.py --write || true
	@echo "Baseline updated: .rowcounts.json"
