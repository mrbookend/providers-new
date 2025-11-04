SHELL := /bin/bash
.PHONY: default help status bc zzz guard-debug
	@echo "status     - git sync status (bbb)"
	@echo "bc         - fast loop: sync + code checks (bbb+ccc)"
	@echo "zzz        - full read-only health chain"
	@echo "guard-debug- run debug-panel guard script"
# Default: quick sync snapshot
default: status

help:
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
