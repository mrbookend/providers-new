#!/usr/bin/env bash
# preflight: sync → branch safety → code audit (read-only by default)
# Modes:
#   preflight                # full audit (read-only) on entire repo
#   preflight pr             # audit only files changed vs origin/main (read-only)
#   preflight --fix          # run ruff fix (WRITES) + format on feature branch
#
# Flags:
#   --allow-dirty            # allow running with unstaged/staged changes
#   --log-file <path>        # override default log path

set -euo pipefail

MODE="audit"          # audit | pr | fix
ALLOW_DIRTY=0
LOG_FILE=""

while (( "$#" )); do
  case "$1" in
    pr) MODE="pr"; shift ;;
    --fix) MODE="fix"; shift ;;
    --allow-dirty) ALLOW_DIRTY=1; shift ;;
    --log-file) LOG_FILE="${2:-}"; shift 2 ;;
    -h|--help)
      cat <<'HLP'
Usage: preflight [pr] [--fix] [--allow-dirty] [--log-file PATH]
HLP
      exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; exit 2 ;;
  esac
done

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || { echo 'ERROR: run inside a git repo' >&2; exit 2; })"
cd "$ROOT"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"
TS="$(date +%Y%m%d-%H%M%S)"
if [ -z "$LOG_FILE" ]; then LOG_FILE="/tmp/preflight-${BRANCH}-${TS}.log"; fi
mkdir -p "$(dirname "$LOG_FILE")"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== PREFLIGHT (${MODE}) ==="
echo "repo: $ROOT"
echo "branch: $BRANCH"
echo "log: $LOG_FILE"

# In-progress ops safety
if [ -e .git/rebase-merge ] || [ -e .git/rebase-apply ] || [ -e .git/MERGE_HEAD ] || [ -e .git/CHERRY_PICK_HEAD ] || [ -e .git/REVERT_HEAD ]; then
  echo "ABORT: Rebase/merge/revert/cherry-pick in progress."; exit 1
fi

# Network sanity
if ! git ls-remote --exit-code origin >/dev/null 2>&1; then
  echo "ABORT: Cannot reach 'origin'."; exit 2
fi
# Sync check (keep local main in lockstep with origin/main)
echo "[1/5] sync check…"
git fetch origin --prune >/dev/null 2>&1 || true

if [ "$BRANCH" = "main" ]; then
  # on main: HEAD must match origin/main
  LOCAL="$(git rev-parse HEAD 2>/dev/null || echo '')"
  REMOTE="$(git rev-parse origin/main 2>/dev/null || echo '')"
  echo "Local:  $LOCAL"
  echo "Remote: $REMOTE"
  if command -v hcrcheck >/dev/null 2>&1; then hcrcheck || true; fi
  if [ -n "$LOCAL" ] && [ -n "$REMOTE" ] && [ "$LOCAL" = "$REMOTE" ]; then
    echo "PASS: commits match (local HEAD == origin/main)"
  else
    echo "WARNING: commits differ on main. Attempting auto-sync…"
    if command -v hcrsync-auto >/dev/null 2>&1; then
      hcrsync-auto
      git fetch origin --prune >/dev/null 2>&1 || true
      LOCAL="$(git rev-parse HEAD 2>/dev/null || echo '')"
      REMOTE="$(git rev-parse origin/main 2>/dev/null || echo '')"
      echo "After auto-sync:"
      echo "Local:  $LOCAL"
      echo "Remote: $REMOTE"
      if [ -n "$LOCAL" ] && [ -n "$REMOTE" ] && [ "$LOCAL" = "$REMOTE" ]; then
        echo "PASS: commits match after auto-sync"
      else
        echo "ABORT: still not in sync on main."
        exit 1
      fi
    else
      echo "ABORT: hcrsync-auto not found; cannot auto-sync."
      exit 1
    fi
  fi
else
  # on a feature branch: ensure local main matches origin/main
  LOCAL_MAIN="$(git rev-parse refs/heads/main 2>/dev/null || echo '')"
  REMOTE="$(git rev-parse origin/main 2>/dev/null || echo '')"
  echo "Local main:  $LOCAL_MAIN"
  echo "Remote main: $REMOTE"
  if [ -z "$LOCAL_MAIN" ]; then
    echo "ABORT: local 'main' branch not found."
    exit 1
  fi
  if [ -n "$REMOTE" ] && [ "$LOCAL_MAIN" = "$REMOTE" ]; then
    echo "PASS: local main matches origin/main"
  else
    echo "WARNING: local main is behind; attempting fast-forward…"
    if command -v hcrsync-auto >/dev/null 2>&1; then
      hcrsync-auto
      git fetch origin --prune >/dev/null 2>&1 || true
      LOCAL_MAIN="$(git rev-parse refs/heads/main 2>/dev/null || echo '')"
      REMOTE="$(git rev-parse origin/main 2>/dev/null || echo '')"
      echo "After auto-sync:"
      echo "Local main:  $LOCAL_MAIN"
      echo "Remote main: $REMOTE"
      if [ -n "$REMOTE" ] && [ "$LOCAL_MAIN" = "$REMOTE" ]; then
        echo "PASS: local main matches after auto-sync"
      else
        echo "ABORT: local main still not in sync."
        exit 1
      fi
    else
      echo "ABORT: hcrsync-auto not found; cannot auto-sync."
      exit 1
    fi
  fi
fi


# Branch safety
if [ "$BRANCH" = "main" ]; then
  echo "ABORT: On 'main'. Create/switch to a feature branch."; exit 1
fi
echo "[2/5] On feature branch: $BRANCH"

# Clean worktree guard
if [ "$ALLOW_DIRTY" -ne 1 ]; then
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "ABORT: Working tree is dirty. Use --allow-dirty to override."; exit 1
  fi
else
  echo "NOTE: --allow-dirty set; proceeding despite local changes."
fi

# Merge commit detector
if git rev-parse --verify -q origin/main >/dev/null; then
  if git rev-list --merges --count origin/main..HEAD | grep -q '^[1-9]'; then
    echo "ABORT: Merge commits detected (origin/main..HEAD). Rebase/cherry-pick onto a clean branch."; exit 1
  fi
else
  echo "NOTE: origin/main not found; skipping merge-commit detector."
fi
echo "[3/5] History is linear (no merge commits)."

# Tool discovery
VENV_RUFF="$ROOT/.venv/bin/ruff"
VENV_PY="$ROOT/.venv/bin/python"
WRAP_RUFF="$(command -v ruff || true)"
[ -x "$VENV_PY" ] || VENV_PY="python3"

run_syntax_repo() {
  mapfile -t PYFILES < <(git ls-files '*.py')
  if [ "${#PYFILES[@]}" -gt 0 ]; then "$VENV_PY" -m py_compile "${PYFILES[@]}"; else echo "(no .py files)"; fi
}
run_syntax_pr() {
  mapfile -t PYFILES < <(git diff --name-only origin/main...HEAD -- '*.py')
  if [ "${#PYFILES[@]}" -eq 0 ]; then echo "No changed Python files vs origin/main."; return 0; fi
  "$VENV_PY" -m py_compile "${PYFILES[@]}"
}
audit_repo() {
  echo "== Syntax =="; run_syntax_repo
  echo "== Ruff lint (read-only) =="; "$VENV_RUFF" check --output-format=github .
  echo "== Ruff format audit (read-only) =="; "$VENV_RUFF" format --check .
}
audit_pr() {
  mapfile -t CHANGED < <(git diff --name-only origin/main...HEAD -- '*.py')
  if [ "${#CHANGED[@]}" -eq 0 ]; then echo "No changed Python files vs origin/main."; return 0; fi
  echo "== Syntax (changed files) =="; "$VENV_PY" -m py_compile "${CHANGED[@]}"
  echo "== Ruff lint (changed files) =="; "$VENV_RUFF" check --output-format=github "${CHANGED[@]}"
  echo "== Ruff format audit (changed files) =="; "$VENV_RUFF" format --check "${CHANGED[@]}"
}

# Ensure Ruff available
if [ -z "$WRAP_RUFF" ] && [ ! -x "$VENV_RUFF" ]; then
  echo "ABORT: Ruff not found in .venv and wrapper not on PATH."; exit 1
fi

echo "[4/5] Code diagnostics…"
case "$MODE" in
  audit)
    if [ -n "$WRAP_RUFF" ] && [ "$WRAP_RUFF" != "$VENV_RUFF" ]; then "$WRAP_RUFF" check --output-format=github . && "$WRAP_RUFF" format --check .; else audit_repo; fi
    ;;
  pr)
    if [ -n "$WRAP_RUFF" ] && [ "$WRAP_RUFF" != "$VENV_RUFF" ]; then "$WRAP_RUFF" pr; else audit_pr; fi
    ;;
  fix)
    echo "== Guarded fix mode =="
    if [ "$BRANCH" = "main" ]; then echo "ABORT: --fix not allowed on 'main'."; exit 1; fi
    if [ "$ALLOW_DIRTY" -ne 1 ]; then
      if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "ABORT: Dirty tree; commit/stash or pass --allow-dirty."; exit 1
      fi
    fi
    if [ -n "$WRAP_RUFF" ] && [ "$WRAP_RUFF" != "$VENV_RUFF" ]; then
      "$WRAP_RUFF" fix
    else
      "$VENV_RUFF" check --fix .
      "$VENV_RUFF" format .
    fi
    ;;
esac

echo "[5/5] DONE: preflight ${MODE} completed successfully."
echo "Log saved to: $LOG_FILE"
