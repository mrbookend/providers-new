#!/usr/bin/env bash
set -euo pipefail

# Only forbid the *package name* "st-aggrid" in requirements files.
if grep -Rni --binary-files=without-match --include='requirements*.txt' -E '^[[:space:]]*st-aggrid(\b|[<>=])' .; then
  echo "Found forbidden 'st-aggrid' in requirements; use 'streamlit-aggrid' instead." >&2; exit 1;
fi
