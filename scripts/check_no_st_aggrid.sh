#!/usr/bin/env bash
set -euo pipefail
if grep -Rni --exclude-dir=".git" -E "\bst[-_]aggrid\b" .; then
  echo "Found forbidden st-aggrid; use streamlit-aggrid instead." >&2; exit 1;
fi
