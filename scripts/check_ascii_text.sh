#!/usr/bin/env bash
set -euo pipefail
# Fail if any non-ASCII byte appears in targeted text files.
# Usage: check staged files (pre-commit passes paths); if none, scan git-tracked.

shopt -s nullglob
mapfile -t FILES < <( ( [ $# -gt 0 ] && printf '%s\n' "$@" || git ls-files ) \
  | grep -E '\.(md|markdown|ya?ml|toml|sh|bash|zsh|txt|ini|cfg|editorconfig|gitignore|gitattributes|json|csv)$' || true )

[ ${#FILES[@]} -eq 0 ] && exit 0

bad=()
for f in "${FILES[@]}"; do
  # GNU grep required on CI runner; C locale ensures byte-wise match
  if LC_ALL=C grep -nP '[^\x00-\x7F]' -- "${f}" >/tmp/ascii_hits.$$ 2>/dev/null; then
    while IFS= read -r line; do bad+=("${f}:${line}"); done </tmp/ascii_hits.$$
  fi
done
rm -f /tmp/ascii_hits.$$ 2>/dev/null || true

if [ ${#bad[@]} -gt 0 ]; then
  printf 'ERROR: non-ASCII detected in text files:\n'
  printf '  - %s\n' "${bad[@]}"
  exit 1
fi
