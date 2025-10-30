#!/usr/bin/env python3
# ASCII-only guard for Python comments & strings.
# Fails on any non-ASCII in COMMENT/STRING tokens (ord > ASCII_MAX).

from __future__ import annotations

import pathlib
import subprocess
import sys
import tokenize
import unicodedata

ASCII_MAX = 127  # avoid magic number


def _scan_file(path: pathlib.Path) -> list[str]:
    msgs: list[str] = []
    try:
        with path.open("rb") as fh:
            for tok in tokenize.tokenize(fh.readline):
                if tok.type not in (tokenize.COMMENT, tokenize.STRING):
                    continue
                s = tok.string
                for idx, ch in enumerate(s, 1):
                    if ord(ch) > ASCII_MAX:
                        name = unicodedata.name(ch, "UNKNOWN")
                        msgs.append(
                            f"{path}:{tok.start[0]}:{tok.start[1] + idx - 1}: "
                            f"NON-ASCII {name} (U+{ord(ch):04X}) in {tokenize.tok_name[tok.type]}"
                        )
    except Exception as e:  # pragma: no cover
        msgs.append(f"{path}:1:1: ERROR reading file: {e}")
    return msgs


def _targets_from_args(args: list[str]) -> list[pathlib.Path]:
    if args:
        return [pathlib.Path(a) for a in args if a.endswith(".py")]
    try:
        out = subprocess.check_output(["git", "ls-files", "*.py"], text=True).strip()
        return [pathlib.Path(p) for p in out.splitlines() if p]
    except Exception:
        return [p for p in pathlib.Path(".").rglob("*.py")]


def main() -> int:
    paths = _targets_from_args(sys.argv[1:])
    all_msgs: list[str] = []
    for p in paths:
        all_msgs.extend(_scan_file(p))
    if all_msgs:
        print("\n".join(all_msgs))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
