#!/usr/bin/env python3
"""
Rowcount guard:
- Prints JSON counts for vendors/categories/services to stdout
- --write : atomically writes .rowcounts.json
Respects SQLITE_PATH (defaults to providers.db).
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from contextlib import suppress

DB_PATH = os.environ.get("SQLITE_PATH", "providers.db")
BASELINE = ".rowcounts.json"


def _load_baseline(path: str = BASELINE) -> dict[str, int]:
    """Load baseline JSON if present; tolerate empty/invalid."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = f.read().strip()
        return json.loads(data) if data else {}
    except json.JSONDecodeError:
        return {}


def _counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Return counts for expected tables; 0 if table missing/error."""
    cur = conn.cursor()
    out: dict[str, int] = {}
    for table in ("vendors", "categories", "services"):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            (n,) = cur.fetchone()
            out[table] = int(n)
        except sqlite3.Error:
            out[table] = 0
    return out


def _generate_text() -> str:
    with sqlite3.connect(DB_PATH) as conn:
        data = _counts(conn)
    # stable order for diffs
    return json.dumps(data, sort_keys=True, indent=2)


def write_baseline() -> int:
    """Atomic write of BASELINE using a temp file + replace."""
    text = _generate_text()
    fd, tmp_path = tempfile.mkstemp(dir=".", prefix=".rowcounts.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            fp.write(text)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, BASELINE)
        print("Wrote .rowcounts.json")
        return 0
    finally:
        with suppress(FileNotFoundError):
            os.unlink(tmp_path)


def main(argv: list[str]) -> int:
    if "--write" in argv:
        return write_baseline()
    # default: print current counts to stdout
    print(_generate_text())
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
