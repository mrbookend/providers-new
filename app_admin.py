from __future__ import annotations

import pandas as pd

# === ANCHOR: SEARCH (start) ===
MIN_SEARCH_LEN = 2


def _fts_match(term: str) -> tuple[str, dict]:
    q = (term or "").strip()
    q = " ".join(t for t in q.split() if t)
    return (
        """
        SELECT v.*
        FROM vendors_fts
        JOIN vendors AS v ON v.id = vendors_fts.rowid
        WHERE vendors_fts MATCH :q
        """,
        {"q": q},
    )


def _run_search(term: str, eng):
    if term and len(term) >= MIN_SEARCH_LEN:
        sql, params = _fts_match(term)
    else:
        sql, params = ("SELECT * FROM vendors", {})
    return pd.read_sql(sql, eng, params=params)


# === ANCHOR: SEARCH (end) ===
