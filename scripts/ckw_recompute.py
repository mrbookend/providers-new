#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from collections import defaultdict
from collections.abc import Iterable

CURRENT_CKW_VER = "ckw-1"

# Tunables / constants (avoid magic numbers)
MIN_TOKEN_LEN = 3
PHONE_MIN_LEN = 7
LAST4_LEN = 4
WORDS_BIGRAM_MIN = 2

_WS_SPLIT = re.compile(r"[^0-9a-zA-Z]+", re.ASCII)
_ASCII_FOLD = str.maketrans(
    "àáâãäåçèéêëìíîïñòóôõöùúûüýÿÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝ",
    "aaaaaaceeeeiiiinooooouuuuyyAAAAAACEEEEIIIINOOOOOUUUUY",
)

STOP = {
    "a",
    "an",
    "and",
    "the",
    "for",
    "of",
    "to",
    "in",
    "on",
    "at",
    "by",
    "llc",
    "inc",
    "co",
    "company",
}


def ascii_fold(s: str) -> str:
    try:
        return s.translate(_ASCII_FOLD)
    except Exception:
        return s


def norm(s: str) -> str:
    if not s:
        return ""
    s = ascii_fold(s).strip().lower()
    return re.sub(r"\s+", " ", s)


def split_tokens(s: str) -> list[str]:
    return [t for t in _WS_SPLIT.split(s or "") if t]


def stable_unique(seq: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def collapse_hyphen_space(tok: str) -> list[str]:
    if "-" in tok or " " in tok:
        return [tok, tok.replace("-", "").replace(" ", "")]
    return [tok]


def is_meaningful_number(tok: str) -> bool:
    return tok.isdigit() and len(tok) >= PHONE_MIN_LEN


def business_name_tokens(name: str) -> list[str]:
    toks: list[str] = []
    for t in split_tokens(norm(name)):
        if t in STOP or len(t) < MIN_TOKEN_LEN:
            continue
        toks.extend(collapse_hyphen_space(t))
    words = [t for t in split_tokens(norm(name)) if t not in STOP and len(t) >= MIN_TOKEN_LEN]
    if len(words) >= WORDS_BIGRAM_MIN:
        toks.append(f"{words[0]} {words[1]}")
    return toks


def website_tokens(url: str) -> list[str]:
    if not url:
        return []
    u = norm(url)
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    host = u.split("/", 1)[0]
    if not host:
        return []
    toks: list[str] = []
    toks.append(host.split(".", 1)[0])  # left-most label
    toks.append(host.replace(".", ""))  # collapsed
    toks.append(host)  # full host
    return stable_unique([t for t in toks if t])


def email_tokens(email: str) -> list[str]:
    if not email or "@" not in email:
        return []
    e = norm(email)
    local, domain = e.split("@", 1)
    toks: list[str] = [local, *collapse_hyphen_space(local)]
    dom_core = domain.split(".", 1)[0]
    toks += [dom_core, domain.replace(".", ""), domain]
    return stable_unique([t for t in toks if t not in STOP and len(t) >= MIN_TOKEN_LEN])


def phone_tokens(phone: str) -> list[str]:
    if not phone:
        return []
    digits = "".join(ch for ch in phone if ch.isdigit())
    toks: list[str] = []
    if len(digits) >= PHONE_MIN_LEN:
        toks.append(digits)
    if len(digits) >= LAST4_LEN:
        toks.append(digits[-LAST4_LEN:])
    return toks


def load_seeds(cur: sqlite3.Cursor) -> tuple[dict, dict]:
    # Ensure table/index exist (idempotent)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ckw_seeds (
            id   INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            term TEXT NOT NULL,
            syn  TEXT NOT NULL
        )
        """
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_ckw_seeds_unique ON ckw_seeds(kind,term,syn)"
    )

    cur.execute("SELECT kind, term, syn FROM ckw_seeds")
    fwd: dict[tuple[str, str], set[str]] = defaultdict(set)
    rev: dict[tuple[str, str], set[str]] = defaultdict(set)
    for kind, term, syn in cur.fetchall():
        k = norm(kind)
        t = norm(term)
        s = norm(syn)
        if not (k and t and s):
            continue
        fwd[(k, t)].add(s)
        rev[(k, s)].add(t)
    return fwd, rev


def expand_with_syns(kind: str, value: str, fwd: dict, rev: dict) -> list[str]:
    k = norm(kind)
    v = norm(value)
    if not v:
        return []
    out: list[str] = [v]
    out.extend(list(fwd.get((k, v), [])))
    out.extend(list(rev.get((k, v), [])))
    return stable_unique(out)


def compute_ckw_row(row: sqlite3.Row, seeds_fwd: dict, seeds_rev: dict) -> list[str]:
    toks: list[str] = []

    # Category / service (with synonyms)
    toks += expand_with_syns("category", row["category"] or "", seeds_fwd, seeds_rev)
    toks += expand_with_syns("service", row["service"] or "", seeds_fwd, seeds_rev)

    # Business name, website/email/phone
    toks += business_name_tokens(row["business_name"] or "")
    toks += website_tokens(row["website"] or "")
    toks += email_tokens(row["email"] or "")
    toks += phone_tokens(row["phone"] or "")

    # Freeform keywords + contact
    toks += [
        t
        for t in split_tokens(norm(row["keywords"] or ""))
        if t not in STOP and len(t) >= MIN_TOKEN_LEN
    ]
    toks += [
        t
        for t in split_tokens(norm(row["contact_name"] or ""))
        if t not in STOP and len(t) >= MIN_TOKEN_LEN
    ]

    manual = [t for t in split_tokens(norm(row["ckw_manual_extra"] or "")) if t]

    filtered: list[str] = []
    for t in toks:
        if t in STOP:
            continue
        if t.isdigit():
            if len(t) >= PHONE_MIN_LEN or len(t) == LAST4_LEN or is_meaningful_number(t):
                filtered.append(t)
            else:
                continue
        elif len(t) >= MIN_TOKEN_LEN:
            filtered.extend(collapse_hyphen_space(t))

    return stable_unique(filtered + manual)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", dest="dry_run", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--where", type=str, default="")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = os.environ.get("SQLITE_PATH", "providers.db")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Fail fast if the vendors table is missing
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='vendors'")
    row = cur.fetchone()
    if row is None:
        print(
            "ERROR: SQLite file has no 'vendors' table.\n"
            f"DB path: {os.environ.get('SQLITE_PATH', 'providers.db')}\n"
            "Fix: set SQLITE_PATH to a DB with the vendors schema (e.g., data/providers.prod.db)."
        )
        conn.close()
        return 2

    seeds_fwd, seeds_rev = load_seeds(cur)

    base = "SELECT * FROM vendors WHERE coalesce(ckw_locked,0)=0"
    if args.where.strip():
        base += f" AND ({args.where})"
    if args.limit and args.limit > 0:
        base += f" LIMIT {int(args.limit)}"

    cur.execute(base)
    rows = cur.fetchall()
    total = len(rows)
    if total == 0:
        print("No unlocked rows matched the filter.")
        conn.close()
        return 0

    updated = 0
    skipped_same = 0
    print(f"Scanning {total} unlocked row(s)...")

    for i, row in enumerate(rows, start=1):
        new_tokens = compute_ckw_row(row, seeds_fwd, seeds_rev)
        new_ckw = " ".join(new_tokens)
        old_ckw = (row["computed_keywords"] or "").strip()
        if new_ckw == old_ckw and (row["ckw_version"] or "") == CURRENT_CKW_VER:
            skipped_same += 1
            if i % 200 == 0:
                print(f"[{i}/{total}] unchanged so far; skipped_same={skipped_same}")
            continue

        if args.dry_run:
            print(f"[dry-run] id={row['id']} :: '{old_ckw}' -> '{new_ckw}'")
            continue

        cur.execute(
            "UPDATE vendors SET computed_keywords=?, ckw_version=? WHERE id=?",
            (new_ckw, CURRENT_CKW_VER, row["id"]),
        )
        updated += 1

        if updated % 200 == 0:
            conn.commit()
            print(f"[{i}/{total}] committed batch; updated={updated}, skipped_same={skipped_same}")

    if not args.dry_run:
        conn.commit()

    print(
        f"Done. scanned={total} updated={updated} skipped_same={skipped_same} dry_run={args.dry_run}"
    )
    conn.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        # 130 = SIGINT
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except SystemExit:
        # Let explicit exits pass through unchanged.
        raise
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)
