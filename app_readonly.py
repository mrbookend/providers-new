"""Read-only Providers app (CI format parity)."""

from __future__ import annotations


import os
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
import streamlit as st

st.set_page_config(page_title="Providers -- Read-Only", page_icon="[book]", layout="wide")

# ---- Config ----
DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")
CSV_SEED = Path("data/providers_seed.csv")  # optional seed file path

# ---- SQLAlchemy engine (SQLite) ----
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)

# ---- Schema DDL (idempotent) ----
DDL = """
CREATE TABLE IF NOT EXISTS vendors (
  id INTEGER PRIMARY KEY,
  business_name TEXT NOT NULL,
  category TEXT NOT NULL,
  service TEXT NOT NULL,
  contact_name TEXT,
  phone TEXT,
  email TEXT,
  website TEXT,
  address TEXT,
  notes TEXT,
  created_at TEXT,
  updated_at TEXT,
  computed_keywords TEXT,
  ckw_locked INTEGER DEFAULT 0,
  ckw_version TEXT
);
CREATE INDEX IF NOT EXISTS idx_vendors_name     ON vendors(business_name COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_vendors_cat      ON vendors(category);
CREATE INDEX IF NOT EXISTS idx_vendors_service  ON vendors(service);
CREATE INDEX IF NOT EXISTS idx_vendors_ckw      ON vendors(computed_keywords);
"""

def ensure_schema() -> None:
    """Create tables/indexes if missing."""
    with ENG.begin() as cx:
        for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
            cx.execute(sa.text(stmt))


def _db_rowcount() -> int:
    with ENG.connect() as cx:
        try:
            return int(cx.execute(sa.text("SELECT COUNT(*) FROM vendors")).scalar() or 0)
        except Exception:
            return 0
# === ANCHOR: ENGINE_HELPER (start) ===
def get_engine():
    """Single point of truth for this app's SQLAlchemy engine."""
    return ENG
# === ANCHOR: ENGINE_HELPER (end) ===


# === ANCHOR: READONLY_BOOTSTRAP (start) ===
def _bootstrap_from_csv_if_needed() -> str:
    """
    Seed the vendors table from CSV in a safe, schema-driven way.
    """
    # 0) Ensure schema exists; otherwise count() will fail and skip seeding
    ensure_schema()

    eng = get_engine()

    # 1) Skip if already populated
    try:
        with eng.connect() as cx:
            cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar_one()
        if (cnt or 0) > 0:
            return f"OK: vendors already populated ({cnt} rows)"
    except Exception as e:
        # If we cannot count even after ensure_schema, bail with error
        return f"SKIP: vendors count check failed: {type(e).__name__}: {e}"

    # 2) Locate seed CSV (env overrides → defaults)
    candidates = [os.environ.get(k) for k in ("SEED_CSV", "PROVIDERS_SEED_CSV", "VENDORS_SEED_CSV")]
    candidates += ["data/providers_seed.csv", "data/vendors_seed.csv"]
    candidates = [p for p in candidates if p]
    seed_path = next((p for p in candidates if Path(p).exists()), None)
    if not seed_path:
        return "SKIP: no seed CSV found"

    # 3) Load CSV
    try:
        df = pd.read_csv(seed_path)
    except Exception as e:
        return f"ERROR: failed to read CSV {seed_path}: {type(e).__name__}: {e}"

    # 4) Normalize headers; drop legacy cols (kept for backward compatibility)
    df.columns = [str(c).strip() for c in df.columns]
    for legacy in ("city", "state", "zip"):
        if legacy in df.columns:
            df = df.drop(columns=[legacy])

    # 5) Probe live schema → choose insertable (non-PK) columns
    try:
        with eng.connect() as cx:
            info = cx.exec_driver_sql("PRAGMA table_info(vendors)").fetchall()
        live_cols = [r[1] for r in info]               # column name
        pk_cols = {r[1] for r in info if (r[5] or 0)}  # pk flag
        insertable = [c for c in live_cols if c not in pk_cols]
        if not insertable:
            return "ERROR: vendors has no insertable columns"
    except Exception as e:
        return f"ERROR: schema probe failed: {type(e).__name__}: {e}"

    # 6) Ensure required live columns exist in df; fill missing with ""
    for c in insertable:
        if c not in df.columns:
            df[c] = ""

    # 7) Optional phone formatting if schema expects phone_fmt
    if "phone_fmt" in insertable and "phone_fmt" not in df.columns and "phone" in df.columns:
        def _fmt_local(raw):
            s = "".join(ch for ch in str(raw or "") if ch.isdigit())
            if len(s) == 11 and s.startswith("1"):
                s = s[1:]
            return f"({s[0:3]}) {s[3:6]}-{s[6:10]}" if len(s) == 10 else (str(raw or "").strip())
        df["phone_fmt"] = df["phone"].map(_fmt_local)

    # 8) Strict projection → only live insertable columns
    df = df[[c for c in insertable if c in df.columns]]

    if df.empty:
        return "SKIP: seed CSV produced zero insertable rows"

    # 9) Insert
    try:
        with eng.begin():
            df.to_sql("vendors", eng, if_exists="append", index=False, method="multi")
        return f"BOOTSTRAP: inserted {len(df)} rows from {Path(seed_path).name}"
    except Exception as e:
        return f"BOOTSTRAP ERROR: {type(e).__name__}: {e}"
# === ANCHOR: READONLY_BOOTSTRAP (end) ===




@st.cache_data(show_spinner=False)
def load_df(q: str) -> pd.DataFrame:
    """Load rows, optional SQL-side LIKE filter on several columns."""
    ensure_schema()
    with ENG.connect() as cx:
        if q:
            return pd.read_sql_query(
                sa.text("""
                  SELECT id,business_name,category,service,contact_name,phone,email,website,
                         address,notes
                  FROM vendors
                  WHERE (business_name LIKE :x OR category LIKE :x OR service LIKE :x
                         OR COALESCE(computed_keywords,'') LIKE :x
                         OR COALESCE(address,'') LIKE :x)
                  ORDER BY business_name COLLATE NOCASE ASC
                """),
                cx,
                params={"x": f"%{q}%"},
            )
        return pd.read_sql_query(
            sa.text("""
              SELECT id,business_name,category,service,contact_name,phone,email,website,
                     address,notes
              FROM vendors
              ORDER BY business_name COLLATE NOCASE ASC
            """),
            cx,
        )


# === ANCHOR: READONLY_BOOTSTRAP_CALL (start) ===
try:
    msg = _bootstrap_from_csv_if_needed()
except Exception as _e:
    msg = f"BOOTSTRAP ERROR: {type(_e).__name__}"

# Toast is optional; guard it so it never crashes app init
try:
    if msg:
        st.toast(msg, icon="[OK]")
except Exception:
    pass
# === ANCHOR: READONLY_BOOTSTRAP_CALL (end) ===


# ---- UI ----
st.title("Providers (Read-Only)")

left, right = st.columns([3, 1])
with left:
    q = st.text_input(
        "Search", value="", placeholder="name, category, service, city, keyword..."
    ).strip()
with right:
    if st.button("Clear"):
        q = ""

df = load_df(q)

if df.empty:
    # Offer a precise hint if the table is empty AND no seed CSV is present
    seed_candidates = ["data/providers_seed.csv", "data/vendors_seed.csv"]
    if _db_rowcount() == 0 and not any(Path(p).exists() for p in seed_candidates):
        st.warning("No data found. Add data/providers_seed.csv (or data/vendors_seed.csv) or set PROVIDERS_DB.")
    else:
        st.info("No matching providers.")
else:
    st.caption(f"Results: {len(df)}")
    st.dataframe(df, use_container_width=True)
    st.download_button(
        "Download CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name="providers.csv",
        mime="text/csv",
    )


# ---- Tiny footer (optional) ----
with st.expander("About this app", expanded=False):
    st.write(
        "This read-only viewer uses a local SQLite database (providers.db). "
        "If the database is empty and data/providers_seed.csv is present, "
        "it imports that CSV once at startup."
    )
