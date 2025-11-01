"""Read-only Providers app (minimal, failsafe)."""

from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
import streamlit as st

# Must be FIRST Streamlit call
st.set_page_config(page_title="Providers — Read-Only", page_icon="[book]", layout="wide")

# === ANCHOR: CONSTANTS (start) ===
DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
# === ANCHOR: CONSTANTS (end) ===


# === ANCHOR: SCHEMA (start) ===
# Minimal schema; uses SELECT * later so legacy cols (city/state/zip) won't break.
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
"""


def ensure_schema() -> None:
    with ENG.begin() as cx:
        for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
            cx.execute(sa.text(stmt))


# === ANCHOR: SCHEMA (end) ===


# === ANCHOR: BOOTSTRAP (start) ===
def _bootstrap_from_csv_if_needed() -> str:
    """If DB empty and seed CSV exists, import once."""
    ensure_schema()
    # Already has rows?
    try:
        with ENG.connect() as cx:
            cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar_one()
        if (cnt or 0) > 0:
            return ""
    except Exception:
        # If table missing or other issue, schema creation above will handle next call
        pass

    # Find a CSV
    candidates = [
        os.environ.get("SEED_CSV"),
        os.environ.get("PROVIDERS_SEED_CSV"),
        os.environ.get("VENDORS_SEED_CSV"),
        "data/providers_seed.csv",
        "data/vendors_seed.csv",
    ]
    seed_path = next((p for p in candidates if p and Path(p).exists()), None)
    if not seed_path:
        return ""

    # Read and normalize
    try:
        df = pd.read_csv(seed_path)
    except Exception as e:
        return f"BOOTSTRAP: failed to read {seed_path}: {type(e).__name__}: {e}"

    # Drop obviously legacy columns if they exist
    for legacy in ("city", "state", "zip"):
        if legacy in df.columns:
            df = df.drop(columns=[legacy])

    # Project to insertable columns (all except PK)
    try:
        with ENG.connect() as cx:
            info = cx.exec_driver_sql("PRAGMA table_info(vendors)").fetchall()
        live_cols = [r[1] for r in info]
        pk_cols = {r[1] for r in info if (r[5] or 0)}
        insertable = [c for c in live_cols if c not in pk_cols]
        for c in insertable:
            if c not in df.columns:
                df[c] = ""
        df = df[[c for c in insertable if c in df.columns]]
        if df.empty:
            return ""
        with ENG.begin():
            df.to_sql("vendors", ENG, if_exists="append", index=False, method="multi")
        return f"BOOTSTRAP: inserted {len(df)} rows from {Path(seed_path).name}"
    except Exception as e:
        return f"BOOTSTRAP ERROR: {type(e).__name__}: {e}"


# === ANCHOR: BOOTSTRAP (end) ===


# === ANCHOR: LOAD_DF (start) ===
@st.cache_data(show_spinner=False)
def load_df(q: str) -> pd.DataFrame:
    """Return providers (optionally SQL-side LIKE on several columns)."""
    ensure_schema()
    with ENG.connect() as cx:
        s = (q or "").strip()
        if s:
            like_sql = (
                "SELECT * FROM vendors "
                "WHERE business_name LIKE :x OR category LIKE :x OR service LIKE :x OR "
                "contact_name LIKE :x OR email LIKE :x OR website LIKE :x OR address LIKE :x OR notes LIKE :x "
                "ORDER BY business_name COLLATE NOCASE ASC"
            )
            return pd.read_sql_query(sa.text(like_sql), cx, params={"x": f"%{s}%"})
        base_sql = "SELECT * FROM vendors ORDER BY business_name COLLATE NOCASE ASC"
        return pd.read_sql_query(sa.text(base_sql), cx)


# === ANCHOR: LOAD_DF (end) ===


# === ANCHOR: SEARCH UI (start) ===
def __on_search_enter__():
    term = (st.session_state.get("__search_box__", "") or "").strip()
    st.session_state["__search_term__"] = term
    st.session_state["__search_box__"] = ""  # auto-clear


st.text_input(
    label="Search",
    key="__search_box__",
    placeholder="Search by name, category, service, etc.",
    label_visibility="collapsed",
    on_change=__on_search_enter__,
)
q = (st.session_state.pop("__search_term__", "") or "").strip()
# === ANCHOR: SEARCH UI (end) ===


# === ANCHOR: BOOTSTRAP TOAST (start) ===
_msg = _bootstrap_from_csv_if_needed()
try:
    show_boot = int(st.secrets.get("SHOW_BOOTSTRAP_POPUPS", 0))
except Exception:
    show_boot = 0
if show_boot:
    pass  # silenced
    #     st.toast(_msg)  # silenced
# === ANCHOR: BOOTSTRAP TOAST (end) ===


# === ANCHOR: BROWSE (start) ===
df = load_df(q)

# Hide Id column if present

# Secrets-driven preferences
browse_order = list(st.secrets.get("BROWSE_ORDER", []))
hide_cols = set(st.secrets.get("HIDE_COLUMNS", []))

# Drop hidden columns that exist
drop_now = [c for c in df.columns if c in hide_cols]
if drop_now:
    df = df.drop(columns=drop_now)

# Build final order: preferred first (that exist), then remaining
pref = [c for c in browse_order if c in df.columns]
rest = [c for c in df.columns if c not in pref]
view_cols = pref + rest
df = df.loc[:, view_cols]

if "id" in df.columns:
    df = df.drop(columns=["id"])
# === HIDE_COLUMNS DROP (auto) ===
_hide_default = [
    "city",
    "state",
    "zip",
    "phone_fmt",
    "computed_keywords",
    "ckw_locked",
    "ckw_version",
    "id",
]
hide_cols = set(st.secrets.get("HIDE_COLUMNS", _hide_default))
drop_now = [c for c in df.columns if c in hide_cols]
if drop_now:
    df = df.drop(columns=drop_now)
# === HIDE_COLUMNS DROP (auto end) ===
st.dataframe(df, use_container_width=False, hide_index=True)
# === ANCHOR: BROWSE (end) ===


# === ANCHOR: HELP (start) ===
with st.expander("Help Section", expanded=False):
    st.write(
        "Read-only viewer for the Providers list. "
        "Database path is set by PROVIDERS_DB (default providers.db). "
        "If empty and a seed CSV is available, the app imports it once at startup."
    )
# === ANCHOR: HELP (end) ===


# === ANCHOR: DOWNLOADS (start) ===
# Buttons on one row: CSV (left) and XLSX (right)
try:
    c1, c2, _sp = st.columns([1, 1, 6])
    with c1:
        _csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="CSV",
            data=_csv_bytes,
            file_name="providers.csv",
            mime="text/csv",
            use_container_width=False,
        )
    with c2:
        _xbuf = BytesIO()
        with pd.ExcelWriter(_xbuf, engine="xlsxwriter") as _writer:
            df.to_excel(_writer, index=False, sheet_name="Providers")
        _xbuf.seek(0)
        st.download_button(
            label="XLSX",
            data=_xbuf,
            file_name="providers.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )
except Exception:
    pass
# Hide columns driven by secrets (fallback keeps legacy/meta cols hidden)
_hide_default = [
    "city",
    "state",
    "zip",
    "phone_fmt",
    "computed_keywords",
    "ckw_locked",
    "ckw_version",
]
hide_cols = set(st.secrets.get("HIDE_COLUMNS", _hide_default))
_drop = [c for c in df.columns if c in hide_cols]
if _drop:
    df = df.drop(columns=_drop)

# === ANCHOR: DOWNLOADS (end) ===
