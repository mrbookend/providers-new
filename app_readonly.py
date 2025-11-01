"""Read-only Providers app (CI format parity)."""

from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
import streamlit as st

st.set_page_config(page_title="Providers -- Read-Only", page_icon="[book]", layout="wide")


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

# Active query for filtering below
search_query = (st.session_state.pop("__search_term__", "") or "").strip()
q = search_query  # legacy alias if downstream uses `q`
# === ANCHOR: SEARCH UI (end) ===
# === ANCHOR: CONSTANTS (start) ===

PHONE_LEN = 10

PHONE_LEN_WITH_CC = 11

# === ANCHOR: CONSTANTS (end) ===

# ---- Config ----
DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")

# ---- SQLAlchemy engine (SQLite) ----
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
# === ANCHOR: ENGINE_HELPER (start) ===


def get_engine():
    return ENG


# === ANCHOR: ENGINE_HELPER (end) ===

# ---- Schema DDL (idempotent) ----
# === ANCHOR: SCHEMA (start) ===
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
  city TEXT,
  state TEXT DEFAULT 'TX',
  zip TEXT,
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

REQUIRED_HEADERS = [
    "id",
    "business_name",
    "category",
    "service",
    "contact_name",
    "phone",
    "email",
    "website",
    "address",
    "city",
    "state",
    "zip",
    "notes",
    "created_at",
    "updated_at",
    "computed_keywords",
    "ckw_locked",
    "ckw_version",
]


def ensure_schema() -> None:
    """Create tables/indexes if missing."""
    with ENG.begin() as cx:
        for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
            cx.execute(sa.text(stmt))


# === ANCHOR: SCHEMA (end) ===


def _db_rowcount() -> int:
    with ENG.connect() as cx:
        try:
            return int(cx.execute(sa.text("SELECT COUNT(*) FROM vendors")).scalar() or 0)
        except Exception:
            return 0


# === ANCHOR: READONLY_BOOTSTRAP (start) ===
def _bootstrap_from_csv_if_needed() -> str:
    """
    Seed the vendors table from CSV in a safe, schema-driven way.
    """
    ensure_schema()
    eng = get_engine()
    msg = ""

    # 1) Count existing rows (fast-path)
    try:
        with eng.connect() as cx:
            cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar_one()
        if (cnt or 0) > 0:
            return f"OK: vendors already populated ({cnt} rows)"
    except Exception as e:
        msg = f"SKIP: vendors count check failed: {type(e).__name__}: {e}"

    # 2) Locate seed CSV (env overrides → defaults)
    if not msg:
        candidates = [
            os.environ.get(k) for k in ("SEED_CSV", "PROVIDERS_SEED_CSV", "VENDORS_SEED_CSV")
        ]
        candidates += ["data/providers_seed.csv", "data/vendors_seed.csv"]
        candidates = [p for p in candidates if p]

        seed_path = next((p for p in candidates if Path(p).exists()), None)
        if not seed_path:
            msg = "SKIP: no seed CSV found"

    # 3) Load CSV

    df = None
    if not msg:
        try:
            df = pd.read_csv(seed_path)
        except Exception as e:
            msg = f"ERROR: failed to read CSV {seed_path}: {type(e).__name__}: {e}"

    # 4) Normalize headers; drop legacy cols
    if not msg and df is not None:
        df.columns = [str(c).strip() for c in df.columns]
        for legacy in ("city", "state", "zip"):
            if legacy in df.columns:
                df = df.drop(columns=[legacy])

    # 5) Probe live schema → choose insertable (non-PK) columns
    insertable = []
    if not msg:
        try:
            with eng.connect() as cx:
                info = cx.exec_driver_sql("PRAGMA table_info(vendors)").fetchall()
            live_cols = [r[1] for r in info]
            pk_cols = {r[1] for r in info if (r[5] or 0)}
            insertable = [c for c in live_cols if c not in pk_cols]
            if not insertable:
                msg = "ERROR: vendors has no insertable columns"
        except Exception as e:
            msg = f"ERROR: schema probe failed: {type(e).__name__}: {e}"

    # 6) Fill missing columns; optional phone formatting
    if not msg and df is not None:
        for c in insertable:
            if c not in df.columns:
                df[c] = ""

        if "phone_fmt" in insertable and "phone_fmt" not in df.columns and "phone" in df.columns:

            def _fmt_local(raw):
                s = "".join(ch for ch in str(raw or "") if ch.isdigit())
                if len(s) == PHONE_LEN_WITH_CC and s.startswith("1"):
                    s = s[1:]
                return (
                    f"({s[0:3]}) {s[3:6]}-{s[6:10]}"
                    if len(s) == PHONE_LEN
                    else (str(raw or "").strip())
                )

            df["phone_fmt"] = df["phone"].map(_fmt_local)

        # Strict projection
        df = df[[c for c in insertable if c in df.columns]]
        if df.empty:
            msg = "SKIP: seed CSV produced zero insertable rows"

    # 7) Insert via pandas, not param SQL
    if not msg and df is not None and not df.empty:
        try:
            with eng.begin():
                df.to_sql("vendors", eng, if_exists="append", index=False, method="multi")

            msg = f"BOOTSTRAP: inserted {len(df)} rows from {Path(seed_path).name}"
        except Exception as e:
            msg = f"BOOTSTRAP ERROR: {type(e).__name__}: {e}"

    return msg


# === ANCHOR: READONLY_BOOTSTRAP (end) ===


# === ANCHOR: LOAD_DF (start) ===


# ---- Optional one-time bootstrap ----
@st.cache_data(show_spinner=False)
def load_df(q: str) -> pd.DataFrame:
    """Load rows, optional SQL-side LIKE filter on several columns."""
    ensure_schema()
    with ENG.connect() as cx:
        if q:
            return pd.read_sql_query(
                """
                SELECT *
                FROM vendors
                WHERE business_name LIKE :x
                   OR category      LIKE :x
                   OR service       LIKE :x
                   OR contact_name  LIKE :x
                   OR email         LIKE :x
                   OR website       LIKE :x
                   OR address       LIKE :x
                   OR notes         LIKE :x
                ORDER BY business_name COLLATE NOCASE ASC
                """,
                cx,
                params={"x": f"%{q}%"},
            )
        return pd.read_sql_query(
            """
            SELECT * FROM vendors
            ORDER BY business_name COLLATE NOCASE ASC
            """,
            cx,
        )


msg = _bootstrap_from_csv_if_needed()
try:
    if msg:
        st.toast(msg, icon="✅")
except Exception:
    pass


# ---- UI ----


df = load_df(q)

_drop_legacy = [c for c in ("city", "state", "zip") if c in df.columns]
if _drop_legacy:
    df = df.drop(columns=_drop_legacy)
# === ANCHOR: UI_RENDER (start) ===
if df.empty:
    # Offer quick hint if the table is empty
    if not Path(DB_PATH).exists():
        st.warning("No database found. Add data/providers_seed.csv or set PROVIDERS_DB.")
    else:
        st.info("No matching providers.")
else:
    # Read preferences from secrets (safe defaults)
    browse_order = list(st.secrets.get("BROWSE_ORDER", []))
    hide_cols = set(st.secrets.get("HIDE_COLUMNS", []))

    # Drop hidden columns that actually exist
    drop_now = [c for c in df.columns if c in hide_cols]
    if drop_now:
        df = df.drop(columns=drop_now)

    # Build final order: preferred order first (only those present), then the rest
    pref = [c for c in browse_order if c in df.columns]
    rest = [c for c in df.columns if c not in pref]
    view_cols = pref + rest
    df = df.loc[:, view_cols]

    st.dataframe(df, use_container_width=True, hide_index=True)

    st.download_button(
        "Download CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name="providers.csv",
        mime="text/csv",
    )
# === ANCHOR: UI_RENDER (end) ===

# ---- Tiny footer (optional) ----
with st.expander("Help Section", expanded=False):
    st.write(
        "This read-only viewer uses a local SQLite database (providers.db). "
        "If the database is empty and data/providers_seed.csv is present, "
        "it imports that CSV once at startup."
    )
# === ANCHOR: FOOTER (end) ===
# === ANCHOR: DOWNLOADS (start) ===
# Assumes `df` is the filtered DataFrame you display.
try:
    c1, c2 = st.columns(2)
    with c1:
        _csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="CSV",
            data=_csv_bytes,
            file_name="providers.csv",
            mime="text/csv",
            use_container_width=True,
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
            use_container_width=True,
        )
except Exception:
    # If df is not defined yet, skip buttons (harmless during early app phases)
    pass
# === ANCHOR: DOWNLOADS (end) ===
# Hide Id column in the Browse view (display-only)
if "id" in df.columns:
    df = df.drop(columns=["id"])
