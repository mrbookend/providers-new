"""Read-only Providers app (minimal, failsafe)."""

from __future__ import annotations

import os
from contextlib import suppress
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

from export_utils import ensure_phone_string, to_xlsx_bytes

_HAS_AGGRID = True
# Must be FIRST Streamlit call
st.set_page_config(page_title="Providers - Read-Only", page_icon="[book]", layout="wide")


# === ANCHOR: READONLY SEARCH INPUT (start) ===
q = st.text_input("Search", value="", placeholder="name, category, service, phone, notes…")
q = (q or "").strip()
# === ANCHOR: READONLY SEARCH INPUT (end) ===
# === ANCHOR: CONSTANTS (start) ===
DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
PHONE_LEN = 10  # 10-digit NANP number
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
    with suppress(Exception):
        with ENG.connect() as cx:
            cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar_one()
        if (cnt or 0) > 0:
            return ""

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
        base_sql = "SELECT * FROM vendors ORDER BY business_name COLLATE NOCASE ASC"
        return pd.read_sql_query(sa.text(base_sql), cx)

        base_sql = "SELECT * FROM vendors ORDER BY business_name COLLATE NOCASE ASC"
        return pd.read_sql_query(sa.text(base_sql), cx)


# === ANCHOR: LOAD_DF (end) ===


# === ANCHOR: SEARCH UI (start) ===
# Controls row: left = Search (~1/2 width), right = two download buttons on same row
controls_left, controls_right_csv, controls_right_xlsx = st.columns([2, 1, 1])


def __on_search_enter__():
    term = (st.session_state.get("__search_box__", "") or "").strip()
    st.session_state["__search_term__"] = term
    st.session_state["__search_box__"] = ""  # auto-clear


with controls_left:
    st.text_input(
        label="Search",
        key="__search_box__",
        placeholder="Search by name, category, service, etc.",
        label_visibility="collapsed",
        on_change=__on_search_enter__,
    )

q = (st.session_state.pop("__search_term__", "") or "").strip()

# Ensure seed import (if needed) before we load data
_msg = _bootstrap_from_csv_if_needed()

# Load filtered dataframe now that q (and DB) are ready
df = load_df(q)

# Two download buttons on the SAME row (right side), built from the filtered df
with suppress(Exception):
    # CSV
    # === ANCHOR: CSV NORMALIZE (start) ===
    _base_df = globals().get("df_export")
    if not isinstance(_base_df, pd.DataFrame):
        _base_df = globals().get("df")
    _df_for_csv = _base_df.copy() if isinstance(_base_df, pd.DataFrame) else None
    if (
        _df_for_csv is not None
        and 'phone' in _df_for_csv.columns
        and 'phone_fmt' in _df_for_csv.columns
    ):
        _df_for_csv['phone'] = _df_for_csv['phone_fmt']
    # (optional) preserve ZIP leading zeros
    # if _df_for_csv is not None and 'zip' in _df_for_csv.columns:
    #     _df_for_csv['zip'] = _df_for_csv['zip'].astype(str)
    # === ANCHOR: CSV NORMALIZE (end) ===

    _csv_bytes = _df_for_csv.to_csv(index=False).encode("utf-8")
    controls_right_csv.download_button(
        label="Download CSV",
        data=_csv_bytes,
        file_name="providers.csv",
        mime="text/csv",
        key="browse_dl_csv",
        use_container_width=False,
    )

# Help directly under the controls row
with st.expander("Help — Browse", expanded=False):
    st.write(
        "Read-only viewer for the Providers list. "
        "Database path is set by PROVIDERS_DB (default providers.db). "
        "If empty and a seed CSV is available, the app imports it once at startup."
    )
# === ANCHOR: SEARCH UI (end) ===


# === ANCHOR: BROWSE RENDER (aggrid) (start) ===
def _render_table(df: pd.DataFrame) -> None:
    """Render read-only table using Ag-Grid when available; fallback to st.dataframe."""
    if not _HAS_AGGRID:
        # === CKW: hide from view in st.dataframe ===
        df_display = df.drop(columns=["computed_keywords"], errors="ignore")
        st.dataframe(df_display, use_container_width=False, hide_index=True)
        return


# === ANCHOR: READONLY GRID PREP (inserted) (start) ===
# Defaults from secrets with safe fallbacks
page_size = int(st.secrets.get("PAGE_SIZE", 0) or 0)
grid_height = int(st.secrets.get("GRID_HEIGHT", 560) or 560)
header_px = int(st.secrets.get("HEADER_PX", 0) or 0)
single_page = bool(st.secrets.get("READONLY_SINGLE_PAGE", False))
font_px = int(st.secrets.get("READONLY_FONT_SIZE_PX", 0) or 0)

custom_css = {}
if font_px > 0:
    custom_css = {
        ".ag-root-wrapper": {"font-size": f"{font_px}px"},
        ".ag-header-cell-label": {"font-size": f"{max(font_px - 1, 10)}px"},
    }

# Base options builder (create once; configure below and in render)

# === ANCHOR: READONLY GRID PREP (inserted) (end) ===

gob = GridOptionsBuilder.from_dataframe(df)


# Keep CKW searchable but hidden in the grid

if "computed_keywords" in df.columns:
    gob.configure_column(
        "computed_keywords", hide=True, sortable=False, filter=False, suppressMenu=True
    )

# Keep CKW searchable but hidden in the grid

if "computed_keywords" in df.columns:
    gob.configure_column(
        "computed_keywords", hide=True, sortable=False, filter=False, suppressMenu=True
    )

# === ANCHOR: PHONE FORMATTER (start) ===
# Render phone as (xxx) xxx-xxxx using JS (Ag-Grid valueFormatter)
_phone_fmt_js = JsCode("""
function(params) {
  const raw = (params.value || "").toString();
  const s = raw.replace(/\D/g, "");
  let t = s;
  if (s.length === 11 && s.startsWith("1")) { t = s.slice(1); }
  if (t.length === 10) { return "(" + t.slice(0,3) + ") " + t.slice(3,6) + "-" + t.slice(6); }
  return raw;
}
""")
if "phone" in df.columns:
    gob.configure_column("phone", valueFormatter=_phone_fmt_js)
# === ANCHOR: PHONE FORMATTER (end) ===

# === ANCHOR: READONLY WIDTHS (start) ===
# Secrets-driven exact pixel widths (case-insensitive)
widths_src = st.secrets.get("COLUMN_WIDTHS_PX_READONLY", {}) or {}
try:
    widths = {str(k).strip().lower(): int(v) for k, v in dict(widths_src).items()}
except Exception:
    widths = {}

# Don't let auto-size fight our fixed pixel widths
gob.configure_default_column(suppressSizeToFit=True)
gob.configure_grid_options(suppressAutoSize=True)

_applied: list[tuple[str, int]] = []
for col in list(df.columns):
    lk = str(col).strip().lower()
    w = widths.get(lk)
    if w:
        gob.configure_column(col, width=w, flex=0)
        _applied.append((col, w))
if int(st.secrets.get("DEBUG_READONLY_WIDTHS", 0) or 0):
    st.caption(
        "Applied fixed widths: "
        + (", ".join(f"{c}={w}" for c, w in _applied) if _applied else "(none)")
    )
# === ANCHOR: READONLY WIDTHS (end) ===

# === ANCHOR: WRAP/STYLE HINTS (start) ===
# Keep most cells single-line; selectively enable wrap on a few wide text columns (if present)
for _col in ("business_name", "address", "category", "service"):
    if _col in df.columns:
        gob.configure_column(_col, wrapText=True, autoHeight=True)
# === ANCHOR: WRAP/STYLE HINTS (end) ===

# === ANCHOR: GRID LAYOUT (start) ===
# Grid layout & pagination (domLayout + optional pagination)
grid_opts: dict = {}
if single_page:
    grid_opts["domLayout"] = "autoHeight"
    page_size = 0
elif page_size > 0:
    grid_opts["domLayout"] = "normal"  # fixed viewport (internal scroll)
    grid_opts["pagination"] = True
    grid_opts["paginationPageSize"] = page_size
else:
    grid_opts["domLayout"] = "normal"  # fixed viewport (internal scroll)

if header_px > 0:
    grid_opts["headerHeight"] = header_px

gob.configure_grid_options(**grid_opts)

# Key varies with widths (and optional per-run nonce) to force re-instantiation
_wsig = "none" if not widths else "|".join(f"{k}:{widths[k]}" for k in sorted(widths))
_always_reset = bool(st.secrets.get("READONLY_ALWAYS_RESET", 1))
_nonce = st.session_state.get("__readonly_grid_nonce__", 0)
if _always_reset:
    _nonce += 1
    st.session_state["__readonly_grid_nonce__"] = _nonce
_grid_key = f"readonly-grid|w={_wsig}|n={_nonce}"
# === ANCHOR: GRID LAYOUT (end) ===

# === ANCHOR: RENDER (start) ===
if single_page or page_size > 0:
    grid_opts = gob.build()
    if header_px > 0:
        grid_opts["headerHeight"] = header_px
    if q:
        grid_opts["quickFilterText"] = q

    AgGrid(
        df,
        gridOptions=grid_opts,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        custom_css=custom_css,
        key=_grid_key,
    )
else:
    grid_opts = gob.build()
    if header_px > 0:
        grid_opts["headerHeight"] = header_px
    if q:
        grid_opts["quickFilterText"] = q

    AgGrid(
        df,
        height=grid_height,
        gridOptions=grid_opts,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        custom_css=custom_css,
        key=_grid_key,
    )
# === ANCHOR: RENDER (end) ===


# === ANCHOR: BROWSE RENDER (aggrid) (end) ===


# === ANCHOR: BROWSE (start) ===
# Hide Id column if present

# Secrets-driven preferences
browse_order = list(st.secrets.get("BROWSE_ORDER", []))

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
_render_table(df)
# === ANCHOR: BROWSE (end) ===

# === ANCHOR: XLSX DOWNLOAD (patched) (start) ===
_base_df = globals().get("df_export")
if not isinstance(_base_df, pd.DataFrame):
    _base_df = globals().get("df")
_df_for_xlsx = (
    _df_for_csv.copy()
    if "_df_for_csv" in globals() and _df_for_csv is not None
    else (_base_df.copy() if isinstance(_base_df, pd.DataFrame) else None)
)
if _df_for_xlsx is not None:
    _df_for_xlsx = ensure_phone_string(_df_for_xlsx)
    _xlsx_bytes = to_xlsx_bytes(_df_for_xlsx, text_cols=("phone", "zip"))
    controls_right_xlsx.download_button(
        label="Download Excel",
        data=_xlsx_bytes,
        file_name="providers.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="browse_dl_xlsx",
        use_container_width=False,
    )
# === ANCHOR: XLSX DOWNLOAD (patched) (end) ===


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
