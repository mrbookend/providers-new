"""Read-only Providers app (minimal, failsafe)."""

from __future__ import annotations

import hashlib
import os
from io import BytesIO
from pathlib import Path
import subprocess

import pandas as pd
import sqlalchemy as sa
import streamlit as st


# === ANCHOR: IMPORTS (aggrid) (start) ===
# Optional Ag-Grid imports (safe at top-level; Ruff-friendly)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder

    _HAS_AGGRID = True
except Exception:
    AgGrid = None  # type: ignore[assignment]
    GridOptionsBuilder = None  # type: ignore[assignment]
    _HAS_AGGRID = False
# === ANCHOR: IMPORTS (aggrid) (end) ===

# Must be FIRST Streamlit call
st.set_page_config(page_title="Providers — Read-Only", page_icon="[book]", layout="wide")

# === ANCHOR: RUNTIME BANNER (start) ===
# Toggle with env or secrets: READONLY_SHOW_STATUS=1
if int(os.environ.get("READONLY_SHOW_STATUS", st.secrets.get("READONLY_SHOW_STATUS", 0))):
    try:
        # Local imports to avoid top-level Ruff E402 and keep optional deps
        import hashlib, time  # used below
        from pathlib import Path
        import git  # GitPython is installed on Cloud

        repo = git.Repo(Path(__file__).resolve().parent)
        sha  = repo.head.commit.hexsha
        br   = getattr(repo.head, "ref", None)
        brn  = getattr(br, "name", "detached")
        p    = Path(__file__)
        h16  = hashlib.sha256(p.read_bytes()).hexdigest()[:16]
        mt   = int(p.stat().st_mtime)

        st.caption(f"Runtime: branch {brn}, commit {sha[:7]}, file sha256 {h16}, mtime {mt}")
    except Exception as e:
        st.caption(f"Runtime: commit unknown ({e})")
# === ANCHOR: RUNTIME BANNER (end) ===


# === ANCHOR: STARTUP BANNER (start) ===
import hashlib
import subprocess

def _file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def _git_head_short() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
        return out
    except Exception:
        return "unknown"

_commit = _git_head_short()
_file_hash = _file_sha256(__file__)
st.caption(f"[readonly] commit={_commit} file_sha256={_file_hash[:16]}")
# === ANCHOR: STARTUP BANNER (end) ===


# === ANCHOR: CONSTANTS (start) ===
DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
PHONE_LEN_WITH_CC = 11  # e.g., leading '1' country code
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

# Build final order: preferred first (that exist), then remaining
pref = [c for c in browse_order if c in df.columns]
rest = [c for c in df.columns if c not in pref]
view_cols = pref + rest
df = df.loc[:, view_cols]

if "id" in df.columns:
    df = df.drop(columns=["id"])


# === ANCHOR: BROWSE RENDER (aggrid) (start) ===
def _render_table(df):
    """Render read-only table using Ag-Grid when available; fallback to st.dataframe."""
    # Fast fallback if Ag-Grid isn't available
    if not _HAS_AGGRID:
        st.dataframe(df, use_container_width=False, hide_index=True)
        return

    # Knobs with sane defaults (read from globals if present)
    single_page = bool(globals().get("single_page", False))
    page_size = int(globals().get("page_size", 0) or 0)
    grid_height = int(globals().get("grid_height", 560))
    header_px = int(globals().get("header_px", 0))
    custom_css = globals().get("custom_css", {})

    # Perf defaults: nowrap + no autoHeight globally
    gob = GridOptionsBuilder.from_dataframe(df)
    gob.configure_default_column(
        wrapText=False,
        autoHeight=False,
        resizable=True,
        cellStyle={"white-space": "nowrap", "line-height": "1.3em"},
        flex=0,
        suppressSizeToFit=True,
    )

    # Wrap + autoHeight only for these columns
    for col in ("business_name", "address"):
        if col in df.columns:
            gob.configure_column(
                col,
                wrapText=True,
                autoHeight=True,
                cellStyle={"white-space": "normal", "line-height": "1.3em"},
            )

    # Grid options
    grid_opts = {}
    if single_page:
        grid_opts["domLayout"] = "autoHeight"
        page_size = 0
    elif page_size > 0:
        grid_opts["domLayout"] = "autoHeight"
        grid_opts["pagination"] = True
        grid_opts["paginationPageSize"] = page_size
    else:
        grid_opts["domLayout"] = "normal"  # fixed viewport (internal scroll)

    if header_px > 0:
        grid_opts["headerHeight"] = header_px
    grid_opts["ensureDomOrder"] = False
    grid_opts["suppressColumnVirtualisation"] = False

    gob.configure_grid_options(**grid_opts)

    # Render: single-page/paged => no explicit height; fixed viewport => height=grid_height
    if single_page or page_size > 0:
        AgGrid(
            df,
            gridOptions=gob.build(),
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            custom_css=custom_css,
        )
    else:
        AgGrid(
            df,
            gridOptions=gob.build(),
            height=grid_height,  # fixed viewport, internal scroll
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            custom_css=custom_css,
        )


# === ANCHOR: BROWSE RENDER (aggrid) (end) ===


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
_drop = [c for c in df.columns if c in hide_cols]
if _drop:
    df = df.drop(columns=_drop)

# === ANCHOR: DOWNLOADS (end) ===
