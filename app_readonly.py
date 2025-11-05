"""Read-only Providers app (minimal, failsafe)."""

# === ANCHOR: IMPORTS (start) ===
from __future__ import annotations

# stdlib
import os
import time
from contextlib import suppress
from pathlib import Path

# third-party
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from st_aggrid import GridOptionsBuilder, JsCode

# local
from export_utils import ensure_phone_string, to_xlsx_bytes

# === ANCHOR: IMPORTS (end) ===
# === ANCHOR: PAGE CONFIG (start) ===
st.set_page_config(page_title="Providers - Read-Only", page_icon="📘", layout="wide")
# === ANCHOR: PAGE CONFIG (end) ===

# === ANCHOR: WHOLE-WORD WRAP (start) ===
# Import the real AgGrid under a private name; wrapper defined below.
try:
    from st_aggrid import AgGrid as _AgGrid
except Exception:
    _AgGrid = None

# Ag-Grid-native approach:
# - defaultColDef.wrapText/autoHeight
# - custom_css to keep whole words (but allow ultra-long tokens to break)
# - JS events to reset row heights when data renders / filters / columns resize
DEFAULT_WRAP_CSS = {
    ".ag-theme-streamlit .ag-cell": {
        "white-space": "normal !important",
        "word-break": "keep-all !important",
        "overflow-wrap": "anywhere !important",  # break long URLs/IDs when needed
        "hyphens": "auto !important",
        "line-height": "1.25 !important",
    },
    ".ag-theme-quartz .ag-cell": {
        "white-space": "normal !important",
        "word-break": "keep-all !important",
        "overflow-wrap": "anywhere !important",
        "hyphens": "auto !important",
        "line-height": "1.25 !important",
    },
}

# Row-height recompute (initial + after user tweaks)
JS_RESET_ROW_HEIGHTS = JsCode(
    """
function(params) {
  const api = params.api;
  if (api && api.resetRowHeights) {
    api.resetRowHeights();
    // run again after the browser paints, to catch late layout
    setTimeout(function(){ try { api.resetRowHeights(); } catch(e){} }, 0);
  }
}
"""
)

if _AgGrid is not None:

    def AgGrid(df, **kwargs):
        # Merge/seed gridOptions
        go = dict(kwargs.pop("gridOptions", {}) or {})

        # Column defaults: wrap + autoHeight everywhere; callers can still override
        dcol = dict(go.get("defaultColDef", {}) or {})
        dcol.setdefault("wrapText", True)
        dcol.setdefault("autoHeight", True)
        go["defaultColDef"] = dcol

        # Let the grid size vertically with content; caller can override if they really want
        go.setdefault("domLayout", "autoHeight")

        # Make sure row heights recompute whenever it matters
        go.setdefault("onFirstDataRendered", JS_RESET_ROW_HEIGHTS)
        go.setdefault("onFilterChanged", JS_RESET_ROW_HEIGHTS)
        go.setdefault("onColumnResized", JS_RESET_ROW_HEIGHTS)

        # Merge CSS so words wrap on spaces; long tokens still break
        user_css = dict(kwargs.pop("custom_css", {}) or {})
        merged_css = dict(DEFAULT_WRAP_CSS)
        merged_css.update(user_css)  # user overrides win
        kwargs["custom_css"] = merged_css

        # Defaults that help wrapping behave
        kwargs.setdefault("fit_columns_on_grid_load", False)  # allow natural widths
        kwargs.setdefault("allow_unsafe_jscode", True)  # enable our JS events

        kwargs["gridOptions"] = go
        return _AgGrid(df, **kwargs)
# === ANCHOR: WHOLE-WORD WRAP (end) ===


# === PAGE CONFIG ===

# (Optional) Live banner so you can verify the running file/time

st.warning(f"READ-ONLY LIVE: {os.path.abspath(__file__)} @ {time.strftime('%H:%M:%S')}")

# === CONSTANTS / ENGINE ===
DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)

# === SCHEMA (minimal, non-breaking) ===
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
  ckw_version TEXT,
  phone_fmt TEXT
);
CREATE INDEX IF NOT EXISTS idx_vendors_name     ON vendors(business_name COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_vendors_cat      ON vendors(category);
CREATE INDEX IF NOT EXISTS idx_vendors_service  ON vendors(service);
CREATE INDEX IF NOT EXISTS idx_vendors_phonefmt ON vendors(phone_fmt);
"""


def ensure_schema() -> None:
    with ENG.begin() as cx:
        for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
            cx.exec_driver_sql(stmt)


# === BOOTSTRAP (optional CSV seed) ===
def _bootstrap_from_csv_if_needed() -> str:
    """If DB empty and seed CSV exists, import once."""
    ensure_schema()
    with suppress(Exception):
        with ENG.connect() as cx:
            cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar_one()
        if (cnt or 0) > 0:
            return ""

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

    try:
        df = pd.read_csv(seed_path)
    except Exception as e:
        return f"BOOTSTRAP: failed to read {seed_path}: {type(e).__name__}: {e}"

    for legacy in ("city", "state", "zip"):
        if legacy in df.columns:
            df = df.drop(columns=[legacy])

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


# === DATA LOAD ===
@st.cache_data(show_spinner=False)
def load_df(q: str) -> pd.DataFrame:
    """Return providers (optionally filtered later in-grid)."""
    ensure_schema()
    with ENG.connect() as cx:
        base_sql = "SELECT * FROM vendors ORDER BY business_name COLLATE NOCASE ASC"
        return pd.read_sql_query(sa.text(base_sql), cx)


# === READ-ONLY PREFS (secrets) ===
def _readonly_prefs_from_secrets() -> dict:
    s = st.secrets
    return {
        "browse_order": list(s.get("BROWSE_ORDER", [])),
        "hide_cols": set(s.get("HIDE_COLUMNS", [])),
        "use_aggrid": int(s.get("READONLY_USE_AGGRID", 1)),
        "always_reset": int(s.get("READONLY_ALWAYS_RESET", 1)),
        "debug_widths": int(s.get("DEBUG_READONLY_WIDTHS", 0)),
        "grid_h": int(s.get("READONLY_GRID_HEIGHT_PX", 420)),
        "font_px": int(s.get("READONLY_FONT_SIZE_PX", 14)),
        "page_size": int(s.get("READONLY_PAGE_SIZE", 0)),
        "header_px": int(s.get("READONLY_HEADER_HEIGHT_PX", 28)),
        "single_page": int(s.get("READONLY_SINGLE_PAGE", 0)),
        "col_widths": dict(s.get("COLUMN_WIDTHS_PX_READONLY", {})),
    }


def _apply_readonly_prefs(df: pd.DataFrame):
    """
    Returns: (df2, view_cols, hidden_cols, prefs)
    - Prefer 'phone_fmt' into 'phone' if non-empty.
    - Enforce BROWSE_ORDER first; append remaining visible in source order.
    - Hide must-hide columns even if secrets misload.
    """
    prefs = _readonly_prefs_from_secrets()
    hide_cols: set[str] = set(prefs["hide_cols"])

    # Fail-safe hiding — ensure internal cols never show
    must_hide = {
        "id",
        "computed_keywords",
        "ckw_locked",
        "ckw_version",
        "ckw_manual_extra",
        "keywords",
        "ckw",
        "phone_fmt",
        "created_at",
        "service_lower",
        "updated_at",
        "updated_by",
    } & set(df.columns)
    hide_cols |= must_hide

    df2 = df.copy()

    # Phone: prefer phone_fmt when present & non-empty
    if "phone" in df2.columns and "phone_fmt" in df2.columns:
        df2["phone"] = df2["phone_fmt"].where(
            df2["phone_fmt"].astype(str).str.len() > 0,
            df2["phone"],
        )

    seed = [c for c in prefs["browse_order"] if c in df2.columns]
    visible_src = [c for c in df2.columns if c not in hide_cols]
    tail = [c for c in visible_src if c not in set(seed)]
    view_cols = seed + tail
    hidden_cols = hide_cols
    return df2, view_cols, hidden_cols, prefs


# === SEARCH / CONTROLS ROW (single search; first search removed) ===
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

# Bootstrap if needed, then load
_msg = _bootstrap_from_csv_if_needed()
df = load_df(q)

# === DOWNLOADS (built from the filtered frame we render) ===
_df_base = df.copy()
with suppress(Exception):
    # CSV
    _df_for_csv = _df_base.copy()
    if "phone" in _df_for_csv.columns and "phone_fmt" in _df_for_csv.columns:
        _df_for_csv["phone"] = _df_for_csv["phone_fmt"].where(
            _df_for_csv["phone_fmt"].astype(str).str.len() > 0,
            _df_for_csv["phone"],
        )
    _csv_bytes = _df_for_csv.to_csv(index=False).encode("utf-8")
    controls_right_csv.download_button(
        label="Download CSV",
        data=_csv_bytes,
        file_name="providers.csv",
        mime="text/csv",
        key="browse_dl_csv",
        use_container_width=False,
    )

    # XLSX
    _df_for_xlsx = ensure_phone_string(_df_base.copy())
    _xlsx_bytes = to_xlsx_bytes(_df_for_xlsx, text_cols=("phone", "zip"))
    controls_right_xlsx.download_button(
        label="Download Excel",
        data=_xlsx_bytes,
        file_name="providers.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="browse_dl_xlsx",
        use_container_width=False,
    )

# Help below the controls row
with st.expander("Help — Browse", expanded=False):
    st.write(
        "Read-only viewer for the Providers list. Database path is set by "
        "`PROVIDERS_DB` (default `providers.db`). If empty and a seed CSV is "
        "available, the app imports it once at startup."
    )


def _render_table(df: pd.DataFrame) -> None:
    """Render read-only table using Ag-Grid when available; fallback to st.dataframe."""
    # Apply prefs and compute display frame (also remaps phone <- phone_fmt, hides must-hide)
    df2, view_cols, _hidden_cols, prefs = _apply_readonly_prefs(df)
    df = df2
    df_display = df[view_cols].copy()
    # Keep keywords searchable, but never display them
    for _col in ("computed_keywords", "keywords"):
        if _col in df_display.columns:
            df_display.drop(columns=[_col], inplace=True)
    # DEBUG — remove after verification
    st.caption("DEBUG view_cols: " + ", ".join(view_cols))
    st.caption("DEBUG df_display cols: " + ", ".join(list(df_display.columns)))

    # Detect AgGrid availability
    has_aggrid = True
    try:
        _ = GridOptionsBuilder  # type: ignore[name-defined]
        _ = AgGrid  # type: ignore[name-defined]
    except NameError:
        has_aggrid = False

    # Fallback: no Ag-Grid or disabled via secrets
    if not has_aggrid or not int(prefs.get("use_aggrid", 1)):
        st.dataframe(df_display, use_container_width=False, hide_index=True)
        return

    # Build grid options from the display frame
    gob = GridOptionsBuilder.from_dataframe(df_display)

    # Hide CKW/keywords defensively (even though we dropped them)
    for _col in ("computed_keywords", "keywords"):
        with suppress(Exception):
            gob.configure_column(_col, hide=True, sortable=False, filter=False, suppressMenu=True)

    # Phone display formatter (JS fallback)
    _phone_fmt_js = JsCode("""function(params){
      const raw=(params.value||"").toString();
      const s=raw.replace(/\\D/g,"");
      let t=s; if(s.length===11&&s.startsWith("1")){t=s.slice(1);}
      if(t.length===10){return "(" + t.slice(0,3) + ") " + t.slice(3,6) + "-" + t.slice(6);}
      return raw;
    }""")
    if "phone" in df_display.columns:
        gob.configure_column("phone", valueFormatter=_phone_fmt_js)

    # Fixed widths from secrets
    widths = {}
    try:
        widths = {
            str(k).strip().lower(): int(v) for k, v in dict(prefs.get("col_widths", {})).items()
        }
    except Exception:
        widths = {}
    gob.configure_default_column(suppressSizeToFit=True)
    gob.configure_grid_options(suppressAutoSize=True)
    for col in df_display.columns:
        lk = str(col).strip().lower()
        if lk in widths:
            gob.configure_column(col, width=widths[lk], flex=0)

    # Selective wrap for a few long text columns
    for _col in ("business_name", "address", "category", "service"):
        if _col in df_display.columns:
            gob.configure_column(_col, wrapText=True, autoHeight=True)

    # Layout & pagination
    grid_opts: dict = {}
    page_size = int(prefs.get("page_size", 0))
    single_page = int(prefs.get("single_page", 0))
    header_px = int(prefs.get("header_px", 0))
    grid_h = int(prefs.get("grid_h", 420))
    font_px = int(prefs.get("font_px", 14))

    if single_page:
        grid_opts["domLayout"] = "autoHeight"
        page_size = 0
    elif page_size > 0:
        grid_opts["domLayout"] = "normal"
        grid_opts["pagination"] = True
        grid_opts["paginationPageSize"] = page_size
    else:
        grid_opts["domLayout"] = "normal"

    if header_px > 0:
        grid_opts["headerHeight"] = header_px

    gob.configure_grid_options(**grid_opts)

    # Quick filter (if the page defined q)
    try:
        quick = q  # type: ignore[name-defined]
    except NameError:
        quick = ""
    opts = gob.build()
    if quick:
        opts["quickFilterText"] = quick

    # CSS for font sizing
    custom_css = {}
    if font_px > 0:
        custom_css = {
            ".ag-root-wrapper": {"font-size": f"{font_px}px"},
            ".ag-header-cell-label": {"font-size": f"{max(font_px - 1, 10)}px"},
        }

    # Force re-instantiation when widths change / when always_reset=1
    always_reset = int(prefs.get("always_reset", 1))
    _nonce = st.session_state.get("__readonly_grid_nonce__", 0)
    if always_reset:
        _nonce += 1
        st.session_state["__readonly_grid_nonce__"] = _nonce
    _wsig = "none" if not widths else "|".join(f"{k}:{widths[k]}" for k in sorted(widths))
    _grid_key = f"readonly-grid|w={_wsig}|n={_nonce}"

    # Render
    if single_page or page_size > 0:
        AgGrid(
            df_display,
            gridOptions=opts,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            custom_css=custom_css,
            key=_grid_key,
        )
    else:
        AgGrid(
            df_display,
            height=grid_h,
            gridOptions=opts,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            custom_css=custom_css,
            key=_grid_key,
        )


# === MAIN ===
_render_table(df)
