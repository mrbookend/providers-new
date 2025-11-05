"""Read-only Providers app (minimal, failsafe)."""

from __future__ import annotations

# === ANCHOR: IMPORTS (start) ===
# stdlib
import io
import os
import os as _os
import tempfile as _tempfile
from contextlib import suppress
from pathlib import Path, Path as _Path

# third-party
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from st_aggrid import GridOptionsBuilder, JsCode

# --- Export helpers (with failsafes) ---
try:
    from export_utils import ensure_phone_string, to_xlsx_bytes  # type: ignore
except Exception:

    def ensure_phone_string(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "phone" in out.columns:
            out["phone"] = out["phone"].astype(str)
        return out

    def to_xlsx_bytes(df: pd.DataFrame, text_cols: tuple[str, ...] = ()) -> bytes:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
            # Force text for specified columns
            if text_cols:
                df2 = df.copy()
                for c in text_cols:
                    if c in df2.columns:
                        df2[c] = df2[c].astype(str)
                df2.to_excel(xw, sheet_name="providers", index=False)
            else:
                df.to_excel(xw, sheet_name="providers", index=False)
        buf.seek(0)
        return buf.read()
# === ANCHOR: IMPORTS (end) ===


# === ANCHOR: PAGE CONFIG (start) ===
st.set_page_config(page_title="Providers - Read-Only", page_icon="📘", layout="wide")
# === ANCHOR: PAGE CONFIG (end) ===


# === FONT SIZE (from secrets) ===
try:
    FONT_PX = int(st.secrets.get("READONLY_FONT_SIZE_PX", 14))
except Exception:
    FONT_PX = 14  # failsafe
st.markdown(
    f"""
    <style>
      :root {{ --ro-font-size: {FONT_PX}px; }}

      /* Base font sizing */
      html, body,
      [data-testid="stAppViewContainer"],
      .block-container {{
        font-size: var(--ro-font-size) !important;
        line-height: 1.35 !important;
      }}

      /* Remove Streamlit header chrome height */
      div[data-testid="stHeader"] {{
        height: 0px !important;
        min-height: 0px !important;
      }}
      /* Slim the main block top padding */
      [data-testid="stAppViewContainer"] > .main .block-container {{
        padding-top: 0.15rem !important;
      }}
      /* Optional: hide the toolbar (3-dot menu) */
      div[data-testid="stToolbar"] {{ display: none !important; }}

      /* Tighter spacing between search / help / header row */
      div[data-testid="stTextInput"] {{ margin-bottom: 0.15rem !important; }}
      div[data-testid="stExpander"] {{ margin-top: 0.15rem !important; margin-bottom: 0.15rem !important; }}
      div[data-testid="stExpander"] summary {{ padding-top: 0.15rem !important; padding-bottom: 0.15rem !important; }}

      /* Ag-Grid header: reduce vertical height/padding */
      .ag-theme-streamlit .ag-header, .ag-theme-quartz .ag-header {{ min-height: 28px !important; }}
      .ag-theme-streamlit .ag-header .ag-header-row, .ag-theme-quartz .ag-header .ag-header-row {{ height: 28px !important; }}
      .ag-theme-streamlit .ag-header-cell, .ag-theme-streamlit .ag-header-group-cell,
      .ag-theme-quartz .ag-header-cell, .ag-theme-quartz .ag-header-group-cell {{
        padding-top: 2px !important; padding-bottom: 2px !important;
      }}

      /* Slightly trim default section spacing */
      .block-container > div {{ margin-top: 0.15rem !important; margin-bottom: 0.15rem !important; }}
    </style>
    """,
    unsafe_allow_html=True,
)


# === HCR: WRITABLE DIR HELPERS (readonly) ===
def _pick_writable_dir(cands):
    for base in cands:
        try:
            base.mkdir(parents=True, exist_ok=True)
            prob = base / ".probe"
            prob.write_text("ok", encoding="utf-8")
            prob.unlink(missing_ok=True)
            return base
        except Exception:
            pass
    fb = _Path(_tempfile.gettempdir()) / "providers-new"
    fb.mkdir(parents=True, exist_ok=True)
    return fb


def _cloud_persistent_dir():
    return _Path("/mount/data/.providers-cache")


def _local_cache_dir():
    xdg = _os.environ.get("XDG_CACHE_HOME")
    return (_Path(xdg) if xdg else _Path.home() / ".cache") / "providers-new"


_env_cache = _os.environ.get("PROVIDERS_CACHE_DIR")
_candidates = []
if _env_cache:
    _candidates.append(_Path(_env_cache))
_candidates += [_cloud_persistent_dir(), _local_cache_dir()]
CACHE_DIR = _pick_writable_dir(_candidates)

EXPORT_DIR = CACHE_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def _preferred_db_paths():
    env_db = _os.environ.get("PROVIDERS_DB")
    if env_db:
        return [_Path(env_db)]
    return [
        _Path("/mount/data/providers.db"),
        _local_cache_dir() / "providers.db",
        _Path.cwd() / "providers.db",
    ]


def _resolve_db_path():
    for cand in _preferred_db_paths():
        try:
            if cand.exists() or cand.parent.exists():
                return str(cand)
        except Exception:
            pass
    return str(CACHE_DIR / "providers.db")


# === ANCHOR: CONFIG -- DB PATH (start) ===
DB_PATH = _resolve_db_path()
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
# === ANCHOR: CONFIG -- DB PATH (end) ===


# === ANCHOR: PHONE UTIL (start) ===
PHONE_NANP_LEN = 10  # digits: NPA-NXX-XXXX
PHONE_NANP_WITH_COUNTRY = 11  # leading '1' + 10 digits
PHONE_COUNTRY_PREFIX = "1"


def _strip_extension(s: str) -> str:
    lower = s.lower()
    for mark in (" ext.", " ext ", " ext:", " x", " x.", " ext", " extension "):
        i = lower.find(mark)
        if i != -1:
            return s[:i]
    return s


# === ANCHOR: PHONE UTIL (end) ===


# === ANCHOR: WHOLE-WORD WRAP (start) ===
try:
    from st_aggrid import AgGrid as _AgGrid
except Exception:
    _AgGrid = None

DEFAULT_WRAP_CSS = {
    ".ag-theme-streamlit .ag-cell": {
        "white-space": "normal !important",
        "word-break": "keep-all !important",
        "overflow-wrap": "anywhere !important",
        "hyphens": "auto !important",
        "line-height": "1.25 !important",
        "font-size": f"{FONT_PX}px !important",
    },
    ".ag-theme-quartz .ag-cell": {
        "white-space": "normal !important",
        "word-break": "keep-all !important",
        "overflow-wrap": "anywhere !important",
        "hyphens": "auto !important",
        "line-height": "1.25 !important",
        "font-size": f"{FONT_PX}px !important",
    },
}


def _emit_css(css_map: dict) -> None:
    css = "".join(
        f"{sel}{{" + ";".join(f"{k}:{v}" for k, v in props.items()) + "}}"
        for sel, props in css_map.items()
    )
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


_emit_css(DEFAULT_WRAP_CSS)

JS_RESET_ROW_HEIGHTS = JsCode(
    """
function(params) {
  const api = params.api;
  if (api && api.resetRowHeights) {
    api.resetRowHeights();
    setTimeout(function(){ try { api.resetRowHeights(); } catch(e){} }, 0);
  }
}
"""
)

if _AgGrid is not None:

    def AgGrid(df, **kwargs):
        go = dict(kwargs.pop("gridOptions", {}) or {})

        dcol = dict(go.get("defaultColDef", {}) or {})
        dcol.setdefault("wrapText", True)
        dcol.setdefault("autoHeight", True)
        go["defaultColDef"] = dcol

        user_css = dict(kwargs.pop("custom_css", {}) or {})
        merged_css = dict(DEFAULT_WRAP_CSS)
        merged_css.update(user_css)
        kwargs["custom_css"] = merged_css

        kwargs.setdefault("fit_columns_on_grid_load", False)
        kwargs.setdefault("allow_unsafe_jscode", True)

        kwargs["gridOptions"] = go
        return _AgGrid(df, **kwargs)
# === ANCHOR: WHOLE-WORD WRAP (end) ===


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
        df = pd.read_csv(seed_path, dtype=str)
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
def load_df() -> pd.DataFrame:
    """Return providers (unfiltered; we filter after computing view cols)."""
    ensure_schema()
    with ENG.connect() as cx:
        base_sql = "SELECT * FROM vendors"
        return pd.read_sql_query(sa.text(base_sql), cx)


# === READ-ONLY PREFS (secrets) ===
def _readonly_prefs_from_secrets() -> dict:
    s = st.secrets
    return {
        "browse_order": list(s.get("BROWSE_ORDER", [])),
        "hide_cols": set(s.get("HIDE_COLUMNS", [])),
        "use_aggrid": int(s.get("READONLY_USE_AGGRID", 1)),
        "debug_widths": int(s.get("DEBUG_READONLY_WIDTHS", 0)),
        "grid_h": int(s.get("READONLY_GRID_HEIGHT_PX", 420)),
        "font_px": int(s.get("READONLY_FONT_SIZE_PX", 14)),
        "page_size": int(s.get("READONLY_PAGE_SIZE", 0)),
        "header_px": int(s.get("READONLY_HEADER_HEIGHT_PX", 28)),
        "single_page": int(s.get("READONLY_SINGLE_PAGE", 0)),
        "col_widths": dict(s.get("COLUMN_WIDTHS_PX_READONLY", {})),
        # NEW:
        "visible_rows": int(s.get("READONLY_VISIBLE_ROWS", 0)),
        "row_px": int(s.get("READONLY_ROW_PX", 28)),
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


# Bootstrap if needed, then load
_msg = _bootstrap_from_csv_if_needed()
df = load_df()


# --- safety shim so we never crash if _fmt_phone isn't present ---
def __fmt_phone_safe(val: object) -> str:
    try:
        return _fmt_phone(val)  # use the canonical one if defined
    except NameError:
        # inline fallback formatter (NANP)
        s = str(val or "").strip()
        lower = s.lower()
        for mark in (" ext.", " ext ", " ext:", " x", " x.", " ext", " extension "):
            i = lower.find(mark)
            if i != -1:
                s = s[:i]
                break
        if "." in s:
            head, tail = s.split(".", 1)
            if head.strip().isdigit() and set(tail.strip()) <= {"0"}:
                s = head.strip()
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) == PHONE_NANP_WITH_COUNTRY and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == PHONE_NANP_LEN:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        return s


# Normalize raw phones early for fallback display paths (and for export)
with suppress(Exception):
    if "phone" in df.columns and "phone_fmt" in df.columns:
        mask = df["phone_fmt"].astype(str).str.len() > 0
        df.loc[~mask, "phone"] = df.loc[~mask, "phone"].map(__fmt_phone_safe)
    elif "phone" in df.columns:
        df["phone"] = df["phone"].map(__fmt_phone_safe)
# === SEARCH / CONTROLS ROW -- 1/3 search, buttons right ===

# Build export bytes for the full dataset
_df_base = df.copy()
_df_for_csv = _df_base.copy()
if "phone" in _df_for_csv.columns and "phone_fmt" in _df_for_csv.columns:
    _df_for_csv["phone"] = _df_for_csv["phone_fmt"].where(
        _df_for_csv["phone_fmt"].astype(str).str.len() > 0, _df_for_csv["phone"]
    )
_csv_bytes = _df_for_csv.to_csv(index=False).encode("utf-8")
_df_for_xlsx = ensure_phone_string(_df_base.copy())
_xlsx_bytes = to_xlsx_bytes(_df_for_xlsx, text_cols=("phone", "zip"))

# Layout: [left=1/3 search] [middle=1/3 spacer] [right=1/3 buttons]
col_search, col_spacer, col_right = st.columns([4, 4, 4])


def __on_search_enter__():
    term = (st.session_state.get("__search_box__", "") or "").strip()
    st.session_state["__search_term__"] = term
    st.session_state["__search_box__"] = ""


with col_search:
    st.text_input(
        label="Search",
        key="__search_box__",
        placeholder="Search by name, category, service, etc.",
        label_visibility="collapsed",
        on_change=__on_search_enter__,
    )

# Right-justify the two small buttons by nesting two columns inside the rightmost third
with col_right:
    btn_l, btn_r = st.columns([1, 1])
    with btn_l:
        st.download_button(
            label="CSV",
            data=_csv_bytes,
            file_name="providers.csv",
            mime="text/csv",
            key="dl_csv_top",
            use_container_width=True,
        )
    with btn_r:
        st.download_button(
            label="XLSX",
            data=_xlsx_bytes,
            file_name="providers.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_xlsx_top",
            use_container_width=True,
        )

# Make the buttons visually small (scoped CSS)
st.markdown(
    "<style>.stDownloadButton button{padding:0.25rem 0.5rem;font-size:0.85rem;}</style>",
    unsafe_allow_html=True,
)

# Pull the search term (if Enter was pressed)
q = (st.session_state.pop("__search_term__", "") or "").strip()

# === DOWNLOADS (built from the CURRENT frame we will render; no silent suppression) ===
_df_base = df.copy()
# --- Build export bytes for the full dataset (not just current view) ---
if "_df_base" not in locals():
    _df_base = df.copy()

_df_for_csv = _df_base.copy()
if "phone" in _df_for_csv.columns and "phone_fmt" in _df_for_csv.columns:
    _df_for_csv["phone"] = _df_for_csv["phone_fmt"].where(
        _df_for_csv["phone_fmt"].astype(str).str.len() > 0,
        _df_for_csv["phone"],
    )
_csv_bytes = _df_for_csv.to_csv(index=False).encode("utf-8")

_df_for_xlsx = ensure_phone_string(_df_base.copy())
_xlsx_bytes = to_xlsx_bytes(_df_for_xlsx, text_cols=("phone", "zip"))

# Help below the controls row
with st.expander("Help Guide", expanded=False):
    st.markdown(
        """
Only recommend providers who do excellent work. If a provider's data needs an edit or update: email: randy83442@gmail.com.

This list stays useful only if we all keep it accurate.

## How to Use This List

## The Basics
- **Search:** Type in the box at the top to filter the list. (Press Enter if you typed a lot.)
- **Sort:** Use **Sort by** and **Order** to change how the rows are ordered.
- **Download:** Click **Download CSV** or **Download XLSX** to save what you're seeing.
  - If you don't type anything in search, the download includes the **entire list**.

---

## Search Tips
- The search looks through **all columns** and doesn't care about capitals.
- Try parts of words:
  - `roof` finds "Roofer" and "Roofing"
  - `inverness` finds addresses/notes with Inverness
  - `773` (or any digits) matches phone numbers with those digits
  - `medicare` finds mentions in Notes/Keywords
- See "**No matching providers.**"? Clear the search or try a shorter word.

---

## Sorting
- **Sort by:** pick the column (Provider, Category, Service, etc.).
- **Order:** **Ascending** (A→Z) or **Descending** (Z→A).
- Text sorting ignores upper/lower case.

---

## Downloads
- **CSV:** good for simple lists or sharing.
- **XLSX (Excel):** good for spreadsheets.
- Downloads reflect your **current view** (after search and sort).
- If there's nothing showing, the download buttons are disabled.

---

## What You're Looking At
- Common columns:
  - **Provider** (business name)
  - **Category** (main type of work)
  - **Service** (sub-type, if used)
  - **Contact Name**
  - **Phone**
  - **Address**
  - **Website**
  - **Notes**
- Some columns are hidden but still searchable (e.g., internal IDs, timestamps).

---

## Scrolling & Layout
- If your screen is narrow, you can scroll sideways to see more columns.
- The table area scrolls vertically; use your mouse wheel or trackpad.
- Long **Notes** and **Address** fields will wrap to new lines.

---

## If You're Not Finding What You Need
1. Clear the search box and press Enter.
2. Check spelling.
3. Try a shorter part of the word (e.g., `plumb`, `medic`).

---

## What You Can't Do Here
- This page is **read-only**. You can't add, edit, or delete providers here.
- To suggest a change, email **randy83442@gmail.com**.

---

## Quick Keyboard Tips
- **Tab / Shift+Tab** moves between controls.
- **Enter** applies the search.
        """,
    )


def _filter_for_dataframe(df_display: pd.DataFrame, term: str) -> pd.DataFrame:
    """Case-insensitive contains across common text columns (fallback mode)."""
    if not term:
        return df_display
    hay_cols = [
        c
        for c in [
            "business_name",
            "category",
            "service",
            "contact_name",
            "phone",
            "website",
            "address",
            "email",
            "notes",
            "computed_keywords",
            "keywords",
        ]
        if c in df_display.columns
    ]
    if not hay_cols:
        return df_display
    t = term.lower()
    mask = False
    for c in hay_cols:
        mask = mask | df_display[c].astype(str).str.lower().str.contains(t, na=False)
    return df_display[mask]


def _render_table(df: pd.DataFrame, quick_term: str) -> None:
    """Render read-only table using Ag-Grid when available; fallback to st.dataframe."""
    # Apply prefs and compute display frame (also remaps phone <- phone_fmt, hides must-hide)
    df2, view_cols, _hidden_cols, prefs = _apply_readonly_prefs(df)
    df = df2
    df_display = df[view_cols].copy()
    # Include hidden search-only columns so quick filter can see them
    for _c in ("computed_keywords", "keywords"):
        if _c in df.columns and _c not in df_display.columns:
            df_display[_c] = df[_c]

    # === ANCHOR: PHONE PREP (start) ===
    if "phone" in df_display.columns:
        fmt_src = (
            df.get("phone_fmt", pd.Series("", index=df.index))
            .astype("string")
            .fillna("")
            .str.strip()
        )
        raw_src = df.get("phone", pd.Series("", index=df.index)).astype("string").fillna("")
        fallback = raw_src.map(__fmt_phone_safe)
        df_display["phone"] = fmt_src.mask(fmt_src.eq(""), fallback).astype("string")
    # === ANCHOR: PHONE PREP (end) ===
    has_aggrid = _AgGrid is not None and int(prefs.get("use_aggrid", 1)) == 1

    if not has_aggrid:
        grid_h = int(prefs.get("grid_h", 420))
        vis_rows = int(prefs.get("visible_rows", 0))
        if vis_rows > 0:
            header_px = int(prefs.get("header_px", 28))
            row_px = int(prefs.get("row_px", 28))
            grid_h = header_px + vis_rows * row_px + 12
        df_filtered = _filter_for_dataframe(df_display, quick_term)
        st.dataframe(df_filtered, height=grid_h, use_container_width=False, hide_index=True)
        return

    # GridOptions via builder
    gob = GridOptionsBuilder.from_dataframe(df_display)

    # Hide CKW/keywords defensively (even though we already dropped them)
    for _col in ("computed_keywords", "keywords"):
        with suppress(Exception):
            gob.configure_column(_col, hide=True, sortable=False, filter=False, suppressMenu=True)

    # Enable sorting + multi-sort
    gob.configure_default_column(
        suppressSizeToFit=True,
        sortable=True,  # NEW: show sort icons on headers
    )
    gob.configure_grid_options(
        suppressAutoSize=True,
        multiSortKey="ctrl",  # NEW: hold Ctrl to add secondary/tertiary sorts
    )

    # JS phone formatter in-grid (if you later use valueFormatter)
    _phone_fmt_js = JsCode(
        """function(params){
      const raw = (params.value ?? "").toString();
      const extMarks = [" ext.", " ext ", " ext:", " x", " x.", " ext", " extension "];
      let base = raw;
      for (const m of extMarks){
        const idx = base.toLowerCase().indexOf(m);
        if (idx !== -1){ base = base.slice(0, idx); break; }
      }
      if (base.includes(".")) {
        const parts = base.split(".");
        const head = parts[0].trim(), tail = parts.slice(1).join(".").trim();
        if (/^\\d+$/.test(head) && /^0*$/.test(tail)) base = head;
      }
      const s = (base.match(/\\d/g) || []).join("");
      let core = "";
      if (s.length === 11 && s.startsWith("1")) core = s.slice(1);
      else if (s.length === 10) core = s;
      else {
        const t = s.startsWith("1") ? s.slice(1) : s;
        if (t.length === 10) core = t;
      }
      if (core.length === 10) {
        return "(" + core.slice(0,3) + ") " + core.slice(3,6) + "-" + core.slice(6);
      }
      return raw;
    }"""
    )

    # Width mapping (optional) from secrets
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
            with suppress(Exception):
                gob.configure_column(col, width=int(widths[lk]))
        # reasoned defaults for wrapping
        if col in ("business_name", "address", "category", "service"):
            gob.configure_column(col, wrapText=True, autoHeight=True)

    # Layout & pagination
    grid_opts = {}
    page_size = int(prefs.get("page_size", 0))
    single_page = int(prefs.get("single_page", 0))
    grid_h = int(prefs.get("grid_h", 420))

    # If a target number of visible rows is set, convert to pixels
    vis_rows = int(prefs.get("visible_rows", 0))
    if vis_rows > 0:
        header_px = int(prefs.get("header_px", 28))
        row_px = int(prefs.get("row_px", 28))
        grid_h = header_px + vis_rows * row_px + 12

    if single_page:
        grid_opts["domLayout"] = "autoHeight"
        page_size = 0
    elif page_size > 0:
        grid_opts["pagination"] = True
        grid_opts["paginationPageSize"] = page_size

    opts = gob.build()
    opts.update(grid_opts)
    opts["includeHiddenColumnsInQuickFilter"] = True

    # Hook the search box into Ag-Grid's quick filter
    if quick_term:
        opts["quickFilterText"] = quick_term

    # Stable key so client-side sort persists
    if single_page or page_size > 0:
        AgGrid(df_display, key="ro-grid", gridOptions=opts, fit_columns_on_grid_load=False)
    else:
        AgGrid(df_display, key="ro-grid", height=grid_h, gridOptions=opts)

    st.markdown(
        f"<style>.stDataFrame div[role='gridcell']{{font-size:{FONT_PX}px !important}}</style>",
        unsafe_allow_html=True,
    )


# === MAIN ===
_render_table(df, q)
