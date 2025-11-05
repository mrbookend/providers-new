"""Read-only Providers app (minimal, failsafe)."""

from __future__ import annotations

# === ANCHOR: IMPORTS (start) ===
# stdlib
import os
import os as _os
import tempfile as _tempfile
from contextlib import suppress

# === HCR: WRITABLE DIR HELPERS (readonly) ===
from pathlib import Path, Path as _Path

# third-party
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from st_aggrid import GridOptionsBuilder, JsCode

# local
from export_utils import ensure_phone_string, to_xlsx_bytes

# === FONT SIZE (from secrets) ===
try:
    FONT_PX = int(st.secrets.get("READONLY_FONT_SIZE_PX", 14))
except Exception:
    FONT_PX = 14  # failsafe
st.markdown(
    f"""
    <style>
      :root {{ --ro-font-size: {FONT_PX}px; }}
      html, body,
      [data-testid="stAppViewContainer"],
      .block-container {{
        font-size: var(--ro-font-size) !important;
        line-height: 1.35 !important;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


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


# === HCR: WRITABLE DIR HELPERS (readonly) END ===


# === ANCHOR: CONFIG — DB PATH (start) ===
DB_PATH = _resolve_db_path()
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
# === ANCHOR: CONFIG — DB PATH (end) ===


# === ANCHOR: IMPORTS (end) ===
# === ANCHOR: PAGE CONFIG (start) ===
st.set_page_config(page_title="Providers - Read-Only", page_icon="📘", layout="wide")
# === ANCHOR: PAGE CONFIG (end) ===

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

        go.setdefault("domLayout", "autoHeight")
        go.setdefault("onFirstDataRendered", JS_RESET_ROW_HEIGHTS)
        go.setdefault("onFilterChanged", JS_RESET_ROW_HEIGHTS)
        go.setdefault("onColumnResized", JS_RESET_ROW_HEIGHTS)

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


# === SEARCH / CONTROLS ROW ===
controls_left, controls_right_csv, controls_right_xlsx = st.columns([2, 1, 1])


def __on_search_enter__():
    term = (st.session_state.get("__search_box__", "") or "").strip()
    st.session_state["__search_term__"] = term
    st.session_state["__search_box__"] = ""


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


# --- safety shim so we never crash if _fmt_phone isn't present ---
def __fmt_phone_safe(val: object) -> str:
    try:
        return _fmt_phone(val)  # use the canonical one if defined
    except NameError:
        # inline fallback formatter (NANP)
        s = str(val or "").strip()
        # strip common extension markers
        lower = s.lower()
        for mark in (" ext.", " ext ", " ext:", " x", " x.", " ext", " extension "):
            i = lower.find(mark)
            if i != -1:
                s = s[:i]
                break
        # drop float-like tails (".0", ".000")
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
        "Read-only viewer for the Providers list. Database path is resolved to a writable "
        "location. If empty and a seed CSV is available, the app imports it once at startup."
    )


def _render_table(df: pd.DataFrame) -> None:
    """Render read-only table using Ag-Grid when available; fallback to st.dataframe."""
    # Apply prefs and compute display frame (also remaps phone <- phone_fmt, hides must-hide)
    df2, view_cols, _hidden_cols, prefs = _apply_readonly_prefs(df)
    df = df2
    df_display = df[view_cols].copy()

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

    # Keep keywords searchable, but never display them
    for _col in ("computed_keywords", "keywords"):
        if _col in df_display.columns:
            df_display.drop(columns=[_col], inplace=True)

    # Detect AgGrid availability
    has_aggrid = _AgGrid is not None
    if not has_aggrid or not int(prefs.get("use_aggrid", 1)):
        st.dataframe(df_display, use_container_width=False, hide_index=True)
        return

    # GridOptions via builder
    gob = GridOptionsBuilder.from_dataframe(df_display)

    # Hide CKW/keywords defensively
    for _col in ("computed_keywords", "keywords"):
        with suppress(Exception):
            gob.configure_column(_col, hide=True, sortable=False, filter=False, suppressMenu=True)

    _phone_fmt_js = JsCode(
        """function(params){
      const raw = (params.value ?? "").toString();

      // strip extension markers before parsing
      const extMarks = [" ext.", " ext ", " ext:", " x", " x.", " ext", " extension "];
      let base = raw;
      for (const m of extMarks){
        const idx = base.toLowerCase().indexOf(m);
        if (idx !== -1){ base = base.slice(0, idx)
                break; }
      }

      // If it looks like "##########.0" (or .000), drop the decimal part first
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
        // If there's extra and it starts with 1, peel one leading '1'
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

    # Selective explicit wrap
    for _col in ("business_name", "address", "category", "service"):
        if _col in df_display.columns:
            gob.configure_column(_col, wrapText=True, autoHeight=True)

    # Layout & pagination
    grid_opts: dict = {}
    page_size = int(prefs.get("page_size", 0))
    single_page = int(prefs.get("single_page", 0))
    _header_px = int(prefs.get("header_px", 0))
    grid_h = int(prefs.get("grid_h", 420))
    _font_px = int(prefs.get("font_px", 14))

    if single_page:
        grid_opts["domLayout"] = "autoHeight"
        page_size = 0
    elif page_size > 0:
        grid_opts["pagination"] = True
        grid_opts["paginationPageSize"] = page_size

    # Force re-instantiation when widths change / when always_reset=1
    always_reset = int(prefs.get("always_reset", 1))
    _nonce = st.session_state.get("__readonly_grid_nonce__", 0)
    if always_reset:
        _nonce += 1
        st.session_state["__readonly_grid_nonce__"] = _nonce

    opts = gob.build()
    opts.update(grid_opts)

    if single_page or page_size > 0:
        AgGrid(
            df_display,
            key=f"ro-grid-{_nonce}",
            gridOptions=opts,
            fit_columns_on_grid_load=False,
        )
    else:
        AgGrid(
            df_display,
            key=f"ro-grid-{_nonce}",
            height=grid_h,
            gridOptions=opts,
        )

    st.markdown(
        f"<style>.stDataFrame div[role='gridcell']{{font-size:{FONT_PX}px !important}}</style>",
        unsafe_allow_html=True,
    )

    # Downloads (from what we rendered)
    left, mid = st.columns([1, 1])
    with left:
        csv_bytes = df_display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV", data=csv_bytes, file_name="providers.csv", mime="text/csv"
        )
    with mid:
        xlsx_bytes = to_xlsx_bytes(df_display)
        st.download_button(
            "Download XLSX",
            data=xlsx_bytes,
            file_name="providers.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# === MAIN ===
_render_table(df)
