# === ANCHOR: IMPORTS (start) ===
# noop: nudge redeploy
# ruff: noqa: I001
from __future__ import annotations

# Standard library
from datetime import datetime
import contextlib
import hashlib
import hmac
import importlib
import json
import os
import pathlib
import re
import subprocess
import time
import uuid

# Third-party
import pandas as pd
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.engine import Engine
import streamlit as st

# App constants
PHONE_LEN = 10
PHONE_LEN_WITH_CC = 11
BROWSE_PREVIEW_ROWS = 20
CSV_MAX_ROWS = 1000
# === ANCHOR: IMPORTS (end) ===


# === ANCHOR: PAGE_CONFIG (start) ===
# --- Page config MUST be the first Streamlit call ---------------------------
if not globals().get("_PAGE_CFG_DONE"):
    try:  # noqa: SIM105
        st.set_page_config(
            page_title="Providers - Admin",
            layout="wide",
            initial_sidebar_state="expanded",
        )
    except Exception:
        pass
    globals()["_PAGE_CFG_DONE"] = True
# === ANCHOR: PAGE_CONFIG (end) ===

# --- Session defaults (safe no-ops) --- START
# Ensure optional callbacks/values exist to avoid KeyError at call sites.
if "_browse_help_render" not in st.session_state:
    st.session_state["_browse_help_render"] = lambda: None
# --- Session defaults (safe no-ops) --- END


# ---------------------------------------------------------------------------


# --- HCR: auto app version (no manual bumps) --------------------------------
# === ANCHOR: AUTO_VER (start) ===
def _auto_app_ver() -> str:
    # Imports moved to module top; keep function lean for Ruff.
    date = datetime.utcnow().strftime("%Y-%m-%d")
    short = os.environ.get("GITHUB_SHA", "")
    if not short:
        try:
            short = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], text=True
            ).strip()
        except Exception:
            short = "local"
    else:
        short = short[:7]
    return f"admin-{date}.{short}"


APP_VER = "auto"
if APP_VER in (None, "", "auto"):
    APP_VER = _auto_app_ver()
# ----------------------------------------------------------------------------


def _sha256_of_this_file() -> str:
    try:
        p = pathlib.Path(__file__)
        return hashlib.sha256(p.read_bytes()).hexdigest()
    except Exception:
        return ""


def _mtime_of_this_file() -> str:
    try:
        ts = pathlib.Path(__file__).stat().st_mtime
        return datetime.fromtimestamp(ts).isoformat(timespec="seconds")
    except Exception as e:
        return str(e) or ""


# --- CKW constants & secrets -------------------------------------------------
CURRENT_CKW_VER = "ckw-1"


def _get_synonyms() -> dict[str, list[str]]:
    """Return category/service synonyms. Can be overridden via secrets['CKW_SYNONYMS']."""
    try:
        s = st.secrets.get("CKW_SYNONYMS", {})
        if isinstance(s, dict):
            # Normalize to lists of str
            return {str(k): [str(x) for x in (v or [])] for k, v in s.items()}
    except Exception:
        pass
    # modest built-ins; expand later as needed
    return {
        "Window Coverings": [
            "blinds",
            "shades",
            "shutters",
            "roller shades",
            "roman shades",
            "motorized",
            "drapes",
        ],
        "Dental": ["dentist", "teeth", "cleaning", "crown", "filling"],
        "Insurance Agent": ["insurance", "homeowners", "auto", "medicare", "coverage", "policy"],
    }


def _tok(v: str) -> list[str]:
    v = (v or "").strip().lower()
    if not v:
        return []
    # split on non-letters/digits, collapse whitespace
    v = re.sub(r"[^a-z0-9]+", " ", v)
    return [t for t in v.split() if t]


def _build_ckw_row(row: dict) -> str:
    """Build computed_keywords from (business_name, category, service, notes, keywords, ckw_manual_extra)."""
    name = str(row.get("business_name") or "")
    cat = str(row.get("category") or "")
    svc = str(row.get("service") or "")
    notes = str(row.get("notes") or "")
    kws = str(row.get("keywords") or "")
    manual = str(row.get("ckw_manual_extra") or "")

    syn = _get_synonyms()
    extras: list[str] = []
    if cat in syn:
        extras.extend(syn[cat])
    if svc in syn:
        extras.extend(syn[svc])

    tokens = _tok(name) + _tok(cat) + _tok(svc) + _tok(kws) + _tok(notes) + _tok(manual) + extras
    seen: set[str] = set()
    out: list[str] = []
    for t in tokens:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return " ".join(out)


# ---------------------------------------------------------------------------#


# === ANCHOR: COMMIT_SYNC_PROBE (start) ===
def _commit_sync_probe() -> dict:
    """
    Returns a dict with file facts and PASS/FAIL checks against optional secrets:
      - EXPECTED_SHA256
      - EXPECTED_APP_VER
    """
    facts = {
        "app_ver": APP_VER,
        "file_path": __file__,
        "file_sha256": _sha256_of_this_file(),
        "file_mtime_utc": _mtime_of_this_file(),
    }
    expected_sha = _get_secret("EXPECTED_SHA256", None)
    expected_ver = _get_secret("EXPECTED_APP_VER", None)

    checks = {}
    if expected_sha:
        checks["sha256_match"] = facts["file_sha256"] == expected_sha
    if expected_ver:
        checks["app_ver_match"] = APP_VER == expected_ver  # noqa: SIM300

    return {"facts": facts, "checks": checks}


def _debug_where_am_i():
    """
    Show the runtime file path, md5, mtime, CWD, and git branch/commit (if available).
    This lets us confirm the Streamlit process is running THIS file.
    """
    try:
        path = __file__
    except NameError:
        path = "(no __file__)"
    try:
        with open(__file__, "rb") as f:
            md5 = hashlib.md5(f.read()).hexdigest()
    except Exception:
        md5 = "(md5 failed)"
    try:
        mtime = os.path.getmtime(__file__)
    except Exception:
        mtime = 0
    try:
        cwd = os.getcwd()
    except Exception:
        cwd = "(no cwd)"

    # git info (best-effort; safe if not a repo)
    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True
        ).strip()
    except Exception:
        branch = "(no git)"
    try:
        commit = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        commit = "(no git)"

    st.caption("RUNNING FILE INSPECTOR")
    st.code(
        {
            "file": path,
            "md5": md5,
            "mtime": mtime,
            "cwd": cwd,
            "git_branch": branch,
            "git_commit": commit,
        },
        language="json",
    )


# === ANCHOR: LIBSQL_REGISTER (start) ===
# ---- register libsql dialect (must be AFTER "import streamlit as st") ----
with contextlib.suppress(Exception):
    importlib.import_module("sqlalchemy_libsql")
# ---- end dialect registration ----


# --- TEMP ENGINE SHIMS (fix F821 for `engine` / `get_engine`) -----------------
def _build_engine_fallback():
    """Prefer existing build_engine(); otherwise use local SQLite as last resort."""
    try:
        # If your file defines build_engine(), prefer it.
        return build_engine()  # type: ignore[name-defined]
    except Exception:
        pass

    _db = os.getenv("DB_PATH", "providers.db")
    return create_engine(f"sqlite+pysqlite:///{_db}")


# Provide get_engine() if missing
if "get_engine" not in globals():

    def get_engine():
        return _build_engine_fallback()


# Legacy global alias to satisfy code paths that reference `engine` directly
if "engine" not in globals():
    try:
        engine = get_engine()
    except Exception:
        engine = None
# --- END TEMP ENGINE SHIMS ----------------------------------------------------


# -----------------------------
# Helpers
# -----------------------------
# --- HCR: h-scroll wrapper + column widths ---------------------------------
def _apply_column_widths(df, widths: dict) -> dict:
    cfg = {}
    for col in df.columns:
        w = widths.get(col)
        if w is None:
            continue
        try:  # noqa: SIM105
            cfg[col] = st.column_config.Column(width=int(w))
        except Exception:
            pass
    return cfg


def _sanitize_seed_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize seed CSV to the current address-only schema."""
    df = df.copy()
    # normalize headers
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    # drop legacy cols we no longer store
    for _c in ("city", "state", "zip"):
        if _c in df.columns:
            df.drop(columns=[_c], inplace=True)
    # allow-only known vendor columns
    whitelist = [
        "category",
        "service",
        "business_name",
        "contact_name",
        "phone",
        "email",
        "website",
        "address",
        "notes",
        "keywords",
        "computed_keywords",
        "ckw_version",
        "ckw_locked",
        "ckw_manual_extra",
        "phone_fmt",
        "created_at",
        "updated_at",
        "updated_by",
    ]
    present = [c for c in whitelist if c in df.columns]
    if present:
        df = df[present]
    return df.fillna("")


def render_table_hscroll(df, *, key="browse_table"):
    df = df.copy()

    def _fmt10(v: str) -> str:
        s = re.sub(r"\D+", "", str(v or ""))
        if len(s) == PHONE_LEN_WITH_CC and s.startswith("1"):
            s = s[1:]
        return f"({s[0:3]}) {s[3:6]}-{s[6:10]}" if len(s) == PHONE_LEN else s

    cols_lower = {c.lower(): c for c in df.columns}

    # format phone (or derive from phone_fmt if phone missing)
    if "phone" in cols_lower:
        c = cols_lower["phone"]
        df[c] = df[c].map(_fmt10)
    elif "phone_fmt" in cols_lower:
        c = cols_lower["phone_fmt"]
        df["phone"] = df[c].map(_fmt10)

    # hide any phone_fmt (case-insensitive exact match)
    drop_exact = [c for c in df.columns if c.strip().lower() == "phone_fmt"]

    widths = dict(st.secrets.get("COLUMN_WIDTHS_PX_ADMIN", {}))
    col_cfg = _apply_column_widths(df, widths)

    st.markdown('<div style="overflow-x:auto; padding-bottom:6px;">', unsafe_allow_html=True)
    st.dataframe(
        df.drop(
            columns=["id", "created_at", "updated_at", "ckw_locked", "ckw_version", *drop_exact],
            errors="ignore",
        ),
        use_container_width=False,
        hide_index=True,
        column_config=(col_cfg or None),
        key=key,
    )


# ----------------------------------------------------------------------------


def _as_bool(v, default=False) -> bool:
    """Best-effort boolean parse for env/secrets flags."""
    if isinstance(v, bool):
        return v
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _ensure_ckw_schema(eng) -> bool:
    """
    Ensure vendors has CKW fields and indexes. Returns True if any change was applied.
    Columns:
      - computed_keywords TEXT
      - ckw_locked INTEGER DEFAULT 0 (0/1)
      - ckw_version TEXT DEFAULT ''
      - ckw_manual_extra TEXT DEFAULT ''
    Index:
      - vendors_ckw (computed_keywords)  -- non-unique
    """
    changed = False
    with eng.begin() as cx:
        # Ensure base table exists (address-only schema; minimal set)
        cx.execute(
            sql_text("""
            CREATE TABLE IF NOT EXISTS vendors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                service TEXT,
                business_name TEXT NOT NULL,
                contact_name TEXT,
                phone TEXT,
                phone_fmt TEXT,
                email TEXT,
                address TEXT,
                website TEXT,
                notes TEXT,
                keywords TEXT,
                computed_keywords TEXT DEFAULT '',
                ckw_version TEXT DEFAULT '',
                ckw_locked INTEGER DEFAULT 0,
                ckw_manual_extra TEXT DEFAULT '',
                created_at TEXT,
                updated_at TEXT,
                updated_by TEXT
            )
        """)
        )

        # Probe live columns
        cols = [r[1] for r in cx.execute(sql_text("PRAGMA table_info(vendors)")).fetchall()]

        def addcol(col, ddl):
            nonlocal changed
            if col not in cols:
                cx.execute(sql_text(f"ALTER TABLE vendors ADD COLUMN {ddl}"))
                cols.append(col)
                changed = True

        addcol("computed_keywords", "computed_keywords TEXT DEFAULT ''")
        addcol("ckw_locked", "ckw_locked INTEGER DEFAULT 0")
        addcol("ckw_version", "ckw_version TEXT DEFAULT ''")
        addcol("ckw_manual_extra", "ckw_manual_extra TEXT DEFAULT ''")

        # Ensure index on computed_keywords (use a stable name)
        idx_rows = cx.execute(sql_text("PRAGMA index_list(vendors)")).fetchall()
        idx_names = {r[1] for r in idx_rows}
        if "vendors_ckw" not in idx_names:
            cx.execute(sql_text("CREATE INDEX vendors_ckw ON vendors(computed_keywords)"))
            changed = True

    return changed


# --- Helper: column widths from secrets ---
def _column_config_from_secrets(cols: list[str]) -> dict:
    cfg = {}
    widths = st.secrets.get("COLUMN_WIDTHS_PX_ADMIN", {}) or {}
    for c in cols:
        w = widths.get(c)
        if isinstance(w, int) and w > 0:
            cfg[c] = st.column_config.Column(width=w)
    return cfg


# --- engine resolver (safe fallback) ---
# === ANCHOR: ENSURE_ENGINE (start) ===
def _ensure_engine(eng):
    if eng is not None:
        return eng
    try:
        e, _info = build_engine()
        return e
    except Exception:
        return None


# --- CKW schema probe ---

# --- CKW write hooks (Add/Edit) ---------------------------------------------


def _ckw_for_form_row(data: dict) -> tuple[str, str]:
    """Return (computed_keywords, ckw_version) for a form row, unless locked."""
    locked_raw = data.get("ckw_locked", 0)
    try:
        locked = bool(int(str(locked_raw)))
    except Exception:
        locked = False
    if locked:
        # keep existing CKW; version unchanged
        return (str(data.get("computed_keywords") or ""), str(data.get("ckw_version") or ""))
    return (_build_ckw_row(data), CURRENT_CKW_VER)


# ---------------------------------------------------------------------------#


# --- CKW schema ensure -------------------------------------------------------


# --- CKW-first filter (read-only) ---
def _filter_df_ckw_first(df, q: str):
    if not isinstance(q, str) or not q.strip():
        return df
    q = q.strip()
    try:
        if "ckw" in df.columns:
            m = df["ckw"].astype(str).str.contains(q, case=False, na=False)
            # widen a bit to avoid false negatives on typos
            widen_cols = [
                c
                for c in ["business_name", "service", "category", "keywords", "notes"]
                if c in df.columns
            ]
            if widen_cols:
                m = m | df[widen_cols].astype(str).apply(
                    lambda s: s.str.contains(q, case=False, na=False)
                ).any(axis=1)
            return df[m]
        else:
            widen_cols = [
                c
                for c in ["business_name", "service", "category", "keywords", "notes"]
                if c in df.columns
            ]
            if not widen_cols:
                return df
            m = (
                df[widen_cols]
                .astype(str)
                .apply(lambda s: s.str.contains(q, case=False, na=False))
                .any(axis=1)
            )
            return df[m]
    except Exception:
        return df


def _get_secret(name: str, default: str | None = None) -> str | None:
    """Prefer Streamlit secrets, fallback to environment, then default."""
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass
    return os.getenv(name, default)


# Deterministic resolution (secrets -> env -> code default)
def _resolve_bool(name: str, code_default: bool) -> bool:
    v = _get_secret(name, None)
    return _as_bool(v, default=code_default)


# === ANCHOR: RESOLVE_STR (start) ===
def _resolve_str(name: str, code_default: str | None) -> str | None:
    v = _get_secret(name, None)
    return v if v is not None else code_default


def _ct_equals(a: str, b: str) -> bool:
    """Constant-time string compare for secrets."""
    return hmac.compare_digest((a or ""), (b or ""))


# -----------------------------
# Hrana/libSQL transient error retry
# -----------------------------
def _is_hrana_stale_stream_error(err: Exception) -> bool:
    s = str(err).lower()
    return ("hrana" in s and "404" in s and "stream not found" in s) or ("stream not found" in s)


def _exec_with_retry(engine: Engine, sql: str, params: dict | None = None, *, tries: int = 2):
    """
    Execute a write (INSERT/UPDATE/DELETE) with a one-time retry on Hrana 'stream not found'.
    Returns the result proxy so you can read .rowcount.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            with engine.begin() as conn:
                return conn.execute(sql_text(sql), params or {})
        except Exception as e:
            if attempt < tries and _is_hrana_stale_stream_error(e):
                try:  # noqa: SIM105
                    engine.dispose()  # drop pooled connections
                except Exception:
                    pass
                time.sleep(0.2)
                continue
            raise


def _fetch_with_retry(
    engine: Engine, sql: str, params: dict | None = None, *, tries: int = 2
) -> pd.DataFrame:
    """
    Execute a read (SELECT) with a one-time retry on Hrana 'stream not found'.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            with engine.connect() as conn:
                res = conn.execute(sql_text(sql), params or {})
                return pd.DataFrame(res.mappings().all())
        except Exception as e:
            if attempt < tries and _is_hrana_stale_stream_error(e):
                try:  # noqa: SIM105
                    engine.dispose()
                except Exception:
                    pass
                time.sleep(0.2)
                continue
            raise


# === Helper: normalize Browse DF (order, phone formatting, hidden cols) ===
PHONE_LEN = 10
PHONE_LEN_WITH_CC = 11


def _normalize_browse_df(df, *, hidden_cols=None):
    """Return (df, view_cols, hidden_cols) for Browse rendering."""
    # ---- phone display normalization (idempotent) ----
    if "phone" in df.columns:
        def _fmt_phone_local(raw: object) -> str:
            s = "".join(ch for ch in str(raw or "") if ch.isdigit())
            if len(s) == PHONE_LEN_WITH_CC and s.startswith("1"):
                s = s[1:]
            return (
                f"({s[0:3]}) {s[3:6]}-{s[6:10]}"
                if len(s) == PHONE_LEN
                else (str(raw or "").strip())
            )
        df["phone"] = df["phone"].map(_fmt_phone_local).fillna("")

    # Hide auxiliary phone column if present
    if "phone_fmt" in df.columns and isinstance(hidden_cols, set):
        hidden_cols.add("phone_fmt")

    hidden_cols = set(hidden_cols or [])

    # Tolerate legacy columns if present
    for legacy in ("city", "state", "zip"):
        if legacy in df.columns:
            hidden_cols.add(legacy)

    # Ensure phone_fmt exists (if raw 'phone' present)
    if "phone_fmt" not in df.columns and "phone" in df.columns:

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

    # Display phone as formatted under 'phone' and keep phone_fmt hidden
    def _fmt_local(raw):
        s = "".join(ch for ch in str(raw or "") if ch.isdigit())
        if len(s) == PHONE_LEN_WITH_CC and s.startswith("1"):
            s = s[1:]
        return f"({s[0:3]}) {s[3:6]}-{s[6:10]}" if len(s) == PHONE_LEN else (str(raw or "").strip())

    if "phone_fmt" in df.columns:
        df["phone"] = df["phone_fmt"].apply(_fmt_local)
    elif "phone" in df.columns:
        df["phone"] = df["phone"].apply(_fmt_local)

    # Never show phone_fmt directly
    hidden_cols.add("phone_fmt")

    # Secrets-driven order: prefer 'phone' just after 'service'; never include 'phone_fmt'
    browse_order = list(st.secrets.get("BROWSE_ORDER", []))
    if browse_order:
        with contextlib.suppress(ValueError):
            browse_order.remove("phone_fmt")
        if "phone" not in browse_order:
            try:
                i = browse_order.index("service") + 1
            except ValueError:
                i = 0
            browse_order.insert(i, "phone")
    else:
        seed = ["business_name", "address", "category", "service", "phone"]
        browse_order = [c for c in seed if c in df.columns] + [
            c for c in df.columns if c not in set(seed)
        ]

    # Visible/view columns (ordered)
    visible_cols = [c for c in df.columns if c not in hidden_cols]
    if browse_order:
        view_cols = [c for c in browse_order if c in visible_cols]
        view_cols += [c for c in visible_cols if c not in view_cols]
    else:
        view_cols = visible_cols

    return df, view_cols, hidden_cols


# moved into __HCR_browse_render() / _normalize_browse_df() after df exists


# --- CKW recompute utilities -------------------------------------------------
def _fetch_vendor_rows_by_ids(eng, ids: list[int]) -> list[dict]:
    if not ids:
        return []
    ph = ",".join("?" for _ in ids)
    sql = f"SELECT * FROM vendors WHERE id IN ({ph})"
    with eng.begin() as cx:
        rows = cx.exec_driver_sql(sql, ids).mappings().all()
    return [dict(r) for r in rows]


def _engine():
    """Return a real SQLAlchemy Engine, unwrapping tuples from get_engine()."""
    eng_raw = get_engine()
    return eng_raw[0] if isinstance(eng_raw, tuple) else eng_raw


def _hscroll_container_open():
    st.markdown(
        '<div style="overflow-x:auto; -webkit-overflow-scrolling:touch;">',
        unsafe_allow_html=True,
    )


def _hscroll_container_close():
    """Close the horizontal scroll container."""
    st.markdown("</div>", unsafe_allow_html=True)


def __HCR_browse_render():
    """Canonical Browse renderer: secrets-driven order/widths, hide meta cols, CSV export of visible columns."""

    # Engine + load
    try:
        eng = _engine()
        df = pd.read_sql("SELECT * FROM vendors", eng)
    except Exception as e:
        st.error(f"Browse load failed: {e}")
        return

    # Hidden/meta columns -- single source of truth
    hidden_cols_default = {
        "id",
        "created_at",
        "updated_at",
        "updated_by",
        "ckw",
        "ckw_locked",
        "ckw_version",
        "ckw_manual_extra",
        "computed_keywords",
        "phone_fmt",  # hide: we display formatted value under 'phone'
    }

    # Normalize DF and derive ordered view columns
    df, view_cols, _hidden_cols = _normalize_browse_df(df, hidden_cols=hidden_cols_default)

    # Render (widths handled elsewhere; keep container width False to honor pixel widths)
    _hscroll_container_open()
    try:
        st.dataframe(
            df[view_cols],
            hide_index=True,
            use_container_width=False,
        )
    finally:
        _hscroll_container_close()

    # CSV download of visible view
    try:
        csv = df[view_cols].to_csv(index=False)
        st.download_button(
            "Download CSV (visible columns)", csv, file_name="providers.csv", mime="text/csv"
        )
    except Exception as e:
        st.info(f"CSV export unavailable: {e}")

    # Render (keep horizontal scroll via wrapper)
    _hscroll_container_open()
    try:
        st.dataframe(
            df[view_cols],
            use_container_width=False,  # keep h-scroll
            hide_index=True,
        )
    finally:
        _hscroll_container_close()

    # Export exactly the visible columns (same order)
    try:
        csv = df[view_cols].to_csv(index=False)
        st.download_button(
            label="Download CSV (visible columns)",
            data=csv,
            file_name="providers_visible.csv",
            mime="text/csv",
            use_container_width=True,
        )
    except Exception as ex:
        st.warning(f"CSV export unavailable: {ex}")


def _update_ckw_for_rows(eng, rows: list[dict], override_locks: bool) -> int:
    if not rows:
        return 0
    upd = 0
    with eng.begin() as cx:
        for r in rows:
            if not override_locks and (r.get("ckw_locked") in (1, "1", True)):
                continue
            new_ckw = _build_ckw_row(r)
            cx.execute(
                sql_text("""
                UPDATE vendors
                   SET computed_keywords = :ckw,
                       ckw_version = :ver
                 WHERE id = :id
            """),
                {"ckw": new_ckw, "ver": CURRENT_CKW_VER, "id": r["id"]},
            )
            upd += 1
    return upd


def recompute_ckw_for_ids(eng, ids: list[int], override_locks: bool = False) -> int:
    rows = _fetch_vendor_rows_by_ids(eng, ids)
    return _update_ckw_for_rows(eng, rows, override_locks)


def recompute_ckw_unlocked(eng) -> int:
    with eng.begin() as cx:
        ids = [
            r[0]
            for r in cx.execute(
                sql_text("""
            SELECT id FROM vendors
            WHERE COALESCE(ckw_locked,0)=0
        """)
            ).fetchall()
        ]
    return recompute_ckw_for_ids(eng, ids, override_locks=False)


def recompute_ckw_all(eng) -> int:
    with eng.begin() as cx:
        ids = [r[0] for r in cx.execute(sql_text("SELECT id FROM vendors")).fetchall()]
    return recompute_ckw_for_ids(eng, ids, override_locks=True)


# ---------- Form state helpers (Add / Edit / Delete) ----------
# Add form keys
ADD_FORM_KEYS = [
    "add_business_name",
    "add_category",
    "add_service",
    "add_contact_name",
    "add_phone",
    "add_address",
    "add_website",
    "add_notes",
    "add_keywords",
]


def _init_add_form_defaults():
    for k in ADD_FORM_KEYS:
        if k not in st.session_state:
            st.session_state[k] = ""
    st.session_state.setdefault("add_form_version", 0)
    st.session_state.setdefault("_pending_add_reset", False)
    st.session_state.setdefault("add_last_done", None)
    st.session_state.setdefault("add_nonce", uuid.uuid4().hex)


def _apply_add_reset_if_needed():
    """Apply queued reset BEFORE rendering widgets to avoid invalid-option errors."""
    if st.session_state.get("_pending_add_reset"):
        for k in ADD_FORM_KEYS:
            st.session_state[k] = ""
        st.session_state["_pending_add_reset"] = False
        st.session_state["add_form_version"] += 1


def _queue_add_form_reset():
    st.session_state["_pending_add_form_reset"] = True
    st.session_state["_pending_add_reset"] = True


# Edit form keys
EDIT_FORM_KEYS = [
    "edit_vendor_id",
    "edit_business_name",
    "edit_category",
    "edit_service",
    "edit_contact_name",
    "edit_phone",
    "edit_address",
    "edit_website",
    "edit_notes",
    "edit_keywords",
    "edit_row_updated_at",
    "edit_last_loaded_id",
]


def _init_edit_form_defaults():
    defaults = {
        "edit_vendor_id": None,
        "edit_business_name": "",
        "edit_category": "",
        "edit_service": "",
        "edit_contact_name": "",
        "edit_phone": "",
        "edit_address": "",
        "edit_website": "",
        "edit_notes": "",
        "edit_keywords": "",
        "edit_row_updated_at": None,
        "edit_last_loaded_id": None,
    }
    for k, v in defaults.items():
        st.session_state.setdefault(k, v)
    st.session_state.setdefault("edit_form_version", 0)
    st.session_state.setdefault("_pending_edit_reset", False)
    st.session_state.setdefault("edit_last_done", None)
    st.session_state.setdefault("edit_nonce", uuid.uuid4().hex)


def _apply_edit_reset_if_needed():
    """
    Apply queued reset BEFORE rendering edit widgets.
    # Also clear the selection (edit_vendor_id) and the selectbox key so the UI returns to "-- Select --".
    """
    if st.session_state.get("_pending_edit_reset"):
        # Clear all edit fields AND selection
        for k in EDIT_FORM_KEYS:
            if k == "edit_vendor_id":  # noqa: SIM114
                st.session_state[k] = None
            elif k in ("edit_row_updated_at", "edit_last_loaded_id"):
                st.session_state[k] = None
            else:
                st.session_state[k] = ""
        # Also drop the legacy selectbox label key if present (from older builds)
        if "edit_provider_label" in st.session_state:
            del st.session_state["edit_provider_label"]
        st.session_state["_pending_edit_reset"] = False
        st.session_state["edit_form_version"] += 1


def _queue_edit_form_reset():
    st.session_state["_pending_edit_form_reset"] = True
    st.session_state["_pending_edit_reset"] = True


# Delete form keys
DELETE_FORM_KEYS = ["delete_vendor_id"]


def _init_delete_form_defaults():
    st.session_state.setdefault("delete_vendor_id", None)
    st.session_state.setdefault("delete_form_version", 0)
    st.session_state.setdefault("_pending_delete_reset", False)
    st.session_state.setdefault("delete_last_done", None)
    st.session_state.setdefault("delete_nonce", uuid.uuid4().hex)


def _apply_delete_reset_if_needed():
    if st.session_state.get("_pending_delete_reset"):
        st.session_state["delete_vendor_id"] = None
        # Also clear the delete selectbox UI key so it resets to sentinel
        if "delete_provider_label" in st.session_state:
            del st.session_state["delete_provider_label"]
        st.session_state["_pending_delete_reset"] = False
        st.session_state["delete_form_version"] += 1


def _queue_delete_form_reset():
    st.session_state["_pending_delete_form_reset"] = True
    st.session_state["_pending_delete_reset"] = True


# Nonce helpers
def _nonce(name: str) -> str:
    return st.session_state.get(f"{name}_nonce")


def _nonce_rotate(name: str) -> None:
    st.session_state[f"{name}_nonce"] = uuid.uuid4().hex


# General-purpose key helpers (used in Category/Service admins)
def _clear_keys(*keys: str) -> None:
    for k in keys:
        if k in st.session_state:
            del st.session_state[k]


def _set_empty(*keys: str) -> None:
    for k in keys:
        st.session_state[k] = ""


def _reset_select(key: str, sentinel: str = "-- Select --") -> None:
    st.session_state[key] = sentinel


# ---------- Category / Service queued reset helpers ----------
def _init_cat_defaults():
    st.session_state.setdefault("cat_form_version", 0)
    st.session_state.setdefault("_pending_cat_reset", False)


def _apply_cat_reset_if_needed():
    if st.session_state.get("_pending_cat_reset"):
        # Clear text inputs
        st.session_state["cat_add"] = ""
        st.session_state["cat_rename"] = ""
        # Reset selects by dropping keys so they render at sentinel on next run
        for k in ("cat_old", "cat_del", "cat_reassign_to"):
            if k in st.session_state:
                del st.session_state[k]
        st.session_state["_pending_cat_reset"] = False
        st.session_state["cat_form_version"] += 1


def _queue_cat_reset():
    st.session_state["_pending_cat_form_reset"] = True
    st.session_state["_pending_cat_reset"] = True


def _init_svc_defaults():
    st.session_state.setdefault("svc_form_version", 0)
    st.session_state.setdefault("_pending_svc_reset", False)


def _apply_svc_reset_if_needed():
    if st.session_state.get("_pending_svc_reset"):
        st.session_state["svc_add"] = ""
        st.session_state["svc_rename"] = ""
        for k in ("svc_old", "svc_del", "svc_reassign_to"):
            if k in st.session_state:
                del st.session_state[k]
        st.session_state["_pending_svc_reset"] = False
        st.session_state["svc_form_version"] += 1


def _queue_svc_reset():
    st.session_state["_pending_svc_form_reset"] = True
    st.session_state["_pending_svc_reset"] = True


# -----------------------------
# Page config & CSS
# -----------------------------
PAGE_TITLE = _resolve_str("page_title", "Vendors Admin") or "Vendors Admin"
SIDEBAR_STATE = _resolve_str("sidebar_state", "expanded") or "expanded"
# set_page_config already called above; keep a marker
_PAGE_CFG_DONE = True

LEFT_PAD_PX = int(_resolve_str("page_left_padding_px", "40") or "40")

st.markdown(
    f"""
    <style>
      [data-testid="stAppViewContainer"] .main .block-container {{
        padding-left: {LEFT_PAD_PX}px !important;
        padding-right: 0 !important;
      }}
      div[data-testid="stDataFrame"] table {{ white-space: nowrap; }}
    </style>
    """,
    unsafe_allow_html=True,
)


# -----------------------------
# Admin sign-in gate (deterministic toggle)
# -----------------------------
# Code defaults (lowest precedence) -- change here if you want different code-fallbacks.
DISABLE_ADMIN_PASSWORD_DEFAULT = True  # True = bypass, False = require password
ADMIN_PASSWORD_DEFAULT = "admin"

DISABLE_LOGIN = _resolve_bool("DISABLE_ADMIN_PASSWORD", DISABLE_ADMIN_PASSWORD_DEFAULT)
ADMIN_PASSWORD = (_resolve_str("ADMIN_PASSWORD", ADMIN_PASSWORD_DEFAULT) or "").strip()

if DISABLE_LOGIN:
    # Bypass gate
    pass
else:
    if not ADMIN_PASSWORD:
        st.error("ADMIN_PASSWORD is not set (Secrets/Env).")
        st.stop()
    if "auth_ok" not in st.session_state:
        st.session_state["auth_ok"] = False
    if not st.session_state["auth_ok"]:
        st.subheader("Admin sign-in")
        pw = st.text_input("Password", type="password", key="admin_pw")
        if st.button("Sign in"):
            if _ct_equals((pw or "").strip(), ADMIN_PASSWORD):
                st.session_state["auth_ok"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
        st.stop()


# -----------------------------
# DB helpers
# -----------------------------
REQUIRED_VENDOR_COLUMNS: list[str] = ["business_name", "category"]  # service optional


# === ANCHOR: BUILD_ENGINE (start) ===
def build_engine() -> tuple[Engine, dict]:
    """Prefer Turso/libsql embedded replica; otherwise local sqlite if FORCE_LOCAL=1."""
    info: dict = {}

    url = (_resolve_str("TURSO_DATABASE_URL", "") or "").strip()
    token = (_resolve_str("TURSO_AUTH_TOKEN", "") or "").strip()
    embedded_path = os.path.abspath(
        _resolve_str("EMBEDDED_DB_PATH", "vendors-embedded.db") or "vendors-embedded.db"
    )

    if not url:
        # No remote configured -> use DB_PATH from secrets/env (defaults to vendors.db)
        db_path = _resolve_str("DB_PATH", "vendors.db") or "vendors.db"
        eng = create_engine(
            f"sqlite:///{db_path}",
            pool_pre_ping=True,
            pool_recycle=300,
            pool_reset_on_return="commit",
        )
        info.update(
            {
                "using_remote": False,
                "sqlalchemy_url": f"sqlite:///{db_path}",
                "dialect": eng.dialect.name,
                "driver": getattr(eng.dialect, "driver", ""),
            }
        )
        return eng, info

    # Embedded replica: local file that syncs to your remote Turso DB
    try:
        # Normalize sync_url: embedded REQUIRES libsql:// (no sqlite+libsql, no ?secure=true)
        raw = url
        if raw.startswith("sqlite+libsql://"):
            host = raw.split("://", 1)[1].split("?", 1)[0]  # drop any ?secure=true
            sync_url = f"libsql://{host}"
        else:
            sync_url = raw.split("?", 1)[0]  # already libsql://...

        eng = create_engine(
            f"sqlite+libsql:///{embedded_path}",
            connect_args={
                "auth_token": token,
                "sync_url": sync_url,
            },
            pool_pre_ping=True,
            pool_recycle=300,
            pool_reset_on_return="commit",
        )
        with eng.connect() as c:
            c.exec_driver_sql("select 1;")

        info.update(
            {
                "using_remote": True,
                "strategy": "embedded_replica",
                "sqlalchemy_url": f"sqlite+libsql:///{embedded_path}",
                "dialect": eng.dialect.name,
                "driver": getattr(eng.dialect, "driver", ""),
                "sync_url": sync_url,
            }
        )
        return eng, info

    except Exception as e:
        info["remote_error"] = f"{e}"
        allow_local = _as_bool(os.getenv("FORCE_LOCAL"), False)
        if allow_local:
            eng = create_engine(
                "sqlite:///vendors.db",
                pool_pre_ping=True,
                pool_recycle=300,
                pool_reset_on_return="commit",
            )
            info.update(
                {
                    "using_remote": False,
                    "sqlalchemy_url": "sqlite:///vendors.db",
                    "dialect": eng.dialect.name,
                    "driver": getattr(eng.dialect, "driver", ""),
                }
            )
            return eng, info

        st.error("Remote DB unavailable and FORCE_LOCAL is not set. Aborting to protect data.")
        raise


def ensure_schema(engine: Engine) -> None:
    # Vendors table aligned to app code: no city/state/zip; includes email + phone_fmt
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS vendors (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          category TEXT NOT NULL,
          service TEXT,
          business_name TEXT NOT NULL,
          contact_name TEXT,
          phone TEXT,
          phone_fmt TEXT,
          email TEXT,
          address TEXT,
          website TEXT,
          notes TEXT,
          keywords TEXT,
          computed_keywords TEXT DEFAULT '',
          ckw_version TEXT DEFAULT '',
          ckw_locked INTEGER DEFAULT 0,
          ckw_manual_extra TEXT DEFAULT '',
          created_at TEXT,
          updated_at TEXT,
          updated_by TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS categories (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS services (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_vendors_cat ON vendors(category)",
        "CREATE INDEX IF NOT EXISTS idx_vendors_bus ON vendors(business_name)",
        "CREATE INDEX IF NOT EXISTS idx_vendors_kw  ON vendors(keywords)",
        # helpful functional indexes for case-insensitive operations used by UI
        "CREATE INDEX IF NOT EXISTS idx_vendors_bus_lower ON vendors(lower(business_name))",
        "CREATE INDEX IF NOT EXISTS idx_vendors_cat_lower ON vendors(lower(category))",
        "CREATE INDEX IF NOT EXISTS idx_vendors_svc_lower ON vendors(lower(service))",
        "CREATE INDEX IF NOT EXISTS idx_vendors_phone ON vendors(phone)",
        "CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)",
    ]
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(sql_text(s))


def sync_reference_tables(engine: Engine) -> dict:
    """
    Populate categories/services from distinct non-empty values in vendors.
    Returns counts inserted for each table.
    """
    inserted = {"categories": 0, "services": 0}
    with engine.begin() as conn:
        # categories
        cats = conn.execute(
            sql_text("""
            SELECT DISTINCT TRIM(category) AS n FROM vendors
            WHERE category IS NOT NULL AND TRIM(category) <> ''
        """)
        ).fetchall()
        for (n,) in cats:
            conn.execute(sql_text("INSERT OR IGNORE INTO categories(name) VALUES(:n)"), {"n": n})
        inserted["categories"] = len(cats)

        # services
        svcs = conn.execute(
            sql_text("""
            SELECT DISTINCT TRIM(service) AS n FROM vendors
            WHERE service IS NOT NULL AND TRIM(service) <> ''
        """)
        ).fetchall()
        for (n,) in svcs:
            conn.execute(sql_text("INSERT OR IGNORE INTO services(name) VALUES(:n)"), {"n": n})
        inserted["services"] = len(svcs)

    return inserted


# === ANCHOR: SEED_IF_EMPTY_START (start) ===
# --- Seed if empty (address-only) --- START
# === ANCHOR: SEED_IF_EMPTY_DEF (start) ===
def _seed_if_empty(eng=None) -> None:
    """Seed vendors from CSV when table exists but has 0 rows (address-only schema)."""
    # Gate via secrets
    allow = int(str(st.secrets.get("ALLOW_SEED_IMPORT", "0")).strip() or "0") == 1
    if not allow:
        return
    seed_csv = str(st.secrets.get("SEED_CSV", "data/providers_seed.csv"))

    # Resolve an Engine if one wasn't provided
    eng = _ensure_engine(eng)
    if eng is None or not hasattr(eng, "begin"):
        try:  # noqa: SIM105
            st.warning("Seed-if-empty skipped: engine object invalid or missing.")
        except Exception:
            pass
        return

    # If table missing or already populated, do nothing.
    try:
        with eng.connect() as cx:
            tables = [
                r[0]
                for r in cx.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).all()
            ]
            if "vendors" not in tables:
                return
            count = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0
            if int(count) > 0:
                return
    except Exception:
        # Can't inspect; bail quietly
        return

    # Load, sanitize to address-only, align columns, append
    try:
        df = pd.read_csv(seed_csv, dtype=str).fillna("")

        # Drop legacy columns (we are address-only now)
        for _ban in ("city", "state", "zip"):
            if _ban in df.columns:
                df.drop(columns=[_ban], inplace=True)

        # Optional light cleanup
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

        # Align to live table columns to tolerate drift
        with eng.connect() as cx:
            cols = [
                r[1] for r in cx.exec_driver_sql("PRAGMA table_info(vendors)").fetchall()
            ]  # r[1] = name
        df = df.reindex(columns=[c for c in cols if c in df.columns], fill_value="")

        # Single append inside a transaction
        with eng.begin():
            df.to_sql("vendors", eng, if_exists="append", index=False)

        try:  # noqa: SIM105
            st.success(f"Seeded vendors from {seed_csv}")
        except Exception:
            pass
    except Exception as e:
        try:  # noqa: SIM105
            st.warning(f"Seed-if-empty skipped: {e}")
        except Exception:
            pass
        return


# --- end Add/Edit form submit ------------------------------------------------


# === ANCHOR: NORMALIZE_PHONE (start) ===
def _normalize_phone(val: str | None) -> str:
    if not val:
        return ""
    digits = re.sub(r"\D", "", str(val))
    if len(digits) == PHONE_LEN_WITH_CC and digits.startswith("1"):
        digits = digits[1:]
    return digits


# === ANCHOR: FORMAT_PHONE (start) ===
def _format_phone(val: str | None) -> str:
    s = re.sub(r"\D", "", str(val or ""))
    if len(s) == PHONE_LEN:
        return f"({s[0:3]}) {s[3:6]}-{s[6:10]}"
    return (val or "").strip()


def _sanitize_url(url: str | None) -> str:
    if not url:
        return ""
    url = url.strip()
    if url and not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    return url


# ------------------------------------------------------------------------
def load_df(engine: Engine) -> pd.DataFrame:
    with engine.begin() as conn:
        df = pd.read_sql(sql_text("SELECT * FROM vendors ORDER BY lower(business_name)"), conn)

    for col in [
        "contact_name",
        "phone",
        "phone_fmt",
        "email",
        "address",
        "website",
        "notes",
        "keywords",
        "service",
        "computed_keywords",
        "ckw_version",
        "ckw_locked",
        "ckw_manual_extra",
        "created_at",
        "updated_at",
        "updated_by",
    ]:
        if col not in df.columns:
            df[col] = ""

    # Display-friendly phone; storage remains digits
    df["phone_fmt"] = df["phone"].apply(_format_phone)

    return df


# === ANCHOR: LIST_NAMES (start) ===
def list_names(engine: Engine, table: str) -> list[str]:
    with engine.begin() as conn:
        rows = conn.execute(sql_text(f"SELECT name FROM {table} ORDER BY lower(name)")).fetchall()
    return [r[0] for r in rows]


def usage_count(engine: Engine, col: str, name: str) -> int:
    with engine.begin() as conn:
        cnt = conn.execute(
            sql_text(f"SELECT COUNT(*) FROM vendors WHERE {col} = :n"), {"n": name}
        ).scalar()
    return int(cnt or 0)


# -----------------------------
# CSV Restore helpers (append-only, ID-checked)
# -----------------------------
def _get_table_columns(engine: Engine, table: str) -> list[str]:
    with engine.connect() as conn:
        res = conn.execute(sql_text(f"SELECT * FROM {table} LIMIT 0"))
        return list(res)


def _fetch_existing_ids(engine: Engine, table: str = "vendors") -> set[int]:
    with engine.connect() as conn:
        rows = conn.execute(sql_text(f"SELECT id FROM {table}")).all()
    return {int(r[0]) for r in rows if r[0] is not None}


def _prepare_csv_for_append(
    engine: Engine,
    csv_df: pd.DataFrame,
    *,
    normalize_phone: bool,
    trim_strings: bool,
    treat_missing_id_as_autoincrement: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, list[int], list[str]]:
    """
    Returns: (with_id_df, without_id_df, rejected_existing_ids, insertable_columns)
    DataFrames are already filtered to allowed columns and safe to insert.
    """
    df = csv_df.copy()

    # Trim strings
    if trim_strings:
        for c in df.columns:
            if pd.api.types.is_object_dtype(df[c]):
                df[c] = df[c].astype(str).str.strip()

    # Normalize phone to digits
    if normalize_phone and "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.replace(r"\D+", "", regex=True)

    db_cols = _get_table_columns(engine, "vendors")
    insertable_cols = [c for c in df.columns if c in db_cols]

    # Required columns present?
    missing_req = [c for c in REQUIRED_VENDOR_COLUMNS if c not in df.columns]
    if missing_req:
        raise ValueError(f"Missing required column(s) in CSV: {missing_req}")

    # Handle id column
    has_id = "id" in df.columns
    existing_ids = _fetch_existing_ids(engine)

    if has_id:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        # Reject rows colliding with existing ids
        mask_conflict = df["id"].notna() & df["id"].astype("Int64").astype(
            "int", errors="ignore"
        ).isin(existing_ids)
        rejected_existing_ids = df.loc[mask_conflict, "id"].dropna().astype(int).tolist()
        df_ok = df.loc[~mask_conflict].copy()

        # Split by having id vs. not
        with_id_df = df_ok[df_ok["id"].notna()].copy()
        without_id_df = (
            df_ok[df_ok["id"].isna()].copy()
            if treat_missing_id_as_autoincrement
            else pd.DataFrame(columns=df.columns)
        )
    else:
        rejected_existing_ids = []
        with_id_df = pd.DataFrame(columns=df.columns)
        without_id_df = df.copy()

    # Limit to insertable columns and coerce NaN->None for DB
    def _prep_cols(d: pd.DataFrame, drop_id: bool) -> pd.DataFrame:
        cols = [c for c in insertable_cols if (c != "id" if drop_id else True)]
        if not cols:
            return pd.DataFrame(columns=[])
        dd = d[cols].copy()
        for c in cols:
            dd[c] = dd[c].where(pd.notnull(dd[c]), None)
        return dd

    with_id_df = _prep_cols(with_id_df, drop_id=False)
    without_id_df = _prep_cols(without_id_df, drop_id=True)

    # Duplicate ids inside the CSV itself?
    if "id" in csv_df.columns:
        dup_ids = (
            csv_df["id"]
            .pipe(pd.to_numeric, errors="coerce")
            .dropna()
            .astype(int)
            .duplicated(keep=False)
        )
        if dup_ids.any():
            dups = sorted(csv_df.loc[dup_ids, "id"].dropna().astype(int).unique().tolist())
            raise ValueError(f"Duplicate id(s) inside CSV: {dups}")

    return with_id_df, without_id_df, rejected_existing_ids, insertable_cols


def _execute_append_only(
    engine: Engine,
    with_id_df: pd.DataFrame,
    without_id_df: pd.DataFrame,
    insertable_cols: list[str],
) -> int:
    """Executes INSERTs in a single transaction. Returns total inserted rows."""
    inserted = 0
    with engine.begin() as conn:
        # with explicit id
        if not with_id_df.empty:
            cols = list(with_id_df.columns)  # includes 'id' by construction
            placeholders = ", ".join(":" + c for c in cols)
            stmt = sql_text(f"INSERT INTO vendors ({', '.join(cols)}) VALUES ({placeholders})")
            conn.execute(stmt, with_id_df.to_dict(orient="records"))
            inserted += len(with_id_df)

        # without id (autoincrement)
        if not without_id_df.empty:
            cols = list(without_id_df.columns)  # 'id' removed already
            placeholders = ", ".join(":" + c for c in cols)
            stmt = sql_text(f"INSERT INTO vendors ({', '.join(cols)}) VALUES ({placeholders})")
            conn.execute(stmt, without_id_df.to_dict(orient="records"))
            inserted += len(without_id_df)

    return inserted


# Patch 5 (2025-10-24): PAGE_SIZE from secrets (bounded, session-backed)
# Reads PAGE_SIZE from st.secrets (int), bounds it [20..1000], default 200,
# exposes get_page_size() and caches it in st.session_state["PAGE_SIZE"].
# ------------------------------------------------------------------------
def _coerce_int(_v, _default):
    try:
        if isinstance(_v, int | float):
            return int(_v)
        if isinstance(_v, str) and _v.strip().lstrip("+-").isdigit():
            return int(_v.strip())
    except Exception:
        pass
    return int(_default)


def _ensure_page_size_in_state():
    try:
        sec = st.secrets
    except Exception:
        sec = {}
    raw = sec.get("PAGE_SIZE", 200)
    n = _coerce_int(raw, 200)
    n = max(20, min(1000, n))
    st.session_state["PAGE_SIZE"] = n


def get_page_size() -> int:
    """Return the effective PAGE_SIZE (from secrets, bounded)."""
    v = st.session_state.get("PAGE_SIZE")
    if isinstance(v, int) and BROWSE_PREVIEW_ROWS <= v <= CSV_MAX_ROWS:
        return v
    _ensure_page_size_in_state()
    return int(st.session_state.get("PAGE_SIZE", 200))


# Patch 11 (2025-10-24): redefine _filter_df_by_query to be case-insensitive
def _filter_df_by_query(df: pd.DataFrame, qq: str | None) -> pd.DataFrame:
    """
    CKW-first, case-insensitive filter.
    Priority:
      1) computed_keywords (or legacy CKW/ckw) if present and non-empty
      2) minimal join of common text columns (fallback)
    No regex; whitespace collapsed; safe per-column coercions.
    """
    try:
        if df is None or getattr(df, "empty", True):
            return df
        s = "" if qq is None else str(qq).strip().lower()
        if s == "":
            return df

        cols = set(map(str, getattr(df, "columns", [])))

        def _minimal_src(_df: pd.DataFrame) -> pd.Series:
            pick = [
                c
                for c in ("business_name", "category", "service", "notes", "keywords")
                if c in cols
            ]
            if pick:
                return _df[pick].astype("string").fillna("").agg(" ".join, axis=1)
            return pd.Series([""] * len(_df), index=_df.index, dtype="string")

        # Prefer CKW when present and non-empty; accept 'computed_keywords', 'CKW', or 'ckw'
        ckw_col = (
            "computed_keywords"
            if "computed_keywords" in cols
            else ("CKW" if "CKW" in cols else ("ckw" if "ckw" in cols else None))
        )
        base = _minimal_src(df)
        if ckw_col:
            ckw = df[ckw_col].astype("string").fillna("")
            base = ckw.where(ckw.str.len() > 0, base)

        src = (
            base.astype("string")
            .fillna("")
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        mask = src.str.contains(s, regex=False, na=False)
        return df.loc[mask]
    except Exception as _e:
        try:  # noqa: SIM105
            st.session_state["_filter_df_error"] = str(_e)
        except Exception:
            pass
        return df


# --- initialize engine and schema (order matters) ----------------------------
engine, engine_info = build_engine()

# Ensure base tables BEFORE CKW add-ons
ensure_schema(engine)

# Now ensure CKW (both legacy 'ckw' and modern 'computed_keywords' are tolerated)
try:  # noqa: SIM105
    st.session_state.get("_ckw_schema_ensure", _ensure_ckw_schema)(engine)
except Exception:
    pass

try:  # noqa: SIM105
    st.session_state["_ENGINE"] = engine
except Exception:
    pass

# Optionally seed (guarded)
_seed_if_empty(engine)

try:  # noqa: SIM105
    sync_reference_tables(engine)
except Exception:
    pass

# Apply WAL PRAGMAs for local SQLite (not libsql driver)
try:
    if not engine_info.get("using_remote", False) and engine_info.get("driver", "") != "libsql":
        with engine.begin() as _conn:
            _conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
            _conn.exec_driver_sql("PRAGMA synchronous=NORMAL;")
except Exception:
    pass

_tabs = st.tabs(
    [
        "Browse Providers",
        "Add / Edit / Delete Provider",
        "Category Admin",
        "Service Admin",
        "Maintenance",
        "Debug",
    ]
)
# (removed duplicate top-level Browse render; only _tabs[0] -> __HCR_browse_render() remains)


# (removed unused _browse_help_block; help is handled by the HCR Help -- Browse section)

# --- HCR: Help -- Browse (secrets-driven) -----------------------------------
func = st.session_state.get("_browse_help_render")
if callable(func):
    func()
# ---- end Help -- Browse ----

# === ANCHOR: TABS_BROWSE_ENTER (start) ===
with _tabs[0]:
    # === Browse (canonical, secrets-driven) ===
    __HCR_browse_render()
# === ANCHOR: TABS_BROWSE_ENTER (end) ===


# Optional top-of-browse help (intentionally disabled here; canonical Browse already rendered)
# (legacy fallback table block intentionally removed)


# NOTE: Browse table is rendered only on the Browse tab via _render_browse_table().
# NOTE: Browse table is rendered only on the Browse tab via _render_browse_table().
# ---------- Add/Edit/Delete Vendor
with _tabs[1]:
    # ===== Add Vendor =====
    st.subheader("Add Vendor")
    _init_add_form_defaults()
    _apply_add_reset_if_needed()  # apply queued reset BEFORE creating widgets

    cats = list_names(engine, "categories")
    servs = list_names(engine, "services")

    add_form_key = f"add_vendor_form_{st.session_state['add_form_version']}"
    with st.form(add_form_key, clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Provider *", key="add_business_name")

            # Category select--options include "" placeholder; we DON'T pass index when using session state
            _add_cat_options = [""] + (cats or [])
            if (st.session_state.get("add_category") or "") not in _add_cat_options:
                st.session_state["add_category"] = ""
            st.selectbox(
                "Category *",
                options=_add_cat_options,
                key="add_category",
                placeholder="Select category",
            )

            # Service select--same pattern
            _add_svc_options = [""] + (servs or [])
            if (st.session_state.get("add_service") or "") not in _add_svc_options:
                st.session_state["add_service"] = ""
            st.selectbox("Service (optional)", options=_add_svc_options, key="add_service")

            st.text_input("Contact Name", key="add_contact_name")
            st.text_input("Phone (10 digits or blank)", key="add_phone")
        with col2:
            st.text_area("Address", height=80, key="add_address")
            st.text_input("Website (https://...)", key="add_website")
            st.text_area("Notes", height=100, key="add_notes")
            st.text_input("Keywords (comma separated)", key="add_keywords")

        submitted = st.form_submit_button("Add Vendor")

    if submitted:
        add_nonce = _nonce("add")
        if st.session_state.get("add_last_done") == add_nonce:
            st.info("Add already processed.")
            st.stop()

        business_name = (st.session_state["add_business_name"] or "").strip()
        category = (st.session_state["add_category"] or "").strip()
        service = (st.session_state["add_service"] or "").strip()
        contact_name = (st.session_state["add_contact_name"] or "").strip()
        phone_norm = _normalize_phone(st.session_state["add_phone"])
        address = (st.session_state["add_address"] or "").strip()
        website = _sanitize_url(st.session_state["add_website"])
        notes = (st.session_state["add_notes"] or "").strip()
        keywords = (st.session_state["add_keywords"] or "").strip()

        # Minimal-change validation: phone must be 10 digits or blank
        if phone_norm and len(phone_norm) != PHONE_LEN:
            st.error("Phone must be 10 digits or blank.")
        elif not business_name or not category:
            st.error("Business Name and Category are required.")
        else:
            try:
                now = datetime.utcnow().isoformat(timespec="seconds")
                _exec_with_retry(
                    engine,
                    """
                    INSERT INTO vendors(category, service, business_name, contact_name, phone, address,
                                        website, notes, keywords, created_at, updated_at, updated_by)
                    VALUES(:category, NULLIF(:service, ''), :business_name, :contact_name, :phone, :address,
                           :website, :notes, :keywords, :now, :now, :user)
                    """,
                    {
                        "category": category,
                        "service": service,
                        "business_name": business_name,
                        "contact_name": contact_name,
                        "phone": phone_norm,
                        "address": address,
                        "website": website,
                        "notes": notes,
                        "keywords": keywords,
                        "now": now,
                        "user": os.getenv("USER", "admin"),
                    },
                )
                st.session_state["add_last_done"] = add_nonce
                st.success(f"Provider added: {business_name}")
                _queue_add_form_reset()
                _nonce_rotate("add")
                st.rerun()
            except Exception as e:
                st.error(f"Add failed: {e}")

    st.divider()
    st.subheader("Edit / Delete Vendor")

    df_all = load_df(engine)

    if df_all.empty:
        st.info("No vendors yet. Use 'Add Vendor' above to create your first record.")
    else:
        # Init + apply resets BEFORE rendering widgets
        _init_edit_form_defaults()
        _init_delete_form_defaults()
        _apply_edit_reset_if_needed()
        _apply_delete_reset_if_needed()

        # ----- EDIT: ID-backed selection with format_func -----
        ids = df_all["id"].astype(int).tolist()
        id_to_row = {int(r["id"]): r for _, r in df_all.iterrows()}

        def _fmt_vendor(i: int | None) -> str:
            if i is None:
                return "-- Select --"
            r = id_to_row.get(int(i), None)
            if r is None:
                return f"{i}"
            cat = r.get("category") or ""
            svc = r.get("service") or ""
            tail = " / ".join([x for x in (cat, svc) if x]).strip(" /")
            name = str(r.get("business_name") or "")
            return f"{name} -- {tail}" if tail else name

        st.selectbox(
            "Select provider to edit (type to search)",
            options=[None] + ids,  # noqa: RUF005
            format_func=_fmt_vendor,
            key="edit_vendor_id",
        )

        # Prefill only when selection changes
        if st.session_state["edit_vendor_id"] is not None:  # noqa: SIM102
            if st.session_state["edit_last_loaded_id"] != st.session_state["edit_vendor_id"]:
                row = id_to_row[int(st.session_state["edit_vendor_id"])]
                st.session_state.update(
                    {
                        "edit_business_name": row.get("business_name") or "",
                        "edit_category": row.get("category") or "",
                        "edit_service": row.get("service") or "",
                        "edit_contact_name": row.get("contact_name") or "",
                        "edit_phone": row.get("phone") or "",
                        "edit_address": row.get("address") or "",
                        "edit_website": row.get("website") or "",
                        "edit_notes": row.get("notes") or "",
                        "edit_keywords": row.get("keywords") or "",
                        "edit_row_updated_at": row.get("updated_at") or "",
                        "edit_last_loaded_id": st.session_state["edit_vendor_id"],
                    }
                )

        # -------- Edit form --------
        edit_form_key = f"edit_vendor_form_{st.session_state['edit_form_version']}"
        with st.form(edit_form_key, clear_on_submit=False):
            col1, col2 = st.columns(2)
            with col1:
                st.text_input("Provider *", key="edit_business_name")

                cats = list_names(engine, "categories")
                servs = list_names(engine, "services")

                _edit_cat_options = [""] + (cats or [])
                if (st.session_state.get("edit_category") or "") not in _edit_cat_options:
                    st.session_state["edit_category"] = ""
                st.selectbox(
                    "Category *",
                    options=_edit_cat_options,
                    key="edit_category",
                    placeholder="Select category",
                )

                _edit_svc_options = [""] + (servs or [])
                if (st.session_state.get("edit_service") or "") not in _edit_svc_options:
                    st.session_state["edit_service"] = ""
                st.selectbox("Service (optional)", options=_edit_svc_options, key="edit_service")

                st.text_input("Contact Name", key="edit_contact_name")
                st.text_input("Phone (10 digits or blank)", key="edit_phone")
            with col2:
                st.text_area("Address", height=80, key="edit_address")
                st.text_input("Website (https://...)", key="edit_website")
                st.text_area("Notes", height=100, key="edit_notes")
                st.text_input("Keywords (comma separated)", key="edit_keywords")

            edited = st.form_submit_button("Save Changes")

        if edited:
            edit_nonce = _nonce("edit")
            if st.session_state.get("edit_last_done") == edit_nonce:
                st.info("Edit already processed.")
                st.stop()

            vid = st.session_state.get("edit_vendor_id")
            if vid is None:
                st.error("Select a vendor first.")
            else:
                bn = (st.session_state["edit_business_name"] or "").strip()
                cat = (st.session_state["edit_category"] or "").strip()
                phone_norm = _normalize_phone(st.session_state["edit_phone"])
                if phone_norm and len(phone_norm) != PHONE_LEN:
                    st.error("Phone must be 10 digits or blank.")
                elif not bn or not cat:
                    st.error("Business Name and Category are required.")
                else:
                    try:
                        prev_updated = st.session_state.get("edit_row_updated_at") or ""
                        now = datetime.utcnow().isoformat(timespec="seconds")
                        res = _exec_with_retry(
                            engine,
                            """
                            UPDATE vendors
                               SET category=:category,
                                   service=NULLIF(:service, ''),
                                   business_name=:business_name,
                                   contact_name=:contact_name,
                                   phone=:phone,
                                   address=:address,
                                   website=:website,
                                   notes=:notes,
                                   keywords=:keywords,
                                   updated_at=:now,
                                   updated_by=:user
                             WHERE id=:id AND (updated_at=:prev_updated OR :prev_updated='')
                        """,
                            {
                                "category": cat,
                                "service": (st.session_state["edit_service"] or "").strip(),
                                "business_name": bn,
                                "contact_name": (
                                    st.session_state["edit_contact_name"] or ""
                                ).strip(),
                                "phone": phone_norm,
                                "address": (st.session_state["edit_address"] or "").strip(),
                                "website": _sanitize_url(st.session_state["edit_website"]),
                                "notes": (st.session_state["edit_notes"] or "").strip(),
                                "keywords": (st.session_state["edit_keywords"] or "").strip(),
                                "now": now,
                                "user": os.getenv("USER", "admin"),
                                "id": int(vid),
                                "prev_updated": prev_updated,
                            },
                        )
                        rowcount = res.rowcount or 0

                        if rowcount == 0:
                            st.warning(
                                "No changes applied (stale selection or already updated). Refresh and try again."
                            )
                        else:
                            st.session_state["edit_last_done"] = edit_nonce
                            st.success(f"Vendor updated: {bn}")
                            _queue_edit_form_reset()
                            _nonce_rotate("edit")
                            st.rerun()
                    except Exception as e:
                        st.error(f"Update failed: {e}")

        st.markdown("---")
        # Use separate delete selection (ID-backed similar approach could be added later)
        sel_label_del = st.selectbox(
            "Select provider to delete (type to search)",
            options=["-- Select --"] + [_fmt_vendor(i) for i in ids],
            key="delete_provider_label",
        )
        if sel_label_del != "-- Select --":
            # map back to id cheaply
            rev = {_fmt_vendor(i): i for i in ids}
            st.session_state["delete_vendor_id"] = int(rev.get(sel_label_del))
        else:
            st.session_state["delete_vendor_id"] = None

        del_form_key = f"delete_vendor_form_{st.session_state['delete_form_version']}"
        with st.form(del_form_key, clear_on_submit=False):
            deleted = st.form_submit_button("Delete Vendor")

        if deleted:
            del_nonce = _nonce("delete")
            if st.session_state.get("delete_last_done") == del_nonce:
                st.info("Delete already processed.")
                st.stop()

            vid = st.session_state.get("delete_vendor_id")
            if vid is None:
                st.error("Select a vendor first.")
            else:
                try:
                    row = df_all.loc[df_all["id"] == int(vid)]
                    prev_updated = (row.iloc[0]["updated_at"] if not row.empty else "") or ""
                    res = _exec_with_retry(
                        engine,
                        """
                        DELETE FROM vendors
                         WHERE id=:id AND (updated_at=:prev_updated OR :prev_updated='')
                    """,
                        {"id": int(vid), "prev_updated": prev_updated},
                    )
                    rowcount = res.rowcount or 0

                    if rowcount == 0:
                        st.warning("No delete performed (stale selection). Refresh and try again.")
                    else:
                        st.session_state["delete_last_done"] = del_nonce
                        st.success("Vendor deleted.")
                        _queue_delete_form_reset()
                        _nonce_rotate("delete")
                        st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")

# ---------- Category Admin
with _tabs[2]:
    st.caption("Category is required. Manage the reference list and reassign vendors safely.")
    _init_cat_defaults()
    _apply_cat_reset_if_needed()

    cats = list_names(engine, "categories")
    cat_opts = ["-- Select --"] + cats  # sentinel first  # noqa: RUF005

    colA, colB = st.columns(2)
    with colA:
        st.subheader("Add Category")
        new_cat = st.text_input("New category name", key="cat_add")
        if st.button("Add Category", key="cat_add_btn"):
            if not (new_cat or "").strip():
                st.error("Enter a name.")
            else:
                try:
                    _exec_with_retry(
                        engine,
                        "INSERT OR IGNORE INTO categories(name) VALUES(:n)",
                        {"n": new_cat.strip()},
                    )
                    st.success("Added (or already existed).")
                    _queue_cat_reset()
                    st.rerun()
                except Exception as e:
                    st.error(f"Add category failed: {e}")

        st.subheader("Rename Category")
        if cats:
            old = st.selectbox("Current", options=cat_opts, key="cat_old")  # no index
            new = st.text_input("New name", key="cat_rename")
            if st.button("Rename", key="cat_rename_btn"):
                if old == "-- Select --":
                    st.error("Pick a category to rename.")
                elif not (new or "").strip():
                    st.error("Enter a new name.")
                else:
                    try:
                        _exec_with_retry(
                            engine,
                            "UPDATE categories SET name=:new WHERE name=:old",
                            {"new": new.strip(), "old": old},
                        )
                        _exec_with_retry(
                            engine,
                            "UPDATE vendors SET category=:new WHERE category=:old",
                            {"new": new.strip(), "old": old},
                        )
                        st.success("Renamed and reassigned.")
                        _queue_cat_reset()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Rename category failed: {e}")

    with colB:
        st.subheader("Delete / Reassign")
        if cats:
            tgt = st.selectbox("Category to delete", options=cat_opts, key="cat_del")  # no index
            if tgt == "-- Select --":
                st.write("Select a category.")
            else:
                cnt = usage_count(engine, "category", tgt)
                st.write(f"In use by {cnt} vendor(s).")
                if cnt == 0:
                    if st.button("Delete category (no usage)", key="cat_del_btn"):
                        try:
                            _exec_with_retry(
                                engine, "DELETE FROM categories WHERE name=:n", {"n": tgt}
                            )
                            st.success("Deleted.")
                            _queue_cat_reset()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Delete category failed: {e}")
                else:
                    repl_options = ["-- Select --"] + [c for c in cats if c != tgt]
                    repl = st.selectbox(
                        "Reassign vendors to...", options=repl_options, key="cat_reassign_to"
                    )  # no index
                    if st.button("Reassign vendor then delete", key="cat_reassign_btn"):
                        if repl == "-- Select --":
                            st.error("Choose a category to reassign to.")
                        else:
                            try:
                                _exec_with_retry(
                                    engine,
                                    "UPDATE vendors SET category=:r WHERE category=:t",
                                    {"r": repl, "t": tgt},
                                )
                                _exec_with_retry(
                                    engine, "DELETE FROM categories WHERE name=:t", {"t": tgt}
                                )
                                st.success("Reassigned and deleted.")
                                _queue_cat_reset()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Reassign+delete failed: {e}")

# ---------- Service Admin
with _tabs[3]:
    st.caption("Service is optional on vendors. Manage the reference list here.")
    _init_svc_defaults()
    _apply_svc_reset_if_needed()

    servs = list_names(engine, "services")
    svc_opts = ["-- Select --"] + servs  # sentinel first  # noqa: RUF005

    colA, colB = st.columns(2)
    with colA:
        st.subheader("Add Service")
        new_s = st.text_input("New service name", key="svc_add")
        if st.button("Add Service", key="svc_add_btn"):
            if not (new_s or "").strip():
                st.error("Enter a name.")
            else:
                try:
                    _exec_with_retry(
                        engine,
                        "INSERT OR IGNORE INTO services(name) VALUES(:n)",
                        {"n": new_s.strip()},
                    )
                    st.success("Added (or already existed).")
                    _queue_svc_reset()
                    st.rerun()
                except Exception as e:
                    st.error(f"Add service failed: {e}")

        st.subheader("Rename Service")
        if servs:
            old = st.selectbox("Current", options=svc_opts, key="svc_old")  # no index
            new = st.text_input("New name", key="svc_rename")
            if st.button("Rename Service", key="svc_rename_btn"):
                if old == "-- Select --":
                    st.error("Pick a service to rename.")
                elif not (new or "").strip():
                    st.error("Enter a new name.")
                else:
                    try:
                        _exec_with_retry(
                            engine,
                            "UPDATE services SET name=:new WHERE name=:old",
                            {"new": new.strip(), "old": old},
                        )
                        _exec_with_retry(
                            engine,
                            "UPDATE vendors SET service=:new WHERE service=:old",
                            {"new": new.strip(), "old": old},
                        )
                        st.success(f"Renamed service: {old} -> {new.strip()}")
                        _queue_svc_reset()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Rename service failed: {e}")

    with colB:
        st.subheader("Delete / Reassign")
        if servs:
            tgt = st.selectbox("Service to delete", options=svc_opts, key="svc_del")  # no index
            if tgt == "-- Select --":
                st.write("Select a service.")
            else:
                cnt = usage_count(engine, "service", tgt)
                st.write(f"In use by {cnt} vendor(s).")
                if cnt == 0:
                    if st.button("Delete service (no usage)", key="svc_del_btn"):
                        try:
                            _exec_with_retry(
                                engine, "DELETE FROM services WHERE name=:n", {"n": tgt}
                            )
                            st.success("Deleted.")
                            _queue_svc_reset()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Delete service failed: {e}")
                else:
                    repl_options = ["-- Select --"] + [s for s in servs if s != tgt]
                    repl = st.selectbox(
                        "Reassign vendors to...", options=repl_options, key="svc_reassign_to"
                    )  # no index
                    if st.button("Reassign vendor then delete service", key="svc_reassign_btn"):
                        if repl == "-- Select --":
                            st.error("Choose a service to reassign to.")
                        else:
                            try:
                                _exec_with_retry(
                                    engine,
                                    "UPDATE vendors SET service=:r WHERE service=:t",
                                    {"r": repl, "t": tgt},
                                )
                                _exec_with_retry(
                                    engine, "DELETE FROM services WHERE name=:t", {"t": tgt}
                                )
                                st.success("Reassigned and deleted.")
                                _queue_svc_reset()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Reassign+delete service failed: {e}")

# ---------- Maintenance
with _tabs[4]:
    st.caption("One-click cleanups for legacy data.")

    # Quick re-sync of reference tables
    if st.button("Backfill Categories/Services from Vendors"):
        try:
            out = sync_reference_tables(engine)
            st.success(
                f"Backfilled reference tables. (categories~{out.get('categories', 0)}, services~{out.get('services', 0)})"
            )
        except Exception as e:
            st.error(f"Backfill failed: {e}")

    st.subheader("Export / Import")

    # Export full, untruncated CSV of all columns/rows
    query = "SELECT * FROM vendors ORDER BY lower(business_name)"
    with engine.begin() as conn:
        full = pd.read_sql(sql_text(query), conn)

    # Dual exports: full dataset -- formatted phones and digits-only
    full_formatted = full.copy()

    def _format_phone_digits(x: str | int | None) -> str:
        s = re.sub(r"\D+", "", str(x or ""))
        return f"({s[0:3]}) {s[3:6]}-{s[6:10]}" if len(s) == PHONE_LEN else s

    if "phone" in full_formatted.columns:
        full_formatted["phone"] = full_formatted["phone"].apply(_format_phone_digits)

    colA, colB = st.columns([1, 1])
    with colA:
        st.download_button(
            "Export all vendors (formatted phones)",
            data=full_formatted.to_csv(index=False).encode("utf-8"),
            file_name=f"providers_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.csv",
            mime="text/csv",
        )
    with colB:
        st.download_button(
            "Export all vendors (digits-only phones)",
            data=full.to_csv(index=False).encode("utf-8"),
            file_name=f"providers_raw_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.csv",
            mime="text/csv",
        )

    # --- CKW tools (NOT in an expander, to avoid nested expanders) --------------------------------
    st.subheader("CKW -- Recompute")
    c1, c2 = st.columns(2)
    if c1.button("Recompute Unlocked", help="Updates rows where ckw_locked = 0"):
        n = recompute_ckw_unlocked(get_engine())
        st.success(f"Recomputed CKW for {n} rows (unlocked).")
    if c2.button("Force Recompute ALL (override locks)", help="Updates every row, ignores locks"):
        n = recompute_ckw_all(get_engine())
        st.success(f"Force-recomputed CKW for {n} rows (ALL).")

    st.divider()

    # CSV Restore (top-level expander; not nested inside another expander)
    with st.expander("CSV Restore (Append-only, ID-checked)", expanded=False):
        st.caption(
            "WARNING: This tool only **appends** rows. "
            "Rows whose `id` already exists are **rejected**. No updates, no deletes."
        )
        uploaded = st.file_uploader(
            "Upload CSV to append into `vendors`", type=["csv"], accept_multiple_files=False
        )

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        with col1:
            dry_run = st.checkbox("Dry run (validate only)", value=True)
        with col2:
            trim_strings = st.checkbox("Trim strings", value=True)
        with col3:
            normalize_phone = st.checkbox("Normalize phone to digits", value=True)
        with col4:
            st.checkbox("Missing id -> autoincrement", value=True)

        if uploaded is not None:
            try:
                df_in = pd.read_csv(uploaded)
                with_id_df, without_id_df, rejected_ids, insertable_cols = _prepare_csv_for_append(
                    engine,
                    df_in,
                    normalize_phone=normalize_phone,
                    trim_strings=trim_strings,
                    treat_missing_id_as_autoincrement=st.session_state.get("auto_id", True),
                )

                planned_inserts = len(with_id_df) + len(without_id_df)

                st.write("**Validation summary**")
                st.write(
                    {
                        "csv_rows": (len(df_in)),
                        "insertable_columns": insertable_cols,
                        "rows_with_explicit_id": (len(with_id_df)),
                        "rows_autoincrement_id": (len(without_id_df)),
                        "rows_rejected_due_to_existing_id": rejected_ids,
                        "planned_inserts": int(planned_inserts),
                    }
                )

                if dry_run:
                    st.success("Dry run complete. No changes applied.")
                else:  # noqa: PLR5501
                    if planned_inserts == 0:
                        st.info("Nothing to insert (all rows rejected or CSV empty after filters).")
                    else:
                        inserted = _execute_append_only(
                            engine, with_id_df, without_id_df, insertable_cols
                        )
                        st.success(
                            f"Inserted {inserted} row(s). Rejected existing id(s): {rejected_ids or 'None'}"
                        )
            except Exception as e:
                st.error(f"CSV restore failed: {e}")

    st.divider()
    st.subheader("Data cleanup")

    if st.button("Normalize phone numbers & Title Case (vendors + categories/services)"):

        def to_title(s: str | None) -> str:
            return ((s or "").strip()).title()

        TEXT_COLS_TO_TITLE = [
            "category",
            "service",
            "business_name",
            "contact_name",
            "address",
            "notes",
            "keywords",
        ]

        changed_vendors = 0
        try:
            with engine.begin() as conn:
                # --- vendors table ---
                rows = conn.execute(sql_text("SELECT * FROM vendors")).fetchall()
                for r in rows:
                    row = dict(r._mapping) if hasattr(r, "_mapping") else dict(r)
                    pid = int(row["id"])

                    vals = {c: to_title(row.get(c)) for c in TEXT_COLS_TO_TITLE}
                    vals["website"] = _sanitize_url((row.get("website") or "").strip())
                    vals["phone"] = _normalize_phone(row.get("phone") or "")
                    vals["phone_fmt"] = _format_phone(row.get("phone") or "")
                    vals["id"] = pid

                    conn.execute(
                        sql_text(
                            """
                            UPDATE vendors
                               SET category=:category,
                                   service=NULLIF(:service,''),
                                   business_name=:business_name,
                                   contact_name=:contact_name,
                                   phone=:phone,
                                   phone_fmt=:phone_fmt,
                                   address=:address,
                                   website=:website,
                                   notes=:notes,
                                   keywords=:keywords
                             WHERE id=:id
                            """
                        ),
                        vals,
                    )
                    changed_vendors += 1

                # --- categories table: retitle + reconcile duplicates by case ---
                cat_rows = conn.execute(sql_text("SELECT name FROM categories")).fetchall()
                for (old_name,) in cat_rows:
                    new_name = to_title(old_name)
                    if new_name != old_name:
                        conn.execute(
                            sql_text("INSERT OR IGNORE INTO categories(name) VALUES(:n)"),
                            {"n": new_name},
                        )
                        conn.execute(
                            sql_text("UPDATE vendors SET category=:new WHERE category=:old"),
                            {"new": new_name, "old": old_name},
                        )
                        conn.execute(
                            sql_text("DELETE FROM categories WHERE name=:old"), {"old": old_name}
                        )

                # --- services table: retitle + reconcile duplicates by case ---
                svc_rows = conn.execute(sql_text("SELECT name FROM services")).fetchall()
                for (old_name,) in svc_rows:
                    new_name = to_title(old_name)
                    if new_name != old_name:
                        conn.execute(
                            sql_text("INSERT OR IGNORE INTO services(name) VALUES(:n)"),
                            {"n": new_name},
                        )
                        conn.execute(
                            sql_text("UPDATE vendors SET service=:new WHERE service=:old"),
                            {"new": new_name, "old": old_name},
                        )
                        conn.execute(
                            sql_text("DELETE FROM services WHERE name=:old"), {"old": old_name}
                        )
            st.success(
                f"Vendors normalized: {changed_vendors}. Categories/services retitled and reconciled."
            )
        except Exception as e:
            st.error(f"Normalization failed: {e}")

    # Backfill timestamps (fix NULL and empty-string)
    if st.button("Backfill created_at/updated_at when missing"):
        try:
            now = datetime.utcnow().isoformat(timespec="seconds")
            with engine.begin() as conn:
                conn.execute(
                    sql_text(
                        """
                        UPDATE vendors
                           SET created_at = CASE WHEN created_at IS NULL OR created_at = '' THEN :now ELSE created_at END,
                               updated_at = CASE WHEN updated_at IS NULL OR updated_at = '' THEN :now ELSE updated_at END
                        """
                    ),
                    {"now": now},
                )
            st.success("Backfill complete.")
        except Exception as e:
            st.error(f"Backfill failed: {e}")

    # Trim extra whitespace across common text fields (preserves newlines in notes)
if st.button("Trim whitespace in text fields (safe)"):
    try:
        changed = 0

        # use existing engine
        with engine.begin() as conn:
            rows = (
                conn.execute(
                    sql_text(
                        """
                    SELECT id, category, service, business_name, contact_name,
                           address, website, notes, keywords, phone, updated_at
                    FROM vendors
                    """
                    )
                )
                .mappings()
                .all()
            )

            def _norm(v: str) -> str:
                s = str(v or "")
                s = re.sub(r"\s+", " ", s).strip()  # collapse all whitespace to single space
                return s

            def _norm_notes(v: str) -> str:
                s = str(v or "").replace("\r\n", "\n")
                s = re.sub(r"[ \t]+", " ", s)  # collapse spaces/tabs only (keep newlines)
                s = re.sub(r"[ \t]*\n[ \t]*", "\n", s)  # trim spaces around newlines
                return s.strip()

            def _norm_phone(v: str) -> str:
                s = re.sub(r"\D+", "", str(v or ""))
                if len(s) == PHONE_LEN_WITH_CC and s.startswith("1"):
                    s = s[1:]
                return s  # store digits-only (10 if valid)

            for r in rows:
                before = dict(r)
                after = {
                    "category": _norm(before["category"]),
                    "service": _norm(before["service"]),
                    "business_name": _norm(before["business_name"]),
                    "contact_name": _norm(before["contact_name"]),
                    "address": _norm(before["address"]),
                    "website": _norm(before["website"]),
                    "notes": _norm_notes(before["notes"]),
                    "keywords": _norm(before["keywords"]),
                    "phone": _norm_phone(before["phone"]),
                }

                if any(v != (before.get(k) or "") for k, v in after.items()):
                    now = datetime.utcnow().isoformat(timespec="seconds")
                    conn.execute(
                        sql_text(
                            """
                            UPDATE vendors
                               SET category = :category,
                                   service = :service,
                                   business_name = :business_name,
                                   contact_name = :contact_name,
                                   address = :address,
                                   website = :website,
                                   notes = :notes,
                                   keywords = :keywords,
                                   phone = :phone,
                                   updated_at = :now
                             WHERE id = :id
                               AND COALESCE(updated_at,'') = COALESCE(:prev_updated,'')
                            """
                        ),
                        {
                            **after,
                            "now": now,
                            "id": before["id"],
                            "prev_updated": before.get("updated_at", ""),
                        },
                    )
                    changed += 1

        st.success(f"Trimmed whitespace for {changed} row(s).")
    except Exception as e:
        st.error(f"Trim failed: {e}")


# ---------- Debug
with _tabs[5]:
    _debug_where_am_i()
    st.info(
        f"Active DB: {engine_info.get('sqlalchemy_url')} - remote={engine_info.get('using_remote')}"
    )

with _tabs[5]:
    # Existing engine info
    st.json(engine_info)

    with engine.begin() as conn:
        vendors_cols = conn.execute(sql_text("PRAGMA table_info(vendors)")).fetchall()
        categories_cols = conn.execute(sql_text("PRAGMA table_info(categories)")).fetchall()
        services_cols = conn.execute(sql_text("PRAGMA table_info(services)")).fetchall()

        # --- Index presence (vendors) ---
        idx_rows = conn.execute(sql_text("PRAGMA index_list(vendors)")).fetchall()
        vendors_indexes = [
            {"seq": r[0], "name": r[1], "unique": bool(r[2]), "origin": r[3], "partial": bool(r[4])}
            for r in idx_rows
        ]

        # --- Null timestamp counts (quick sanity) ---
        created_at_nulls = (
            conn.execute(
                sql_text("SELECT COUNT(*) FROM vendors WHERE created_at IS NULL OR created_at=''")
            ).scalar()
            or 0
        )
        updated_at_nulls = (
            conn.execute(
                sql_text("SELECT COUNT(*) FROM vendors WHERE updated_at IS NULL OR updated_at=''")
            ).scalar()
            or 0
        )

        counts = {
            "vendors": conn.execute(sql_text("SELECT COUNT(*) FROM vendors")).scalar() or 0,
            "categories": conn.execute(sql_text("SELECT COUNT(*) FROM categories")).scalar() or 0,
            "services": conn.execute(sql_text("SELECT COUNT(*) FROM services")).scalar() or 0,
        }


# ------------------------------------------------------------------------
# Patch 1 (2025-10-24): Enable horizontal scrolling for all dataframes/tables.
# ------------------------------------------------------------------------


def _enable_horizontal_scroll() -> None:
    try:  # noqa: SIM105
        st.markdown(
            """
            <style>
            /* Make Streamlit dataframes and table containers horizontally scrollable */
            div[data-testid="stHorizontalBlock"] div[aria-live="polite"] > div:has(div[data-testid="stDataFrame"]),
            div[data-testid="stHorizontalBlock"] div[aria-live="polite"] > div:has(table) {
                overflow-x: auto !important;
                overscroll-behavior-x: contain;
                -webkit-overflow-scrolling: touch;
            }
            /* Prevent cells from forcing extreme widths; allow wrap or clip */
            div[data-testid="stDataFrame"] .st-emotion-cache-1y4p8pa,
            div[data-testid="stDataFrame"] .st-emotion-cache-1wmy9hl {
                overflow-x: auto !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
    except Exception:
        pass


_enable_horizontal_scroll()
# ------------------------------------------------------------------------

# Patch 2 (2025-10-24): Secrets-driven exact pixel column widths (global)
# ------------------------------------------------------------------------


def _apply_exact_column_widths_from_secrets() -> None:
    try:
        cfg = dict(st.secrets.get("COLUMN_WIDTHS_PX_ADMIN", {}))
        if not cfg:
            return
        # Serialize once; provide raw map; lower-cased map is generated inline in JS
        cfg_raw = {str(k): int(v) for k, v in cfg.items() if str(v).isdigit() or isinstance(v, int)}
        st.markdown(
            f"""
<script>
(function() {{
  const cfgRaw = {json.dumps(cfg_raw)};
  const cfgLow = {json.dumps({k.lower(): v for k, v in cfg_raw.items()})};
  // Utility: set width on a TH cell if its text matches a key
  function setWidth(th) {{
    if (!th) return;
    const label = (th.innerText || "").trim();
    if (!label) return;
    const keyRaw = label;
    const keyLow = label.toLowerCase();
    let px = cfgRaw[keyRaw];
    if (px === undefined) px = cfgLow[keyLow];
    if (typeof px === "number" && px > 0) {{
      th.style.width = px + "px";
      th.style.minWidth = px + "px";
      th.style.maxWidth = px + "px";
      if (th.parentElement) {{
        th.parentElement.style.width = px + "px";
        th.parentElement.style.minWidth = px + "px";
        th.parentElement.style.maxWidth = px + "px";
      }}
    }}
  }}
  function scan() {{
    document.querySelectorAll('div[data-testid="stDataFrame"] th').forEach(setWidth);
    document.querySelectorAll('table thead th').forEach(setWidth);
  }}
  const root = document.querySelector('div[data-testid="stAppViewContainer"]') || document.body;
  const obs = new MutationObserver((muts) => {{
    clearTimeout(window.__stColWidthTimer);
    window.__stColWidthTimer = setTimeout(scan, 0);
  }});
  obs.observe(root, {{ childList: true, subtree: true }});
  scan();
}})();
</script>
""",
            unsafe_allow_html=True,
        )
    except Exception:
        pass


_apply_exact_column_widths_from_secrets()
# # -------------------------------------------------------

# Patch 3 (2025-10-24): Help -- Browse helper (secrets-driven, reusable)
# ------------------------------------------------------------------------


def _as_bool_patch3(v, default=False):
    try:
        if isinstance(v, bool):
            return v
        if isinstance(v, int | float):
            return v != 0
        if isinstance(v, str):
            s = v.strip().lower()
            return s in {"1", "true", "yes", "y", "on"}
        return default
    except Exception:
        return default


def _read_text_file_patch3(path: str) -> str:
    try:
        if not path:
            return ""
        if not os.path.isabs(path):
            path = os.path.abspath(path)
        if not os.path.exists(path):
            return ""
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except Exception:
        return ""


def _load_browse_help_md() -> str:
    try:
        sec = st.secrets
    except Exception:
        return ""
    inline_md = str(sec.get("BROWSE_HELP_MD", "") or "").strip()
    file_hint = str(sec.get("BROWSE_HELP_FILE", "") or "").strip()
    file_md = _read_text_file_patch3(file_hint) if file_hint else ""
    content = file_md.strip() or inline_md
    return content


def render_browse_help_expander() -> None:
    """Render the Help -- Browse expander if SHOW_BROWSE_HELP is true and content exists."""
    try:
        sec = st.secrets
    except Exception:
        return
    show = _as_bool_patch3(sec.get("SHOW_BROWSE_HELP", False), default=False)
    if not show:
        return
    md = _load_browse_help_md()
    if not md:
        return
    with st.expander("Help -- Browse", expanded=False):
        st.markdown(md)


# Expose a callable so main/Browse can invoke without re-import details.
st.session_state["_browse_help_render"] = render_browse_help_expander


# Initialize once at import time (safe, idempotent)
_ensure_page_size_in_state()
# ------------------------------------------------------------------------
# Patch 7 (2025-10-24): CKW schema helpers (additive only; no auto-exec)
# ---------------------------------------------------------------------------------------------------------------------------------------------


def _vendors_has_column(eng, col: str) -> bool:
    try:
        with eng.connect() as cx:
            rows = cx.exec_driver_sql("PRAGMA table_info(vendors)").fetchall()
        names = {str(r[1]).lower() for r in rows}
        return col.lower() in names
    except Exception:
        return False


# (removed legacy inline browse block; canonical __HCR_browse_render() is used)
