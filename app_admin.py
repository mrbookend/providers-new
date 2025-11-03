from __future__ import annotations

# === ANCHOR: IMPORTS (start) ===
# noop: nudge redeploy
# ruff: noqa: I001

# Standard library
import contextlib
from datetime import datetime
import hashlib
import hmac
import importlib
import os
import pathlib
import re
import subprocess
import time

# Third-party
import pandas as pd
from sqlalchemy.dialects import registry as _sa_registry  # type: ignore
from sqlalchemy import create_engine, text as sql_text
from sqlalchemy.engine import Engine
import streamlit as st

# App constants
PHONE_LEN = 10
PHONE_LEN_WITH_CC = 11
BROWSE_PREVIEW_ROWS = 20
CSV_MAX_ROWS = 1000
# === ANCHOR: IMPORTS (end) ===

# === ANCHOR: NOUNS (start) ===
NOUN_SINGULAR = "Provider"
NOUN_PLURAL = "Providers"
# === ANCHOR: NOUNS (end) ===


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
# Register libsql dialect; ignore if already registered or package missing.
with contextlib.suppress(Exception):
    # Prefer explicit registry (works even without entry points)
    _sa_registry.register("libsql", "sqlalchemy_libsql", "dialect")
    # Also import the module to ensure it is loaded (harmless if already present)
    importlib.import_module("sqlalchemy_libsql")
# === ANCHOR: LIBSQL_REGISTER (end) ===


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


# --- ANCHOR: helpers (fetch by ids) (start) ---
def _fetch_vendor_rows_by_ids(eng: Engine, ids: list[int]) -> list[dict]:
    """Fetch vendors rows by integer IDs; returns list of dicts. Safe for empty list."""
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    sql = f"SELECT * FROM vendors WHERE id IN ({placeholders})"
    with eng.connect() as cx:
        rows = cx.exec_driver_sql(sql, ids).mappings().all()
    return [dict(r) for r in rows]


# --- ANCHOR: helpers (fetch by ids) (end) ---


# === ANCHOR: ENGINE (start) ===
def build_engine():
    """
    Prefer Turso/libsql when TURSO_* secrets exist; else fallback to local SQLite.
    Returns: (engine, info_dict)
    """

    # Register libsql dialect if available; ignore if already registered or package missing.
    with contextlib.suppress(Exception):
        _sa_registry.register("libsql", "sqlalchemy_libsql", "dialect")

    # Read secrets/env (env wins if both present)
    def _get_secret(name: str) -> str:
        val = os.environ.get(name)
        if val:
            return val
        try:
            if name in st.secrets:
                return str(st.secrets[name])
        except Exception:
            pass
        return ""

    turso_url = _get_secret("TURSO_DATABASE_URL")
    turso_token = _get_secret("TURSO_AUTH_TOKEN")

    # Try Turso/libsql first (only if both URL and token are present and libsql_experimental is installed)
    try:
        libsql = importlib.import_module("libsql_experimental")  # type: ignore
    except Exception:
        libsql = None

    if libsql and turso_url and turso_token:

        def _creator():
            # TLS is automatic when using https:// URL; libsql handles negotiation.
            return libsql.connect(database=turso_url, auth_token=turso_token)

        eng = create_engine(
            "sqlite+libsql://",
            creator=_creator,
            pool_pre_ping=True,
        )
        return eng, {
            "using_remote": True,
            "sqlalchemy_url": "sqlite+libsql://",
            "driver": "libsql",
            "database": turso_url,
        }

    # Fallback to local SQLite
    db_path = _get_secret("DB_PATH") or "providers.db"
    eng = create_engine(
        f"sqlite:///{db_path}",
        pool_pre_ping=True,
    )
    return eng, {
        "using_remote": False,
        "sqlalchemy_url": f"sqlite:///{db_path}",
        "driver": "pysqlite",
        "database": db_path,
    }


# === ANCHOR: ENGINE (end) ===
# === ANCHOR: DB_QUICK_PROBES (start) ===
with contextlib.suppress(Exception):
    _qp_eng, _qp_info = build_engine()
    try:
        with st.expander("DB quick probes", expanded=False):
            # 1) Ping
            ok = False
            with contextlib.suppress(Exception), _qp_eng.connect() as c:
                c.exec_driver_sql("select 1;")
                ok = True

            st.write({"ping_ok": ok})

            # 2) List tables (SQLite / libsql)
            tables: list[str] = []
            with contextlib.suppress(Exception), _qp_eng.connect() as c:
                rows = c.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).all()
                tables = [r[0] for r in rows]

            st.write({"tables": tables})

            # 3) Optional: row count for vendors (if present)
            vendors_count = None
            if "vendors" in tables:
                with contextlib.suppress(Exception), _qp_eng.connect() as c:
                    vendors_count = c.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar_one()

            st.write({"vendors_count": vendors_count})
    finally:
        with contextlib.suppress(Exception):
            _qp_eng.dispose()
# === ANCHOR: DB_QUICK_PROBES (end) ===

# === ANCHOR: DB_INDEX_PARITY (start) ===
with contextlib.suppress(Exception):
    _ip_eng, _ = build_engine()
    try:
        with st.expander("Index parity (diagnostic only)", expanded=False):
            # Note: no idx on id; SQLite INTEGER PRIMARY KEY is implicitly indexed.
            expected = {
                # keep aligned with project baseline (no-op if absent)
                "idx_vendors_phone": "CREATE INDEX IF NOT EXISTS idx_vendors_phone ON vendors(phone)",
                "idx_vendors_ckw": "CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)",
                # optional (present in some baselines)
                "idx_vendors_bus_lower": "CREATE INDEX IF NOT EXISTS idx_vendors_bus_lower ON vendors(lower(business_name))",
            }
            present: set[str] = set()
            with contextlib.suppress(Exception), _ip_eng.connect() as c:
                rows = c.exec_driver_sql("PRAGMA index_list('vendors')").mappings().all()
                present = {row["name"] for row in rows if "name" in row}

            missing = [k for k in expected if k not in present]

            st.write(
                {
                    "present_indexes": sorted(present),
                    "expected_indexes": list(expected),
                    "missing_indexes": missing,
                    "note": "Diagnostic only; no schema changes performed here.",
                }
            )

            # --- Create-missing button (idempotent; stays inside the expander & try) ---
            if missing:
                with st.container():
                    if st.button("Create missing indexes (idempotent)", type="primary"):
                        created: list[str] = []
                        with contextlib.suppress(Exception), _ip_eng.begin() as cx:
                            for name in missing:
                                if name == "idx_vendors_id":
                                    cx.exec_driver_sql(
                                        "CREATE INDEX IF NOT EXISTS idx_vendors_id ON vendors(id)"
                                    )
                                    created.append(name)
                        st.success({"created": created})
    finally:
        with contextlib.suppress(Exception):
            _ip_eng.dispose()
# === ANCHOR: DB_INDEX_PARITY (end) ===
# === ANCHOR: DB_INDEX_MAINT (start) ===
with st.expander("Index maintenance", expanded=False):
    st.caption("Create expected indexes if missing (idempotent).")
    if st.button("Create missing indexes now"):
        try:
            _fix_eng, _fix_info = build_engine()
            try:
                expected_sql = {
                    "idx_vendors_phone": "CREATE INDEX IF NOT EXISTS idx_vendors_phone ON vendors(phone)",
                    "idx_vendors_ckw": "CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)",
                    "idx_vendors_bus_lower": "CREATE INDEX IF NOT EXISTS idx_vendors_bus_lower ON vendors(lower(business_name))",
                }
                with _fix_eng.begin() as cx:
                    # Create each index; IF NOT EXISTS makes this safe to run repeatedly.
                    for _name, sql in expected_sql.items():
                        cx.exec_driver_sql(sql)
                st.success("Index creation attempted (idempotent). Re-open Index parity to verify.")
            finally:
                with contextlib.suppress(Exception):
                    _fix_eng.dispose()
        except Exception as e:
            st.error(f"Index maintenance failed: {e}")


# === ANCHOR: DB_INDEX_MAINT (end) ===
# === ANCHOR: INDEX_MAINTENANCE (drop-legacy) ===
def _drop_legacy_vendor_indexes() -> dict:
    """
    Drop legacy vendor indexes we no longer want. Idempotent & safe on SQLite/libsql.
    Returns a dict with 'attempted', 'dropped', and 'failed'.
    """
    eng = get_engine()
    # Some code paths may return (engine, meta/info). Normalize to a bare Engine.
    if isinstance(eng, tuple) and eng:
        eng = eng[0]

    legacy = [
        "idx_vendors_bus",
        "idx_vendors_cat",
        "idx_vendors_cat_lower",
        "idx_vendors_kw",
        "idx_vendors_svc_lower",
        "vendors_ckw",
    ]
    attempted, dropped, failed = [], [], []

    # Use connect(); DDL auto-commits on SQLite/libsql.
    with eng.connect() as cx:
        for name in legacy:
            attempted.append(name)
            try:
                cx.exec_driver_sql(f'DROP INDEX IF EXISTS "{name}"')
                dropped.append(name)
            except Exception as e:
                # Record failure; continue.
                failed.append((name, str(e)))

    return {"attempted": attempted, "dropped": dropped, "failed": failed}


# === ANCHOR: INDEX_MAINTENANCE_UI (drop-legacy) ===
with st.expander("Index maintenance — drop legacy vendor indexes"):
    st.warning(
        "This will drop legacy vendor indexes and keep only the three agreed ones: "
        "idx_vendors_phone, idx_vendors_ckw, idx_vendors_bus_lower. "
        "Operation is idempotent."
    )
    if st.button("Drop legacy vendor indexes now", type="primary"):
        res = _drop_legacy_vendor_indexes()
        st.success(f"Dropped: {', '.join(res['dropped']) or '(none)'}")
        st.caption(f"Attempted: {', '.join(res['attempted'])}")


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
    with eng.connect() as cx:
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


# --- ANCHOR: normalize (start) ---
def _normalize_browse_df(
    df: pd.DataFrame,
    hidden_cols: set[str],
) -> tuple[pd.DataFrame, list[str], set[str]]:
    """Return (df, ordered_view_cols, hidden_cols) after hiding legacy fields, formatting phone, and applying secrets-driven order."""
    # Hide legacy/aux columns if present
    for legacy in ("city", "state", "zip", "phone_fmt"):
        if legacy in df.columns:
            hidden_cols.add(legacy)

    # Phone: ALWAYS format into the visible 'phone' column (idempotent)
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

    # Secrets-driven order
    browse_order = list(st.secrets.get("BROWSE_ORDER", []))
    # Enforce admin browse order (CKW fields end of table)
    pref = [c for c in browse_order if c in df.columns]
    rest = [c for c in df.columns if c not in pref]
    view_cols = pref + rest
    df = df.loc[:, view_cols]

    if browse_order:
        # keep only visible columns from the secret
        seed = [c for c in browse_order if c in df.columns and c not in hidden_cols]
    else:
        # default: category → service → phone → business_name → address
        default_seed = ["category", "service", "phone", "business_name", "address"]
        seed = [c for c in default_seed if c in df.columns and c not in hidden_cols]

    # Visible/view columns (ordered)
    visible_cols = [c for c in df.columns if c not in hidden_cols]
    # Build final view columns: seed first, then remaining visible columns in existing order
    view_cols = seed + [c for c in visible_cols if c not in set(seed)]
    # --- ANCHOR: ADMIN BROWSE - CKW last column (start) ---
    # Ensure 'computed_keywords' is visible and placed as the last column in Admin Browse.
    if "computed_keywords" in df.columns:
        # Unhide if previously hidden
        if "computed_keywords" in hidden_cols:
            hidden_cols = [c for c in hidden_cols if c != "computed_keywords"]
        # Move to end (dedupe first if already present)
        if "computed_keywords" in view_cols:
            view_cols = [c for c in view_cols if c != "computed_keywords"]
        view_cols.append("computed_keywords")
    # --- ANCHOR: ADMIN BROWSE - CKW last column (end) ---

    # --- ANCHOR: ADMIN BROWSE - CKW last column (start) ---
    # Ensure 'computed_keywords' is visible and placed as the last column in Admin Browse.
    if "computed_keywords" in df.columns:
        if "computed_keywords" in hidden_cols:
            hidden_cols = [c for c in hidden_cols if c != "computed_keywords"]
        if "computed_keywords" in view_cols:
            view_cols = [c for c in view_cols if c != "computed_keywords"]
        view_cols.append("computed_keywords")
    # --- ANCHOR: ADMIN BROWSE - CKW last column (start) ---
    # Ensure 'computed_keywords' is visible and placed as the last column in Admin Browse.
    if "computed_keywords" in df.columns:
        # Unhide regardless of container type (set or list)
        if isinstance(hidden_cols, set):
            hidden_cols.discard("computed_keywords")
        else:
            hidden_cols = [c for c in hidden_cols if c != "computed_keywords"]

        # Force to last position (dedupe first)
        if "computed_keywords" in view_cols:
            view_cols = [c for c in view_cols if c != "computed_keywords"]
        view_cols.append("computed_keywords")
    # --- ANCHOR: ADMIN BROWSE - CKW last column (end) ---
    # --- ANCHOR: ADMIN BROWSE - CKW last column (end) ---
