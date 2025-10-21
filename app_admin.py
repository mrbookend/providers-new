# -*- coding: utf-8 -*-
from __future__ import annotations

# ── Streamlit page config MUST be the first Streamlit command ─────────────
import streamlit as st


def _safe_page_config() -> None:
    try:
        st.set_page_config(
            page_title="Providers — Admin",
            page_icon="🛠️",
            layout="wide",
            initial_sidebar_state="expanded",
        )
    except Exception:
        # Streamlit Cloud can enqueue before this runs; ignore duplicate/ordering error.
        pass


_safe_page_config()

# ---- Session defaults (must run before any UI uses these) ----
if "DATA_VER" not in st.session_state:
    st.session_state["DATA_VER"] = 0
if "q" not in st.session_state:
    st.session_state["q"] = ""


# ── Stdlib ────────────────────────────────────────────────────────────────
import os
import csv
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import urlparse

# ── Third-party ───────────────────────────────────────────────────────────
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine

# ──────────────────────────────────────────────────────────────────────────
# Globals / constants
# ──────────────────────────────────────────────────────────────────────────
APP_VER = "admin-2025-10-20.2"
DB_PATH = os.getenv("DB_PATH", "providers.db")
SEED_CSV = os.getenv("SEED_CSV", "data/providers_seed.csv")
ALLOW_SEED_IMPORT = int(os.getenv("ALLOW_SEED_IMPORT", "1"))
# ---- Help content (one-time; safe even if re-run) ----
try:
    HELP_MD  # type: ignore[name-defined]
except NameError:
    HELP_MD = """
# Browse Help

Use the **Search** box to filter by name, category, service, notes, phone, or website.
- **Clear** resets the search.
- **Download CSV** exports the current **filtered** results; when no filter is applied it exports **all** records.
- Sorting/filtering is controlled by the Admin app (no per-column filters here).

_Replace this with your long, book-style help content when ready._
"""

PAGE_SIZE = 200
MAX_RENDER_ROWS = 1000
# ---- CKW algorithm version ----
CURRENT_VER = int(os.getenv("CKW_CURRENT_VER", "1"))

# ---- Data cache version (for @st.cache_data invalidation) ----
if "DATA_VER" not in st.session_state:
    st.session_state["DATA_VER"] = 0

MAX_RENDER_ROWS_ADMIN = int(os.getenv("MAX_RENDER_ROWS_ADMIN", str(MAX_RENDER_ROWS)))

# Columns to display on Browse (Admin)
# - "keywords" is the human-curated column (ckw_manual_extra)
# - "computed_keywords" is the algorithm output
BROWSE_COLUMNS = [
    "business_name",
    "category",
    "service",
    "keywords",            # human-curated (ckw_manual_extra)
    "computed_keywords",   # algorithm output
    "contact_name",
    "phone",
    "email",
    "website",
    "address",
    "notes",
]


# Fallback widths (px). Secrets may override.
DEFAULT_COLUMN_WIDTHS_PX_ADMIN: Dict[str, int] = {
    "business_name": 260,
    "category": 160,
    "service": 200,
    "keywords": 360,
    "computed_keywords": 600,   # make this very visible
    "contact_name": 160,
    "phone": 140,
    "email": 240,
    "website": 240,
    "address": 260,
    "notes": 320,
}


# ──────────────────────────────────────────────────────────────────────────
# Helpers (string / time)
# ──────────────────────────────────────────────────────────────────────────

# ---- computed keywords builder --------------------------------------------
_STOP = {"and", "&", "the", "of", "for", "to", "a", "an", "in", "on", "at"}
def _norm_token(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else " " for ch in s or "")

def _split_tokens(s: str) -> list[str]:
    return [t for t in _norm_token(s).split() if t and t not in _STOP]

def _unique_join(parts: list[str]) -> str:
    seen, out = set(), []
    for p in parts:
        p = p.strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return " ".join(out)

def _build_ckw(row: dict[str, str], *, seed: list[str] | None,
               syn_service: list[str] | None, syn_category: list[str] | None) -> str:
    base = []
    # Core fields → tokens
    base += _split_tokens(row.get("business_name", ""))
    base += _split_tokens(row.get("category", ""))
    base += _split_tokens(row.get("service", ""))
    base += [t for t in _split_tokens(row.get("notes", "")) if 3 <= len(t) <= 20]
    # curated expansions
    if syn_service:
        base += [t for t in syn_service if t and t not in _STOP]
    if syn_category:
        base += [t for t in syn_category if t and t not in _STOP]
    # per-(category,service) seeds (phrases + tokens)
    if seed:
        for kw in seed:
            if not kw:
                continue
            base.append(kw.lower())
            base += _split_tokens(kw)
    # manual extras (phrases + tokens), unioned, never overwritten
    manual = (row.get("ckw_manual_extra") or "").strip()
    if manual:
        s = manual.replace("|", ",").replace(";", ",")
        for piece in [p.strip() for p in s.split(",") if p.strip()]:
            base.append(piece.lower())
            base += _split_tokens(piece)
    return _unique_join(base)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _digits_only(s: str | None) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())


def _host_only(url: str | None) -> str:
    if not url:
        return ""
    try:
        u = urlparse(url if "://" in url else f"https://{url}")
        host = (u.netloc or u.path or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _tokenize_for_ckw(*parts: str) -> List[str]:
    tokens: List[str] = []
    for part in parts:
        if not part:
            continue
        for raw in str(part).replace("/", " ").replace("-", " ").split():
            t = raw.strip().lower()
            if t:
                tokens.append(t)
    # de-dup but keep input order
    seen = set()
    out: List[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# ──────────────────────────────────────────────────────────────────────────
# Engine (cached)
# ──────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    # Ensure parent dir exists for local sqlite path
    try:
        d = os.path.dirname(DB_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return sa.create_engine(f"sqlite:///{DB_PATH}", future=True)


# ──────────────────────────────────────────────────────────────────────────
# SCHEMA: create-if-missing (UNCACHED; safe to call every run)
# ──────────────────────────────────────────────────────────────────────────
def ensure_schema_uncached() -> str:
    """
    Create vendors table/indexes if missing; add missing columns; create lookups.
    Returns a short status string.
    """
    eng = get_engine()
    altered: List[str] = []
    with eng.begin() as cx:
        # 1) Create vendors table if missing (no city/state/zip)
        cx.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS vendors (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              business_name TEXT NOT NULL,
              category TEXT,
              service TEXT,
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
              ckw_version INTEGER DEFAULT 0,
              ckw_manual_extra TEXT
            )
            """
        )

                # 2) Add columns idempotently (older DBs may lack some)
        cols = [r[1] for r in cx.exec_driver_sql("PRAGMA table_info(vendors)").all()]
        want_cols = {
            "computed_keywords": "ALTER TABLE vendors ADD COLUMN computed_keywords TEXT",
            "ckw_locked": "ALTER TABLE vendors ADD COLUMN ckw_locked INTEGER DEFAULT 0",
            "ckw_version": "ALTER TABLE vendors ADD COLUMN ckw_version INTEGER DEFAULT 0",
            "ckw_manual_extra": "ALTER TABLE vendors ADD COLUMN ckw_manual_extra TEXT",
        }
        for c, stmt in want_cols.items():
            if c not in cols:
                cx.exec_driver_sql(stmt)
                altered.append(c)

        # 3) Indexes
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_business_name ON vendors(business_name)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_category ON vendors(category)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_service ON vendors(service)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_cat_svc ON vendors(category, service)")
        # NOCASE companions so ORDER BY ... COLLATE NOCASE and case-insensitive filters can use an index
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_business_name_nocase ON vendors(business_name COLLATE NOCASE)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_category_nocase ON vendors(category COLLATE NOCASE)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_service_nocase ON vendors(service COLLATE NOCASE)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_cat_svc_nocase ON vendors(category COLLATE NOCASE, service COLLATE NOCASE)")

        # 4) Lookup tables (simple)
        cx.exec_driver_sql("CREATE TABLE IF NOT EXISTS categories (name TEXT PRIMARY KEY)")
        cx.exec_driver_sql("CREATE TABLE IF NOT EXISTS services (name TEXT PRIMARY KEY)")

        # Seed/refresh lookups (idempotent; only non-empty trimmed names)
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO categories(name)
            SELECT DISTINCT COALESCE(TRIM(category),'')
            FROM vendors
            WHERE COALESCE(TRIM(category),'') <> ''
        """)
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO services(name)
            SELECT DISTINCT COALESCE(TRIM(service),'')
            FROM vendors
            WHERE COALESCE(TRIM(service),'') <> ''
        """)

    return "Schema OK"

# ──────────────────────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────────────────────
# ONE-TIME SEED FROM CSV IF EMPTY (UNCACHED)
# ──────────────────────────────────────────────────────────────────────────
def bootstrap_from_csv_if_needed() -> str | None:
    if not ALLOW_SEED_IMPORT:
        return None
    csv_path = SEED_CSV
    if not os.path.exists(csv_path):
        return None

    eng = get_engine()
    with eng.begin() as cx:
        n = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0
        if n > 0:
            return None

        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        rows: List[dict[str, Any]] = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            headers = set(rdr.fieldnames or [])
            # Only insert columns that exist in table
            table_cols = [r[1] for r in cx.exec_driver_sql("PRAGMA table_info(vendors)").all()]
            allowed = [h for h in headers if h in table_cols]

            for r in rdr:
                row = {k: (r.get(k) or "").strip() for k in allowed}
                if "phone" in row and row["phone"]:
                    row["phone"] = _digits_only(row["phone"])
                if "created_at" in table_cols and not row.get("created_at"):
                    row["created_at"] = now
                if "updated_at" in table_cols and not row.get("updated_at"):
                    row["updated_at"] = now
                rows.append(row)

        if not rows:
            return None

        cols_sql = ", ".join(rows[0].keys())
        placeholders = ", ".join([f":{c}" for c in rows[0].keys()])
        cx.exec_driver_sql(f"INSERT INTO vendors ({cols_sql}) VALUES ({placeholders})", rows)
        return f"Bootstrap inserted {len(rows)} rows"


# ──────────────────────────────────────────────────────────────────────────
# CKW: compute & writebacks
# ──────────────────────────────────────────────────────────────────────────
def compute_ckw(row: Dict[str, Any]) -> str:
    """
    Lightweight immediate CKW used on add/edit; full algorithm (with seeds/synonyms/manual union)
    runs in Maintenance recompute. We still include manual extras here for decent first-pass.
    """
    parts: List[str] = []
    parts.extend(_tokenize_for_ckw(row.get("category", "")))
    parts.extend(_tokenize_for_ckw(row.get("service", "")))
    parts.extend(_tokenize_for_ckw(row.get("business_name", "")))
    pdig = _digits_only(row.get("phone"))
    if pdig:
        parts.append(pdig)
    host = _host_only(row.get("website"))
    if host:
        parts.extend(_tokenize_for_ckw(host))
    parts.extend(_tokenize_for_ckw(row.get("address", "")))
    parts.extend(_tokenize_for_ckw(row.get("notes", "")))
    # Include manual extras immediately
    manual = (row.get("ckw_manual_extra") or "").strip()
    if manual:
        s = manual.replace("|", ",").replace(";", ",")
        for piece in [p.strip() for p in s.split(",") if p.strip()]:
            parts.append(piece.lower())
            parts.extend(_tokenize_for_ckw(piece))
    # de-dup
    seen, out = set(), []
    for t in parts:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return " ".join(out).strip()


def recompute_ckw_for_ids(eng: Engine, ids: List[int]) -> int:
    if not ids:
        return 0
    updated = 0
    with eng.begin() as cx:
        for vid in ids:
            r = cx.exec_driver_sql(
                "SELECT business_name,category,service,contact_name,phone,email,website,address,notes,ckw_manual_extra "
                "FROM vendors WHERE id=:id",
                {"id": vid},
            ).mappings().first()
            if not r:
                continue
            ckw = compute_ckw(dict(r))
            cx.exec_driver_sql(
                "UPDATE vendors SET computed_keywords=:ckw, ckw_version=:ver, updated_at=:u WHERE id=:id",
                {"ckw": ckw, "ver": CURRENT_VER, "u": _now_iso(), "id": vid},
            )
            updated += 1
    return updated


def recompute_ckw_all(eng: Engine) -> int:
    with eng.begin() as cx:
        ids = [row[0] for row in cx.exec_driver_sql("SELECT id FROM vendors").all()]
    return recompute_ckw_for_ids(eng, ids)


def list_categories(eng: Engine) -> List[str]:
    with eng.begin() as cx:
        rows = cx.exec_driver_sql("SELECT name FROM categories ORDER BY name COLLATE NOCASE").all()
    return [r[0] for r in rows]


def list_services(eng: Engine) -> List[str]:
    with eng.begin() as cx:
        rows = cx.exec_driver_sql("SELECT name FROM services ORDER BY name COLLATE NOCASE").all()
    return [r[0] for r in rows]


def ensure_lookup_value(eng: Engine, table: str, name: str) -> None:
    if not name:
        return
    with eng.begin() as cx:
        cx.exec_driver_sql(f"INSERT OR IGNORE INTO {table}(name) VALUES (:n)", {"n": name.strip()})
def refresh_lookups(eng: Engine) -> None:
    """Idempotently upsert categories/services from vendors."""
    with eng.begin() as cx:
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO categories(name)
            SELECT DISTINCT COALESCE(TRIM(category),'')
            FROM vendors
            WHERE COALESCE(TRIM(category),'') <> ''
        """)
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO services(name)
            SELECT DISTINCT COALESCE(TRIM(service),'')
            FROM vendors
            WHERE COALESCE(TRIM(service),'') <> ''
        """)
        
def refresh_lookups(eng: Engine) -> None:
    """Idempotently upsert categories/services from vendors."""
    with eng.begin() as cx:
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO categories(name)
            SELECT DISTINCT COALESCE(TRIM(category),'')
            FROM vendors
            WHERE COALESCE(TRIM(category),'') <> ''
        """)
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO services(name)
            SELECT DISTINCT COALESCE(TRIM(service),'')
            FROM vendors
            WHERE COALESCE(TRIM(service),'') <> ''
        """)
        
def refresh_lookups(eng: Engine) -> None:
    """Idempotent refresh of categories/services from vendors."""
    with eng.begin() as cx:
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO categories(name)
            SELECT DISTINCT COALESCE(TRIM(category),'')
            FROM vendors
            WHERE COALESCE(TRIM(category),'') <> ''
        """)
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO services(name)
            SELECT DISTINCT COALESCE(TRIM(service),'')
            FROM vendors
            WHERE COALESCE(TRIM(service),'') <> ''
        """)


# ──────────────────────────────────────────────────────────────────────────
# CKW helpers: seeds + synonyms
# ──────────────────────────────────────────────────────────────────────────
def ensure_ckw_seeds_table() -> None:
    """
    Create ckw_seeds if missing. Schema: one row per (category, service); keywords is JSON or delimited text.
    """
    eng = get_engine()
    with eng.begin() as cx:
        cx.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS ckw_seeds (
                category TEXT NOT NULL,
                service  TEXT NOT NULL,
                keywords TEXT NOT NULL,
                PRIMARY KEY (category, service)
            )
            """
        )
        cx.exec_driver_sql(
            """
            CREATE INDEX IF NOT EXISTS idx_ckw_seeds_cat_svc
            ON ckw_seeds(category, service)
            """
        )

def _load_ckw_seed(cx, category: str | None, service: str | None) -> list[str]:
    """
    Schema: ckw_seeds(category TEXT, service TEXT, keywords TEXT)
    - Accepts keywords as JSON array or delimited text (comma/pipe/semicolon).
    - Returns a de-duplicated list[str].
    - If the table doesn't exist, returns [] (quietly).
    """
    cat = (category or "").strip()
    svc = (service or "").strip()
    if not cat and not svc:
        return []

    try:
        rows = cx.exec_driver_sql(
            "SELECT keywords FROM ckw_seeds WHERE category = :c AND service = :s",
            {"c": cat, "s": svc},
        ).all()
    except sqlite3.OperationalError as e:
        # Table not found → treat as no seeds configured
        if "no such table: ckw_seeds" in str(e).lower():
            return []
        raise
    except Exception:
        # Any other seed-load failure: fail soft
        return []

    if not rows:
        return []

    out: list[str] = []
    for (raw,) in rows:
        if raw is None:
            continue
        s = str(raw).strip()
        # Try JSON array first
        try:
            import json
            parsed = json.loads(s)
            if isinstance(parsed, list):
                out.extend([str(x).strip() for x in parsed if str(x).strip()])
                continue
        except Exception:
            pass
        # Delimited fallback
        for delim in ("|", ";"):
            s = s.replace(delim, ",")
        out.extend([p.strip() for p in s.split(",") if p.strip()])

    # De-dup, stable order
    seen, uniq = set(), []
    for t in out:
        tl = t.lower()
        if tl in seen:
            continue
        seen.add(tl)
        uniq.append(t)
    return uniq


@st.cache_resource
def _get_ckw_synonyms_map() -> dict:
    """
    Load optional synonyms from env or st.secrets.
    Expect JSON like: {"service":{"garage doors":["garage door","opener"]},
                       "category":{"insurance":["policy","broker"]}}
    Keys should be lowercase.
    """
    import json, os
    blob = os.getenv("CKW_SYNONYMS_JSON", "")
    if not blob:
        try:
            blob = st.secrets.get("CKW_SYNONYMS_JSON", "")
        except Exception:
            blob = ""
    if blob:
        try:
            m = json.loads(blob) if isinstance(blob, str) else blob
            if isinstance(m, dict):
                m.setdefault("service", {})
                m.setdefault("category", {})
                m["service"]  = {str(k).lower(): v for k, v in m["service"].items()}
                m["category"] = {str(k).lower(): v for k, v in m["category"].items()}
                return m
        except Exception:
            pass
    return {"service": {}, "category": {}}

def _load_synonyms_service(service: str | None) -> list[str]:
    name = (service or "").strip().lower()
    if not name:
        return []
    vals = _get_ckw_synonyms_map().get("service", {}).get(name, [])
    return [str(x).strip() for x in vals if str(x).strip()]

def _load_synonyms_category(category: str | None) -> list[str]:
    name = (category or "").strip().lower()
    if not name:
        return []
    vals = _get_ckw_synonyms_map().get("category", {}).get(name, [])
    return [str(x).strip() for x in vals if str(x).strip()]


@st.cache_data(show_spinner=False)
def _has_ckw_column(data_ver: int) -> bool:
    eng = get_engine()
    with eng.connect() as cx:
        try:
            rows = cx.exec_driver_sql("PRAGMA table_info(vendors)").all()
            return any((r[1] if isinstance(r, tuple) else r["name"]) == "computed_keywords" for r in rows)
        except Exception:
            return False

@st.cache_data(show_spinner=False)
def count_rows(q: str, data_ver: int = 0) -> int:
    where = ""
    params: dict[str, Any] = {}
    if q:
        where = """
            WHERE (
                COALESCE(business_name,'') || ' ' ||
                COALESCE(category,'')      || ' ' ||
                COALESCE(service,'')       || ' ' ||
                COALESCE(notes,'')         || ' ' ||
                COALESCE(phone,'')         || ' ' ||
                COALESCE(website,'')
            ) LIKE :q
        """
        params["q"] = f"%{q}%"

    sql = f"SELECT COUNT(*) FROM vendors {where}"
    eng = get_engine()  # FIX: was build_engine()
    with eng.begin() as cx:
        return int(cx.exec_driver_sql(sql, params).scalar() or 0)


@st.cache_data(show_spinner=False)
def search_ids_ckw_first(q: str, limit: int, offset: int, data_ver: int) -> list[int]:
    """Return vendor IDs with CKW hits ranked before generic text hits; stable order otherwise."""
    eng = get_engine()
    q_norm = (q or "").strip().lower()
    with eng.connect() as cx:
        if q_norm:
            if _has_ckw_column(data_ver):
                sql = sa.text(
                    """
                    WITH m AS (
                      SELECT id, 1 AS pri
                      FROM vendors
                      WHERE LOWER(COALESCE(computed_keywords,'')) LIKE :q1
                      UNION ALL
                      SELECT id, 2 AS pri
                      FROM vendors
                      WHERE LOWER(
                        COALESCE(business_name,'')||' '||
                        COALESCE(category,'')||' '||
                        COALESCE(service,'')||' '||
                        COALESCE(notes,'')||' '||
                        COALESCE(phone,'')||' '||
                        COALESCE(website,'')||' '||
                        COALESCE(address,'')
                      ) LIKE :q2
                    ),
                    r AS (
                      SELECT id, MIN(pri) AS pri
                      FROM m
                      GROUP BY id
                    )
                    SELECT id
                    FROM r
                    ORDER BY pri ASC, id ASC
                    LIMIT :limit OFFSET :offset
                    """
                )
                params = {"q1": f"%{q_norm}%", "q2": f"%{q_norm}%", "limit": int(limit), "offset": int(offset)}
            else:
                sql = sa.text(
                    """
                    SELECT id
                    FROM vendors
                    WHERE LOWER(
                      COALESCE(business_name,'')||' '||
                      COALESCE(category,'')||' '||
                      COALESCE(service,'')||' '||
                      COALESCE(notes,'')||' '||
                      COALESCE(phone,'')||' '||
                      COALESCE(website,'')||' '||
                      COALESCE(address,'')
                    ) LIKE :q
                    ORDER BY business_name COLLATE NOCASE ASC, id ASC
                    LIMIT :limit OFFSET :offset
                    """
                )
                params = {"q": f"%{q_norm}%", "limit": int(limit), "offset": int(offset)}
        else:
            sql = sa.text(
                """
                SELECT id
                FROM vendors
                ORDER BY business_name COLLATE NOCASE ASC, id ASC
                LIMIT :limit OFFSET :offset
                """
            )
            params = {"limit": int(limit), "offset": int(offset)}
        return [row[0] for row in cx.execute(sql, params).all()]


@st.cache_data(show_spinner=False)
def fetch_rows_by_ids(ids: tuple[int, ...], data_ver: int) -> pd.DataFrame:
    """Fetch display columns for a given tuple of IDs. Empty-safe."""
    if not ids:
        return pd.DataFrame(columns=[
            "id",
            "business_name",
            "category",
            "service",
            "contact_name",
            "phone",
            "email",
            "website",
            "address",
            "notes",
            "created_at",
            "updated_at",
            "keywords",
            "computed_keywords",
            "ckw_locked",
            "ckw_version",
        ])
    eng = get_engine()
    placeholders = ",".join([f":id{i}" for i in range(len(ids))])
    params = {f"id{i}": v for i, v in enumerate(ids)}
    with eng.connect() as cx:
        sql = sa.text(
            f"""
            SELECT
              id, business_name, category, service, contact_name, phone, email,
              website, address, notes,
              created_at, updated_at,
              COALESCE(ckw_manual_extra,'') AS keywords,
              COALESCE(computed_keywords,'') AS computed_keywords,
              IFNULL(ckw_locked,0) AS ckw_locked,
              COALESCE(ckw_version,0) AS ckw_version
            FROM vendors
            WHERE id IN ({placeholders})
            """
        )
        df = pd.read_sql(sql, cx, params=params)
    if "business_name" in df.columns:
        df = df.sort_values(["business_name", "id"], kind="stable", ignore_index=True)
    return df

def _select_vendor_ids_for_ckw(cx, *, mode: str, current_ver: int, override_locks: bool) -> list[int]:
    """
    mode: "stale" or "all"
    - stale: unlocked AND (ckw_version != current_ver OR computed_keywords IS NULL OR computed_keywords = '')
    - all:   all rows; if not override_locks, exclude ckw_locked=1
    """
    if mode == "stale":
        sql = (
            "SELECT id FROM vendors "
            "WHERE COALESCE(ckw_locked,0)=0 "
            "AND (COALESCE(ckw_version,0) <> :v OR computed_keywords IS NULL OR computed_keywords='')"
        )
        rows = cx.exec_driver_sql(sql, {"v": current_ver}).all()
    elif mode == "all":
        if override_locks:
            sql = "SELECT id FROM vendors"
            rows = cx.exec_driver_sql(sql).all()
        else:
            sql = "SELECT id FROM vendors WHERE COALESCE(ckw_locked,0)=0"
            rows = cx.exec_driver_sql(sql).all()
    else:
        return []
    return [int(r[0]) for r in rows]

def _chunk_iter(seq, n=500):
    it = iter(seq)
    while True:
        chunk = list([x for _, x in zip(range(n), it)])
        if not chunk:
            return
        yield chunk

def _fetch_rows_for_ids(cx, ids: list[int]) -> list[dict]:
    # Use a dynamic IN list to avoid the "IN :ids" pitfall.
    placeholders = ",".join([f":id{i}" for i in range(len(ids))])
    sql = (
        "SELECT id, business_name, category, service, notes, ckw_locked, ckw_version, updated_at, ckw_manual_extra "
        f"FROM vendors WHERE id IN ({placeholders})"
    )
    params = {f"id{i}": v for i, v in enumerate(ids)}
    return [dict(r) for r in cx.exec_driver_sql(sql, params).mappings().all()]
    
def _recompute_ckw_for_ids(ids: list[int], *, override_locks: bool) -> tuple[int, int]:
    """
    Returns: (n_selected, n_updated)
    """
    if not ids:
        return (0, 0)
    eng = get_engine()
    total_selected = 0
    total_updated = 0
    with eng.begin() as cx:
        for chunk in _chunk_iter(ids, n=500):
            rows = _fetch_rows_for_ids(cx, chunk)
            total_selected += len(rows)
            updates = []
            for row in rows:
                if row.get("ckw_locked") and not override_locks:
                    continue
                # Load expansions
                seed = _load_ckw_seed(cx, row.get("category",""), row.get("service",""))
                syn_svc = _load_synonyms_service(row.get("service",""))
                syn_cat = _load_synonyms_category(row.get("category",""))
                new_ckw = _build_ckw(row, seed=seed, syn_service=syn_svc, syn_category=syn_cat)
                updates.append({
                    "ckw": new_ckw,
                    "ver": CURRENT_VER,
                    "id": row["id"],
                })
            if updates:
                # executemany
                cx.exec_driver_sql(
                    "UPDATE vendors SET computed_keywords=:ckw, ckw_version=:ver, updated_at=:u WHERE id=:id",
                    [{"ckw": u["ckw"], "ver": u["ver"], "id": u["id"], "u": _now_iso()} for u in updates],
                )
                total_updated += len(updates)
    return (total_selected, total_updated)

# ──────────────────────────────────────────────────────────────────────────
# CRUD helpers
# ──────────────────────────────────────────────────────────────────────────
def insert_vendor(eng: Engine, data: Dict[str, Any]) -> int:
    row = dict(data)
    row["phone"] = _digits_only(row.get("phone"))
    row["computed_keywords"] = compute_ckw(row)
    row["created_at"] = row["updated_at"] = _now_iso()
    with eng.begin() as cx:


        res = cx.exec_driver_sql(
            sa.text(
                """
                INSERT INTO vendors (
                  business_name,category,service,contact_name,phone,email,website,
                  address,notes,ckw_manual_extra,created_at,updated_at,computed_keywords
                )
                VALUES (
                  :business_name,:category,:service,:contact_name,:phone,:email,:website,
                  :address,:notes,:ckw_manual_extra,:created_at,:updated_at,:computed_keywords
                )
                """
            ),
            row,
        )
        new_id = int(res.lastrowid or 0)
    ensure_lookup_value(eng, "categories", row.get("category", ""))
    ensure_lookup_value(eng, "services", row.get("service", ""))
    return new_id


def update_vendor(eng: Engine, vid: int, data: Dict[str, Any]) -> None:
    row = dict(data)
    row["phone"] = _digits_only(row.get("phone"))
    row["computed_keywords"] = compute_ckw(row)
    row["updated_at"] = _now_iso()
    row["id"] = vid
    with eng.begin() as cx:
        cx.exec_driver_sql(
            sa.text(
                """
                UPDATE vendors
                SET business_name=:business_name,
                    category=:category,
                    service=:service,
                    contact_name=:contact_name,
                    phone=:phone,
                    email=:email,
                    website=:website,
                    address=:address,
                    notes=:notes,
                    ckw_manual_extra=:ckw_manual_extra,
                    computed_keywords=:computed_keywords,
                    updated_at=:updated_at
                WHERE id=:id
                """
            ),
            row,
        )
    ensure_lookup_value(eng, "categories", row.get("category", ""))
    ensure_lookup_value(eng, "services", row.get("service", ""))


def delete_vendor(eng: Engine, vid: int) -> None:
    with eng.begin() as cx:
        cx.exec_driver_sql("DELETE FROM vendors WHERE id=:id", {"id": vid})


# ──────────────────────────────────────────────────────────────────────────
# UI helpers
# ──────────────────────────────────────────────────────────────────────────
def _column_config_from_widths(widths: Dict[str, int]) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    for col, w in widths.items():
        if col in BROWSE_COLUMNS:
            cfg[col] = st.column_config.TextColumn(col.replace("_", " ").title(), width=w)
    return cfg


# ──────────────────────────────────────────────────────────────────────────
# Main App
# ──────────────────────────────────────────────────────────────────────────
def _has_table(eng: Engine, name: str) -> bool:
    try:
        with eng.connect() as cx:
            rows = cx.exec_driver_sql(f"PRAGMA table_info({name})").all()
        return bool(rows)
    except Exception:
        return False

def main() -> None:
    # ---- DATA_VER init (cache-buster for @st.cache_data) ----
    if "DATA_VER" not in st.session_state:
        st.session_state["DATA_VER"] = 0
    DATA_VER = st.session_state["DATA_VER"]

    # ---- Build engine early and ensure schema BEFORE any queries ----
    eng = get_engine()
    try:
        msg_schema = ensure_schema_uncached()
        if os.getenv("SHOW_STATUS") == "1":
            st.caption(msg_schema)
    except Exception as e:
        st.warning(f"Schema check failed: {e}")

    # Optional seed if empty (safe no-op otherwise)
    try:
        msg_seed = bootstrap_from_csv_if_needed()
        if msg_seed and os.getenv("SHOW_STATUS") == "1":
            st.caption(msg_seed)
    except Exception as e:
        st.warning(f"Bootstrap skipped: {e}")

    # Helper used by the Browse tab "Clear" button.
    # Important: this only sets a FLAG. We clear the actual query BEFORE rendering the text_input.

        # ── Bootstrap (must be above # Tabs): safe state + DB readiness ─────────────
    # Ensure session-state defaults exist before any UI uses them
    if "DATA_VER" not in st.session_state:
        st.session_state["DATA_VER"] = 0
    if "q" not in st.session_state:
        st.session_state["q"] = ""

    def _resolve_engine_for_probe():
        """Prefer the app's real engine; fall back to local sqlite."""
        try:
            return get_engine()
        except Exception:
            pass
        db_path = globals().get("DB_PATH", "providers.db")
        return sa.create_engine(f"sqlite:///{db_path}", future=True)

    # Safe DB readiness probe: check for the actual 'vendors' table
    try:
        _eng = _resolve_engine_for_probe()
        with _eng.connect() as _cx:
            # Record DB path for diagnostics
            _db_row = _cx.exec_driver_sql("PRAGMA database_list").first()
            st.session_state["DB_PATH_INFO"] = (_db_row[2] if _db_row else None)

            # TRUE readiness only if 'vendors' exists
            _row = _cx.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='vendors'"
            ).first()
        DB_READY = bool(_row)
    except Exception:
        DB_READY = False

    # Make readiness available everywhere
    st.session_state["DB_READY"] = DB_READY


# Tabs
tab_browse, tab_manage, tab_catsvc, tab_maint = st.tabs(
    ["Browse", "Add / Edit / Delete", "Category / Service", "Maintenance"]
)

# ─────────────────────────────────────────────────────────────────────
# Browse (Admin)
# ─────────────────────────────────────────────────────────────────────
with tab_browse:
    # ---- Compact search row (50% width; label collapsed; inline Clear) ----
    c1, c2, c3 = st.columns([0.5, 0.12, 0.38])
    q = c1.text_input(
        label="Search",  # label intentionally hidden
        value=st.session_state.get("q", ""),
        placeholder="Search name, category, service, notes, phone, website…",
        label_visibility="collapsed",
    )
    if c2.button("Clear", use_container_width=True):
        q = ""
    st.session_state["q"] = q

    # [BEGIN ANCHOR — keep this line above]
    try:
        # Ensure DATA_VER exists (cache-buster used by your cached funcs)
        if "DATA_VER" not in st.session_state:
            st.session_state["DATA_VER"] = 0
        DATA_VER = st.session_state["DATA_VER"]

        # Count (no _engine param; fetch_page no longer used)
        total = count_rows(q=q, data_ver=DATA_VER)
        st.caption(f"{total} matching provider(s)")

        # Resolve row IDs (CKW-first search) and load rows
        if not _has_table(get_engine(), "vendors"):
            st.warning(
                "Database not initialized yet (no 'vendors' table). "
                "See Maintenance → Quick Engine Probe / Seed."
            )
            st.stop()
        else:
            ids = search_ids_ckw_first(q=q, limit=MAX_RENDER_ROWS, offset=0, data_ver=DATA_VER)
            if not ids:
                vdf = pd.DataFrame()
            else:
                vdf = fetch_rows_by_ids(tuple(ids), DATA_VER)

        # ---- Ensure desired columns exist; set display order ----
        BASE_COLS = ["business_name", "category", "service", "phone", "website", "notes"]
        CKW_COLS  = ["keywords", "computed_keywords"]
        META_COLS = ["created_at", "updated_at"]

        if vdf.empty:
            st.info("No matches.")
            st.stop()

        for col in CKW_COLS + META_COLS:
            if col not in vdf.columns:
                vdf[col] = ""

        preferred = [c for c in BASE_COLS + CKW_COLS + META_COLS if c in vdf.columns]
        remaining = [c for c in vdf.columns if c not in preferred]
        display_cols = preferred + remaining

        # ---- Prepare view (rename, hide controls) ----
        _src = vdf.copy()
        if "computed_keywords" not in _src.columns:
            _src["computed_keywords"] = ""
        if "ckw_manual_extra" in _src.columns:
            _src = _src.rename(columns={"ckw_manual_extra": "keywords"})

        _HIDE_EXACT = {"id", "created_at", "updated_at", "ckw_locked", "ckw_version"}
        def _is_ckw_control(col: str) -> bool:
            return col.startswith("ckw_")

        _visible = [c for c in _src.columns if c not in _HIDE_EXACT and not _is_ckw_control(c)]

        ORDER = [
            "business_name", "category", "service",
            "keywords", "computed_keywords",
            "phone", "website", "notes",
        ]
        _ordered = [c for c in ORDER if c in _visible] + [c for c in _visible if c not in ORDER]

        # Column widths + labels
        _cfg = {}
        for c in _ordered:
            w = DEFAULT_COLUMN_WIDTHS_PX_ADMIN.get(c, 220)
            label = "Keywords" if c == "keywords" else ("CKW" if c == "computed_keywords" else c.replace("_", " ").title())
            _cfg[c] = st.column_config.TextColumn(label, width=w)

        # -------- Hidden/control-char scanning + sanitization --------
        import re, json
        from datetime import datetime as _dt
        _HIDDEN_RX = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\u200B-\u200F\u202A-\u202E\u2060]")

        def _to_str_safe(x):
            if x is None:
                return ""
            if isinstance(x, _dt):
                return x.isoformat(sep=" ", timespec="seconds")
            if isinstance(x, (bytes, bytearray)):
                try:
                    x = x.decode("utf-8", errors="replace")
                except Exception:
                    return str(x)
            if isinstance(x, dict):
                try:
                    return json.dumps(x, ensure_ascii=False)
                except Exception:
                    return str(x)
            if isinstance(x, (list, tuple, set)):
                return ", ".join("" if (v is None) else str(v) for v in x)
            try:
                return "" if pd.isna(x) else str(x)
            except Exception:
                return str(x)

        def _strip_hidden(s: str) -> str:
            return _HIDDEN_RX.sub("", s)

        _view = _src.loc[:, _ordered] if not _src.empty else _src

        # Diagnostics (first 300 rows)
        _issues: dict[str, dict[str, list]] = {}
        if not _view.empty:
            for col in _ordered:
                risky, hidden = [], []
                for idx, val in _view[col].head(300).items():
                    if isinstance(val, (dict, list, tuple, set, bytes, bytearray, _dt)):
                        risky.append((int(idx), type(val).__name__))
                    if isinstance(val, str) and _HIDDEN_RX.search(val):
                        hidden.append(int(idx))
                if risky or hidden:
                    _issues[col] = {"risky_types": risky[:5], "hidden_char_rows": hidden[:5]}

        with st.expander("Browse diagnostics (click to open)", expanded=False):
            if _issues:
                st.write({"columns_with_issues": _issues})
                st.caption("Shown: first 5 examples per column. Values are normalized for safe rendering/export.")
            else:
                st.caption("No obvious mixed types or hidden characters detected in the first 300 rows.")

        # Normalize → strings + strip hidden chars
        _view_safe = _view.applymap(lambda v: _strip_hidden(_to_str_safe(v))) if not _view.empty else _view

        # Render
        st.dataframe(
            _view_safe,
            column_config=_cfg,
            use_container_width=True,
            hide_index=True,
            height=520,
        )

        # ---- Bottom toolbar (CSV export + help) ----
        bt1, bt_sp = st.columns([0.2, 0.8])
        if not _view_safe.empty:
            csv_bytes = _view_safe.to_csv(index=False).encode("utf-8")
            bt1.download_button(
                "Download CSV",
                data=csv_bytes,
                file_name="providers.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with st.expander("Help — How to use Browse (click to open)", expanded=False):
            st.markdown(HELP_MD)

    except Exception as e:
        st.error(f"Browse failed: {e}")
        st.stop()
    # [END ANCHOR — keep this line below]




    # Use a local data_ver (don’t rely on a global alias that might be undefined)
    data_ver = st.session_state.get("DATA_VER", 0)

    # ---- Resolve row IDs (CKW-first search) and load rows ----
    if not _has_table(get_engine(), "vendors"):
        st.warning(
            "Database not initialized yet (no 'vendors' table). "
            "See Maintenance → Quick Engine Probe / Seed."
        )
        st.stop()
    else:
        try:
            ids = search_ids_ckw_first(q=q, limit=MAX_RENDER_ROWS, offset=0, data_ver=data_ver)
            if not ids:
                st.info("No matches.")
                vdf = pd.DataFrame()
            else:
                vdf = fetch_rows_by_ids(tuple(ids), data_ver)
        except Exception as e:
            st.error(f"Browse failed: {e}")
            vdf = pd.DataFrame()

    # ---- Ensure desired columns exist; set display order ----
    BASE_COLS = ["business_name", "category", "service", "phone", "website", "notes"]
    CKW_COLS  = ["keywords", "computed_keywords"]  # adjust if schema differs
    META_COLS = ["created_at", "updated_at"]

    for col in CKW_COLS + META_COLS:
        if col not in vdf.columns:
            vdf[col] = ""

    preferred = [c for c in BASE_COLS + CKW_COLS + META_COLS if c in vdf.columns]
    remaining = [c for c in vdf.columns if c not in preferred]
    display_cols = preferred + remaining

                                # ---- Table (horizontal scroll via wide container; index hidden) ----
    # Show Keywords & CKW; scan & sanitize for Arrow safety; enforce order & widths (horizontal scroll).

    _src = vdf.copy()

    # Ensure display columns exist even on older DBs
    if "computed_keywords" not in _src.columns:
        _src["computed_keywords"] = ""
    if "ckw_manual_extra" not in _src.columns:
        _src["ckw_manual_extra"] = ""

    # Friendlier label for manual extras
    _src = _src.rename(columns={"ckw_manual_extra": "keywords"})

    # Columns to hide (keep 'keywords' and 'computed_keywords' visible)
    _HIDE_EXACT = {"id", "created_at", "updated_at", "ckw_locked", "ckw_version"}

    def _is_ckw_control(col: str) -> bool:
        return col.startswith("ckw_")  # hide internal CKW controls/metadata

    _visible = [c for c in _src.columns if c not in _HIDE_EXACT and not _is_ckw_control(c)]

    ORDER = [
        "business_name",
        "category",
        "service",
        "keywords",             # curated extras
        "computed_keywords",    # CKW
        "phone",
        "website",
        "notes",
    ]
    _ordered = [c for c in ORDER if c in _visible] + [c for c in _visible if c not in ORDER]

    # Column widths + labels (force horizontal scroll when total width > page)
    _cfg = {}
    for c in _ordered:
        w = DEFAULT_COLUMN_WIDTHS_PX_ADMIN.get(c, 220)
        label = "Keywords" if c == "keywords" else ("CKW" if c == "computed_keywords" else c.replace("_", " ").title())
        _cfg[c] = st.column_config.TextColumn(label, width=w)

    # -------- Hidden/control-char scanning + sanitization --------
    import re, json
    from datetime import datetime as _dt

    # Control chars (except tab/newline), DEL, zero-width & bidi marks
    _HIDDEN_RX = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\u200B-\u200F\u202A-\u202E\u2060]")

    def _to_str_safe(x):
        if x is None:
            return ""
        if isinstance(x, _dt):
            return x.isoformat(sep=" ", timespec="seconds")
        if isinstance(x, (bytes, bytearray)):
            try:
                x = x.decode("utf-8", errors="replace")
            except Exception:
                return str(x)
        if isinstance(x, dict):
            try:
                return json.dumps(x, ensure_ascii=False)
            except Exception:
                return str(x)
        if isinstance(x, (list, tuple, set)):
            return ", ".join("" if (v is None) else str(v) for v in x)
        try:
            return "" if pd.isna(x) else str(x)
        except Exception:
            return str(x)

    def _strip_hidden(s: str) -> str:
        return _HIDDEN_RX.sub("", s)  # remove hidden/control chars (keep tab/newline)

    _view = _src.loc[:, _ordered] if not _src.empty else _src

    # Diagnostics (first 300 rows) — optional but helpful
    _issues: dict[str, dict[str, list]] = {}
    if not _view.empty:
        for col in _ordered:
            risky, hidden = [], []
            for idx, val in _view[col].head(300).items():
                if isinstance(val, (dict, list, tuple, set, bytes, bytearray, _dt)):
                    risky.append((int(idx), type(val).__name__))
                if isinstance(val, str) and _HIDDEN_RX.search(val):
                    hidden.append(int(idx))
            if risky or hidden:
                _issues[col] = {"risky_types": risky[:5], "hidden_char_rows": hidden[:5]}

    with st.expander("Browse diagnostics (click to open)", expanded=False):
        if _issues:
            st.write({"columns_with_issues": _issues})
            st.caption("Shown: first 5 examples per column. Values are normalized for safe rendering/export.")
        else:
            st.caption("No obvious mixed types or hidden characters detected in the first 300 rows.")

    # Normalize → strings + strip hidden chars
    _view_safe = _view.applymap(lambda v: _strip_hidden(_to_str_safe(v))) if not _view.empty else _view

    # Render
    st.dataframe(
        _view_safe,
        column_config=_cfg,
        use_container_width=True,
        hide_index=True,
        height=520,
    )

    # ---- Bottom toolbar (CSV export + help) ----
    try:
        bt1, bt_sp = st.columns([0.2, 0.8])

        if not _view_safe.empty:
            csv_bytes = _view_safe.to_csv(index=False).encode("utf-8")  # exports visible columns/order
            bt1.download_button(
                "Download CSV",
                data=csv_bytes,
                file_name="providers.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with st.expander("Help — How to use Browse (click to open)", expanded=False):
            st.markdown(HELP_MD)

    except Exception as e:
        st.warning(f"CSV download/help unavailable: {e}")


# ─────────────────────────────────────────────────────────────────────
# Add / Edit / Delete  (guarded to avoid crashes when tables missing)
# ─────────────────────────────────────────────────────────────────────
with tab_manage:
    if not DB_READY:
        st.info("Database not ready — skipping Add/Edit UI because required tables are missing.")
    else:
        eng = get_engine()  # ensure local scope
        lc, rc = st.columns([1, 1], gap="large")
        
        # ---------- Add (left) ----------
        with lc:
            st.subheader("Add Provider")

            cats = list_categories(eng) if _has_table(eng, "categories") else []
            srvs = list_services(eng) if _has_table(eng, "services") else []

            bn = st.text_input("Business Name *", key="bn_add")

            # Category select or new
            ccol1, ccol2 = st.columns([1, 1])
            cat_choice = ccol1.selectbox("Category *", options=["— Select —"] + cats, key="cat_add_sel")
            cat_new = ccol2.text_input("New Category (optional)", key="cat_add_new")
            category = (cat_new or "").strip() or (cat_choice if cat_choice != "— Select —" else "")

            # Service select or new
            scol1, scol2 = st.columns([1, 1])
            srv_choice = scol1.selectbox("Service *", options=["— Select —"] + srvs, key="srv_add_sel")
            srv_new = scol2.text_input("New Service (optional)", key="srv_add_new")
            service = (srv_new or "").strip() or (srv_choice if srv_choice != "— Select —" else "")

            contact_name = st.text_input("Contact Name", key="contact_add")
            phone = st.text_input("Phone", key="phone_add")
            email = st.text_input("Email", key="email_add")
            website = st.text_input("Website", key="website_add")
            address = st.text_input("Address", key="address_add")
            notes = st.text_area("Notes", height=100, key="notes_add")

            keywords_manual = st.text_area(
                "Keywords",
                value="",
                help=(
                    "Optional, comma/pipe/semicolon-separated phrases to always include. "
                    "Example: garage door, torsion spring, opener repair"
                ),
                height=80,
                key="kw_add",
            )

            disabled = not (bn.strip() and category and service)
            if st.button("Add Provider", type="primary", disabled=disabled, key="btn_add_provider"):
                data = {
                    "business_name": bn.strip(),
                    "category": category.strip(),
                    "service": service.strip(),
                    "contact_name": contact_name.strip(),
                    "phone": phone.strip(),
                    "email": email.strip(),
                    "website": website.strip(),
                    "address": address.strip(),
                    "notes": notes.strip(),
                    "ckw_manual_extra": (keywords_manual or "").strip(),
                }
                vid = insert_vendor(eng, data)
                ensure_lookup_value(eng, "categories", data["category"])
                ensure_lookup_value(eng, "services", data["service"])
                st.session_state["DATA_VER"] += 1
                refresh_lookups(get_engine())
                st.success(
                    f"Added provider #{vid}: {data['business_name']}  — run “Recompute ALL” to apply keywords."
                )


        # ---------- Edit (right) ----------
        with rc:
            st.subheader("Edit Provider")
            with eng.begin() as cx:
                rows = cx.exec_driver_sql(
                    "SELECT id, business_name FROM vendors ORDER BY business_name COLLATE NOCASE"
                ).all()

            if not rows:
                st.info("No providers yet.")
            else:
                labels = [f"#{i} — {n}" for (i, n) in rows]
                sel = st.selectbox("Pick a provider", options=labels, key="pick_edit_sel")
                sel_id = int(rows[labels.index(sel)][0])

                with eng.begin() as cx:
                    r = cx.exec_driver_sql(
                        "SELECT business_name,category,service,contact_name,phone,email,website,"
                        "address,notes,ckw_manual_extra FROM vendors WHERE id=:id",
                        {"id": sel_id},
                    ).mappings().first()

                if r:
                    bn_e = st.text_input("Business Name *", value=r["business_name"], key="bn_edit")

                    cats = list_categories(eng)
                    srvs = list_services(eng)

                    e_c1, e_c2 = st.columns([1, 1])
                    cat_choice_e = e_c1.selectbox(
                        "Category *", options=["— Select —"] + cats,
                        index=(cats.index(r["category"]) + 1) if r["category"] in cats else 0,
                        key="cat_edit_sel",
                    )
                    cat_new_e = e_c2.text_input("New Category (optional)", key="cat_edit_new")
                    category_e = (cat_new_e or "").strip() or (
                        cat_choice_e if cat_choice_e != "— Select —" else r["category"]
                    )

                    e_s1, e_s2 = st.columns([1, 1])
                    srv_choice_e = e_s1.selectbox(
                        "Service *", options=["— Select —"] + srvs,
                        index=(srvs.index(r["service"]) + 1) if r["service"] in srvs else 0,
                        key="srv_edit_sel",
                    )
                    srv_new_e = e_s2.text_input("New Service (optional)", key="srv_edit_new")
                    service_e = (srv_new_e or "").strip() or (
                        srv_choice_e if srv_choice_e != "— Select —" else r["service"]
                    )

                    contact_name_e = st.text_input("Contact Name", value=r["contact_name"] or "", key="contact_edit")
                    phone_e = st.text_input("Phone", value=r["phone"] or "", key="phone_edit")
                    email_e = st.text_input("Email", value=r["email"] or "", key="email_edit")
                    website_e = st.text_input("Website", value=r["website"] or "", key="website_edit")
                    address_e = st.text_input("Address", value=r["address"] or "", key="address_edit")
                    notes_e = st.text_area("Notes", value=r["notes"] or "", height=100, key="notes_edit")

                    keywords_manual_e = st.text_area(
                        "Keywords",
                        value=(r.get("ckw_manual_extra") or ""),
                        help="Optional, comma/pipe/semicolon-separated phrases that will be UNIONED during recompute.",
                        height=80,
                        key="kw_edit",
                    )

                    if st.button("Save Changes", type="primary", key="save_changes_btn"):
                        data = {
                            "business_name": bn_e.strip(),
                            "category": category_e.strip(),
                            "service": service_e.strip(),
                            "contact_name": contact_name_e.strip(),
                            "phone": phone_e.strip(),
                            "email": email_e.strip(),
                            "website": website_e.strip(),
                            "address": address_e.strip(),
                            "notes": notes_e.strip(),
                            "ckw_manual_extra": (keywords_manual_e or "").strip(),
                        }
                        update_vendor(eng, sel_id, data)
                        ensure_lookup_value(eng, "categories", data["category"])
                        ensure_lookup_value(eng, "services", data["service"])
                        st.session_state["DATA_VER"] += 1
                        st.success(f"Saved changes to provider #{sel_id}.  — run “Recompute ALL” to apply keywords.")

        # ▼▼ Delete Provider (partial-word find + dropdown; checkbox gate only) ▼▼
        if "DATA_VER" not in st.session_state:
            st.session_state["DATA_VER"] = 0

        st.markdown("### Delete Provider")
        st.caption("Danger zone: Permanently removes a record from **vendors**.")

        # Local fallback to get an engine if your app doesn't expose get_engine()
def _get_engine_fallback():
    try:
        return get_engine()  # your real builder if present
    except Exception:
        pass
    import sqlalchemy as sa
    db_path = globals().get("DB_PATH", "providers.db")
    return sa.create_engine(f"sqlite:///{db_path}", future=True)

# Small helpers (local to this section)
def _has_table(engine, name: str) -> bool:
    try:
        with engine.connect() as cx:
            q = "SELECT name FROM sqlite_master WHERE type='table' AND name=:n"
            return cx.exec_driver_sql(q, {"n": name}).first() is not None
    except Exception:
        return False

def _strip_ctrl(s: str) -> str:
    if not isinstance(s, str):
        return s
    return "".join(ch for ch in s if ch == "\t" or ch == "\n" or ord(ch) >= 32)

def _as_text(v) -> str:
    import json, datetime as _dt
    if v is None:
        return ""
    if isinstance(v, (list, dict, set, tuple)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            v = str(v)
    if isinstance(v, (_dt.datetime, _dt.date)):
        try:
            if isinstance(v, _dt.datetime) and v.tzinfo is not None:
                v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
            return v.isoformat(sep=" ", timespec="seconds")
        except Exception:
            return str(v)
    return _strip_ctrl(str(v))

from sqlalchemy import text as sql_text

@st.cache_data(show_spinner=False)
def _list_providers_min(data_ver: int):
    """Minimal list for delete UI; cached by data_ver."""
    eng2 = _get_engine_fallback()
    if not _has_table(eng2, "vendors"):
        # Table missing; surface empty list so caller can warn & stop.
        return [], {}

    with eng2.connect() as cx2:
        q = sql_text("""
            SELECT id, business_name, category, service
            FROM vendors
            ORDER BY business_name COLLATE NOCASE, id
        """)
        rows2 = [dict(r2) for r2 in cx2.execute(q).mappings().all()]

    labels2, by_label2 = [], {}
    for r2 in rows2:
        bid = _as_text(r2.get("id"))
        bnm = _as_text(r2.get("business_name"))
        cat = _as_text(r2.get("category"))
        srv = _as_text(r2.get("service"))
        label2 = f"{bid} — {bnm} ({cat} → {srv})"
        labels2.append(label2)
        by_label2[label2] = {"id": bid, "business_name": bnm}
    return labels2, by_label2

# Resolve DATA_VER for cache-busting
if "DATA_VER" not in st.session_state:
    st.session_state["DATA_VER"] = 0

labels, by_label = _list_providers_min(st.session_state["DATA_VER"])

# Guard: if no table / no rows, inform and stop
if not labels:
    st.warning("No providers to delete yet (missing table or empty list). Initialize or seed the database in **Maintenance**.")
    st.stop()

# ---- Select provider (no find box; full list) ----
sel = st.selectbox(
    "Select provider to delete",
    options=labels,
    index=None,
    placeholder="Choose a provider…",
    key="del_select_label",
)

row = by_label.get(sel) if sel else None

if row:
    ok_checkbox = st.checkbox(
        "I understand this action is **permanent** and cannot be undone.",
        key="del_perm_ack",
        value=st.session_state.get("del_perm_ack", False),
    )

    del_btn = st.button(
        "Delete provider",
        type="primary",
        disabled=not ok_checkbox,
        help="Enabled only after you tick the permanent-action checkbox.",
    )

    if del_btn and ok_checkbox:
        try:
            eng3 = _get_engine_fallback()
            with eng3.begin() as cx3:  # transactional delete
                dq = sql_text("DELETE FROM vendors WHERE id = :id")
                res = cx3.execute(dq, {"id": row["id"]})
                # Optional: check rowcount for existence
                if hasattr(res, "rowcount") and res.rowcount == 0:
                    st.warning(f"No provider found with id={row['id']}. It may have been removed already.")
                else:
                    st.success(f"Deleted provider id={row['id']} ({row['business_name']}).")

            # Invalidate caches and clear controls
            st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
            st.session_state["del_select_label"] = None
            st.session_state["del_perm_ack"] = False

        except Exception as e:
            st.error(f"Delete failed: {e}")
# ▲▲ End Delete Provider ▲▲

# ─────────────────────────────────────────────────────────────────────
# Category / Service management
# ─────────────────────────────────────────────────────────────────────
with tab_catsvc:
    eng = get_engine()  # ensure local scope
    cc, ss = st.columns([1, 1], gap="large")

    # Categories
    with cc:
        try:
            cats = list_categories(eng)
        except Exception as e:
            cats = []
            st.error(f"Failed to load categories: {e}")

        new_cat = st.text_input("New Category", key="add_cat")
        if st.button("Add Category", key="btn_add_cat") and new_cat.strip():
            try:
                ensure_lookup_value(eng, "categories", new_cat.strip())
                st.session_state["DATA_VER"] += 1
                st.success(f"Added category: {new_cat.strip()}")
            except Exception as e:
                st.error(f"Add failed: {e}")

        # Helpers kept local to this section (valid Python; optional to move top-level)
        def ensure_lookup_value(eng: Engine, table: str, name: str) -> None:
            if not name:
                return
            with eng.begin() as cx:
                cx.exec_driver_sql(f"INSERT OR IGNORE INTO {table}(name) VALUES (:n)", {"n": name.strip()})

        def refresh_lookups(eng: Engine) -> None:
            """Idempotently upsert categories/services from vendors."""
            with eng.begin() as cx:
                cx.exec_driver_sql("""
                    INSERT OR IGNORE INTO categories(name)
                    SELECT DISTINCT COALESCE(TRIM(category),'')
                    FROM vendors
                    WHERE COALESCE(TRIM(category),'') <> ''
                """)
                cx.exec_driver_sql("""
                    INSERT OR IGNORE INTO services(name)
                    SELECT DISTINCT COALESCE(TRIM(service),'')
                    FROM vendors
                    WHERE COALESCE(TRIM(service),'') <> ''
                """)

        st.markdown("**Delete Category** (only if unused)")
        del_cat = st.selectbox("Pick category", options=["— Select —"] + cats, key="del_cat")
        if del_cat != "— Select —":
            try:
                with eng.begin() as cx:
                    cnt = cx.exec_driver_sql(
                        "SELECT COUNT(*) FROM vendors WHERE category=:c",
                        {"c": del_cat},
                    ).scalar() or 0
                st.caption(f"In use by {cnt} provider(s)")
                if cnt == 0 and st.button("Delete Category", type="secondary", key="btn_del_cat"):
                    with eng.begin() as cx:
                        cx.exec_driver_sql("DELETE FROM categories WHERE name=:n", {"n": del_cat})
                    st.session_state["DATA_VER"] += 1
                    st.warning(f"Deleted category: {del_cat}")
                elif cnt > 0:
                    st.info("Category is in use; reassign it below instead.")
            except Exception as e:
                st.error(f"Delete check failed: {e}")

        st.markdown("**Reassign Category** (rename / move all vendors)")
        if cats:
            from_cat = st.selectbox("From", options=cats, key="rc_from")
            to_cat = st.text_input("To (new or existing)", key="rc_to")
            if st.button("Reassign Category for All Matching Vendors", key="btn_reassign_cat") and from_cat and to_cat.strip():
                try:
                    to_val = to_cat.strip()
                    with eng.begin() as cx:
                        cx.exec_driver_sql(
                            "UPDATE vendors SET category=:to WHERE category=:from",
                            {"to": to_val, "from": from_cat},
                        )
                    ensure_lookup_value(eng, "categories", to_val)
                    with eng.begin() as cx:
                        cx.exec_driver_sql("DELETE FROM categories WHERE name=:n", {"n": from_cat})
                    changed = recompute_ckw_all(eng)
                    st.session_state["DATA_VER"] += 1
                    st.success(
                        f"Reassigned category '{from_cat}' → '{to_val}'. "
                        f"Recomputed CKW for {changed} provider(s)."
                    )
                except Exception as e:
                    st.error(f"Reassign failed: {e}")

    # Services
    with ss:
        try:
            srvs = list_services(eng)
        except Exception as e:
            srvs = []
            st.error(f"Failed to load services: {e}")

        new_srv = st.text_input("New Service", key="add_srv")
        if st.button("Add Service", key="btn_add_srv") and new_srv.strip():
            try:
                ensure_lookup_value(eng, "services", new_srv.strip())
                st.session_state["DATA_VER"] += 1
                st.success(f"Added service: {new_srv.strip()}")
            except Exception as e:
                st.error(f"Add failed: {e}")

        st.markdown("**Delete Service** (only if unused)")
        del_srv = st.selectbox("Pick service", options=["— Select —"] + srvs, key="del_srv")
        if del_srv != "— Select —":
            try:
                with eng.begin() as cx:
                    cnt = cx.exec_driver_sql(
                        "SELECT COUNT(*) FROM vendors WHERE service=:s",
                        {"s": del_srv},
                    ).scalar() or 0
                st.caption(f"In use by {cnt} provider(s)")
                if cnt == 0 and st.button("Delete Service", type="secondary", key="btn_del_srv"):
                    with eng.begin() as cx:
                        cx.exec_driver_sql("DELETE FROM services WHERE name=:n", {"n": del_srv})
                    st.session_state["DATA_VER"] += 1
                    st.warning(f"Deleted service: {del_srv}")
                elif cnt > 0:
                    st.info("Service is in use; reassign it below instead.")
            except Exception as e:
                st.error(f"Delete check failed: {e}")

        st.markdown("**Reassign Service** (rename / move all vendors)")
        if srvs:
            from_srv = st.selectbox("From", options=srvs, key="rs_from")
            to_srv = st.text_input("To (new or existing)", key="rs_to")
            if st.button("Reassign Service for All Matching Vendors", key="btn_reassign_srv") and from_srv and to_srv.strip():
                try:
                    to_val = to_srv.strip()
                    with eng.begin() as cx:
                        cx.exec_driver_sql(
                            "UPDATE vendors SET service=:to WHERE service=:from",
                            {"to": to_val, "from": from_srv},
                        )
                    ensure_lookup_value(eng, "services", to_val)
                    with eng.begin() as cx:
                        cx.exec_driver_sql("DELETE FROM services WHERE name=:n", {"n": from_srv})
                    changed = recompute_ckw_all(eng)
                    st.session_state["DATA_VER"] += 1
                    st.success(
                        f"Reassigned service '{from_srv}' → '{to_val}'. "
                        f"Recomputed CKW for {changed} provider(s)."
                    )
                except Exception as e:
                    st.error(f"Reassign failed: {e}")

# ─────────────────────────────────────────────────────────────────────
# Maintenance
# ─────────────────────────────────────────────────────────────────────
with tab_maint:
    st.subheader("Maintenance — Computed Keywords (CKW)")
    try:
        eng = get_engine()
        with eng.connect() as cx:
            _prov_cnt = int(cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0)
        st.caption(f"Providers in scope: {_prov_cnt}")
    except Exception as e:
        st.warning(f"Count unavailable: {e}")
    ...
    # (rest of your Maintenance block unchanged)
    ...

if __name__ == "__main__":
    main()
