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

# ── Stdlib ────────────────────────────────────────────────────────────────
import os
import csv
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
APP_VER = "admin-2025-10-19.8"
DB_PATH = os.getenv("DB_PATH", "providers.db")
SEED_CSV = os.getenv("SEED_CSV", "data/providers_seed.csv")
ALLOW_SEED_IMPORT = int(os.getenv("ALLOW_SEED_IMPORT", "1"))

PAGE_SIZE = 200
MAX_RENDER_ROWS = 1000
# ---- CKW algorithm version ----
CURRENT_VER = int(os.getenv("CKW_CURRENT_VER", "1"))

# ---- Data cache version (for @st.cache_data invalidation) ----
if "DATA_VER" not in st.session_state:
    st.session_state["DATA_VER"] = 0

MAX_RENDER_ROWS_ADMIN = int(os.getenv("MAX_RENDER_ROWS_ADMIN", str(MAX_RENDER_ROWS)))

# Columns to display on Browse (Admin). CKW visible here for validation.
BROWSE_COLUMNS = [
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
    "computed_keywords",
]

# Fallback widths (px). Secrets may override.
DEFAULT_COLUMN_WIDTHS_PX_ADMIN: Dict[str, int] = {
    "business_name": 260,
    "category": 160,
    "service": 220,
    "contact_name": 180,
    "phone": 140,
    "email": 240,
    "website": 240,
    "address": 280,
    "city": 110,
    "state": 80,
    "zip": 90,
    "notes": 360,
    "computed_keywords": 420,
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
    base += _split_tokens(row.get("business_name", ""))
    base += _split_tokens(row.get("category", ""))
    base += _split_tokens(row.get("service", ""))
    base += [t for t in _split_tokens(row.get("notes", "")) if 3 <= len(t) <= 20]
    if syn_service:
        base += [t for t in syn_service if t and t not in _STOP]
    if syn_category:
        base += [t for t in syn_category if t and t not in _STOP]
    if seed:
        for kw in seed:
            if not kw:
                continue
            base.append(kw.lower())
            base += _split_tokens(kw)
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
        # 1) Create vendors table if missing (includes city/state/zip to match queries)
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
              city TEXT,
              state TEXT,
              zip TEXT,
              notes TEXT,
              created_at TEXT,
              updated_at TEXT,
              computed_keywords TEXT,
              ckw_locked INTEGER DEFAULT 0,
              ckw_version INTEGER DEFAULT 0
            )
            """
        )

        # 2) Add columns idempotently (older DBs may lack some)
        cols = [r[1] for r in cx.exec_driver_sql("PRAGMA table_info(vendors)").all()]
        want_cols = {
            "city": "ALTER TABLE vendors ADD COLUMN city TEXT",
            "state": "ALTER TABLE vendors ADD COLUMN state TEXT",
            "zip": "ALTER TABLE vendors ADD COLUMN zip TEXT",
            "computed_keywords": "ALTER TABLE vendors ADD COLUMN computed_keywords TEXT",
            "ckw_locked": "ALTER TABLE vendors ADD COLUMN ckw_locked INTEGER DEFAULT 0",
            "ckw_version": "ALTER TABLE vendors ADD COLUMN ckw_version INTEGER DEFAULT 0",
        }
        for c, stmt in want_cols.items():
            if c not in cols:
                cx.exec_driver_sql(stmt)
                altered.append(c)

        # 3) Indexes
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_business_name ON vendors(business_name)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)")

        # 4) Lookup tables (simple)
        cx.exec_driver_sql("CREATE TABLE IF NOT EXISTS categories (name TEXT PRIMARY KEY)")
        cx.exec_driver_sql("CREATE TABLE IF NOT EXISTS services (name TEXT PRIMARY KEY)")
        # Seed lookups if empty
        if (cx.exec_driver_sql("SELECT COUNT(*) FROM categories").scalar() or 0) == 0:
            cx.exec_driver_sql(
                "INSERT OR IGNORE INTO categories(name) "
                "SELECT DISTINCT TRIM(category) FROM vendors WHERE IFNULL(TRIM(category),'')<>''"
            )
        if (cx.exec_driver_sql("SELECT COUNT(*) FROM services").scalar() or 0) == 0:
            cx.exec_driver_sql(
                "INSERT OR IGNORE INTO services(name) "
                "SELECT DISTINCT TRIM(service) FROM vendors WHERE IFNULL(TRIM(service),'')<>''"
            )

    if altered:
        return f"Schema OK (added: {', '.join(altered)})"
    return "Schema OK"


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
                if "state" in row and row["state"]:
                    row["state"] = row["state"].upper()
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
    return " ".join(parts).strip()


def recompute_ckw_for_ids(eng: Engine, ids: List[int]) -> int:
    if not ids:
        return 0
    updated = 0
    with eng.begin() as cx:
        for vid in ids:
            r = cx.exec_driver_sql(
                "SELECT business_name,category,service,contact_name,phone,email,website,address,notes "
                "FROM vendors WHERE id=:id",
                {"id": vid},
            ).mappings().first()
            if not r:
                continue
            ckw = compute_ckw(dict(r))
            cx.exec_driver_sql(
                "UPDATE vendors SET computed_keywords=:ckw, updated_at=:u WHERE id=:id",
                {"ckw": ckw, "u": _now_iso(), "id": vid},
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


# ──────────────────────────────────────────────────────────────────────────
# CKW-first search helpers (hashable-only, no engine param)
# ──────────────────────────────────────────────────────────────────────────
# ---- CKW helpers: seeds + synonyms ----------------------------------------
def _load_ckw_seed(cx, category: str | None, service: str | None) -> list[str]:
    """
    Schema: ckw_seeds(category TEXT, service TEXT, keywords TEXT)
    - Accepts keywords as JSON array or delimited text (comma/pipe/semicolon).
    - Returns a de-duplicated list[str].
    """
    cat = (category or "").strip()
    svc = (service or "").strip()
    if not cat and not svc:
        return []

    rows = cx.exec_driver_sql(
        "SELECT keywords FROM ckw_seeds WHERE category = :c AND service = :s",
        {"c": cat, "s": svc},
    ).all()
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


# ---- CKW Seeds: ensure + existence check ---------------------------------
def _ckw_seeds_exists(cx) -> bool:
    try:
        row = cx.exec_driver_sql(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ckw_seeds'"
        ).first()
        return bool(row)
    except Exception:
        # libsql/Turso fallback: try probing the table
        try:
            cx.exec_driver_sql("SELECT 1 FROM ckw_seeds LIMIT 1")
            return True
        except Exception:
            return False

def ensure_ckw_seeds_table() -> None:
    """
    Create ckw_seeds if missing. Schema: one row per (category, service); keywords is JSON or delimited text.
    """
    eng = get_engine()
    with eng.begin() as cx:
        cx.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS ckw_seeds (
                category TEXT NOT NULL,
                service  TEXT NOT NULL,
                keywords TEXT NOT NULL,
                PRIMARY KEY (category, service)
            )
        """)
        # Optional: an index to help lookups even though PK covers it
        cx.exec_driver_sql("""
            CREATE INDEX IF NOT EXISTS idx_ckw_seeds_cat_svc
            ON ckw_seeds(category, service)
        """)


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
def count_rows(q: str, data_ver: int) -> int:
    eng = get_engine()
    q_norm = (q or "").strip().lower()
    with eng.connect() as cx:
        if q_norm:
            if _has_ckw_column(data_ver):
                sql = sa.text(
                    """
                    SELECT COUNT(*) FROM (
                      SELECT id FROM vendors WHERE LOWER(COALESCE(computed_keywords,'')) LIKE :q1
                      UNION
                      SELECT id FROM vendors
                      WHERE LOWER(
                        COALESCE(business_name,'')||' '||
                        COALESCE(category,'')||' '||
                        COALESCE(service,'')||' '||
                        COALESCE(notes,'')||' '||
                        COALESCE(phone,'')||' '||
                        COALESCE(website,'')||' '||
                        COALESCE(address,'')||' '||
                        COALESCE(city,'')||' '||
                        COALESCE(state,'')||' '||
                        COALESCE(zip,'')
                      ) LIKE :q2
                    )
                    """
                )
                params = {"q1": f"%{q_norm}%", "q2": f"%{q_norm}%"}
            else:
                sql = sa.text(
                    """
                    SELECT COUNT(*) FROM vendors
                    WHERE LOWER(
                      COALESCE(business_name,'')||' '||
                      COALESCE(category,'')||' '||
                      COALESCE(service,'')||' '||
                      COALESCE(notes,'')||' '||
                      COALESCE(phone,'')||' '||
                      COALESCE(website,'')||' '||
                      COALESCE(address,'')||' '||
                      COALESCE(city,'')||' '||
                      COALESCE(state,'')||' '||
                      COALESCE(zip,'')
                    ) LIKE :q
                    """
                )
                params = {"q": f"%{q_norm}%"}
        else:
            sql = sa.text("SELECT COUNT(*) FROM vendors")
            params = {}
        return int(cx.execute(sql, params).scalar() or 0)


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
                        COALESCE(address,'')||' '||
                        COALESCE(city,'')||' '||
                        COALESCE(state,'')||' '||
                        COALESCE(zip,'')
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
                      COALESCE(address,'')||' '||
                      COALESCE(city,'')||' '||
                      COALESCE(state,'')||' '||
                      COALESCE(zip,'')
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
            "city",
            "state",
            "zip",
            "notes",
            "created_at",
            "updated_at",
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
              website, address, city, state, zip, notes,
              created_at, updated_at,
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
        "SELECT id, business_name, category, service, notes, ckw_locked, ckw_version, updated_at "
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
                    "UPDATE vendors SET computed_keywords=:ckw, ckw_version=:ver WHERE id=:id",
                    updates,
                )
                total_updated += len(updates)
    return (total_selected, total_updated)

# ──────────────────────────────────────────────────────────────────────────
# CRUD helpers
# ──────────────────────────────────────────────────────────────────────────
def insert_vendor(eng: Engine, data: Dict[str, Any]) -> int:
    row = dict(data)
    row["computed_keywords"] = compute_ckw(row)
    row["created_at"] = row["updated_at"] = _now_iso()
    with eng.begin() as cx:
        res = cx.exec_driver_sql(
            sa.text(
                """
                INSERT INTO vendors (
                  business_name,category,service,contact_name,phone,email,website,
                  address,city,state,zip,notes,created_at,updated_at,computed_keywords
                )
                VALUES (
                  :business_name,:category,:service,:contact_name,:phone,:email,:website,
                  :address,:city,:state,:zip,:notes,:created_at,:updated_at,:computed_keywords
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
                    city=:city,
                    state=:state,
                    zip=:zip,
                    notes=:notes,
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

    # Tabs
    tab_browse, tab_manage, tab_catsvc, tab_maint = st.tabs(
        ["Browse", "Add / Edit / Delete", "Category / Service", "Maintenance"]
    )

    # ──────────────────────────────────────────────────────────────────────
    # Browse (Admin)
    # ──────────────────────────────────────────────────────────────────────
    with tab_browse:
        c1, c2 = st.columns([1, 0.3])
        q = c1.text_input(
            "Search",
            value=st.session_state.get("q", ""),
            placeholder="name, category, service, notes, phone, website…",
            key="browse_search",
        )
        if c2.button("Clear", key="browse_clear"):
            q = ""
        st.session_state["q"] = q

        # CKW-first search (no pager; cap)
        limit = MAX_RENDER_ROWS_ADMIN
        offset = 0
        try:
            ids = search_ids_ckw_first(q, limit=limit, offset=offset, data_ver=DATA_VER)
        except Exception as e:
            st.error(f"Search failed: {e}")
            ids = []

        if not ids:
            try:
                with eng.connect() as cx:
                    target = cx.exec_driver_sql("PRAGMA database_list").fetchone()[2]
                    total_cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0
                st.info(
                    f"No matches. DB: {target} | vendors: {total_cnt}. "
                    "Tip: click **Clear** to reset search, or set DB_PATH in secrets."
                )
            except Exception as e:
                st.info(f"No matches. (Diagnostics failed: {e})")

        if len(ids) == limit:
            st.caption(f"Showing first {limit} matches (cap). Refine your search to narrow further.")

        try:
            df = fetch_rows_by_ids(tuple(ids), DATA_VER)
        except Exception as e:
            st.error(f"Fetch failed: {e}")
            df = pd.DataFrame(columns=BROWSE_COLUMNS)

        widths = dict(DEFAULT_COLUMN_WIDTHS_PX_ADMIN)
        try:
            widths.update(st.secrets.get("COLUMN_WIDTHS_PX_ADMIN", {}))
        except Exception:
            pass
        colcfg = _column_config_from_widths(widths)

        st.dataframe(
            df[BROWSE_COLUMNS] if not df.empty else df,
            hide_index=True,
            use_container_width=True,
            column_config=colcfg,
        )

    # ──────────────────────────────────────────────────────────────────────
    # Add / Edit / Delete
    # ──────────────────────────────────────────────────────────────────────
    with tab_manage:
        lc, rc = st.columns([1, 1], gap="large")

        # ---------- Add (left) ----------
        with lc:
            st.subheader("Add Provider")
            cats = list_categories(eng)
            srvs = list_services(eng)

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
            ac1, ac2, ac3 = st.columns([1, 0.5, 0.5])
            city = ac1.text_input("City", key="city_add")
            state = ac2.text_input("State", key="state_add")
            zip_ = ac3.text_input("Zip", key="zip_add")
            notes = st.text_area("Notes", height=100, key="notes_add")

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
                    "city": city.strip(),
                    "state": state.strip(),
                    "zip": zip_.strip(),
                    "notes": notes.strip(),
                }
                vid = insert_vendor(eng, data)
                ensure_lookup_value(eng, "categories", data["category"])
                ensure_lookup_value(eng, "services", data["service"])
                st.session_state["DATA_VER"] += 1
                st.success(f"Added provider #{vid}: {data['business_name']}")

            # ---------- Delete (left, under Add) ----------
            st.divider()
            st.subheader("Delete Provider")
            with eng.begin() as cx:
                opts = cx.exec_driver_sql(
                    "SELECT id, business_name FROM vendors ORDER BY business_name COLLATE NOCASE"
                ).all()
            if opts:
                labels = [f"#{i} — {n}" for (i, n) in opts]
                pick = st.selectbox("Select provider to delete", options=["— Select —"] + labels, key="del_pick")
                if pick != "— Select —":
                    idx = labels.index(pick)
                    del_id = int(opts[idx][0])
                    confirm = st.checkbox("I understand this will permanently delete the provider.", key="del_confirm")
                    ack = st.text_input("Type DELETE to confirm", key="del_ack")
                    if st.button("Delete", type="secondary", disabled=not (confirm and ack == "DELETE"), key="btn_delete"):
                        delete_vendor(eng, del_id)
                        st.session_state["DATA_VER"] += 1
                        st.warning(f"Deleted provider #{del_id}.")
            else:
                st.info("No providers to delete.")

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
                        "address,city,state,zip,notes FROM vendors WHERE id=:id",
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
                    category_e = (cat_new_e or "").strip() or (cat_choice_e if cat_choice_e != "— Select —" else r["category"])

                    e_s1, e_s2 = st.columns([1, 1])
                    srv_choice_e = e_s1.selectbox(
                        "Service *", options=["— Select —"] + srvs,
                        index=(srvs.index(r["service"]) + 1) if r["service"] in srvs else 0,
                        key="srv_edit_sel",
                    )
                    srv_new_e = e_s2.text_input("New Service (optional)", key="srv_edit_new")
                    service_e = (srv_new_e or "").strip() or (srv_choice_e if cat_choice_e != "— Select —" else r["service"])

                    contact_name_e = st.text_input("Contact Name", value=r["contact_name"] or "", key="contact_edit")
                    phone_e = st.text_input("Phone", value=r["phone"] or "", key="phone_edit")
                    email_e = st.text_input("Email", value=r["email"] or "", key="email_edit")
                    website_e = st.text_input("Website", value=r["website"] or "", key="website_edit")
                    address_e = st.text_input("Address", value=r["address"] or "", key="address_edit")
                    ac1e, ac2e, ac3e = st.columns([1, 0.5, 0.5])
                    city_e = ac1e.text_input("City", value=r["city"] or "", key="city_edit")
                    state_e = ac2e.text_input("State", value=r["state"] or "", key="state_edit")
                    zip_e = ac3e.text_input("Zip", value=r["zip"] or "", key="zip_edit")
                    notes_e = st.text_area("Notes", value=r["notes"] or "", height=100, key="notes_edit")

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
                            "city": city_e.strip(),
                            "state": state_e.strip(),
                            "zip": zip_e.strip(),
                            "notes": notes_e.strip(),
                        }
                        update_vendor(eng, sel_id, data)
                        ensure_lookup_value(eng, "categories", data["category"])
                        ensure_lookup_value(eng, "services", data["service"])
                        st.session_state["DATA_VER"] += 1
                        st.success(f"Saved changes to provider #{sel_id}.")



    # ──────────────────────────────────────────────────────────────────────
    # Category / Service management
    # ──────────────────────────────────────────────────────────────────────
    with tab_catsvc:
        #st.subheader("Manage Categories & Services")
        cc, ss = st.columns([1, 1], gap="large")

        # Categories
        with cc:
            st.markdown("**Categories**")
            cats = list_categories(eng)
            
            st.markdown("**Add Category**")
            new_cat = st.text_input("New Category", key="add_cat")
            if st.button("Add Category") and new_cat.strip():
                ensure_lookup_value(eng, "categories", new_cat.strip())
                st.session_state["DATA_VER"] += 1
                st.success(f"Added category: {new_cat.strip()}")

            st.markdown("**Delete Category** (only if unused)")
            del_cat = st.selectbox("Pick category", options=["— Select —"] + cats, key="del_cat")
            if del_cat != "— Select —":
                with eng.begin() as cx:
                    cnt = cx.exec_driver_sql(
                        "SELECT COUNT(*) FROM vendors WHERE category=:c",
                        {"c": del_cat},
                    ).scalar() or 0
                st.caption(f"In use by {cnt} provider(s)")
                if cnt == 0 and st.button("Delete Category", type="secondary"):
                    with eng.begin() as cx:
                        cx.exec_driver_sql("DELETE FROM categories WHERE name=:n", {"n": del_cat})
                    st.session_state["DATA_VER"] += 1
                    st.warning(f"Deleted category: {del_cat}")
                elif cnt > 0:
                    st.info("Category is in use; reassign it below instead.")

            st.markdown("**Reassign Category** (rename / move all vendors)")
            if cats:
                from_cat = st.selectbox("From", options=cats, key="rc_from")
                to_cat = st.text_input("To (new or existing)", key="rc_to")
                if st.button("Reassign Category for All Matching Vendors") and from_cat and to_cat.strip():
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

        # Services
        with ss:
            st.markdown("**Services**")
            srvs = list_services(eng)
            
            st.markdown("**Add Service**")
            new_srv = st.text_input("New Service", key="add_srv")
            if st.button("Add Service") and new_srv.strip():
                ensure_lookup_value(eng, "services", new_srv.strip())
                st.session_state["DATA_VER"] += 1
                st.success(f"Added service: {new_srv.strip()}")

            st.markdown("**Delete Service** (only if unused)")
            del_srv = st.selectbox("Pick service", options=["— Select —"] + srvs, key="del_srv")
            if del_srv != "— Select —":
                with eng.begin() as cx:
                    cnt = cx.exec_driver_sql(
                        "SELECT COUNT(*) FROM vendors WHERE service=:s",
                        {"s": del_srv},
                    ).scalar() or 0
                st.caption(f"In use by {cnt} provider(s)")
                if cnt == 0 and st.button("Delete Service", type="secondary"):
                    with eng.begin() as cx:
                        cx.exec_driver_sql("DELETE FROM services WHERE name=:n", {"n": del_srv})
                    st.session_state["DATA_VER"] += 1
                    st.warning(f"Deleted service: {del_srv}")
                elif cnt > 0:
                    st.info("Service is in use; reassign it below instead.")

            st.markdown("**Reassign Service** (rename / move all vendors)")
            if srvs:
                from_srv = st.selectbox("From", options=srvs, key="rs_from")
                to_srv = st.text_input("To (new or existing)", key="rs_to")
                if st.button("Reassign Service for All Matching Vendors") and from_srv and to_srv.strip():
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

        # ──────────────────────────────────────────────────────────────────────
    # Maintenance
    # ──────────────────────────────────────────────────────────────────────
    with tab_maint:
        st.subheader("Maintenance — Computed Keywords (CKW)")
        st.caption("CKW is auto-updated on Add/Edit and when you reassign categories/services. Use these for targeted or bulk recomputes.")

        # --- CKW seeds diagnostics (SAFE and inside the tab) --------------
        try:
            eng = get_engine()
            with eng.begin() as cx:
                # Check if ckw_seeds exists (SQLite/libsql-safe)
                exists_row = cx.exec_driver_sql(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ckw_seeds'"
                ).first()
                exists = bool(exists_row)

            if not exists:
                st.info("ckw_seeds table does not exist yet.")
                if st.button("Create ckw_seeds table", key="create_ckw_seeds"):
                    try:
                        eng = get_engine()
                        with eng.begin() as cx:
                            cx.exec_driver_sql("""
                                CREATE TABLE IF NOT EXISTS ckw_seeds (
                                    category TEXT NOT NULL,
                                    service  TEXT NOT NULL,
                                    keywords TEXT NOT NULL,
                                    PRIMARY KEY (category, service)
                                )
                            """)
                            cx.exec_driver_sql("""
                                CREATE INDEX IF NOT EXISTS idx_ckw_seeds_cat_svc
                                ON ckw_seeds(category, service)
                            """)
                        st.success("ckw_seeds table created.")
                    except Exception as e:
                        st.error(f"Create table failed: {e}")
            else:
                # Show schema + a small sample if present
                eng = get_engine()
                with eng.begin() as cx:
                    cols = cx.exec_driver_sql("PRAGMA table_info(ckw_seeds)").all()
                    st.caption("ckw_seeds columns (cid, name, type, notnull, dflt, pk):")
                    st.code("\n".join(str(c) for c in cols))
                    sample = cx.exec_driver_sql("SELECT * FROM ckw_seeds LIMIT 5").mappings().all()
                if sample:
                    st.caption("ckw_seeds sample rows:")
                    st.code("\n".join(str(dict(r)) for r in sample))
                else:
                    st.info("ckw_seeds exists but has no rows yet.")
        except Exception as e:
            st.info(f"Seeds diagnostics: {e}")

        # Optional: insert a couple of example JSON seeds to test recompute
        try:
            eng = get_engine()
            with eng.begin() as cx:
                exists_row = cx.exec_driver_sql(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ckw_seeds'"
                ).first()
                seeds_table_exists = bool(exists_row)
        except Exception:
            seeds_table_exists = False

        if seeds_table_exists and st.button("Insert example CKW seeds (JSON)", key="ckw_seed_examples"):
            try:
                eng = get_engine()
                with eng.begin() as cx:
                    cx.exec_driver_sql(
                        "INSERT OR REPLACE INTO ckw_seeds(category, service, keywords) VALUES (:c,:s,:k)",
                        [
                            {"c": "Home Repair & Trades", "s": "Garage Doors",
                             "k": '["garage door","opener","torsion spring","panel","track","remote"]'},
                            {"c": "Insurance", "s": "Insurance Agent",
                             "k": '["insurance","policy","broker","homeowners","auto","umbrella","medicare"]'},
                        ],
                    )
                st.success("Inserted example seeds. Now run STALE or ALL.")
            except Exception as e:
                st.error(f"Insert failed: {e}")

        # ---- Single provider recompute -----------------------------------
        try:
            eng = get_engine()
            with eng.begin() as cx:
                opts = cx.exec_driver_sql(
                    "SELECT id, business_name FROM vendors ORDER BY business_name COLLATE NOCASE"
                ).all()
        except Exception as e:
            opts = []
            st.error(f"Failed to load providers: {e}")

        if opts:
            labels = [f"#{pid} — {name}" for (pid, name) in opts]
            sel_label = st.selectbox("Recompute CKW for one provider", options=["— Select —"] + labels)
            if sel_label != "— Select —":
                idx = labels.index(sel_label)
                vid = int(opts[idx][0])

                if st.button("Recompute keywords for this provider", key="ckw_one"):
                    try:
                        n_sel, n_upd = _recompute_ckw_for_ids([vid], override_locks=False)
                        st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
                        st.success(f"Selected: {n_sel} | Updated: {n_upd} (provider id={vid})")
                    except Exception as e:
                        st.error(f"Recompute failed: {e}")
        else:
            st.info("No providers found.")

        # ---- Bulk recompute ----------------------------------------------
        st.divider()
        st.subheader("Bulk recompute")
        st.caption("Use STALE for safe, minimal updates. Use ALL for full rebuilds; you can override locks if needed.")

        c1, c2, c3 = st.columns(3)
        if c1.button("Recompute STALE (unlocked only)", key="ckw_stale"):
            try:
                eng = get_engine()
                with eng.begin() as cx:
                    ids = _select_vendor_ids_for_ckw(
                        cx, mode="stale", current_ver=CURRENT_VER, override_locks=False
                    )
                n_sel, n_upd = _recompute_ckw_for_ids(ids, override_locks=False)
                st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
                st.success(f"Stale selection: {n_sel} | Updated: {n_upd}")
            except Exception as e:
                st.error(f"Stale recompute failed: {e}")

        override = c2.checkbox(
            "Override locks for ALL",
            value=False,
            help="If checked, locked providers will be recomputed too.",
        )
        if c3.button("Recompute ALL", key="ckw_all"):
            try:
                eng = get_engine()
                with eng.begin() as cx:
                    ids = _select_vendor_ids_for_ckw(
                        cx, mode="all", current_ver=CURRENT_VER, override_locks=override
                    )
                n_sel, n_upd = _recompute_ckw_for_ids(ids, override_locks=override)
                st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
                st.success(f"All selection: {n_sel} | Updated: {n_upd} (override_locks={override})")
            except Exception as e:
                st.error(f"ALL recompute failed: {e}")




if __name__ == "__main__":
    main()
