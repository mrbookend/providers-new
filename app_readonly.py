from __future__ import annotations

import csv
import os
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text as T
import streamlit as st

# ---- Streamlit page config MUST be first ----
st.set_page_config(page_title="Providers -- Read-Only", page_icon="[book]", layout="wide")
# === ANCHOR: READONLY_ENGINE (start) ===
def _build_engine_readonly() -> sa.Engine:
    """
    Prefer Turso/libsql when TURSO_* secrets are present; otherwise fall back to local SQLite.
    """
    url = (os.environ.get("TURSO_DATABASE_URL") or "").strip()
    tok = (os.environ.get("TURSO_AUTH_TOKEN") or "").strip()
    if url and tok:
        # Accept either libsql://HOST or sqlite+libsql://HOST
        if url.startswith("sqlite+libsql://"):
            sa_url = url if "authToken=" in url else f"{url}?authToken={tok}"
        else:
            host = url.replace("libsql://", "")
            sa_url = f"sqlite+libsql://{host}?authToken={tok}"
        return sa.create_engine(sa_url, pool_pre_ping=True)
    # Fallback: local file DB
    return sa.create_engine("sqlite:///providers.db", pool_pre_ping=True)

@lru_cache(maxsize=1)
def get_engine() -> sa.Engine:
    return _build_engine_readonly()
# === ANCHOR: READONLY_ENGINE (end) ===

# === ANCHOR: READONLY_ENGINE_PROBES (start) ===
def _engine_probe_info(eng) -> dict:
    info = {
        "using_remote": False,
        "sqlalchemy_url": "",
        "dialect": "",
        "driver": "",
        "tables": [],
        "vendors_count": None,
    }
    try:
        info["sqlalchemy_url"] = str(eng.url)
        info["dialect"] = getattr(eng.dialect, "name", "")
        info["driver"] = getattr(eng.dialect, "driver", "")
        d = (info["driver"] or "").lower()
        u = (info["sqlalchemy_url"] or "").lower()
        info["using_remote"] = ("libsql" in d) or ("libsql" in u)
        with eng.begin() as cx:
            info["tables"] = [
                r[0]
                for r in cx.exec_driver_sql(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            ]
            if "vendors" in info["tables"]:
                info["vendors_count"] = cx.exec_driver_sql(
                    "SELECT COUNT(*) FROM vendors"
                ).scalar()
    except Exception as e:
        info["error"] = str(e)
    return info

def readonly_render_engine_status(eng) -> None:
    import streamlit as st  # local import to avoid early Streamlit calls
    info = _engine_probe_info(eng)
    st.write(
        {
            "using_remote": info.get("using_remote"),
            "sqlalchemy_url": info.get("sqlalchemy_url"),
            "dialect": info.get("dialect"),
            "driver": info.get("driver"),
            "tables": info.get("tables"),
            "vendors_count": info.get("vendors_count"),
            "error": info.get("error"),
        }
    )
# === ANCHOR: READONLY_ENGINE_PROBES (end) ===


# ---- Config ----
DB_PATH = os.environ.get("PROVIDERS_DB", "providers.db")
CSV_SEED = Path("data/providers_seed.csv")  # optional seed file path

# ---- SQLAlchemy engine (SQLite) ----
ENG = sa.create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)

# ---- Schema DDL (idempotent) ----
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
            cx.execute(T(stmt))


def _db_rowcount() -> int:
    with ENG.connect() as cx:
        try:
            return int(cx.execute(T("SELECT COUNT(*) FROM vendors")).scalar() or 0)
        except Exception:
            return 0


def _bootstrap_from_csv_if_needed() -> str | None:
    """
    If DB is missing/empty and CSV seed exists, import it once.
    Returns a status string if import happened, else None.
    """
    need_bootstrap = (not Path(DB_PATH).exists()) or (_db_rowcount() == 0)
    if not need_bootstrap or not CSV_SEED.exists():
        return None

    # Ensure schema first (in case DB file didn't exist)
    ensure_schema()

    # Minimal CSV loader (append; no dedupe; assumes header row)
    n = 0
    with ENG.begin() as cx, CSV_SEED.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        # Normalize headers: strip whitespace; tolerate missing optional columns
        headers = [h.strip() for h in (r.fieldnames or [])]
        for h in REQUIRED_HEADERS:
            if h not in headers:
                headers.append(h)
        insert_cols = [c for c in REQUIRED_HEADERS if c in headers]
        placeholders = ",".join(f":{c}" for c in insert_cols)
        sql = T(f"INSERT INTO vendors ({','.join(insert_cols)}) VALUES ({placeholders})")
        for row in r:
            clean = {}
            for c in insert_cols:
                v = row.get(c)
                clean[c] = v.strip() if isinstance(v, str) else v
            # allow SQLite to auto-assign id when blank
            if not str(clean.get("id") or "").strip():
                clean.pop("id", None)
            cx.execute(sql, clean)
            n += 1
    return f"Bootstrapped {n} rows from {CSV_SEED}"


@st.cache_data(show_spinner=False)
def load_df(q: str) -> pd.DataFrame:
    """Load rows, optional SQL-side LIKE filter on several columns."""
    ensure_schema()
    with ENG.connect() as cx:
        if q:
            return pd.read_sql_query(
                T("""
                  SELECT id,business_name,category,service,contact_name,phone,email,website,
                         address,city,state,zip,notes
                  FROM vendors
                  WHERE (business_name LIKE :x OR category LIKE :x OR service LIKE :x
                         OR COALESCE(computed_keywords,'') LIKE :x
                         OR COALESCE(city,'') LIKE :x OR COALESCE(state,'') LIKE :x)
                  ORDER BY business_name COLLATE NOCASE ASC
                """),
                cx,
                params={"x": f"%{q}%"},
            )
        return pd.read_sql_query(
            T("""
              SELECT id,business_name,category,service,contact_name,phone,email,website,
                     address,city,state,zip,notes
              FROM vendors
              ORDER BY business_name COLLATE NOCASE ASC
            """),
            cx,
        )

# === ANCHOR: READONLY_DIAGS_BLOCK (start) ===
def _readonly_diag_block() -> None:
    if (os.environ.get("READONLY_SHOW_DIAGS") or "").strip() == "1":
        import streamlit as st
        with st.expander("Database — Engine Status", expanded=False):
            eng = get_engine()
            readonly_render_engine_status(eng)
# === ANCHOR: READONLY_DIAGS_BLOCK (end) ===


# ---- Optional one-time bootstrap ----
msg = _bootstrap_from_csv_if_needed()
if msg:
    st.toast(msg, icon="[OK]")  # harmless locally; on Cloud shows a small toast

# ---- UI ----
st.title("Providers (Read-Only)")

left, right = st.columns([3, 1])
with left:
    q = st.text_input(
        "Search", value="", placeholder="name, category, service, city, keyword..."
    ).strip()
with right:
    if st.button("Clear"):
        q = ""

df = load_df(q)

if df.empty:
    # Offer quick hint if the table is empty and no seed CSV existed
    if not Path(DB_PATH).exists() and not CSV_SEED.exists():
        st.warning(
            "No database or seed CSV found. Add data/providers_seed.csv or set PROVIDERS_DB."
        )
    else:
        st.info("No matching providers.")
else:
    st.caption(f"Results: {len(df)}")
    st.dataframe(df, use_container_width=True)
    st.download_button(
        "Download CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name="providers.csv",
        mime="text/csv",
    )
# === ANCHOR: READONLY_DIAGS_CALL (insert) ===
_readonly_diag_block()

# ---- Tiny footer (optional) ----
with st.expander("About this app", expanded=False):
    st.write(
        "This read-only viewer uses a local SQLite database (providers.db). "
        "If the database is empty and data/providers_seed.csv is present, "
        "it imports that CSV once at startup."
    )
