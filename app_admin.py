# app_admin.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Providers — Admin (providers-new)

This build applies the requested changes:
- Hide row numbers in Browse; do not show `id`.
- Browse column order: category → service → business_name, then others; show computed_keywords.
- Permanently remove city/state/zip from UI and inserts (kept out of schema for this app).
- Add computed_keywords on insert; normalize phone to digits.
- Ensure helpers are defined BEFORE bootstrap; robust bootstrap from CSV when table empty.

Scope: Standalone, minimal-yet-complete Admin suitable for providers-new repo
(target DB: ./providers.db, seed CSV: ./data/providers_seed.csv)

NOTE: If you later reintroduce Turso/libsql, replace build_engine() accordingly.
"""

# ---- Streamlit page config MUST be the first Streamlit command ----
import streamlit as st
st.set_page_config(
    page_title="Providers — Admin",
    page_icon="🛠️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---- Stdlib ----
import os
import csv
from datetime import datetime
from typing import Any, Iterable

# ---- Third-party ----
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

# =============================
#   Global constants / policy
# =============================
APP_VER = "admin-2025-10-19"
DB_PATH = os.getenv("DB_PATH", "providers.db")
SEED_CSV = os.getenv("SEED_CSV", "data/providers_seed.csv")
PAGE_SIZE = 200
MAX_RENDER_ROWS = 1000

# Only these columns appear in Browse (order matters) and in CSV export
BROWSE_DISPLAY_COLUMNS = [
    "category",
    "service",
    "business_name",
    "contact_name",
    "phone",
    "email",
    "website",
    "address",
    "notes",
    "computed_keywords",
    "created_at",
    "updated_at",
]

# =============================
#   Helpers (must be BEFORE bootstrap)
# =============================
def _digits_only(s: str | None) -> str:
    """Return only digits from a string; safe on None."""
    return "".join(ch for ch in (s or "") if ch.isdigit())


def _compute_keywords(category: str, service: str, business_name: str) -> str:
    """Basic computed_keywords seed from category/service/business_name.
    Lowercase tokenize; dedup in order.
    """
    toks: list[str] = []
    for s in (category or "", service or "", business_name or ""):
        for t in (s or "").lower().split():
            if t and t not in toks:
                toks.append(t)
    return " ".join(toks)


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================
#   Engine & schema
# =============================
@st.cache_resource(show_spinner=False)
def build_engine() -> Engine:
    # SQLite file in repo root (providers-new)
    url = f"sqlite+pysqlite:///{DB_PATH}"
    eng = sa.create_engine(url, future=True)
    return eng


def ensure_schema(engine: Engine) -> None:
    """Create minimal schema if missing. No city/state/zip by design here."""
    with engine.begin() as cx:
        cx.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS vendors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_name TEXT NOT NULL,
                category TEXT NOT NULL,
                service TEXT NOT NULL,
                contact_name TEXT,
                phone TEXT,
                email TEXT,
                website TEXT,
                address TEXT,
                notes TEXT,
                computed_keywords TEXT,
                ckw_locked INTEGER DEFAULT 0,
                ckw_version TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        # Basic helpful indexes
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_business_name ON vendors(business_name)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_category_service ON vendors(category, service)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)"
        )


# =============================
#   Bootstrap (idempotent)
# =============================
def _bootstrap_from_csv_if_needed(engine: Engine, csv_path: str = SEED_CSV) -> str:
    """
    If vendors is empty, load seed CSV and insert rows.
    Expected CSV headers (extras ignored):
      business_name,category,service,contact_name,phone,email,website,address,notes
    """
    try:
        with engine.begin() as cx:
            cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0
            if int(cnt) > 0:
                return f"Bootstrap skipped (vendors already has {cnt} rows)."

            if not os.path.exists(csv_path):
                raise RuntimeError(f"Seed CSV not found: {csv_path}")

            df = pd.read_csv(csv_path)
            want = {
                "business_name",
                "category",
                "service",
                "contact_name",
                "phone",
                "email",
                "website",
                "address",
                "notes",
            }
            missing = sorted(want - set(df.columns))
            if missing:
                raise RuntimeError(
                    "Seed CSV missing columns: " + ", ".join(missing)
                )

            df = df.copy()
            df["phone"] = df["phone"].map(_digits_only)
            df["computed_keywords"] = [
                _compute_keywords(r.get("category", ""), r.get("service", ""), r.get("business_name", ""))
                for r in df.to_dict(orient="records")
            ]
            now = _now_iso()
            df["created_at"], df["updated_at"] = now, now

            rows = df[[
                "business_name",
                "category",
                "service",
                "contact_name",
                "phone",
                "email",
                "website",
                "address",
                "notes",
                "computed_keywords",
                "created_at",
                "updated_at",
            ]].to_dict(orient="records")

            insert_sql = """
            INSERT INTO vendors (
                business_name, category, service, contact_name,
                phone, email, website, address, notes,
                computed_keywords, created_at, updated_at
            ) VALUES (
                :business_name, :category, :service, :contact_name,
                :phone, :email, :website, :address, :notes,
                :computed_keywords, :created_at, :updated_at
            )
            """
            for r in rows:
                cx.execute(sql_text(insert_sql), r)

            cnt2 = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0
            return f"Bootstrap inserted {len(rows)} rows (total now {cnt2})."

    except Exception as e:
        raise RuntimeError(f"Bootstrap error: {e}")


# =============================
#   Data access helpers
# =============================
@st.cache_data(show_spinner=False)
def fetch_page(engine: Engine, q: str, offset: int = 0, limit: int = PAGE_SIZE) -> pd.DataFrame:
    """Fetch vendors page with trivial LIKE filter on a composed blob of key fields."""
    where = ""
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if q:
        where = "WHERE (business_name || ' ' || category || ' ' || service || ' ' || IFNULL(contact_name,'') || ' ' || IFNULL(notes,'')) LIKE :q"
        params["q"] = f"%{q}%"
    sql = f"""
        SELECT business_name, category, service, contact_name, phone, email, website,
               address, notes, computed_keywords, created_at, updated_at
        FROM vendors
        {where}
        ORDER BY category COLLATE NOCASE ASC, service COLLATE NOCASE ASC, business_name COLLATE NOCASE ASC
        LIMIT :limit OFFSET :offset
    """
    with engine.begin() as cx:
        rows = cx.exec_driver_sql(sql, params).fetchall()
        cols = [
            "business_name","category","service","contact_name","phone","email","website",
            "address","notes","computed_keywords","created_at","updated_at"
        ]
        df = pd.DataFrame(rows, columns=cols)
    return df


@st.cache_data(show_spinner=False)
def count_rows(engine: Engine, q: str) -> int:
    where = ""
    params: dict[str, Any] = {}
    if q:
        where = "WHERE (business_name || ' ' || category || ' ' || service || ' ' || IFNULL(contact_name,'') || ' ' || IFNULL(notes,'')) LIKE :q"
        params["q"] = f"%{q}%"
    sql = f"SELECT COUNT(*) FROM vendors {where}"
    with engine.begin() as cx:
        return int(cx.exec_driver_sql(sql, params).scalar() or 0)


def insert_row(engine: Engine, row: dict[str, Any]) -> int:
    now = _now_iso()
    row = dict(row)
    row.setdefault("created_at", now)
    row.setdefault("updated_at", now)
    insert_sql = """
        INSERT INTO vendors (
            business_name, category, service, contact_name,
            phone, email, website, address, notes,
            computed_keywords, created_at, updated_at
        ) VALUES (
            :business_name, :category, :service, :contact_name,
            :phone, :email, :website, :address, :notes,
            :computed_keywords, :created_at, :updated_at
        )
    """
    with engine.begin() as cx:
        cx.execute(sql_text(insert_sql), row)
        new_id = int(cx.exec_driver_sql("SELECT last_insert_rowid()").scalar())
    return new_id


# =============================
#   UI — Tabs
# =============================
engine = build_engine()
ensure_schema(engine)

# Status banner
try:
    with engine.begin() as cx:
        target = os.path.abspath(DB_PATH)
        cnt = int(cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0)
    st.caption(f"DB target: {target} | vendors: {cnt} | {APP_VER}")
except Exception as e:
    st.error(f"DB diagnostics failed: {e}")

# One-time bootstrap if needed
try:
    msg = _bootstrap_from_csv_if_needed(engine, SEED_CSV)
    if msg:
        st.caption(msg)
except Exception as e:
    st.error(str(e))


tab_browse, tab_add = st.tabs(["Browse", "Add"])

# ---- Browse ----
with tab_browse:
    st.subheader("Browse Providers")
    c1, c2 = st.columns([3, 1])
    with c1:
        q = st.text_input("Search", value=st.session_state.get("q", ""), placeholder="name, category, service, notes…")
    with c2:
        if st.button("Clear"):
            q = ""
    st.session_state["q"] = q

    total = count_rows(engine, q)
    st.caption(f"{total} matching provider(s)")

    df = fetch_page(engine, q, 0, PAGE_SIZE)

    # Ensure columns exist, then reindex to policy order
    for col in BROWSE_DISPLAY_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    vdf = df.reindex(columns=BROWSE_DISPLAY_COLUMNS)
# ==== BEGIN: Browse render (safe, policy-aligned) ====
MAX_RENDER_ROWS = 1000
_src = vdf if "vdf" in locals() else df

if _src is None or _src.empty:
    st.info("No matching providers. Tip: try fewer words.")
else:
    _render = _src.copy()

    # Never show these if present
    for col in ("id", "city", "state", "zip"):
        if col in _render.columns:
            _render.drop(columns=[col], inplace=True)

    # Ensure all policy columns exist (prevents KeyError on reindex)
    for col in BROWSE_DISPLAY_COLUMNS:
        if col not in _render.columns:
            _render[col] = ""

    # Reorder & restrict to the approved set
    _render = _render.reindex(columns=BROWSE_DISPLAY_COLUMNS)

    # Cap and remove row-number index
    _render = _render.head(MAX_RENDER_ROWS).reset_index(drop=True)

    st.dataframe(_render, use_container_width=True, hide_index=True)
# ==== END: Browse render (safe, policy-aligned) ====

# Render grid (hide row numbers, no `id`)
    if vdf.empty:
        st.info("No matching providers. Tip: try fewer words.")
    else:
        show_df = vdf.head(MAX_RENDER_ROWS).copy()
        st.dataframe(show_df, use_container_width=True, hide_index=True)

    # CSV export aligned to the same policy
    if total > 0:
        export_df = vdf.copy()
        csv_bytes = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV (Browse view)",
            data=csv_bytes,
            file_name="providers_browse.csv",
            mime="text/csv",
        )


# ---- Add ----
with tab_add:
    st.subheader("Add Provider")
    with st.form("add_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            business_name = st.text_input("Business Name *")
            category = st.text_input("Category *")
            service = st.text_input("Service *")
        with c2:
            contact_name = st.text_input("Contact Name")
            phone = st.text_input("Phone")
            email = st.text_input("Email")
        with c3:
            website = st.text_input("Website")
            address = st.text_input("Address")
            notes = st.text_area("Notes", height=80)

        submitted = st.form_submit_button("Add")

    if submitted:
        # Validate
        missing = [
            n for n, v in [
                ("business_name", business_name),
                ("category", category),
                ("service", service),
            ] if not v or not str(v).strip()
        ]
        if missing:
            st.error(", ".join(missing) + " are required.")
        else:
            ckws = _compute_keywords(category.strip(), service.strip(), business_name.strip())
            row = dict(
                business_name=business_name.strip(),
                category=category.strip(),
                service=service.strip(),
                contact_name=(contact_name or "").strip() or None,
                phone=_digits_only(phone) if phone else None,
                email=(email or "").strip() or None,
                website=(website or "").strip() or None,
                address=(address or "").strip() or None,
                notes=(notes or "").strip() or None,
                computed_keywords=ckws,
            )
            try:
                new_id = insert_row(engine, row)
                st.success(f"Added provider ID {new_id}")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Insert failed: {e}")


# Footer
st.caption("© Providers Admin • " + APP_VER)
