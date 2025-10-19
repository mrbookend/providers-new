# app_admin.py
# -*- coding: utf-8 -*-
from __future__ import annotations

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
from datetime import datetime, timezone
from typing import Any, Iterable

# ---- Third-party ----
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

# =============================
#   Global constants / policy
# =============================
APP_VER = "admin-2025-10-19.4"
DB_PATH = os.getenv("DB_PATH", "providers.db")
SEED_CSV = os.getenv("SEED_CSV", "data/providers_seed.csv")
PAGE_SIZE = 200
MAX_RENDER_ROWS = 1000

# Controls whether first-run empty DB will attempt a seed import.
# Flip to 0 to skip CSV seeding entirely.
ALLOW_SEED_IMPORT = int(os.getenv("ALLOW_SEED_IMPORT", "1"))

BROWSE_DISPLAY_COLUMNS = [
    "business_name",
    "category",
    "service",
    "contact_name",
    "phone",
    "email",
    "website",
    "city",
    "state",
    "notes",
    "updated_at",
]

# =============================
#   Helpers
# =============================
def _digits_only(s: str | None) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())

def _format_phone(s: str | None) -> str:
    d = _digits_only(s)
    if len(d) == 10:
        return f"({d[0:3]}) {d[3:6]}-{d[6:10]}"
    return s or ""

def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

# =============================
#   Engine (uncached core)
# =============================
def _build_engine_uncached(db_path: str) -> Engine:
    # SQLite file database; creates the file on first connection if not exists
    url = sa.URL.create("sqlite", database=db_path)
    eng = sa.create_engine(url, future=True)
    return eng

# =============================
#   Patch A: Schema bootstrap + optional seed import (UNCACHED)
# =============================
def _ensure_schema_uncached(engine: Engine, allow_seed: bool = True) -> str:
    """
    Ensure vendors schema exists. If vendors is empty and allow_seed is True,
    attempt a one-time CSV import from SEED_CSV.

    Returns a short status message for UI display.
    """
    ddl = """
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
        city TEXT,
        state TEXT,
        zip TEXT,
        notes TEXT,
        created_at TEXT,
        updated_at TEXT,
        computed_keywords TEXT,
        ckw_locked INTEGER DEFAULT 0,
        ckw_version INTEGER DEFAULT 1
    );
    """
    created = False
    seeded = False

    with engine.begin() as cx:
        cx.exec_driver_sql(ddl)
        created = True  # table exists (created or already there)

        # Count existing rows
        n = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0

        # Optional seed import if empty
        if allow_seed and ALLOW_SEED_IMPORT and n == 0 and os.path.exists(SEED_CSV):
            # Read CSV with pandas for safety and normalization
            df = pd.read_csv(SEED_CSV)
            # Normalize expected columns
            expected = [
                "business_name","category","service","contact_name","phone","email",
                "website","address","city","state","zip","notes"
            ]
            for col in expected:
                if col not in df.columns:
                    df[col] = ""

            # Clean & format rows
            rows: list[dict[str, Any]] = []
            for _, r in df.iterrows():
                phone_raw = str(r.get("phone", "") or "")
                rows.append({
                    "business_name": str(r.get("business_name", "") or "").strip(),
                    "category": str(r.get("category", "") or "").strip(),
                    "service": str(r.get("service", "") or "").strip(),
                    "contact_name": str(r.get("contact_name", "") or "").strip(),
                    "phone": _format_phone(phone_raw),
                    "email": str(r.get("email", "") or "").strip(),
                    "website": str(r.get("website", "") or "").strip(),
                    "address": str(r.get("address", "") or "").strip(),
                    "city": str(r.get("city", "") or "").strip(),
                    "state": (str(r.get("state", "") or "").strip() or "TX"),
                    "zip": str(r.get("zip", "") or "").strip(),
                    "notes": str(r.get("notes", "") or "").strip(),
                    "created_at": _now_iso(),
                    "updated_at": _now_iso(),
                    "computed_keywords": "",
                    "ckw_locked": 0,
                    "ckw_version": 1,
                })

            if rows:
                cols = [
                    "business_name","category","service","contact_name","phone","email",
                    "website","address","city","state","zip","notes",
                    "created_at","updated_at","computed_keywords","ckw_locked","ckw_version"
                ]
                placeholders = ",".join([f":{c}" for c in cols])
                sql = f"INSERT INTO vendors ({','.join(cols)}) VALUES ({placeholders})"
                cx.execute(sa.text(sql), rows)
                seeded = True

    if seeded:
        return f"Schema OK; seed imported from {SEED_CSV}"
    if created:
        return "Schema OK"
    return "No schema changes"

# =============================
#   Data access (read-only browse)
# =============================
def _fetch_count(engine: Engine, q: str = "") -> int:
    # Simple count; extend with WHERE if you wire search later
    with engine.connect() as cx:
        n = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar()
    return int(n or 0)

def _fetch_page(engine: Engine, limit: int, offset: int) -> pd.DataFrame:
    sql = """
        SELECT
            business_name, category, service, contact_name, phone, email,
            website, city, state, notes, updated_at
        FROM vendors
        ORDER BY business_name COLLATE NOCASE ASC
        LIMIT :limit OFFSET :offset
    """
    with engine.connect() as cx:
        df = pd.read_sql(sa.text(sql), cx, params={"limit": limit, "offset": offset})
    # Post-format phone for display
    if "phone" in df.columns:
        df["phone"] = df["phone"].map(_format_phone)
    return df

# =============================
#   UI
# =============================
def main() -> None:
    st.title("Providers — Admin")
    st.caption(f"Version: {APP_VER}")
# ---- Session defaults (cache invalidation for data) ----
if "DATA_VER" not in st.session_state:
    st.session_state["DATA_VER"] = 0
DATA_VER = st.session_state["DATA_VER"]

    # ---- Build engine (cache inside main after page_config)
    @st.cache_resource
    def build_engine() -> Engine:
        return _build_engine_uncached(DB_PATH)

    eng = build_engine()

    # ---- Patch B: ensure schema (and optional seed) immediately after engine build
    status_msg = _ensure_schema_uncached(eng, allow_seed=True)

    # Small status line (toggle with env SHOW_STATUS="1")
    if os.getenv("SHOW_STATUS", "0") in ("1", "true", "TRUE"):
        try:
            with eng.connect() as _cx:
                try:
                    rows = _cx.exec_driver_sql("PRAGMA database_list").fetchall()
                    db_target = rows[0][2] if rows and len(rows[0]) >= 3 else DB_PATH
                except Exception:
                    db_target = DB_PATH
                vendors_cnt = _cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar()
            st.caption(f"DB target: {db_target} | vendors: {int(vendors_cnt or 0)} | {status_msg}")
        except Exception as e:
            st.error(f"DB diagnostics failed: {e}")
    else:
        st.caption(status_msg)
@st.cache_data(show_spinner=False)
def count_rows(_engine: Engine, q: str = "", _data_ver: int = 0) -> int:
    """
    Count rows (optionally later: apply WHERE for q). Cached by (q, DATA_VER).
    `_engine` name starts with underscore so Streamlit ignores it for hashing.
    """
    with _engine.connect() as cx:
        n = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar()
    return int(n or 0)

    # ---- Tabs
    tab_browse, tab_add = st.tabs(["Browse", "Add"])

    # ---- Browse tab
    with tab_browse:
        st.subheader("Browse Providers")
        # Simple pager (no per-column filters in this minimal admin)
        total = 0
        try:
            total = _fetch_count(eng)
        except Exception as e:
            st.error(f"Browse failed (count): {e}")
            return

        limit = PAGE_SIZE
        pages = max(1, (total + limit - 1) // limit)
        page_num = st.number_input("Page", min_value=1, max_value=pages, value=1, step=1)
        offset = (int(page_num) - 1) * limit

        try:
            vdf = _fetch_page(eng, limit=limit, offset=offset)
        except Exception as e:
            st.error(f"Browse failed (page): {e}")
            return

        if vdf.empty:
            st.info("No providers found.")
        else:
            # Clamp visible columns
            missing = [c for c in BROWSE_DISPLAY_COLUMNS if c not in vdf.columns]
            if missing:
                for m in missing:
                    vdf[m] = ""
            vdf = vdf[BROWSE_DISPLAY_COLUMNS].head(MAX_RENDER_ROWS)
            st.dataframe(vdf, use_container_width=True)

            # CSV download of current page
            csv_bytes = vdf.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download current page (CSV)",
                data=csv_bytes,
                file_name="providers_page.csv",
                mime="text/csv",
            )

    # ---- Add tab (minimal placeholder to keep file lean)
    with tab_add:
        st.subheader("Add Provider")
        with st.form("add_form"):
            business_name = st.text_input("Business Name *")
            category = st.text_input("Category *")
            service = st.text_input("Service *")
            contact_name = st.text_input("Contact Name")
            phone = st.text_input("Phone")
            email = st.text_input("Email")
            website = st.text_input("Website")
            address = st.text_input("Address")
            city = st.text_input("City")
            state = st.text_input("State", value="TX")
            zip_code = st.text_input("ZIP")
            notes = st.text_area("Notes", height=80)
            submitted = st.form_submit_button("Add Provider")

        if submitted:
            if not business_name.strip() or not category.strip() or not service.strip():
                st.error("Business Name, Category, and Service are required.")
            else:
                try:
                    with eng.begin() as cx:
                        sql = """
                        INSERT INTO vendors (
                            business_name, category, service, contact_name, phone, email,
                            website, address, city, state, zip, notes,
                            created_at, updated_at, computed_keywords, ckw_locked, ckw_version
                        ) VALUES (
                            :business_name, :category, :service, :contact_name, :phone, :email,
                            :website, :address, :city, :state, :zip, :notes,
                            :created_at, :updated_at, :computed_keywords, :ckw_locked, :ckw_version
                        )
                        """
                        params = {
                            "business_name": business_name.strip(),
                            "category": category.strip(),
                            "service": service.strip(),
                            "contact_name": contact_name.strip(),
                            "phone": _format_phone(phone),
                            "email": email.strip(),
                            "website": website.strip(),
                            "address": address.strip(),
                            "city": city.strip(),
                            "state": state.strip() or "TX",
                            "zip": zip_code.strip(),
                            "notes": notes.strip(),
                            "created_at": _now_iso(),
                            "updated_at": _now_iso(),
                            "computed_keywords": "",
                            "ckw_locked": 0,
                            "ckw_version": 1,
                        }
                        cx.execute(sa.text(sql), params)
                    st.success("Provider added.")
                except Exception as e:
                    st.error(f"Add failed: {e}")

if __name__ == "__main__":
    main()
