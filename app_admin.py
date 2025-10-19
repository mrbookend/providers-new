# app_admin.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Providers — Admin (providers-new)

Design notes (concise):
- Guarded st.set_page_config() is the first Streamlit command to survive Cloud pre-enqueue.
- No @st.cache_data at module level. We define UNCACHED core functions only.
- Caching is (re)applied inside main() after page_config:
    - Engine via @st.cache_resource
    - Data via st.cache_data with only hashable args (q/limit/offset/DATA_VER)
- DATA_VER in session_state bumps after writes to invalidate data caches.
"""

# ---- Streamlit page config MUST be the first Streamlit command ----
import streamlit as st


def _safe_page_config() -> None:
    """Guarded page config so Cloud pre-enqueue or double-runs don't crash."""
    try:
        st.set_page_config(
            page_title="Providers — Admin",
            page_icon="🛠️",
            layout="wide",
            initial_sidebar_state="expanded",
        )
    except Exception:
        # Cloud sometimes re-runs or enqueues before config; ignore duplicate/ordering errors.
        pass


_safe_page_config()

# ---- Stdlib ----
import os
from datetime import datetime
from typing import Any

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
#   Small helpers (pure)
# =============================


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _digits_only(s: str | None) -> str:
    if not s:
        return ""
    return "".join(ch for ch in str(s) if ch.isdigit())


def _format_phone(s: str | None) -> str:
    d = _digits_only(s)
    if len(d) == 10:
        return f"({d[0:3]}) {d[3:6]}-{d[6:10]}"
    return s or ""


# =============================
#   Engine builder (UNCACHED core)
# =============================


def _build_engine_uncached() -> Engine:
    # sqlite path; check_same_thread=False to be Streamlit-friendly
    eng = sa.create_engine(
        f"sqlite:///{DB_PATH}",
        connect_args={"check_same_thread": False},
        future=True,
    )
    return eng


# =============================
#   Data access (UNCACHED core)
# =============================


def _count_rows_uncached(eng: Engine, q: str) -> int:
    """Return total count matching the simple LIKE filter (uncached core)."""
    where = ""
    params: dict[str, Any] = {}
    if q:
        where = (
            "WHERE (business_name || ' ' || category || ' ' || service || ' ' || "
            "IFNULL(contact_name,'') || ' ' || IFNULL(notes,'')) LIKE :q"
        )
        params["q"] = f"%{q}%"

    sql = f"SELECT COUNT(*) AS n FROM vendors {where}"
    with eng.connect() as cx:
        val = cx.execute(sql_text(sql), params).scalar()
        return int(val or 0)


def _fetch_page_uncached(
    eng: Engine, q: str, offset: int = 0, limit: int = PAGE_SIZE
) -> pd.DataFrame:
    """Fetch vendors page with a simple LIKE filter over key fields (uncached core)."""
    where = ""
    params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
    if q:
        where = (
            "WHERE (business_name || ' ' || category || ' ' || service || ' ' || "
            "IFNULL(contact_name,'') || ' ' || IFNULL(notes,'')) LIKE :q"
        )
        params["q"] = f"%{q}%"

    sql = f"""
        SELECT
          business_name, category, service, contact_name, phone, email, website,
          address, notes, computed_keywords, created_at, updated_at
        FROM vendors
        {where}
        ORDER BY category COLLATE NOCASE ASC,
                 service  COLLATE NOCASE ASC,
                 business_name COLLATE NOCASE ASC
        LIMIT :limit OFFSET :offset
    """
    with eng.connect() as cx:
        df = pd.read_sql_query(sql_text(sql), cx, params=params)
    return df


def insert_row(eng: Engine, row: dict[str, Any]) -> int:
    """Insert a vendor row (writes are never cached)."""
    now = _now_iso()
    data = dict(row)
    data.setdefault("created_at", now)
    data.setdefault("updated_at", now)

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
    with eng.begin() as cx:
        cx.execute(sql_text(insert_sql), data)
        new_id = int(cx.exec_driver_sql("SELECT last_insert_rowid()").scalar())
    return new_id


# =============================
#   MAIN APP
# =============================


def main() -> None:
    # page_config already set by _safe_page_config()

    st.write(f"### Providers — Admin (providers-new)")

    # A tiny status banner (DB path + quick count), non-fatal if missing table.
    try:
        # Re-wrap the core engine with resource caching (post-config only)
        build_engine = st.cache_resource(show_spinner=False)(_build_engine_uncached)

        # Simple cache-buster that increments after writes
        if "DATA_VER" not in st.session_state:
            st.session_state["DATA_VER"] = 0
        DATA_VER = st.session_state["DATA_VER"]

        # Cached, hashable-args wrappers (engine captured in closure)
        def count_rows(q: str, data_ver: int = 0) -> int:
            eng = build_engine()
            return st.cache_data(show_spinner=False)(
                lambda _q, _v: _count_rows_uncached(eng, _q)
            )(q, data_ver)

        def fetch_page(
            q: str, offset: int = 0, limit: int = PAGE_SIZE, data_ver: int = 0
        ) -> pd.DataFrame:
            eng = build_engine()
            return st.cache_data(show_spinner=False)(
                lambda _q, _o, _l, _v: _fetch_page_uncached(eng, _q, _o, _l)
            )(q, offset, limit, data_ver)

        # Diagnostics caption
        try:
            eng = build_engine()
            with eng.connect() as cx:
                target = DB_PATH
                cnt = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar()
            st.caption(f"DB target: {target} | vendors: {int(cnt or 0)} | {APP_VER}")
        except Exception as e_diag:
            st.error(f"DB diagnostics failed: {e_diag}")

        tab_browse, tab_add = st.tabs(["Browse", "Add"])

        # -----------------------------
        # Browse tab
        # -----------------------------
        with tab_browse:
            st.subheader("Browse Providers")

            # Search row
            c1, c2 = st.columns([3, 1])
            with c1:
                q = st.text_input(
                    "Search",
                    placeholder="name, category, service, notes…",
                    value=st.session_state.get("q", ""),
                )
            with c2:
                if st.button("Clear"):
                    q = ""
            st.session_state["q"] = q

            # Totals
            total = count_rows(q, DATA_VER)
            st.caption(f"{total} matching provider(s)")

            # Pagination (simple)
            if "page_offset" not in st.session_state:
                st.session_state["page_offset"] = 0
            page_offset = st.session_state["page_offset"]

            # Reset offset if query changed
            if st.session_state.get("_last_q") != q:
                page_offset = 0
                st.session_state["page_offset"] = 0
            st.session_state["_last_q"] = q

            # Page controls
            cprev, cinfo, cnext = st.columns([1, 2, 1])
            with cprev:
                if st.button("◀ Prev", disabled=(page_offset <= 0)):
                    page_offset = max(0, page_offset - PAGE_SIZE)
                    st.session_state["page_offset"] = page_offset
            with cnext:
                if st.button(
                    "Next ▶", disabled=(page_offset + PAGE_SIZE >= max(0, total))
                ):
                    pass
                else:
                    if page_offset + PAGE_SIZE < total:
                        page_offset += PAGE_SIZE
                        st.session_state["page_offset"] = page_offset
            with cinfo:
                start = 0 if total == 0 else page_offset + 1
                end = min(total, page_offset + PAGE_SIZE)
                st.write(f"Showing {start}–{end} of {total}")

            # Fetch and render
            if total == 0:
                st.info("No matching providers. Tip: try fewer words.")
            else:
                df = fetch_page(q, offset=page_offset, limit=PAGE_SIZE, data_ver=DATA_VER)
                if df.empty:
                    st.info("No results on this page. Use Prev.")
                else:
                    # Render (cap rows if needed)
                    show_df = df.head(MAX_RENDER_ROWS).copy()
                    # Phone formatting for display
                    if "phone" in show_df.columns:
                        show_df["phone"] = show_df["phone"].map(_format_phone)
                    st.dataframe(
                        show_df[BROWSE_DISPLAY_COLUMNS],
                        use_container_width=True,
                        hide_index=True,
                    )

                    # CSV download of the (filtered) page shown
                    csv_bytes = show_df[BROWSE_DISPLAY_COLUMNS].to_csv(
                        index=False
                    ).encode("utf-8")
                    st.download_button(
                        "Download this page (CSV)",
                        data=csv_bytes,
                        file_name="providers_page.csv",
                        mime="text/csv",
                    )

        # -----------------------------
        # Add tab
        # -----------------------------
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
                missing = []
                if not business_name.strip():
                    missing.append("Business Name")
                if not category.strip():
                    missing.append("Category")
                if not service.strip():
                    missing.append("Service")

                if missing:
                    st.error(", ".join(missing) + " are required.")
                else:
                    try:
                        # Normalize phone to digits for storage
                        phone_digits = _digits_only(phone)

                        # Build row (computed_keywords left blank; add later if desired)
                        row = {
                            "business_name": business_name.strip(),
                            "category": category.strip(),
                            "service": service.strip(),
                            "contact_name": contact_name.strip(),
                            "phone": phone_digits,
                            "email": (email or "").strip(),
                            "website": (website or "").strip(),
                            "address": (address or "").strip(),
                            "notes": (notes or "").strip(),
                            "computed_keywords": "",
                            "created_at": _now_iso(),
                            "updated_at": _now_iso(),
                        }

                        eng = build_engine()
                        new_id = insert_row(eng, row)

                        # Bump data version to bust caches
                        st.session_state["DATA_VER"] = st.session_state.get(
                            "DATA_VER", 0
                        ) + 1
                        st.cache_data.clear()

                        st.success(f"Added provider ID {new_id}")
                        st.rerun()
                    except Exception as e_ins:
                        st.error(f"Insert failed: {e_ins}")

        # Footer
        st.caption("© Providers Admin • " + APP_VER)

    except Exception as e:
        # Top-level safety net for unexpected issues
        st.error(f"App crashed: {e}")


if __name__ == "__main__":
    main()
