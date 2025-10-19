# -*- coding: utf-8 -*-
from __future__ import annotations

# ---- Streamlit page config MUST be the first Streamlit command ----
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

# ---- Stdlib ----
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

# ---- Third-party ----
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine


# (If you had a big triple-quoted docstring or bullet text at the top, move it BELOW this block or delete it.)



# =============================
#   Global constants / policy
# =============================
APP_VER = "admin-2025-10-19.final"
DB_PATH = os.getenv("DB_PATH", "providers.db")
MAX_RENDER_ROWS_ADMIN = int(os.getenv("MAX_RENDER_ROWS_ADMIN", "500"))

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
    "notes": 360,
    "computed_keywords": 420,
}

# =============================
#   Helpers (string / time)
# =============================

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
        # strip common www.
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
    # de-dup but keep stable-ish order
    seen = set()
    out: List[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# =============================
#   Database / schema
# =============================

@st.cache_resource(show_spinner=False)
def build_engine() -> Engine:
    # Ensure parent dir exists for local sqlite path
    try:
        d = os.path.dirname(DB_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    eng = sa.create_engine(f"sqlite:///{DB_PATH}", future=True)
    return eng


def ensure_schema(eng: Engine) -> None:
    """Create tables/columns/indexes idempotently.
    - vendors main table
    - categories/services lookup tables (seeded from vendors)
    - computed_keywords column + indexes
    """
    with eng.begin() as cx:
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
                created_at TEXT,
                updated_at TEXT,
                computed_keywords TEXT,
                ckw_locked INTEGER DEFAULT 0,
                ckw_version INTEGER DEFAULT 1
            )
            """
        )
        # Add missing column computed_keywords if old DB
        try:
            cx.exec_driver_sql("SELECT computed_keywords FROM vendors LIMIT 1")
        except Exception:
            cx.exec_driver_sql("ALTER TABLE vendors ADD COLUMN computed_keywords TEXT")
        # Indexes
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_business_name ON vendors(business_name COLLATE NOCASE)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)"
        )
        # Lookup tables
        cx.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS categories (name TEXT PRIMARY KEY)"
        )
        cx.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS services (name TEXT PRIMARY KEY)"
        )
        # Seed lookups from existing vendors if empty
        cnt_cat = cx.exec_driver_sql("SELECT COUNT(*) FROM categories").scalar() or 0
        if cnt_cat == 0:
            cx.exec_driver_sql(
                "INSERT OR IGNORE INTO categories(name) SELECT DISTINCT TRIM(category) FROM vendors WHERE IFNULL(TRIM(category),'')<>''"
            )
        cnt_srv = cx.exec_driver_sql("SELECT COUNT(*) FROM services").scalar() or 0
        if cnt_srv == 0:
            cx.exec_driver_sql(
                "INSERT OR IGNORE INTO services(name) SELECT DISTINCT TRIM(service) FROM vendors WHERE IFNULL(TRIM(service),'')<>''"
            )


# =============================
#   CKW: compute & writebacks
# =============================

def compute_ckw(row: Dict[str, Any]) -> str:
    # Base parts
    parts: List[str] = []
    parts.extend(_tokenize_for_ckw(row.get("category", "")))
    parts.extend(_tokenize_for_ckw(row.get("service", "")))
    parts.extend(_tokenize_for_ckw(row.get("business_name", "")))
    # Add digits-only phone and host
    pdig = _digits_only(row.get("phone"))
    if pdig:
        parts.append(pdig)
    host = _host_only(row.get("website"))
    if host:
        parts.extend(_tokenize_for_ckw(host))
    # Address/notes lightly tokenized (cheap safety net)
    parts.extend(_tokenize_for_ckw(row.get("address", "")))
    parts.extend(_tokenize_for_ckw(row.get("notes", "")))
    # Compose
    ckw = " ".join(parts).strip()
    return ckw


def recompute_ckw_for_ids(eng: Engine, ids: List[int]) -> int:
    if not ids:
        return 0
    updated = 0
    with eng.begin() as cx:
        for vid in ids:
            r = cx.exec_driver_sql(
                "SELECT business_name,category,service,contact_name,phone,email,website,address,notes FROM vendors WHERE id=:id",
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


def recompute_ckw_for_catserv(eng: Engine, category: str, service: str) -> int:
    with eng.begin() as cx:
        ids = [
            row[0]
            for row in cx.exec_driver_sql(
                "SELECT id FROM vendors WHERE category=:c AND service=:s",
                {"c": category, "s": service},
            ).all()
        ]
    return recompute_ckw_for_ids(eng, ids)


def recompute_ckw_all(eng: Engine) -> int:
    with eng.begin() as cx:
        ids = [row[0] for row in cx.exec_driver_sql("SELECT id FROM vendors").all()]
    return recompute_ckw_for_ids(eng, ids)


# =============================
#   Lookup lists (categories / services)
# =============================

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


# =============================
#   Search (CKW-first with fallback)
# =============================

def _build_like_filter(tokens: List[str], columns: List[str]) -> Tuple[str, Dict[str, Any]]:
    """Return SQL and params for AND of tokens across OR of columns."""
    clauses: List[str] = []
    params: Dict[str, Any] = {}
    for t_idx, tok in enumerate(tokens):
        or_parts: List[str] = []
        for c in columns:
            key = f"t{t_idx}_{c}"
            or_parts.append(f"{c} LIKE :{key}")
            params[key] = f"%{tok}%"
        clauses.append("(" + " OR ".join(or_parts) + ")")
    where_sql = " AND ".join(clauses) if clauses else "1=1"
    return where_sql, params


def _tokenize_query(q: str) -> List[str]:
    return [t.lower().strip() for t in q.split() if t.strip()]


def search_ids_ckw_first(eng: Engine, q: str, limit: int) -> List[int]:
    tokens = _tokenize_query(q)
    with eng.begin() as cx:
        if tokens:
            # 1) Try CKW only
            w1, p1 = _build_like_filter(tokens, ["computed_keywords"])
            sql1 = f"SELECT id FROM vendors WHERE {w1} ORDER BY business_name COLLATE NOCASE LIMIT :lim"
            ids = [r[0] for r in cx.exec_driver_sql(sql1, {**p1, "lim": limit}).all()]
            if ids:
                return ids
            # 2) Fallback fields
            fields = [
                "business_name",
                "category",
                "service",
                "notes",
                "phone",
                "website",
                "address",
            ]
            w2, p2 = _build_like_filter(tokens, fields)
            sql2 = f"SELECT id FROM vendors WHERE {w2} ORDER BY business_name COLLATE NOCASE LIMIT :lim"
            return [r[0] for r in cx.exec_driver_sql(sql2, {**p2, "lim": limit}).all()]
        else:
            sql = "SELECT id FROM vendors ORDER BY business_name COLLATE NOCASE LIMIT :lim"
            return [r[0] for r in cx.exec_driver_sql(sql, {"lim": limit}).all()]


def fetch_rows_by_ids(eng: Engine, ids: List[int]) -> pd.DataFrame:
    if not ids:
        return pd.DataFrame(columns=BROWSE_COLUMNS)
    id_list = ",".join(str(i) for i in ids)
    sql = f"""
        SELECT {', '.join(BROWSE_COLUMNS)}
        FROM vendors
        WHERE id IN ({id_list})
        ORDER BY business_name COLLATE NOCASE
    """
    with eng.begin() as cx:
        df = pd.read_sql_query(sql, cx.connection)
    return df


# =============================
#   CRUD helpers
# =============================

def insert_vendor(eng: Engine, data: Dict[str, Any]) -> int:
    row = dict(data)
    row["computed_keywords"] = compute_ckw(row)
    row["created_at"] = row["updated_at"] = _now_iso()
    with eng.begin() as cx:
        res = cx.exec_driver_sql(
            sql_text(
                """
                INSERT INTO vendors (business_name,category,service,contact_name,phone,email,website,address,notes,created_at,updated_at,computed_keywords)
                VALUES (:business_name,:category,:service,:contact_name,:phone,:email,:website,:address,:notes,:created_at,:updated_at,:computed_keywords)
                """
            ),
            row,
        )
        new_id = int(res.lastrowid or 0)
    # Ensure lookups
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
            sql_text(
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
                    computed_keywords=:computed_keywords,
                    updated_at=:updated_at
                WHERE id=:id
                """
            ),
            row,
        )
    # Ensure lookups
    ensure_lookup_value(eng, "categories", row.get("category", ""))
    ensure_lookup_value(eng, "services", row.get("service", ""))


def delete_vendor(eng: Engine, vid: int) -> None:
    with eng.begin() as cx:
        cx.exec_driver_sql("DELETE FROM vendors WHERE id=:id", {"id": vid})


# =============================
#   UI helpers
# =============================

def _column_config_from_widths(widths: Dict[str, int]) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    for col, w in widths.items():
        if col in BROWSE_COLUMNS:
            cfg[col] = st.column_config.TextColumn(col.replace("_", " ").title(), width=w)
    return cfg


# =============================
#   Main App
# =============================

def main() -> None:
    eng = build_engine()
    ensure_schema(eng)

    st.title("Providers — Admin")
    st.caption(f"Version: {APP_VER}")

    tab_browse, tab_manage, tab_catsvc, tab_maint = st.tabs([
        "Browse",
        "Add / Edit / Delete",
        "Category / Service",
        "Maintenance",
    ])

    # ---------------------
    # Browse (Admin)
    # ---------------------
    with tab_browse:
        # (Per Randy) Remove the header lines at the *top of the Browse page*.
        # We keep them above the tabs globally, so nothing here.

        # Search box
        c1, c2 = st.columns([1, 0.3])
        q = c1.text_input("Search", value=st.session_state.get("q", ""), placeholder="name, category, service, notes, phone, website…")
        if c2.button("Clear"):
            q = ""
        st.session_state["q"] = q

        # Search CKW-first, fallback if needed; cap results; no Prev/Next
        ids = search_ids_ckw_first(eng, q, limit=MAX_RENDER_ROWS_ADMIN)
        n = len(ids)
        if n == MAX_RENDER_ROWS_ADMIN:
            st.info(f"Showing first {MAX_RENDER_ROWS_ADMIN} matches (cap). Refine your search to narrow further.")
        df = fetch_rows_by_ids(eng, ids)

        # Column widths (secrets override)
        widths = dict(DEFAULT_COLUMN_WIDTHS_PX_ADMIN)
        try:
            widths.update(st.secrets.get("COLUMN_WIDTHS_PX_ADMIN", {}))
        except Exception:
            pass
        colcfg = _column_config_from_widths(widths)

        # Render single-line cells; horizontal scroll via column widths
        st.dataframe(
            df[BROWSE_COLUMNS],
            hide_index=True,
            use_container_width=True,
            column_config=colcfg,
        )

    # ---------------------
    # Add / Edit / Delete
    # ---------------------
    with tab_manage:
        lc, rc = st.columns([1, 1], gap="large")

        # ---------- Add (left) ----------
        with lc:
            st.subheader("Add Provider")
            cats = list_categories(eng)
            srvs = list_services(eng)

            bn = st.text_input("Business Name *")

            # Category select or new
            ccol1, ccol2 = st.columns([1, 1])
            cat_choice = ccol1.selectbox("Category *", options=["— Select —"] + cats)
            cat_new = ccol2.text_input("New Category (optional)")
            category = cat_new.strip() if cat_new.strip() else (cat_choice if cat_choice != "— Select —" else "")

            # Service select or new
            scol1, scol2 = st.columns([1, 1])
            srv_choice = scol1.selectbox("Service *", options=["— Select —"] + srvs)
            srv_new = scol2.text_input("New Service (optional)")
            service = srv_new.strip() if srv_new.strip() else (srv_choice if srv_choice != "— Select —" else "")

            contact_name = st.text_input("Contact Name")
            phone = st.text_input("Phone")
            email = st.text_input("Email")
            website = st.text_input("Website")
            address = st.text_input("Address")
            notes = st.text_area("Notes", height=100)

            disabled = not (bn.strip() and category and service)
            if st.button("Add Provider", type="primary", disabled=disabled):
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
                }
                vid = insert_vendor(eng, data)
                ensure_lookup_value(eng, "categories", category)
                ensure_lookup_value(eng, "services", service)
                st.success(f"Added provider #{vid}: {data['business_name']}")

            # ---------- Delete (left, under Add) ----------
            st.divider()
            st.subheader("Delete Provider")
            # Lightweight selector: list business names with id
            with eng.begin() as cx:
                opts = cx.exec_driver_sql(
                    "SELECT id, business_name FROM vendors ORDER BY business_name COLLATE NOCASE"
                ).all()
            if opts:
                labels = [f"#{i} — {n}" for (i, n) in opts]
                pick = st.selectbox("Select provider to delete", options=["— Select —"] + labels)
                if pick != "— Select —":
                    idx = labels.index(pick)
                    del_id = int(opts[idx][0])
                    confirm = st.checkbox("I understand this will permanently delete the provider.")
                    ack = st.text_input("Type DELETE to confirm")
                    if st.button("Delete", type="secondary", disabled=not (confirm and ack == "DELETE")):
                        delete_vendor(eng, del_id)
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
                sel = st.selectbox("Pick a provider", options=labels)
                sel_id = int(rows[labels.index(sel)][0])
                with eng.begin() as cx:
                    r = cx.exec_driver_sql(
                        "SELECT business_name,category,service,contact_name,phone,email,website,address,notes FROM vendors WHERE id=:id",
                        {"id": sel_id},
                    ).mappings().first()
                if r:
                    bn_e = st.text_input("Business Name *", value=r["business_name"])

                    cats = list_categories(eng)
                    srvs = list_services(eng)

                    e_c1, e_c2 = st.columns([1, 1])
                    cat_choice_e = e_c1.selectbox("Category *", options=["— Select —"] + cats, index=(cats.index(r["category"]) + 1) if r["category"] in cats else 0)
                    cat_new_e = e_c2.text_input("New Category (optional)")
                    category_e = cat_new_e.strip() if cat_new_e.strip() else (cat_choice_e if cat_choice_e != "— Select —" else r["category"]) 

                    e_s1, e_s2 = st.columns([1, 1])
                    srv_choice_e = e_s1.selectbox("Service *", options=["— Select —"] + srvs, index=(srvs.index(r["service"]) + 1) if r["service"] in srvs else 0)
                    srv_new_e = e_s2.text_input("New Service (optional)")
                    service_e = srv_new_e.strip() if srv_new_e.strip() else (srv_choice_e if srv_choice_e != "— Select —" else r["service"]) 

                    contact_name_e = st.text_input("Contact Name", value=r["contact_name"] or "")
                    phone_e = st.text_input("Phone", value=r["phone"] or "")
                    email_e = st.text_input("Email", value=r["email"] or "")
                    website_e = st.text_input("Website", value=r["website"] or "")
                    address_e = st.text_input("Address", value=r["address"] or "")
                    notes_e = st.text_area("Notes", value=r["notes"] or "", height=100)

                    if st.button("Save Changes", type="primary"):
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
                        }
                        update_vendor(eng, sel_id, data)
                        ensure_lookup_value(eng, "categories", data["category"]) 
                        ensure_lookup_value(eng, "services", data["service"]) 
                        st.success(f"Saved changes to provider #{sel_id}.")

    # ---------------------
    # Category / Service management
    # ---------------------
    with tab_catsvc:
        st.subheader("Manage Categories & Services")

        cc, ss = st.columns([1, 1], gap="large")

        # ---- Category management (left)
        with cc:
            st.markdown("**Categories**")
            cats = list_categories(eng)
            st.caption(f"{len(cats)} categories")
            st.write(", ".join(cats) or "—")

            st.markdown("**Add Category**")
            new_cat = st.text_input("New Category", key="add_cat")
            if st.button("Add Category") and new_cat.strip():
                ensure_lookup_value(eng, "categories", new_cat.strip())
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
                    changed = recompute_ckw_for_catserv(eng, to_val, "%")  # not exact; do full recompute below
                    changed = recompute_ckw_all(eng)  # safe and simple at small scale
                    st.success(f"Reassigned category '{from_cat}' → '{to_val}'. Recomputed CKW for {changed} provider(s).")

        # ---- Service management (right)
        with ss:
            st.markdown("**Services**")
            srvs = list_services(eng)
            st.caption(f"{len(srvs)} services")
            st.write(", ".join(srvs) or "—")

            st.markdown("**Add Service**")
            new_srv = st.text_input("New Service", key="add_srv")
            if st.button("Add Service") and new_srv.strip():
                ensure_lookup_value(eng, "services", new_srv.strip())
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
                    st.success(f"Reassigned service '{from_srv}' → '{to_val}'. Recomputed CKW for {changed} provider(s).")

    # ---------------------
    # Maintenance
    # ---------------------
    with tab_maint:
        st.subheader("Maintenance — Computed Keywords (CKW)")
        st.caption("CKW is auto-updated on Add/Edit and when you reassign categories/services. Use these for bulk fixes.")

        # Single vendor recompute
        with eng.begin() as cx:
            opts = cx.exec_driver_sql(
                "SELECT id, business_name FROM vendors ORDER BY business_name COLLATE NOCASE"
            ).all()
        if opts:
            labels = [f"#{i} — {n}" for (i, n) in opts]
            sel = st.selectbox("Recompute CKW for one provider", options=["— Select —"] + labels)
            if sel != "— Select —":
                vid = int(opts[labels.index(sel)][0])
                if st.button("Recompute for this provider"):
                    n = recompute_ckw_for_ids(eng, [vid])
                    st.success(f"Recomputed CKW for {n} provider(s).")
        else:
            st.info("No providers yet.")

        # Recompute all
        st.divider()
        if st.button("Recompute CKW for ALL providers", type="secondary"):
            n = recompute_ckw_all(eng)
            st.success(f"Recomputed CKW for {n} provider(s).")


if __name__ == "__main__":
    main()
