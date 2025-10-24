# -*- coding: utf-8 -*-
from __future__ import annotations
# SOT: mrbookend/providers-new · branch main · app_admin.py
# Any behavior change must bump APP_VER and pass CI guards.

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

# Tighten global page padding (left/right)
st.markdown(
    """
    <style>
      .block-container{
        padding-left: 8px !important;
        padding-right: 16px !important;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---- Session defaults (must run before any UI uses these) ----

import os


def _get_secret_int(name: str, default: int) -> int:
    try:
        v = st.secrets.get(name, default)
        return int(v)
    except Exception:
        return int(default)


# Cache-buster for @st.cache_data
st.session_state.setdefault("DATA_VER", 0)

# Browse / UI prefs
st.session_state.setdefault("SEARCH_Q", "")
st.session_state.setdefault("SHOW_HELP", False)
st.session_state.setdefault("BROWSE_PAGE", 0)
st.session_state.setdefault("PAGE_SIZE", _get_secret_int("MAX_RENDER_ROWS_ADMIN", 500))

# Flags toggled elsewhere (defensive defaults)
st.session_state.setdefault("CKW_BUSY", False)
st.session_state.setdefault("DB_READY", False)

# ---- Session defaults (must run before any UI uses these) ----
if "DATA_VER" not in st.session_state:
    st.session_state["DATA_VER"] = 0
if "q" not in st.session_state:
    st.session_state["q"] = ""

# ── Stdlib ────────────────────────────────────────────────────────────────
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
APP_VER = "admin-2025-10-22.1"
DB_PATH = os.getenv("DB_PATH", st.secrets.get("DB_PATH", "providers.db"))
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
MAX_RENDER_ROWS_ADMIN = int(
    os.getenv("MAX_RENDER_ROWS_ADMIN", st.secrets.get("MAX_RENDER_ROWS_ADMIN", "1000"))
)

# ---- CKW algorithm version (single source of truth) ----
# Bump default when you change CKW logic; env can override.
CURRENT_VER = int(os.getenv("CKW_CURRENT_VER", "2"))

# Columns to display on Browse (Admin)
# - "keywords" is the human-curated column (ckw_manual_extra)
# - "computed_keywords" is the algorithm output
BROWSE_COLUMNS = [
    "business_name",
    "category",
    "service",
    "keywords",  # human-curated (ckw_manual_extra)
    "computed_keywords",  # algorithm output
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
    "computed_keywords": 600,
    "contact_name": 160,
    "contact name": 160,
    "phone": 140,
    "email": 240,
    "email address": 240,
    "website": 240,
    "address": 260,
    "notes": 320,
    "ckw": 360,
}


def _merge_widths_with_secrets(defaults: Dict[str, int]) -> Dict[str, int]:
    overrides: Dict[str, int] = {}
    try:
        raw = st.secrets.get("COLUMN_WIDTHS_PX_ADMIN", {})
        if isinstance(raw, dict):
            for k, v in raw.items():
                try:
                    overrides[str(k)] = int(v)
                except Exception:
                    continue
    except Exception:
        pass
    out = dict(defaults)
    out.update(overrides)
    return out


COLUMN_WIDTHS_PX_ADMIN = _merge_widths_with_secrets(DEFAULT_COLUMN_WIDTHS_PX_ADMIN)
# Default visual order for Browse (aliases included)
DEFAULT_ORDER = [
    "business_name",
    "category",
    "service",
    "phone",
    "contact name",
    "website",
    "address",
    "email address",
    "notes",
    "keywords",
    "ckw",
]


def _order_from_secrets(default: list[str]) -> list[str]:
    try:
        raw = st.secrets.get("BROWSE_ORDER", None)
        if not raw:
            return default
        # Allow comma-separated string OR proper TOML array
        if isinstance(raw, str):
            cand = [c.strip() for c in raw.split(",") if c.strip()]
        elif isinstance(raw, (list, tuple)):
            cand = [str(c).strip() for c in raw if str(c).strip()]
        else:
            return default

        # Only keep known columns; ignore typos
        # Include tech/originals so secrets can intentionally unhide them.
        known = set(
            [
                "business_name",
                "category",
                "service",
                "phone",
                "contact name",
                "website",
                "address",
                "email address",
                "notes",
                "keywords",
                "ckw",
                # Tech/originals that can be explicitly requested:
                "id",
                "created_at",
                "updated_at",
                "ckw_locked",
                "ckw_version",
                "contact_name",
                "email",
                "computed_keywords",
            ]
        )
        ordered = [c for c in cand if c in known]
        return ordered or default
    except Exception:
        return default


ORDER = _order_from_secrets(DEFAULT_ORDER)

# ── Curated baseline seeds by service ──
CURATED_SEEDS_BY_SERVICE: dict[str, str] = {
    "Plumber": "plumber, plumbing, leak repair, drain cleaning, clogged drain, pipe burst, water heater, toilet repair, faucet repair, garbage disposal",
    "Electrician": "electrician, electrical repair, breaker panel, outlet install, light fixtures, ceiling fan, wiring, gfci, ev charger, generator",
    "HVAC": "ac repair, air conditioning, heater repair, furnace, hvac service, tune up, thermostat, refrigerant, ductwork, mini split",
    "Roofing": "roofer, roof repair, shingles, roof leak, replacement, hail damage, flashing, ridge vent, gutter repair",
    "Garage Door": "garage door, opener, spring repair, door repair, track, remote, new door, keypad",
    "Handyman": "handyman, home repair, drywall, minor plumbing, minor electrical, faucet, disposal, light install, tv mount",
    "Pest Control": "pest control, exterminator, ants, roaches, spiders, scorpions, termites, mosquito, wasps, rodent",
    "Lawn Care": "lawn mowing, lawn service, yard work, edging, trimming, weed control, lawn fertilization, aeration, mulch",
    "Landscaping": "landscaper, landscape design, planting, shrubs, irrigation, sprinkler repair, sod, xeriscape, rock, beds",
    "Tree Service": "tree trimming, tree removal, stump grinding, arborist, pruning, oak wilt",
    "Irrigation": "sprinkler repair, irrigation, controller, leak, heads, drip system, backflow, winterize",
    "House Cleaning": "house cleaning, maid service, deep clean, move out clean, recurring clean, kitchen, bathroom, dusting",
    "Window Cleaning": "window cleaning, windows, screens, hard water, exterior windows, interior windows",
    "Pressure Washing": "pressure wash, power wash, driveway, sidewalk, siding, fence, deck, patio, mildew",
    "Pool Service": "pool service, pool cleaning, chemicals, pump repair, filter, green pool, opening, closing",
    "Appliance Repair": "appliance repair, refrigerator, washer, dryer, dishwasher, oven, stove, microwave, ice maker",
    "Flooring": "flooring, install, hardwood, laminate, vinyl plank, tile, carpet, baseboards",
    "Painter": "painter, painting, interior paint, exterior paint, cabinet paint, trim, drywall repair",
    "Countertops": "countertops, quartz, granite, install, fabrication, backsplash",
    "Windows & Doors": "windows, replacement windows, energy efficient, entry door, patio door, install",
    "Fencing": "fence, fencing, cedar, privacy fence, repair, gates, staining",
    "Auto Repair": "auto repair, mechanic, brakes, oil change, check engine, tires, alignment, battery, ac service",
    "Dentist": "dentist, dental cleaning, exam, fillings, crown, root canal, emergency dentist, whitening",
    "Optometrist": "eye doctor, optometrist, eye exam, glasses, contacts, prescription, vision",
    "Primary Care": "primary care, family doctor, annual physical, wellness, blood work, same day appointment",
    "Urgent Care": "urgent care, walk in, same day, stitches, x ray, flu test, covid test",
    "Insurance Agent": "insurance agent, auto insurance, home insurance, renters, umbrella, quotes, policy review, claims",
    "Real Estate Agent": "realtor, real estate agent, buy a home, sell a home, listing agent, buyer agent, cma",
    "Veterinarian": "veterinarian, vet clinic, vaccines, spay, neuter, dental, emergency vet, wellness exam",
}

# ──────────────────────────────────────────────────────────────────────────
# Helpers (string / time)
# ──────────────────────────────────────────────────────────────────────────

_STOP = {"and", "&", "the", "of", "for", "to", "a", "an", "in", "on", "at"}


def _norm_token(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else " " for ch in s or "")


def _split_tokens(s: str) -> list[str]:
    return [t for t in _norm_token(s).split() if t and t not in _STOP]


# --- Window-coverings keyword packs (module scope) ---
# Define once (safe if already defined earlier)
if "BLINDS_FAM" not in globals():
    BLINDS_FAM = (
        "wood blinds",
        "faux wood blinds",
        "vertical blinds",
        "mini blinds",
        "aluminum blinds",
        "cordless blinds",
    )
    SHADES_FAM = (
        "roller shades",
        "solar shades",
        "roman shades",
        "cellular shades",
        "honeycomb shades",
        "pleated shades",
        "zebra shades",
        "blackout shades",
        "sheer shades",
    )
    DRAPERY_FAM = ("curtains", "drapes", "curtain rod", "drapery hardware")
    ACCESSORIES = ("valances", "cornices")
    ACTIONS = (
        "design",
        "consultation",
        "design consultation",
        "measure",
        "install",
        "installation",
        "in-home",
    )
    MOTORIZED = ("motorized shades", "motorized blinds", "motorized drapes")

    def _explode_phrases(phrases: list[str] | tuple[str, ...]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for ph in phrases:
            phl = (ph or "").lower().strip()
            if not phl:
                continue
            if phl not in seen:
                out.append(phl)
                seen.add(phl)
            for t in _split_tokens(phl):
                if t not in seen:
                    out.append(t)
                    seen.add(t)
        return out


def _unique_join(parts: list[str]) -> str:
    seen, out = set(), []
    for p in parts:
        p = p.strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return " ".join(out)


def _build_ckw(
    row: dict[str, str],
    *,
    seed: list[str] | None,
    syn_service: list[str] | None,
    syn_category: list[str] | None,
) -> str:
    base: list[str] = []

    # ---- 1) Core sources -----------------------------------------------
    base += _split_tokens(row.get("business_name", ""))
    base += _split_tokens(row.get("category", ""))
    base += _split_tokens(row.get("service", ""))
    base += [t for t in _split_tokens(row.get("notes", "")) if 3 <= len(t) <= 20]

    # Synonyms
    if syn_service:
        base += [t for t in syn_service if t and t not in _STOP]
    if syn_category:
        base += [t for t in syn_category if t and t not in _STOP]

    # Seeds (phrases → phrase + headwords)
    if seed:
        for kw in seed:
            kw_l = (kw or "").lower().strip()
            if kw_l:
                base.append(kw_l)
                base += _split_tokens(kw_l)

    # Manual extras
    manual = (row.get("ckw_manual_extra") or "").strip()
    if manual:
        s = manual.replace("|", ",").replace(";", ",")
        for piece in [p.strip() for p in s.split(",") if p.strip()]:
            piece_l = piece.lower()
            base.append(piece_l)
            base += _split_tokens(piece_l)

    # ---- 2) Rule-based expansions for window coverings (additive) -------
    svc = (row.get("service") or "").lower()
    cat = (row.get("category") or "").lower()
    triggers = (
        ("window" in svc)
        or ("window" in cat)
        or any(
            k in svc or k in cat
            for k in ("treatment", "blinds", "shades", "drap", "curtain", "shutter")
        )
    )
    if triggers:
        base += _explode_phrases(BLINDS_FAM)
        base += _explode_phrases(SHADES_FAM)
        base += _explode_phrases(DRAPERY_FAM)
        base += _explode_phrases(ACCESSORIES)
        base += _explode_phrases(ACTIONS)
        base += _explode_phrases(MOTORIZED)

    # ---- 3) Filter junk + stable de-dup + budgeted trim -----------------
    def _is_junk(tok: str) -> bool:
        if not tok:
            return True
        if tok.isdigit() and len(tok) <= 2:
            return True
        return False

    seen: set[str] = set()
    uniq: list[str] = []
    for t in base:
        t = t.strip()
        if not t or _is_junk(t):
            continue
        if t not in seen:
            seen.add(t)
            uniq.append(t)

    # Tiering keeps the most valuable terms first; then cap to budget
    TIERS = ([], [], [], [], [])  # 0..4
    for t in uniq:
        if (" shades" in t) or (" blinds" in t) or t in {"shades", "blinds", "shutters"}:
            TIERS[0].append(t)
        elif t in {"motorized", "motorized shades", "motorized blinds", "motorized drapes"}:
            TIERS[1].append(t)
        elif t in {"install", "installation", "measure", "design", "consultation", "in-home"}:
            TIERS[2].append(t)
        elif t in {"valances", "cornices", "curtains", "drapes"} or "drapery" in t:
            TIERS[3].append(t)
        else:
            TIERS[4].append(t)

    BUDGET = 32
    out: list[str] = []
    for bucket in TIERS:
        for t in bucket:
            if len(out) >= BUDGET:
                break
            if t not in out:
                out.append(t)
        if len(out) >= BUDGET:
            break

    return " ".join(out)


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
    seen = set()
    out: List[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def get_ckw_seed(category: str | None, service: str | None) -> str:
    """
    Resolve best seed for (category, service) with fallback to category-wide ('').
    Returns '' if none found.
    """
    cat = (category or "").strip()
    svc = (service or "").strip()
    if not cat and not svc:
        return ""

    if cat and svc:
        with get_engine().connect() as cx:
            row = cx.exec_driver_sql(
                "SELECT seed FROM ckw_seeds WHERE category=:c AND service=:s",
                {"c": cat, "s": svc},
            ).first()
            if row:
                return row[0] or ""

    if cat:
        with get_engine().connect() as cx:
            row = cx.exec_driver_sql(
                "SELECT seed FROM ckw_seeds WHERE category=:c AND service=''",
                {"c": cat},
            ).first()
            if row:
                return row[0] or ""

    return ""


def ensure_ckw_seeds_table() -> str:
    """
    Create ckw_seeds if missing.
    Keys: (category, service). Use service='' to mean "category-wide" seed.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS ckw_seeds (
        id INTEGER PRIMARY KEY,
        category TEXT NOT NULL,
        service  TEXT NOT NULL,  -- '' means category-wide
        seed     TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now'))
    );
    """
    idx = "CREATE UNIQUE INDEX IF NOT EXISTS idx_ckw_seeds_cat_srv ON ckw_seeds(category, service);"
    with get_engine().begin() as cx:
        cx.exec_driver_sql(ddl)
        cx.exec_driver_sql(idx)
    with get_engine().connect() as cx:
        row = cx.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ckw_seeds'"
        ).first()
        if row is None:
            raise RuntimeError("ckw_seeds creation failed verification")
    return "ckw_seeds table present (created if missing)"


# ──────────────────────────────────────────────────────────────────────────
# Engine (cached)
# ──────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    try:
        d = os.path.dirname(DB_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return sa.create_engine(f"sqlite:///{DB_PATH}", future=True)


# ──────────────────────────────────────────────────────────────────────────
# Schema & bootstrap
# ──────────────────────────────────────────────────────────────────────────
def ensure_schema_uncached() -> str:
    """
    Create vendors table/indexes if missing; add missing columns; create lookups.
    Returns a short status string.
    """
    with get_engine().begin() as cx:
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

        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_business_name ON vendors(business_name)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_ckw ON vendors(computed_keywords)"
        )
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_category ON vendors(category)")
        cx.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_vendors_service ON vendors(service)")
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_cat_svc ON vendors(category, service)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_business_name_nocase ON vendors(business_name COLLATE NOCASE)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_category_nocase ON vendors(category COLLATE NOCASE)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_service_nocase ON vendors(service COLLATE NOCASE)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_cat_svc_nocase ON vendors(category COLLATE NOCASE, service COLLATE NOCASE)"
        )

        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_ckw_version ON vendors(ckw_version)"
        )
        cx.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS idx_vendors_ckw_locked  ON vendors(ckw_locked)"
        )

        cx.exec_driver_sql("CREATE TABLE IF NOT EXISTS categories (name TEXT PRIMARY KEY)")
        cx.exec_driver_sql("CREATE TABLE IF NOT EXISTS services (name TEXT PRIMARY KEY)")
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO categories(name)
            SELECT DISTINCT COALESCE(TRIM(category),'') FROM vendors
            WHERE COALESCE(TRIM(category),'') <> ''
        """)
        cx.exec_driver_sql("""
            INSERT OR IGNORE INTO services(name)
            SELECT DISTINCT COALESCE(TRIM(service),'') FROM vendors
            WHERE COALESCE(TRIM(service),'') <> ''
        """)

    return "Schema OK"


def bootstrap_from_csv_if_needed() -> str | None:
    if not ALLOW_SEED_IMPORT:
        return None
    csv_path = SEED_CSV
    if not os.path.exists(csv_path):
        return None

    with get_engine().begin() as cx:
        n = cx.exec_driver_sql("SELECT COUNT(*) FROM vendors").scalar() or 0
        if n > 0:
            return None

        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        rows: List[dict[str, Any]] = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            headers = set(rdr.fieldnames or [])
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
    manual = (row.get("ckw_manual_extra") or "").strip()
    if manual:
        s = manual.replace("|", ",").replace(";", ",")
        for piece in [p.strip() for p in s.split(",") if p.strip()]:
            parts.append(piece.lower())
            parts.extend(_tokenize_for_ckw(piece))
    seen, out = set(), []
    for t in parts:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return " ".join(out).strip()


# >>> CKW recompute (engine-style) BEGIN ---------------------------------------


def _preload_seed_map() -> dict[tuple[str, str], str]:
    seed_map: dict[tuple[str, str], str] = {}
    try:
        with get_engine().connect() as cx:
            rows = (
                cx.exec_driver_sql("SELECT category, service, seed FROM ckw_seeds").mappings().all()
            )
            for r in rows:
                cat = (r["category"] or "").strip()
                svc = (r["service"] or "").strip()
                seed = (r["seed"] or "").strip()
                seed_map[(cat, svc)] = seed
                if cat and svc == "" and (cat, "") not in seed_map:
                    seed_map[(cat, "")] = seed
    except Exception:
        pass
    return seed_map


def _resolve_seed(
    seed_map: dict[tuple[str, str], str], category: str | None, service: str | None
) -> list[str]:
    """Return a normalized list of seed phrases for (category, service), with category-wide fallback."""
    cat = (category or "").strip()
    svc = (service or "").strip()
    s = ""
    if cat and svc and (cat, svc) in seed_map:
        s = seed_map[(cat, svc)]
    elif cat and (cat, "") in seed_map:
        s = seed_map[(cat, "")]
    if not s:
        return []
    return [p.strip().lower() for p in s.split(",") if p.strip()]


def _row_for_builder(r: dict[str, Any]) -> dict[str, str]:
    """Normalize a DB row into the dict that _build_ckw expects."""
    return {
        "business_name": r.get("business_name") or "",
        "category": r.get("category") or "",
        "service": r.get("service") or "",
        "notes": r.get("notes") or "",
        "ckw_manual_extra": r.get("ckw_manual_extra") or "",
    }


def recompute_ckw_for_ids(ids: list[int]) -> int:
    if not ids:
        return 0

    seed_map = _preload_seed_map()
    placeholders = ",".join([":id" + str(i) for i in range(len(ids))])
    id_params = {("id" + str(i)): int(v) for i, v in enumerate(ids)}

    with get_engine().connect() as cx:
        rows = (
            cx.exec_driver_sql(
                f"""
            SELECT id, category, service, business_name, notes,
                   COALESCE(ckw_manual_extra,'') AS ckw_manual_extra,
                   COALESCE(computed_keywords,'') AS computed_keywords,
                   IFNULL(ckw_locked,0) AS ckw_locked
            FROM vendors
            WHERE id IN ({placeholders})
            """,
                id_params,
            )
            .mappings()
            .all()
        )

    updates = []
    for r in rows:
        row_id = int(r["id"])
        seed_list = _resolve_seed(seed_map, r.get("category"), r.get("service"))
        syn_service = _load_synonyms_service(r.get("service"))
        syn_category = _load_synonyms_category(r.get("category"))
        new_ckw = _build_ckw(
            _row_for_builder(r),
            seed=seed_list,
            syn_service=syn_service,
            syn_category=syn_category,
        )
        old_ckw = r.get("computed_keywords") or ""
        if new_ckw != old_ckw:
            updates.append({"id": row_id, "ckw": new_ckw})

    if not updates:
        return 0

    with get_engine().begin() as cx:
        cx.exec_driver_sql(
            """
            UPDATE vendors
            SET computed_keywords = :ckw,
                ckw_version      = :ver
            WHERE id = :id
            """,
            [{"ckw": u["ckw"], "id": u["id"], "ver": CURRENT_VER} for u in updates],
        )

    return len(updates)


def recompute_ckw_all() -> int:
    """Full recompute for all vendors, ignoring ckw_locked."""
    seed_map = _preload_seed_map()

    with get_engine().connect() as cx:
        rows = (
            cx.exec_driver_sql(
                """
            SELECT id, category, service, business_name, notes,
                   COALESCE(ckw_manual_extra,'') AS ckw_manual_extra,
                   COALESCE(computed_keywords,'') AS computed_keywords
            FROM vendors
            """
            )
            .mappings()
            .all()
        )

    updates = []
    for r in rows:
        row_id = int(r["id"])
        seed_list = _resolve_seed(seed_map, r.get("category"), r.get("service"))
        syn_service = _load_synonyms_service(r.get("service"))
        syn_category = _load_synonyms_category(r.get("category"))
        new_ckw = _build_ckw(
            _row_for_builder(r),
            seed=seed_list,
            syn_service=syn_service,
            syn_category=syn_category,
        )
        old_ckw = r.get("computed_keywords") or ""
        if new_ckw != old_ckw:
            updates.append({"id": row_id, "ckw": new_ckw})

    if not updates:
        return 0

    with get_engine().begin() as cx:
        cx.exec_driver_sql(
            """
            UPDATE vendors
            SET computed_keywords = :ckw,
                ckw_version      = :ver
            WHERE id = :id
            """,
            [{"ckw": u["ckw"], "id": u["id"], "ver": CURRENT_VER} for u in updates],
        )

    return len(updates)


def recompute_ckw_all_respect_locks() -> int:
    """Full recompute for all vendors where ckw_locked=0 (respects locks)."""
    seed_map = _preload_seed_map()

    with get_engine().connect() as cx:
        rows = (
            cx.exec_driver_sql(
                """
            SELECT id, category, service, business_name, notes,
                   COALESCE(ckw_manual_extra,'') AS ckw_manual_extra,
                   COALESCE(computed_keywords,'') AS computed_keywords
            FROM vendors
            WHERE IFNULL(ckw_locked,0) = 0
            """
            )
            .mappings()
            .all()
        )

    updates = []
    for r in rows:
        row_id = int(r["id"])
        seed_list = _resolve_seed(seed_map, r.get("category"), r.get("service"))
        syn_service = _load_synonyms_service(r.get("service"))
        syn_category = _load_synonyms_category(r.get("category"))
        new_ckw = _build_ckw(
            _row_for_builder(r),
            seed=seed_list,
            syn_service=syn_service,
            syn_category=syn_category,
        )
        old_ckw = r.get("computed_keywords") or ""
        if new_ckw != old_ckw:
            updates.append({"id": row_id, "ckw": new_ckw})

    if not updates:
        return 0

    with get_engine().begin() as cx:
        cx.exec_driver_sql(
            """
            UPDATE vendors
            SET computed_keywords = :ckw,
                ckw_version      = :ver
            WHERE id = :id
            """,
            [{"ckw": u["ckw"], "id": u["id"], "ver": CURRENT_VER} for u in updates],
        )

    return len(updates)


# <<< CKW recompute END ---------------------------------------------------------

# ──────────────────────────────────────────────────────────────────────────
# CKW helpers: seeds + synonyms (optional)
# ──────────────────────────────────────────────────────────────────────────


@st.cache_resource
def _get_ckw_synonyms_map() -> dict:
    import json

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
                m["service"] = {str(k).lower(): v for k, v in m["service"].items()}
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


# ──────────────────────────────────────────────────────────────────────────
# Cached data functions used by Browse
# ──────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _has_ckw_column(data_ver: int) -> bool:
    with get_engine().connect() as cx:
        try:
            rows = cx.exec_driver_sql("PRAGMA table_info(vendors)").all()
            return any(
                (r[1] if isinstance(r, tuple) else r["name"]) == "computed_keywords" for r in rows
            )
        except Exception:
            return False


@st.cache_data(show_spinner=False)
def count_rows(q: str, data_ver: int = 0) -> int:
    where = ""
    params: dict[str, Any] = {}
    if q:
        where = """
    WHERE LOWER(
        COALESCE(computed_keywords,'') || ' ' ||
        COALESCE(business_name,'')     || ' ' ||
        COALESCE(category,'')          || ' ' ||
        COALESCE(service,'')           || ' ' ||
        COALESCE(notes,'')             || ' ' ||
        COALESCE(phone,'')             || ' ' ||
       COALESCE(website,'')            || ' ' ||
        COALESCE(address,'')
    ) LIKE :q
"""
        params["q"] = f"%{(q or '').strip().lower()}%"
    sql = f"SELECT COUNT(*) FROM vendors {where}"
    with get_engine().begin() as cx:
        return int(cx.exec_driver_sql(sql, params).scalar() or 0)


@st.cache_data(show_spinner=False)
def search_ids_ckw_first(q: str, limit: int, offset: int, data_ver: int) -> list[int]:
    """Return vendor IDs with CKW hits ranked before generic text hits; stable order otherwise."""
    q_norm = (q or "").strip().lower()
    with get_engine().connect() as cx:
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
                params = {
                    "q1": f"%{q_norm}%",
                    "q2": f"%{q_norm}%",
                    "limit": int(limit),
                    "offset": int(offset),
                }
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
        return pd.DataFrame(
            columns=[
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
            ]
        )
    placeholders = ",".join([f":id{i}" for i in range(len(ids))])
    params = {f"id{i}": v for i, v in enumerate(ids)}
    with get_engine().connect() as cx:
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


# ──────────────────────────────────────────────────────────────────────────
# CRUD helpers
# ──────────────────────────────────────────────────────────────────────────
def insert_vendor(data: Dict[str, Any]) -> int:
    """Insert a vendor row and return new id. Empty-safe & type-safe."""
    # Map UI/friendly keys → DB column names
    keymap = {
        "business_name": "business_name",
        "category": "category",
        "service": "service",
        "phone": "phone",
        "website": "website",
        "address": "address",
        "notes": "notes",
        "email": "email",
        "email address": "email",
        "contact_name": "contact_name",
        "contact name": "contact_name",
        "keywords": "ckw_manual_extra",
        "ckw": "computed_keywords",
        "computed_keywords": "computed_keywords",
    }

    allowed = [
        "business_name",
        "category",
        "service",
        "phone",
        "website",
        "address",
        "notes",
        "email",
        "contact_name",
        "ckw_manual_extra",
        "computed_keywords",
        "ckw_locked",
        "ckw_version",
    ]

    # Normalize → params
    params: Dict[str, Any] = {}
    for k, v in (data or {}).items():
        col = keymap.get(k, k)
        if col not in allowed:
            continue
        if col == "ckw_locked":
            params[col] = 1 if bool(v) else 0
        elif v is None:
            params[col] = None
        elif isinstance(v, (int, float)):
            params[col] = v
        else:
            s = str(v)
            params[col] = "".join(ch for ch in s if ch >= " " or ch == "\n").strip()

    params.setdefault("ckw_locked", 0)
    params.setdefault("ckw_version", 1)

    if not params.get("business_name"):
        raise ValueError("business_name is required")

    cols = list(params.keys())
    col_list = ", ".join(cols)
    val_list = ", ".join(f":{c}" for c in cols)

    sql_returning = f"INSERT INTO vendors ({col_list}) VALUES ({val_list}) RETURNING id"
    sql_basic = f"INSERT INTO vendors ({col_list}) VALUES ({val_list})"

    import sqlalchemy as sa

    with get_engine().begin() as cx:
        try:
            res = cx.exec_driver_sql(sa.text(sql_returning).text, params)
            row = res.first()
            if row and row[0] is not None:
                return int(row[0])
        except Exception:
            try:
                res = cx.exec_driver_sql(sa.text(sql_basic).text, params)
            except TypeError:
                import os

                if os.getenv("SHOW_DEBUG") == "1":
                    st.error("TypeError during INSERT (likely placeholder/params mismatch).")
                    st.code(
                        {
                            "sql": sql_basic,
                            "cols": cols,
                            "param_types": {k: type(v).__name__ for k, v in params.items()},
                        }
                    )
                raise
            return int(res.lastrowid)

        # Defensive fallback
        return int(cx.exec_driver_sql(sa.text(sql_basic).text, params).lastrowid)


def update_vendor(
    vendor_id: int,
    data: Dict[str, Any],
    prev_updated: str | None = None,  # optional optimistic concurrency
) -> int:
    """
    Update a vendor row by id. Returns number of rows changed (0 = no-op/stale).
    - Maps friendly keys ("contact name", "email address", "keywords", "ckw") to DB columns.
    - Normalizes types (bool→int, string scrub).
    - Uses NULLIF(:service,'') so empty string -> NULL.
    - If prev_updated is provided, enforces optimistic concurrency:
      WHERE COALESCE(updated_at,'') = COALESCE(:prev_updated,'')
    """
    keymap = {
        "business_name": "business_name",
        "category": "category",
        "service": "service",
        "phone": "phone",
        "website": "website",
        "address": "address",
        "notes": "notes",
        "email": "email",
        "email address": "email",
        "contact_name": "contact_name",
        "contact name": "contact_name",
        "keywords": "ckw_manual_extra",
        "ckw": "computed_keywords",
        "computed_keywords": "computed_keywords",
        "ckw_locked": "ckw_locked",
        "ckw_version": "ckw_version",
    }
    allowed = {
        "business_name",
        "category",
        "service",
        "phone",
        "website",
        "address",
        "notes",
        "email",
        "contact_name",
        "ckw_manual_extra",
        "computed_keywords",
        "ckw_locked",
        "ckw_version",
    }

    # Normalize inputs
    named: Dict[str, Any] = {}
    for k, v in (data or {}).items():
        col = keymap.get(k, k)
        if col not in allowed:
            continue
        if col == "ckw_locked":
            named[col] = 1 if bool(v) else 0
        elif v is None:
            named[col] = None
        elif isinstance(v, (int, float)):
            named[col] = v
        else:
            s = str(v)
            named[col] = "".join(ch for ch in s if ch >= " " or ch == "\n").strip()

    if not named:
        return 0

    # Dynamic SET + params
    set_clauses: list[str] = []
    params: Dict[str, Any] = {"id": int(vendor_id)}

    for col, val in named.items():
        if col == "service":
            set_clauses.append("service = NULLIF(:service,'')")  # empty string => NULL
            params["service"] = val
        else:
            set_clauses.append(f"{col} = :{col}")
            params[col] = val

    # Always bump updated_at
    from datetime import datetime, timezone

    params["updated_at"] = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    set_clauses.append("updated_at = :updated_at")

    where_clause = "id = :id"
    if prev_updated is not None:
        where_clause += " AND COALESCE(updated_at,'') = COALESCE(:prev_updated,'')"
        params["prev_updated"] = prev_updated

    sql = f"UPDATE vendors SET {', '.join(set_clauses)} WHERE {where_clause}"

    import sqlalchemy as sa

    with get_engine().begin() as cx:
        res = cx.exec_driver_sql(sa.text(sql).text, params)
        changed = int(res.rowcount or 0)

    if prev_updated is not None and changed == 0:
        raise RuntimeError("Stale write: updated_at mismatch; reload before saving.")

    return changed


# ──────────────────────────────────────────────────────────────────────────
# Lookup helpers
# ──────────────────────────────────────────────────────────────────────────
def list_categories() -> List[str]:
    with get_engine().begin() as cx:
        rows = cx.exec_driver_sql("SELECT name FROM categories ORDER BY name COLLATE NOCASE").all()
    return [r[0] for r in rows]


def list_services() -> List[str]:
    with get_engine().begin() as cx:
        rows = cx.exec_driver_sql("SELECT name FROM services ORDER BY name COLLATE NOCASE").all()
    return [r[0] for r in rows]


def ensure_lookup_value(table: str, name: str) -> None:
    if not name:
        return
    with get_engine().begin() as cx:
        cx.exec_driver_sql(f"INSERT OR IGNORE INTO {table}(name) VALUES (:n)", {"n": name.strip()})


def refresh_lookups() -> None:
    """Idempotent refresh of categories/services from vendors."""
    with get_engine().begin() as cx:
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
# UI helpers
# ──────────────────────────────────────────────────────────────────────────
def _has_table(name: str) -> bool:
    try:
        with get_engine().connect() as cx:
            rows = cx.exec_driver_sql(f"PRAGMA table_info({name})").all()
        return bool(rows)
    except Exception:
        return False


# --- One-shot clearing helpers -------------------------------------------
def _pop_keys(keys: list[str]) -> None:
    for k in keys:
        st.session_state.pop(k, None)


def _clear_after(scope: str) -> None:
    st.session_state["_after_action_clear"] = scope
    st.rerun()

    # Render Add/Edit/Delete inside its tab
    render_add_edit_delete(tab_manage)


# ──────────────────────────────────────────────────────────────────────────
# Main App
# ──────────────────────────────────────────────────────────────────────────
def main() -> None:
    # ---- Cache-buster init ----
    if "DATA_VER" not in st.session_state:
        st.session_state["DATA_VER"] = 0

    # ---- Ensure schema + optional CSV bootstrap (only once if empty) ----
    try:
        msg_schema = ensure_schema_uncached()
        if os.getenv("SHOW_STATUS") == "1":
            st.caption(msg_schema)
    except Exception as e:
        st.warning(f"Schema check failed: {e}")

    DB_READY = _has_table("vendors")
    if not DB_READY:
        try:
            msg_seed = bootstrap_from_csv_if_needed()
            if msg_seed and os.getenv("SHOW_STATUS") == "1":
                st.caption(msg_seed)
            DB_READY = _has_table("vendors")
        except Exception as e:
            st.warning(f"Bootstrap skipped: {e}")

    st.session_state["DB_READY"] = bool(DB_READY)

    # ---- Single set of tabs for in-function rendering (we'll remove top-level duplicates in next step) ----
    # [removed] duplicate top-level tabs — lifecycle now in main()


    # ---- TEMP: simple captions so you can see the tabs are alive from main() ----
with tab_browse:
    # ─────────────────────────────────────────────────────────────────────
    # Browse (Admin)
    # ─────────────────────────────────────────────────────────────────────
    # --- Search bar (single-click Clear) ---
    c1, c2, _ = st.columns([0.5, 0.12, 0.38])
    q = c1.text_input(
        label="Search",
        key="q",
        placeholder="Search name, category, service, notes, phone, website…",
        label_visibility="collapsed",
    )
    if c2.button("Clear", use_container_width=True):
        if "q" in st.session_state:
            del st.session_state["q"]
        st.rerun()

    q = st.session_state.get("q", "")

    # Count matching rows
    try:
        total = count_rows(q=q, data_ver=st.session_state.get("DATA_VER", 0))
    except Exception as e:
        st.error(f"Browse failed (count): {e}")
        st.stop()

    # Resolve IDs and load rows
    try:
        ids = search_ids_ckw_first(
            q=q,
            limit=PAGE_SIZE,
            offset=0,
            data_ver=st.session_state.get("DATA_VER", 0),
        )
        if not ids:
            df = pd.DataFrame()
        else:
            df = fetch_rows_by_ids(tuple(ids), st.session_state.get("DATA_VER", 0))
    except Exception as e:
        st.error(f"Browse failed (load): {e}")
        st.stop()

    # Base frame: include BROWSE_COLUMNS + anything explicitly requested in ORDER
    _base_cols = list(BROWSE_COLUMNS)
    for c in ORDER:
        if c not in _base_cols:
            _base_cols.append(c)

    if df.empty:
        df = pd.DataFrame(columns=_base_cols)
    else:
        for col in _base_cols:
            if col not in df.columns:
                df[col] = ""
        df = df.reindex(columns=_base_cols, fill_value="")

    # Hide heavy/internal columns and originals replaced with aliases
    _TECH_COLS = {"id", "created_at", "updated_at", "ckw_locked", "ckw_version"}
    _ALIAS_ORIGS = {
        "contact_name",
        "email",
        "computed_keywords",
    }  # originals replaced by friendly aliases
    _hide = set(_TECH_COLS | _ALIAS_ORIGS)

    # If a tech/original column is explicitly requested in ORDER, don't hide it.
    for col in list(_hide):
        if col in ORDER:
            _hide.discard(col)

    def _is_ckw_control(col: str) -> bool:
        # Only hide ckw_* controls if they're NOT explicitly requested in ORDER
        return col.startswith("ckw_") and col not in ORDER

    # Create alias columns (idempotent)
    _src = df.copy()
    if not _src.empty:
        if "contact name" not in _src.columns and "contact_name" in _src.columns:
            _src["contact name"] = _src["contact_name"].fillna("")
        if "email address" not in _src.columns and "email" in _src.columns:
            _src["email address"] = _src["email"].fillna("")
        if "keywords" not in _src.columns and "ckw_manual_extra" in _src.columns:
            _src["keywords"] = _src["ckw_manual_extra"].fillna("")
        if "ckw" not in _src.columns and "computed_keywords" in _src.columns:
            _src["ckw"] = _src["computed_keywords"].fillna("")

    # Visible columns and enforced order
    _visible = [c for c in _src.columns if c not in _hide and not _is_ckw_control(c)]
    _ordered = [c for c in ORDER if c in _visible] + [c for c in _visible if c not in ORDER]

    # Hidden/control-char scanning + sanitization helpers
    import json as _json
    from datetime import datetime as _dt
    import re as _re

    _HIDDEN_RX = _re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F\u200B-\u200F\u202A-\u202E\u2060]")

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
                return _json.dumps(x, ensure_ascii=False)
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

    # Build the view and normalize
    _view = _src.loc[:, _ordered] if not _src.empty else _src
    _view_safe = (
        _view.applymap(lambda v: _strip_hidden(_to_str_safe(v))) if not _view.empty else _view
    )

    # Render (AgGrid with exact pixel widths + autosize-to-contents on first render)
    try:
        from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
    except Exception:
        st.error(
            "Missing dependency: streamlit-aggrid. Add to requirements.txt: streamlit-aggrid==0.3.4.post3"
        )
    else:
        # Respect final ordered columns that actually exist in the DataFrame
        _cols_present = [c for c in _ordered if (not _view_safe.empty and c in _view_safe.columns)]
        _df = _view_safe[_cols_present] if _cols_present else _view_safe

        # Read exact widths from secrets (ints); fallback to 160 if missing/bad
        _px_map = dict(st.secrets.get("COLUMN_WIDTHS_PX_ADMIN", {}))

        gb = GridOptionsBuilder.from_dataframe(
            _df,
            enableValue=False,
            enableRowGroup=False,
            enablePivot=False,
        )
        go = gb.build()

        # Auto-size columns to fit contents (runs once on first data render)
        go["onFirstDataRendered"] = JsCode("""
        function(params) {
          var all = params.columnApi.getAllDisplayedColumns().map(c => c.getColId());
          params.columnApi.autoSizeColumns(all, false);
        }
        """)

        # Default column behavior: resizable, no wrap, bounded widths
        go["defaultColDef"] = {
            "resizable": True,
            "wrapText": False,
            "autoHeight": False,
            "minWidth": 60,
            "maxWidth": 1000,
        }

        # Apply per-column pixel widths and common options (+ tooltips)
        for col in go.get("columnDefs", []):
            name = col.get("field")
            width_px = 160
            if name in _px_map:
                try:
                    width_px = int(_px_map[name])
                except Exception:
                    pass
            col["width"] = width_px
            col["resizable"] = True
            col["sortable"] = True
            col["wrapText"] = False
            col["autoHeight"] = False
            col["tooltipField"] = name

        # Do NOT auto-stretch to container; keep your exact pixels
        go["suppressSizeToFit"] = True
        go["domLayout"] = "normal"

        AgGrid(
            _df,
            gridOptions=go,
            update_mode=GridUpdateMode.NO_UPDATE,
            height=520,
            fit_columns_on_grid_load=False,  # keep exact widths; allow horizontal scroll
            allow_unsafe_jscode=True,  # required for JsCode callback
        )

    # ---- Bottom toolbar: CSV + Help ----
    bt1, bt2 = st.columns([0.22, 0.78])

    if not _view_safe.empty:
        _export_df = _view_safe
        with bt1:
            st.download_button(
                "Download CSV",
                data=_export_df.to_csv(index=False),
                file_name="providers.csv",
                mime="text/csv",
                use_container_width=True,
            )

    with st.expander("Help — How to use Browse (click to open)", expanded=False):
        st.markdown(HELP_MD)

with tab_maint:
    # ─────────────────────────────────────────────────────────────────────
    # Maintenance — Diagnostics & CKW
    # ─────────────────────────────────────────────────────────────────────

    # ---- CKW recompute tools -------------------------------------------------
    st.subheader("Computed Keywords (CKW)")
    ckw_col1, ckw_col2 = st.columns([0.35, 0.65])

    with ckw_col1:
        if st.button("Recompute CKW (respect locks)", type="primary", use_container_width=True, key="btn_ckw_respect"):
            try:
                changed = recompute_ckw_all_respect_locks()
                st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
                st.success(f"Recomputed {changed} vendor(s) (ckw_locked respected).")
            except Exception as e:
                st.error(f"CKW recompute failed: {e}")

    with ckw_col2:
        if st.button("Force Recompute ALL (override locks)", use_container_width=True, key="btn_ckw_all"):
            try:
                changed = recompute_ckw_all()
                st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
                st.warning(f"Force-recomputed {changed} vendor(s). Locks overridden.")
            except Exception as e:
                st.error(f"CKW force recompute failed: {e}")

    # ---- CKW seed admin / coverage ------------------------------------------
    st.markdown("### CKW Seeds (curated baseline)")
    try:
        msg = ensure_ckw_seeds_table()
        st.caption(msg)
    except Exception as e:
        st.error(f"Seed table check failed: {e}")

    try:
        # Coverage: how many (category, service) combos present in vendors vs seeds defined
        with get_engine().connect() as cx:
            combos = cx.exec_driver_sql("""
                SELECT DISTINCT
                    COALESCE(TRIM(category),'') AS category,
                    COALESCE(TRIM(service),'')  AS service
                FROM vendors
                WHERE COALESCE(TRIM(category),'') <> '' AND COALESCE(TRIM(service),'') <> ''
            """).mappings().all()
            combo_count = len(combos)
            seed_count  = cx.exec_driver_sql("SELECT COUNT(*) FROM ckw_seeds").scalar() or 0
        st.caption(f"Seed coverage: {seed_count} / {combo_count} (category, service) combos have seeds.")
    except Exception as e:
        st.warning(f"Coverage probe failed: {e}")

    # ---- Quick diagnostics ----------------------------------------------------
    st.subheader("Diagnostics")
    try:
        import json as _json
        from collections import OrderedDict

        with get_engine().connect() as cx:
            # Basic engine / path
            db_path = DB_PATH
            engine_url = f"sqlite:///{DB_PATH}"

            # Table counts
            tables = OrderedDict()
            for t in ("vendors", "categories", "services", "ckw_seeds", "sqlite_sequence"):
                try:
                    cnt = cx.exec_driver_sql(f"SELECT COUNT(*) FROM {t}").scalar()
                    if cnt is not None:
                        tables[t] = int(cnt)
                except Exception:
                    # table might not exist
                    pass

            # Vendors with empty phone (simple integrity probe)
            phones_suspect = []
            try:
                rows = cx.exec_driver_sql("""
                    SELECT id, COALESCE(phone,'') AS phone
                    FROM vendors
                    WHERE COALESCE(TRIM(phone),'') = ''
                    LIMIT 100
                """).mappings().all()
                for r in rows:
                    phones_suspect.append({"id": int(r["id"]), "phone": r["phone"]})
            except Exception:
                pass

        st.caption(f"DB path: {db_path}")
        st.caption(f"Engine: {engine_url}")
        st.markdown("**Table & Row Counts**")
        st.code(_json.dumps({"tables": tables}, indent=2))

        st.markdown("**Integrity Self-Test (read-only)**")
        st.code(_json.dumps({"integrity_issues": {"phones_suspect": phones_suspect}}, indent=2))
    except Exception as e:
        st.error(f"Diagnostics failed: {e}")

    # ---- Full CSV backup ------------------------------------------------------
    st.subheader("Full CSV Backup")
    try:
        with get_engine().connect() as cx:
            df_all = pd.read_sql(sa.text("SELECT * FROM vendors ORDER BY business_name COLLATE NOCASE, id"), cx)
        st.download_button(
            "Download ALL vendors as CSV",
            data=df_all.to_csv(index=False),
            file_name="providers_full_backup.csv",
            mime="text/csv",
            use_container_width=True,
        )
    except Exception as e:
        st.warning(f"CSV backup failed: {e}")

    # ---- Cache clear ----------------------------------------------------------
    st.subheader("Caches")
    if st.button("Clear @st.cache_data (force refresh)", key="btn_clear_cache_data", use_container_width=True):
        try:
            st.cache_data.clear()
            st.success("Cleared cache_data.")
            st.rerun()
        except Exception as e:
            st.error(f"Clear cache_data failed: {e}")


    # ---- Add / Edit / Delete ----
    render_add_edit_delete(tab_manage)

# [removed] duplicate top-level Browse — Browse now rendered inside main()

    # ─────────────────────────────────────────────────────────────────────
    # Add / Edit / Delete
    # ─────────────────────────────────────────────────────────────────────


def render_add_edit_delete(tab_manage):
    with tab_manage:
        # --- One-shot post-action clearing (runs before any widgets are created) ---
        _clear = st.session_state.pop("_after_action_clear", None)

        if _clear == "add":
            for k in (
                "bn_add",
                "cat_add_sel",
                "srv_add_sel",
                "contact_add",
                "phone_add",
                "email_add",
                "website_add",
                "address_add",
                "notes_add",
                "kw_add",
                "btn_add_provider",
            ):
                st.session_state.pop(k, None)

        elif _clear == "edit":
            for k in (
                "pick_edit_sel",
                "bn_edit",
                "cat_edit_sel",
                "srv_edit_sel",
                "contact_edit",
                "phone_edit",
                "email_edit",
                "website_edit",
                "address_edit",
                "notes_edit",
                "kw_edit",
                "save_changes_btn",
            ):
                st.session_state.pop(k, None)

        elif _clear == "delete":
            st.session_state.pop("del_select_id", None)

        if not st.session_state.get("DB_READY"):
            st.info(
                "Database not ready — skipping Add/Edit/Delete because required tables are missing."
            )
            return

        eng = get_engine()
        lc, rc = st.columns([1, 1], gap="large")

        # ---------- Add ----------
        with lc:
            # TODO: replace with your real Add form
            st.caption("Add Provider (placeholder)")

        # ---------- Edit ----------
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
                sel = st.selectbox(
                    "Pick a provider",
                    options=labels,
                    key="edit_pick_provider",
                )
                sel_id = int(rows[labels.index(sel)][0])

                with eng.begin() as cx:
                    r = (
                        cx.exec_driver_sql(
                            "SELECT business_name,category,service,contact_name,phone,email,website,"
                            "address,notes,ckw_manual_extra FROM vendors WHERE id=:id",
                            {"id": sel_id},
                        )
                        .mappings()
                        .first()
                    )

                if r:
                    bn_e = st.text_input("Business Name *", value=r["business_name"], key="bn_edit")

                    cats = list_categories()
                    srvs = list_services()

                    e_c1, e_c2 = st.columns([1, 1])

                    cat_choice_e = e_c1.selectbox(
                        "Category *",
                        options=["— Select —"] + cats,
                        index=(cats.index(r["category"]) + 1) if r["category"] in cats else 0,
                        key="cat_edit_sel",
                    )
                    srv_choice_e = e_c2.selectbox(
                        "Service *",
                        options=["— Select —"] + srvs,
                        index=(srvs.index(r["service"]) + 1) if r["service"] in srvs else 0,
                        key="srv_edit_sel",
                    )

                    category_e = r["category"] if cat_choice_e == "— Select —" else cat_choice_e
                    service_e = r["service"] if srv_choice_e == "— Select —" else srv_choice_e

                    contact_name_e = st.text_input(
                        "Contact Name", value=r["contact_name"] or "", key="contact_edit"
                    )
                    phone_e = st.text_input("Phone", value=r["phone"] or "", key="phone_edit")
                    email_e = st.text_input("Email", value=r["email"] or "", key="email_edit")
                    website_e = st.text_input(
                        "Website", value=r["website"] or "", key="website_edit"
                    )
                    address_e = st.text_input(
                        "Address", value=r["address"] or "", key="address_edit"
                    )
                    notes_e = st.text_area(
                        "Notes", value=r["notes"] or "", height=100, key="notes_edit"
                    )

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
                        update_vendor(sel_id, data)
                        st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
                        _clear_after("edit")
                        st.success(
                            f'Saved changes to provider #{sel_id}. — run "Recompute ALL" to apply keywords.'
                        )

                        update_vendor(eng, sel_id, data)
                        st.session_state["DATA_VER"] = st.session_state.get("DATA_VER", 0) + 1
                        _clear_after("edit")
                        st.success(
                            f'Saved changes to provider #{sel_id}. — run "Recompute ALL" to apply keywords.'
                        )

                # ---------- Delete ----------
                st.markdown("### Delete Provider")
                st.caption("Danger zone: Permanently removes a record from **vendors**.")

                # Build options directly from DB (no dependency on Browse df)
                try:
                    with get_engine().connect() as cx:
                        rows = cx.exec_driver_sql(
                            "SELECT id, business_name FROM vendors "
                            "ORDER BY business_name COLLATE NOCASE, id"
                        ).fetchall()
                    options: list[tuple[int, str]] = [
                        (int(r[0]), f"{int(r[0])} — {str(r[1])}") for r in rows
                    ]
                except Exception:
                    options = []

                selected_id = st.selectbox(
                    "Select provider to delete",
                    options=[o[0] for o in options],
                    format_func=lambda _id: dict(options).get(_id, str(_id)),
                    index=0 if options else None,
                    key="delete_select_id",
                )

                col_del, _ = st.columns([0.25, 0.75])
                with col_del:
                    if st.button("Delete Provider", type="primary", use_container_width=True):
                        if selected_id is None:
                            st.warning("No provider selected.")
                        else:
                            try:
                                with get_engine().begin() as cx3:
                                    cx3.exec_driver_sql(
                                        "DELETE FROM vendors WHERE id = :id",
                                        {"id": int(selected_id)},
                                    )
                                st.session_state["DATA_VER"] = (
                                    st.session_state.get("DATA_VER", 0) + 1
                                )
                                st.success(f"Deleted provider id={selected_id}.")
                                _clear_after("delete")  # sets flag + reruns
                            except Exception as e:
                                st.error(f"Delete failed: {e}")

    # ─────────────────────────────────────────────────────────────────────
    # Category / Service management
    # ─────────────────────────────────────────────────────────────────────
    # Removed here to avoid NameError: tab_catsvc (this tab is rendered in main()).
    # The canonical Category/Service UI lives in the main() block below.

    # ─────────────────────────────────────────────────────────────────────
    # Maintenance (Diagnostics & CKW)
    # ─────────────────────────────────────────────────────────────────────
    # Removed here to avoid NameError: tab_maint (this tab is rendered in main()).
    # The canonical Maintenance UI lives in the main() block below.

    # ---------- Cache Clear ----------
    st.markdown("**Caches**")
    clicked_clear_cache = st.button(
        "Clear @st.cache_data (force Browse refresh)",
        key="clear_cache_data",
    )

    if clicked_clear_cache:
        try:
            st.cache_data.clear()
            st.success("Cleared cache_data.")
            st.rerun()  # immediately re-run to reflect cleared caches
        except Exception as e:
            st.error(f"Clear cache_data failed: {e}")


if __name__ == "__main__":
    main()
