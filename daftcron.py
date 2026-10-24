#!/usr/bin/env python3
"""
daftcron — a housing-listing watcher for daft.ie.

Standard library only. No pip install. Requires Python 3.11+ (for tomllib).

Data path
---------
discovery  https://www.daft.ie/api/sitemap/sitemap-ad-details-pages.xml?from=N
           Explicitly Allow'ed in https://www.daft.ie/robots.txt
           Reverse-chronological, 200 ads per page, 342 pages in the index.
           Each entry carries a real <lastmod> timestamp.

detail     Each listing page embeds a JSON blob at
           <script id="__NEXT_DATA__">. Its props.pageProps holds:
             .dfpTargetingValues -> clean flat dict (county, ber, beds, price,
                                    propertyType, sellerType) — the primary source
             .listing            -> rich record (point, seller, images, dates)

Pipeline
--------
harvest -> prefilter -> fetch -> normalise -> store -> score -> alert -> report

Commands
--------
  baseline      Seed the seen-set from existing stock WITHOUT alerting.
  scan          Find new listings, score them, alert on the good ones.
  top [N]       Print the current best listings.
  report        Regenerate out/report.html.
  stats         Coverage summary for the local database.
  export        Dump current matches to CSV.
  test-telegram Send a test message to confirm credentials.
"""

from __future__ import annotations

import argparse
import csv
import email
import html
import json
import os
import re
import shutil
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from email import policy as email_policy
from email import utils as email_utils
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    sys.exit("daftcron requires Python 3.11+ (stdlib tomllib). Found: %s" % sys.version)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.toml"
DB_PATH = ROOT / "data" / "daftcron.db"
REPORT_PATH = ROOT / "out" / "report.html"
CSV_PATH = ROOT / "out" / "matches.csv"

BASE_URL = "https://www.daft.ie"
SITEMAP_URL = "https://www.daft.ie/api/sitemap/sitemap-ad-details-pages.xml"
SITEMAP_PAGE_SIZE = 200

# BER bands, best to worst. Used for both scoring and the min_ber filter.
BER_ORDER = ["A", "B", "C", "D", "E", "F", "G"]
BER_POINTS = {"A": 1.00, "B": 0.80, "C": 0.56, "D": 0.32, "E": 0.12, "F": 0.04, "G": 0.00}

# Phrases that show up in the deposit-fraud listings that plague the Irish
# rental market. Each hit raises a scam flag.
SCAM_PHRASES = [
    "western union",
    "moneygram",
    "bank transfer only",
    "send deposit",
    "deposit before viewing",
    "currently abroad",
    "i am abroad",
    "out of the country",
    "keys will be posted",
    "no viewing",
    "whatsapp only",
    "crypto",
]

# ─────────────────────────────────────────────────────────────────────────────
# Small helpers
# ─────────────────────────────────────────────────────────────────────────────


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _first(value, default=None):
    """dfpTargetingValues values are single-element lists. Unwrap them."""
    if isinstance(value, list):
        return value[0] if value else default
    return value if value is not None else default


def _to_int(value, default=0) -> int:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else default


def _leading_int(value, default=0) -> int:
    """'4 Bed' -> 4, '1 Bath' -> 1."""
    if isinstance(value, (int, float)):
        return int(value)
    if not value:
        return default
    m = re.search(r"\d+", str(value))
    return int(m.group()) if m else default


def _singular(word: str) -> str:
    """'houses' -> 'house'. Leaves 'semi-detached' alone."""
    word = (word or "").strip().lower()
    if word.endswith("ies"):
        return word[:-3] + "y"
    if word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def _ber_letter(value) -> str:
    """'B2' -> 'B'. Returns '' when unknown."""
    if not value:
        return ""
    if isinstance(value, list):
        value = value[0] if value else ""
    m = re.match(r"\s*([A-Ga-g])", str(value))
    return m.group(1).upper() if m else ""


def _days_since(date_str: str) -> int | None:
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            when = datetime.strptime(str(date_str)[: len(fmt) + 6], fmt)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - when).days
        except ValueError:
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Load config.toml, then overlay secrets.toml, then environment variables.

    The split exists so config.toml — which holds all the tuned settings — can
    be versioned safely, while credentials live in a file that is ignored by
    git. Order of precedence, lowest to highest:
        config.toml  <  secrets.toml  <  environment variables
    """
    if not path.exists():
        sys.exit(f"Config not found: {path}")
    with path.open("rb") as fh:
        cfg = tomllib.load(fh)

    secrets_path = path.parent / "secrets.toml"
    if secrets_path.exists():
        try:
            with secrets_path.open("rb") as fh:
                secrets = tomllib.load(fh)
        except tomllib.TOMLDecodeError as exc:
            sys.exit(f"secrets.toml is not valid TOML: {exc}")
        for section, values in secrets.items():
            if isinstance(values, dict):
                cfg.setdefault(section, {}).update(values)
            else:
                cfg[section] = values

    # Environment variables win over both files, so credentials need never be
    # written to disk at all.
    notify = cfg.setdefault("notify", {})
    notify["telegram_token"] = os.environ.get(
        "DAFTCRON_TELEGRAM_TOKEN", notify.get("telegram_token", "")
    )
    notify["telegram_chat_id"] = os.environ.get(
        "DAFTCRON_TELEGRAM_CHAT", notify.get("telegram_chat_id", "")
    )
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────


# curl is used as the HTTP transport instead of urllib.
#
# Reason: daft.ie sits behind a WAF that fingerprints the TLS ClientHello.
# Python's ssl module produces a distinctive fingerprint and is refused with
# HTTP 403, while curl with an identical User-Agent and identical headers is
# served normally. Verified 2026-10-02. This is not a header or rate-limit
# issue and no header combination fixes it.
#
# curl ships with macOS and every mainstream Linux distribution, so this costs
# nothing in portability and keeps the zero-dependency guarantee.
#
# DO NOT add --http1.1 to the argument list below. Forcing HTTP/1.1 also
# returns 403 — verified flag-by-flag on 2026-10-02. The endpoint requires
# HTTP/2; --compressed, --max-time and custom headers are all safe.
CURL = shutil.which("curl")


def _curl_once(url: str, user_agent: str, timeout: int) -> tuple[int, str]:
    """Single request. Returns (http_status, body). Status 0 = transport failure."""
    handle, path = tempfile.mkstemp(prefix="daftcron-")
    os.close(handle)
    try:
        proc = subprocess.run(
            [
                CURL,
                "--silent",
                "--show-error",
                "--location",          # ad URLs get re-slugged; follow the redirect
                "--compressed",
                "--max-time", str(timeout),
                "--user-agent", user_agent,
                "--header", "Accept-Language: en-IE,en;q=0.9",
                "--output", path,
                "--write-out", "%{http_code}",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 15,
        )
        code = (proc.stdout or "").strip()
        status = int(code) if code.isdigit() else 0
        if status != 200:
            return status, (proc.stderr or "").strip()
        with open(path, "rb") as fh:
            return 200, fh.read().decode("utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        return 0, "timeout"
    except Exception as exc:  # noqa: BLE001
        return 0, f"{type(exc).__name__}: {exc}"
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def http_get(url: str, user_agent: str, retries: int = 3, timeout: int = 30) -> str | None:
    """Fetch a URL. Returns the body, or None if the resource is unavailable."""
    if not CURL:
        sys.exit(
            "curl not found on PATH.\n"
            "daftcron uses curl rather than Python's urllib because daft.ie's WAF\n"
            "refuses Python's TLS fingerprint with HTTP 403."
        )

    for attempt in range(1, retries + 1):
        status, payload = _curl_once(url, user_agent, timeout)
        if status == 200:
            return payload
        # 404 / 410 mean the ad was withdrawn. Normal, not an error.
        if status in (404, 410):
            return None
        if status == 403:
            log(f"  ! HTTP 403 — blocked. If this persists, slow down the scan. {url}")
            return None
        if attempt == retries:
            log(f"  ! request failed ({status or 'no response'}) {payload[:120]} {url}")
            return None
        time.sleep(1.5 * attempt)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — harvest the sitemap
# ─────────────────────────────────────────────────────────────────────────────


def harvest(cfg: dict, pages: int) -> dict[int, dict]:
    """Walk the newest `pages` sitemap pages. Returns {ad_id: {url, lastmod}}."""
    daft = cfg["daft"]
    allowed_types = set(daft.get("listing_types", ["for-rent"]))
    user_agent = daft["user_agent"]

    found: dict[int, dict] = {}
    for page in range(pages):
        offset = page * SITEMAP_PAGE_SIZE
        xml = http_get(f"{SITEMAP_URL}?from={offset}", user_agent)
        if not xml:
            log(f"sitemap page {page} (from={offset}) unavailable — stopping")
            break

        pairs = re.findall(
            r"<loc>\s*([^<]+?)\s*</loc>\s*<lastmod>\s*([^<]*?)\s*</lastmod>",
            xml,
            re.S,
        )
        if not pairs:
            break

        for loc, lastmod in pairs:
            parts = [p for p in urllib.parse.urlparse(loc).path.split("/") if p]
            if len(parts) < 3:
                continue
            if parts[0] not in allowed_types:
                continue
            ad_id = _to_int(parts[-1])
            if not ad_id or ad_id in found:
                continue
            found[ad_id] = {"url": loc, "lastmod": lastmod, "slug": parts[1] if len(parts) > 2 else ""}

        if len(pairs) < SITEMAP_PAGE_SIZE:
            break  # reached the end of the index

    return found


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3/4 — fetch and normalise one listing
# ─────────────────────────────────────────────────────────────────────────────


def _extract_point(listing: dict) -> tuple[float | None, float | None]:
    """`point` shows up in a couple of shapes depending on ad type."""
    point = listing.get("point")
    if isinstance(point, dict):
        lat = point.get("lat") or point.get("latitude")
        lng = point.get("lng") or point.get("lon") or point.get("longitude")
        if lat is not None and lng is not None:
            return float(lat), float(lng)
        coords = point.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) == 2:
            return float(coords[1]), float(coords[0])
    return None, None


def parse_listing(page_html: str, ad_id: int, url: str) -> dict | None:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', page_html, re.S)
    if not m:
        return None
    try:
        payload = json.loads(m.group(1))
        page_props = payload["props"]["pageProps"]
        listing = page_props.get("listing") or {}
    except (json.JSONDecodeError, KeyError, TypeError):
        return None
    if not listing:
        return None

    dfp = page_props.get("dfpTargetingValues") or {}
    non_formatted = listing.get("nonFormatted") or {}
    seller = listing.get("seller") or {}
    media = listing.get("media") or {}
    facilities = listing.get("facilities")

    price = _to_int(non_formatted.get("price") or _first(dfp.get("price"), 0))
    beds = _leading_int(listing.get("numBedrooms")) or _to_int(_first(dfp.get("beds"), 0))
    baths = _leading_int(listing.get("numBathrooms"))

    if isinstance(facilities, list):
        facilities = ", ".join(str(f) for f in facilities)
    description = (listing.get("description") or "").strip()

    images = media.get("images") if isinstance(media.get("images"), list) else []
    lat, lng = _extract_point(listing)

    return {
        "id": ad_id,
        "url": BASE_URL + (listing.get("seoFriendlyPath") or url),
        "title": (listing.get("title") or "").strip(),
        "price": price,
        "beds": beds,
        "baths": baths,
        "property_type": _singular(_first(dfp.get("propertyType")) or listing.get("propertyType") or ""),
        "county": (_first(dfp.get("countyName")) or "").strip(),
        "area": (_first(dfp.get("areaName")) or listing.get("areaName") or "").strip(),
        "ber": _ber_letter(listing.get("ber")) or _ber_letter(_first(dfp.get("ber"))),
        "seller_type": (_first(dfp.get("sellerType")) or seller.get("sellerType") or "").lower(),
        "seller_name": seller.get("name") or "",
        "images": len(images),
        "views": _to_int(listing.get("listingViews")),
        "first_publish": listing.get("firstPublishDate") or "",
        "publish_date": listing.get("publishDate") or "",
        "last_update": listing.get("lastUpdateDate") or "",
        "back_on_market": 1 if listing.get("backOnMarket") else 0,
        "lat": lat,
        "lng": lng,
        "description": description[:4000],
        "facilities": (facilities or "")[:600],
    }


def fetch_detail(cfg: dict, ad_id: int, url: str) -> dict | None:
    page = http_get(url, cfg["daft"]["user_agent"])
    if not page:
        return None
    return parse_listing(page, ad_id, url)


# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id             INTEGER PRIMARY KEY,
    url            TEXT,
    title          TEXT,
    price          INTEGER,
    beds           INTEGER,
    baths          INTEGER,
    property_type  TEXT,
    county         TEXT,
    area           TEXT,
    eircode        TEXT,
    agent_email    TEXT,
    agent_phone    TEXT,
    ber            TEXT,
    seller_type    TEXT,
    seller_name    TEXT,
    images         INTEGER,
    views          INTEGER,
    first_publish  TEXT,
    publish_date   TEXT,
    last_update    TEXT,
    back_on_market INTEGER DEFAULT 0,
    lat            REAL,
    lng            REAL,
    description    TEXT,
    facilities     TEXT,
    first_seen     TEXT,
    last_seen      TEXT,
    score          INTEGER DEFAULT 0,
    reasons        TEXT,
    flags          TEXT,
    alerted        INTEGER DEFAULT 0,
    gone           INTEGER DEFAULT 0,
    -- 1 = ad says bills are included, 0 = ad says they are extra, NULL = not
    -- known. Never set automatically: the alert email carries no description,
    -- so daftcron cannot read this. See the `mark` command.
    bills_included INTEGER
);
CREATE INDEX IF NOT EXISTS idx_listings_score ON listings(score DESC);
CREATE INDEX IF NOT EXISTS idx_listings_first_seen ON listings(first_seen DESC);

CREATE TABLE IF NOT EXISTS price_history (
    id      INTEGER,
    price   INTEGER,
    seen_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_price_history ON price_history(id, seen_at);

-- Median price by (county, bed count) — a LIKE-FOR-LIKE baseline.
--
-- The previous version grouped by the address slug. That slug is unique per
-- listing, so no bucket ever reached even two samples and this table sat
-- permanently empty — silently disabling the value component AND the
-- below-market check that lived inside it.
--
-- County is still coarse (Rathcoole and Ballsbridge are both "Dublin"), but
-- coarse is enough to catch a price that is simply wrong, which is all this is
-- for. It is not a valuation.
CREATE TABLE IF NOT EXISTS price_stats (
    county          TEXT,
    beds            INTEGER,
    median_price    REAL,
    samples         INTEGER,
    updated         TEXT,
    PRIMARY KEY (county, beds)
);

CREATE TABLE IF NOT EXISTS runs (
    ts        TEXT,
    harvested INTEGER,
    fetched   INTEGER,
    new       INTEGER,
    alerted   INTEGER,
    mode      TEXT
);
"""


def open_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    """Add columns introduced after a database was first created.

    CREATE TABLE IF NOT EXISTS silently does nothing on an existing table, so a
    new column would never appear and every insert would fail. Cheap to check.
    """
    wanted = {
        "eircode": "TEXT",
        "agent_email": "TEXT",
        "agent_phone": "TEXT",
        "bills_included": "INTEGER",
    }
    existing = {row[1] for row in conn.execute("PRAGMA table_info(listings)")}
    for column, decl in wanted.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {column} {decl}")
    conn.commit()


def existing_ids(conn: sqlite3.Connection) -> set[int]:
    return {row[0] for row in conn.execute("SELECT id FROM listings")}


def store(conn: sqlite3.Connection, rec: dict) -> tuple[bool, int | None]:
    """Upsert a listing. Returns (is_new, previous_price_if_changed)."""
    stamp = now_iso()
    row = conn.execute("SELECT id, price FROM listings WHERE id = ?", (rec["id"],)).fetchone()

    if row is None:
        conn.execute(
            """INSERT INTO listings (
                   id, url, title, price, beds, baths, property_type, county, area,
                   eircode, agent_email, agent_phone, ber, seller_type,
                   seller_name, images, views, first_publish, publish_date,
                   last_update, back_on_market, lat, lng, description, facilities,
                   first_seen, last_seen
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                rec["id"], rec["url"], rec["title"], rec["price"], rec["beds"],
                rec["baths"], rec["property_type"], rec["county"], rec["area"],
                rec.get("eircode", ""), rec.get("agent_email", ""),
                rec.get("agent_phone", ""), rec["ber"], rec["seller_type"],
                rec["seller_name"], rec["images"], rec["views"],
                rec["first_publish"], rec["publish_date"], rec["last_update"],
                rec["back_on_market"], rec["lat"], rec["lng"], rec["description"],
                rec["facilities"], stamp, stamp,
            ),
        )
        conn.execute(
            "INSERT INTO price_history (id, price, seen_at) VALUES (?,?,?)",
            (rec["id"], rec["price"], stamp),
        )
        return True, None

    previous_price = row["price"]
    changed = previous_price != rec["price"]
    conn.execute(
        """UPDATE listings SET price=?, beds=?, baths=?, ber=?, images=?, views=?,
               last_update=?, back_on_market=?, last_seen=?, gone=0
           WHERE id=?""",
        (
            rec["price"], rec["beds"], rec["baths"], rec["ber"], rec["images"],
            rec["views"], rec["last_update"], rec["back_on_market"], stamp, rec["id"],
        ),
    )
    if changed:
        conn.execute(
            "INSERT INTO price_history (id, price, seen_at) VALUES (?,?,?)",
            (rec["id"], rec["price"], stamp),
        )
    return False, (previous_price if changed else None)


MIN_BUCKET = 3   # below this a median is noise rather than a baseline


def refresh_price_stats(conn: sqlite3.Connection) -> None:
    """Median PRICE per (county, bed count) — a like-for-like baseline.

    Keyed on county + beds, NOT on the address slug. The slug is unique per
    listing, so the old grouping could never accumulate samples and the table
    sat permanently empty. That emptiness silently disabled two separate
    things: the value component, and the below-market check inside it.

    Also compares median PRICE rather than €/bed. €/bed falls as properties get
    bigger — a 2-bed costs more than a 1-bed but nowhere near twice — so a
    €/bed comparison across bed counts is meaningless. It made a 3-bed in Meath
    look like triple the "value" of a 1-bed in Dublin, which is why the value
    component was switched off in the first place.
    """
    rows = conn.execute(
        """SELECT county, beds, price FROM listings
           WHERE price > 200 AND beds > 0 AND county != ''"""
    ).fetchall()

    buckets: dict[tuple[str, int], list[int]] = {}
    national: dict[int, list[int]] = {}
    for row in rows:
        beds = int(row["beds"])
        price = int(row["price"])
        buckets.setdefault((str(row["county"]).strip().lower(), beds), []).append(price)
        national.setdefault(beds, []).append(price)

    conn.execute("DELETE FROM price_stats")
    stamp = now_iso()

    def put(county: str, beds: int, values: list[int]) -> None:
        if len(values) < MIN_BUCKET:
            return
        conn.execute(
            """INSERT INTO price_stats (county, beds, median_price, samples, updated)
               VALUES (?,?,?,?,?)""",
            (county, beds, statistics.median(values), len(values), stamp),
        )

    for (county, beds), values in buckets.items():
        put(county, beds, values)

    # National figure per bed count, stored under county "*".
    #
    # A young database leaves many county buckets too thin to trust — Dublin
    # 2-bed held two samples — and a weak baseline still catches a price that is
    # plainly wrong, which is the entire job. The county figure supersedes this
    # on its own as volume grows, with no further changes needed.
    for beds, values in national.items():
        put("*", beds, values)

    conn.commit()


def price_medians(conn: sqlite3.Connection) -> dict[tuple[str, int], float]:
    """{(county_lower, beds): median_price} — empty until buckets fill up."""
    return {
        (row["county"], row["beds"]): row["median_price"]
        for row in conn.execute("SELECT county, beds, median_price FROM price_stats")
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage 6 — filters and scoring
# ─────────────────────────────────────────────────────────────────────────────


def bills_allowance(cfg: dict) -> int:
    """What a rent-only property costs on top of the rent, per month."""
    return int(cfg.get("filters", {}).get("bills_allowance", 0) or 0)


def effective_cost(rec: dict, cfg: dict) -> int | None:
    """What this listing actually costs per month, not what the ad says.

    Rent alone is not comparable across listings. A €1,380 flat with everything
    included is cheaper than a €1,300 one where you pay heat, power, broadband
    and bins — and at this budget that distinction decides several a month.

    Unknown bills are treated as YOUR cost rather than as included. That is the
    conservative direction: an unmarked listing is judged more expensive than it
    may be, instead of being flattered into an alert it has not earned.
    """
    price = rec.get("price")
    if not price:
        return None
    if rec.get("bills_included") == 1:
        return int(price)
    return int(price) + bills_allowance(cfg)


def rail_served(rec: dict, cfg: dict) -> bool:
    """True if the listing looks to be in a town on a Dublin commuter line.

    Matched the same way as [commute.times] — substring against the address
    slug, with Daft's "-co-" normalised away — so the two tables behave alike.
    Inherits the same weakness: a short town name can appear inside a longer
    address.
    """
    towns = [str(t).lower() for t in (cfg.get("rail", {}).get("towns") or [])]
    if not towns:
        return False
    haystack = " ".join([
        str(rec.get("area") or ""),
        str(rec.get("title") or ""),
        str(rec.get("url") or ""),
    ]).lower()
    compact = re.sub(r"-+co-+|-+county-+", "-", haystack)
    return any(t in h for t in towns for h in (haystack, compact))


def passes_filters(rec: dict, cfg: dict) -> tuple[bool, str]:
    f = cfg["filters"]

    if f.get("max_price") and rec["price"] and rec["price"] > f["max_price"]:
        return False, f"price {rec['price']} > max {f['max_price']}"

    # Total cost, but ONLY when bills are known to be extra. An unmarked listing
    # might be all-inclusive, and rejecting it here would discard precisely the
    # candidates worth a look. So this enforces the real budget against what we
    # know, and leaves what we don't know to scoring.
    max_total = f.get("max_total_cost") or 0
    if max_total and rec.get("bills_included") == 0:
        cost = effective_cost(rec, cfg)
        if cost and cost > max_total:
            return False, (f"total {cost} > {max_total} budget "
                           "(bills confirmed not included)")
    if f.get("min_price") and rec["price"] and rec["price"] < f["min_price"]:
        return False, "price below min"
    if f.get("min_beds") and rec["beds"] and rec["beds"] < f["min_beds"]:
        return False, f"beds {rec['beds']} < min {f['min_beds']}"
    if f.get("max_beds") and rec["beds"] and rec["beds"] > f["max_beds"]:
        return False, "beds above max"

    counties = [c.lower() for c in f.get("counties") or []]
    if counties and rec["county"].lower() not in counties:
        return False, f"county {rec['county'] or '?'} not targeted"

    types = [_singular(t) for t in f.get("property_types") or []]
    if types and rec["property_type"] not in types:
        return False, f"type {rec['property_type'] or '?'} not targeted"

    min_ber = (f.get("min_ber") or "").upper()
    if min_ber in BER_ORDER:
        if rec["ber"] not in BER_ORDER:
            return False, "no BER published"
        if BER_ORDER.index(rec["ber"]) > BER_ORDER.index(min_ber):
            return False, f"BER {rec['ber']} below {min_ber}"

    haystack = (rec["title"] + " " + rec["description"]).lower()
    for word in f.get("exclude_keywords") or []:
        if word.lower() in haystack:
            return False, f"excluded keyword: {word}"

    return True, ""


def commute_minutes(rec: dict, cfg: dict) -> int | None:
    """One-way drive minutes for this listing, from [commute.times].

    Matches table keys as substrings of the listing URL, title and area. An
    exact key match would never fire: Daft's area slug is the whole address
    (e.g. 'maryfield-drive-artane-dublin-5'), so the table key has to be found
    within it rather than equalled. Longest key wins, so a specific entry like
    'dunboyne-meath' beats a shorter one.

    Returns None when nothing matches, which scores neutrally and raises a flag
    rather than inventing a number.
    """
    table = cfg.get("commute", {}).get("times") or {}
    if not table:
        return None
    lookup = {str(k).lower(): v for k, v in table.items()}

    haystack = " ".join([
        str(rec.get("area") or ""),
        str(rec.get("title") or ""),
        str(rec.get("url") or ""),
    ]).lower()

    # Daft spells the county "co-<county>" INSIDE the slug:
    #     4-beechmount-grove-navan-co-meath
    # The table keys are written the way a person writes them — "navan-meath" —
    # and that substring simply is not present in the slug above. So every town
    # entry failed to match, every listing fell through to the bare county
    # default, and the whole table had no effect. It looked well maintained and
    # did nothing.
    #
    # Normalising the haystack, rather than rewriting fifty keys, keeps the keys
    # readable and keeps them unambiguous: "navan-meath" cannot be confused with
    # Navan Road in Dublin 7, whereas a bare "navan" could.
    compact = re.sub(r"-+co-+|-+county-+", "-", haystack)
    # Dublin postal districts are written both ways — "dublin-15" and "dublin-d15".
    compact = re.sub(r"dublin-d(\d{1,2})(?![a-z0-9])", r"dublin-\1", compact)
    haystacks = (haystack, compact)

    for key in sorted(lookup, key=len, reverse=True):
        if any(key in h for h in haystacks):
            try:
                return int(lookup[key])
            except (TypeError, ValueError):
                return None

    # County-level fallback, so an area that is not individually listed is
    # still bracketed rather than scored neutral. Without this a town 90 minutes
    # away in an accepted county scores the same as one ten minutes away, which
    # makes the whole component useless.
    county = str(rec.get("county") or "").strip().lower()
    if county in lookup:
        try:
            return int(lookup[county])
        except (TypeError, ValueError):
            return None
    return None


def score_listing(rec: dict, cfg: dict, medians: dict[str, float],
                  previous_price: int | None) -> tuple[int, list[str], list[str]]:
    s = cfg["scoring"]
    reasons: list[str] = []
    flags: list[str] = []
    # Scoring compares TOTAL monthly cost, not the headline rent. Falls back to
    # max_price so a config that has not been migrated still works.
    max_total = (cfg["filters"].get("max_total_cost")
                 or cfg["filters"].get("max_price") or 0)
    allowance = bills_allowance(cfg)
    cost = effective_cost(rec, cfg)

    # Value needs area medians, which only exist once enough listings have been
    # seen. Until then it is dropped from the denominator rather than scored as
    # zero — otherwise every listing is penalised for a gap in our own data for
    # the first few weeks, and nothing ever clears the alert threshold.
    value_enabled = bool(medians) and bool(s.get("w_value"))

    # A price below the sanity floor is probably not the rent: a room advertised
    # as a studio, a weekly figure read as monthly, or bait. Treated separately
    # from the scoring below, because a cheaper listing otherwise scores HIGHER
    # on price — so the least credible figure would earn the most points.
    sanity_floor = s.get("min_sane_price", 0) or 0
    implausible = bool(
        sanity_floor and rec["price"] and rec["price"] < sanity_floor
    )

    # ── price headroom ───────────────────────────────────────────────────────
    # A tight budget is the normal case here, so being *within* budget earns
    # most of the component and only the last slice rewards being well under.
    # Scoring purely on headroom gives zero to nearly every listing in a search
    # that is already capped just above the real ceiling — which would mean a
    # perfect flat at exactly your budget scored the same as a bad one.
    if max_total and cost and not implausible:
        ratio = cost / max_total
        if ratio <= 1.0:
            under = (1.0 - ratio) / 0.3          # full credit at 30% under
            pts = round(s["w_price_headroom"] * (0.6 + 0.4 * min(1.0, under)))
            band = (f"{int((1 - ratio) * 100)}% under budget" if ratio < 0.9
                    else "within budget")
        else:
            pts = 0
            band = "over budget"
        if rec.get("bills_included") == 1:
            reasons.append(f"€{cost} all bills included — {band}")
        elif rec.get("bills_included") == 0:
            reasons.append(f"€{cost} with bills — {band}")
        else:
            reasons.append(f"€{cost} assuming you pay bills — {band}")
    elif implausible:
        pts = 0
        reasons.append(f"€{rec['price']} — price not credible, not rewarded")
    else:
        pts = 0
        reasons.append(f"€{rec['price']}")

    # Where inclusiveness would flip the verdict: the rent is close enough to
    # the budget that the bills allowance decides it. daftcron cannot resolve
    # this — the alert email carries no description, so the ad's bills line
    # never arrives. It can only point at the ones worth checking.
    #
    # Deliberately a REASON, not a flag. Flags carry the scam penalty, and this
    # is an invitation to read the ad, not a warning about the listing.
    if (rec.get("bills_included") is None and rec["price"]
            and max_total - allowance < rec["price"] <= max_total):
        reasons.append(
            f"bills unclear: €{rec['price']} is within €{allowance} of budget, "
            "so all-inclusive or not decides whether this fits"
        )

    # ── BER ──────────────────────────────────────────────────────────────────
    # Skipped entirely when the weight is zero. On the email route Daft supplies
    # BER only as an image filename, so it is never readable — flagging "no BER"
    # on every listing would just dock 20 points across the board.
    if not s.get("w_ber"):
        pts_ber = 0
    elif rec["ber"] in BER_POINTS:
        pts_ber = round(s["w_ber"] * BER_POINTS[rec["ber"]])
        if rec["ber"] in ("A", "B"):
            reasons.append(f"BER {rec['ber']} — low heating cost")
        elif rec["ber"] in ("F", "G"):
            flags.append(f"poor BER {rec['ber']} — high heating bills")
    else:
        pts_ber = round(s["w_ber"] * 0.25)  # small credit for unknown, not zero
        flags.append("no BER published")

    # ── size preference ──────────────────────────────────────────────────────
    # Optional, and off unless ideal_beds is set. Accepting a range (min_beds to
    # max_beds) is a filter; this is a preference — a 2-bed is not "better value"
    # than a 1-bed in any absolute sense, it is better if it is the size you
    # actually want. Scored by distance from the ideal, not by more-is-better.
    ideal_beds = int(s.get("ideal_beds", 0) or 0)
    pts_beds = 0
    if ideal_beds and rec["beds"]:
        gap = abs(rec["beds"] - ideal_beds)
        weight = s.get("w_beds", 0)
        if gap == 0:
            pts_beds = weight
            reasons.append(f"{rec['beds']} bed — the size you want")
        elif gap == 1:
            pts_beds = round(weight * 0.5)
        elif gap == 2:
            pts_beds = round(weight * 0.2)

    # ── value vs like-for-like median ────────────────────────────────────────
    # PRICE against the same county and bed count, not €/bed against an area.
    # See refresh_price_stats for why €/bed is the wrong instrument.
    beds_n = rec.get("beds") or 0
    # County-level bucket first; fall back to the national figure for that bed
    # count when the county one is too thin to trust. `basis` is carried so the
    # flag can say which it used, instead of implying a precision it lacks.
    median = medians.get((str(rec.get("county") or "").strip().lower(), beds_n))
    basis = rec["county"] or "Ireland"
    if not median:
        median = medians.get(("*", beds_n))
        basis = "Ireland"
    pts_value = 0
    if value_enabled and median and rec["price"]:
        ratio = rec["price"] / median
        if ratio <= 0.80:
            pts_value = s["w_value"]
            reasons.append(
                f"€{rec['price']} vs {rec['beds']}-bed median €{median:.0f} — well below")
        elif ratio <= 0.95:
            pts_value = round(s["w_value"] * 0.72)
            reasons.append(
                f"€{rec['price']} vs {rec['beds']}-bed median €{median:.0f} — below")
        elif ratio <= 1.10:
            pts_value = round(s["w_value"] * 0.40)

    # Below-market check, DELIBERATELY OUTSIDE the value_enabled gate above.
    #
    # Turning off the value SCORE must not turn off fraud detection — they are
    # unrelated concerns. Gating this behind w_value meant it never ran at all,
    # which is how a €700 two-bed in Dublin 24 reached the top of the list
    # unchallenged. The comment on min_sane_price already makes this argument
    # for the absolute floor; the relative check simply never got the same
    # treatment.
    if median and rec["price"]:
        ratio = rec["price"] / median
        if ratio < s["scam_price_ratio"]:
            flags.append(
                f"€{rec['price']} is {int((1 - ratio) * 100)}% below the "
                f"{beds_n}-bed median for {basis} (€{median:.0f}) — verify legitimacy"
            )

    # ── freshness ────────────────────────────────────────────────────────────
    days = _days_since(rec["first_publish"])
    if days is None:
        pts_fresh = round(s["w_freshness"] * 0.3)
    elif days <= 0:
        pts_fresh = s["w_freshness"]
        reasons.append("published today")
    elif days <= 2:
        pts_fresh = round(s["w_freshness"] * 0.70)
        reasons.append(f"published {days}d ago")
    elif days <= 7:
        pts_fresh = round(s["w_freshness"] * 0.35)
    else:
        pts_fresh = 0

    # ── price drop ───────────────────────────────────────────────────────────
    pts_drop = 0
    if previous_price and rec["price"] and rec["price"] < previous_price:
        pts_drop = s["w_price_drop"]
        reasons.append(f"price cut €{previous_price} → €{rec['price']}")

    # ── risk signals ─────────────────────────────────────────────────────────
    if rec["back_on_market"]:
        flags.append("back on market — previously withdrawn")

    # Absolute sanity floor, deliberately independent of area medians — those may
    # not exist yet, and disabling the value component must not disable scam
    # detection with it. A studio at €100/month is a room, a weekly rate read as
    # monthly, or bait; it should never top the list.
    if implausible:
        flags.append(
            f"€{rec['price']}/mo is implausibly low — room, weekly rate, or bait"
        )
    # Only judge on image count when the source actually provides one.
    images = rec.get("images")
    if images is not None:
        if rec["seller_type"] == "private" and images < 3:
            flags.append(f"private seller, {images} image(s) — thin listing")
        if images == 0:
            flags.append("no images")

    haystack = (rec["title"] + " " + rec["description"]).lower()
    for phrase in SCAM_PHRASES:
        if phrase in haystack:
            flags.append(f'description contains "{phrase}"')
            break

    # ── commute ──────────────────────────────────────────────────────────────
    # For a hybrid week this is often the largest hidden cost of a move:
    # 20 extra minutes each way, three days a week, is roughly 90 hours a year.
    pts_commute = 0
    commute_cfg = cfg.get("commute", {})
    commute_on = bool(commute_cfg.get("enabled")) and s.get("w_commute", 0)
    if commute_on:
        w_commute = s.get("w_commute", 0)
        minutes = commute_minutes(rec, cfg)
        days = int(commute_cfg.get("days_per_week", 0) or 0)
        if minutes is None:
            pts_commute = round(w_commute * 0.5)   # mid-table, not a guess
            # A note, not a flag: a missing table entry is a configuration gap
            # in daftcron, not a risk signal about the listing, so it must not
            # attract the scam penalty.
            reasons.append(
                f"commute unknown for '{rec['area'] or '?'}' — add to [commute.times]"
            )
        else:
            ideal = float(commute_cfg.get("ideal_minutes", 15) or 15)
            worst = float(commute_cfg.get("max_minutes", 60) or 60)
            if minutes <= ideal:
                pts_commute = w_commute
            elif minutes >= worst:
                pts_commute = 0
            else:
                span = max(1.0, worst - ideal)
                pts_commute = round(w_commute * (1 - (minutes - ideal) / span))
            if days:
                hours = round(minutes * 2 * days * 46 / 60)
                reasons.append(f"{minutes} min drive × {days} days ≈ {hours} h/yr")
            else:
                reasons.append(f"{minutes} min drive")

    # ── rail access ──────────────────────────────────────────────────────────
    # ADDED at the end, not weighted in the middle, and that distinction is the
    # whole point.
    #
    # As a weighted component rail would sit in the denominator, which LOWERS
    # the score of every listing with no station. That re-ranks the entire list
    # — and shifts the alert threshold — for something meant to be a nudge.
    # This is here to let a Naas or a Dunboyne edge ahead, not to penalise
    # Crumlin for having no train. It adds, and it can never subtract.
    train_bonus = 0
    if s.get("rail_bonus", 0) and rail_served(rec, cfg):
        train_bonus = int(s["rail_bonus"])
        reasons.append("on a Dublin commuter rail line")

    # Normalise across whichever weights are actually in play, so enabling or
    # disabling commute does not silently rescale everything else.
    available = (
        s["w_price_headroom"] + s["w_ber"]
        + (s["w_value"] if value_enabled else 0)
        + s["w_freshness"] + s["w_price_drop"]
        + (s.get("w_commute", 0) if commute_on else 0)
        + (s.get("w_beds", 0) if ideal_beds else 0)
    )
    raw = (pts + pts_ber + pts_value + pts_fresh + pts_drop
           + pts_commute + pts_beds)
    total = round(100 * raw / available) if available else 0
    total -= s["scam_penalty"] * len(flags)
    total += train_bonus
    return max(0, min(100, total)), reasons, flags


# ─────────────────────────────────────────────────────────────────────────────
# Notification
# ─────────────────────────────────────────────────────────────────────────────


def _telegram_error(exc) -> str:
    """Turn a Telegram API error into something actionable.

    The API returns a JSON body with a human-readable `description`, which is
    far more useful than the bare status code — 'chat not found' and
    'bot was blocked' need completely different fixes.
    """
    detail = ""
    try:
        payload = json.loads(exc.read().decode("utf-8", "replace"))
        detail = str(payload.get("description") or "")
    except Exception:  # noqa: BLE001 - body may be empty or not JSON
        pass

    code = exc.code
    low = detail.lower()
    if code == 401:
        return "token rejected (401) — the token is wrong or was revoked"
    if code == 404:
        return ("malformed token (404) — it should be digits, a colon, then a "
                "long string, e.g. 8123456789:AAF3k...")
    if code == 403:
        return f"forbidden (403) — {detail or 'you may have blocked or stopped the bot'}"
    if code == 429:
        return f"rate limited (429) — {detail or 'too many messages, slow down'}"
    if code == 400 and "chat not found" in low:
        return ("chat id not found (400) — check the number, and make sure you "
                "have sent the bot a message at least once")
    if code == 400 and "not enough rights" in low:
        return f"insufficient rights (400) — {detail}"
    return f"HTTP {code} {detail}".strip()


def telegram_send(cfg: dict, text: str) -> bool:
    notify = cfg["notify"]
    token = (notify.get("telegram_token") or "").strip()
    chat_id = (notify.get("telegram_chat_id") or "").strip()
    if not token or not chat_id:
        log("  ! Telegram not configured — token or chat id is missing")
        return False

    body = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "false",
    }).encode()
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as exc:
        log(f"  ! Telegram: {_telegram_error(exc)}")
        return False
    except Exception as exc:  # noqa: BLE001
        log(f"  ! Telegram: {type(exc).__name__}: {exc}")
        return False


# Agent contact details appear in SOME Daft alert emails and not others, so this
# is deliberately opportunistic: anything found is used, anything absent is
# omitted without complaint.
#
# These are bare patterns over the flattened text rather than markup, because no
# sample containing agent details has been seen yet. Guessing at markup for an
# unseen format is exactly how the first parser went wrong, so this avoids the
# question entirely — it will pick details up whatever the surrounding HTML.
AGENT_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
AGENT_PHONE_RE = re.compile(
    r"(?:\+353|00353|0)\s?\d{1,2}[\s\-]?\d{3}[\s\-]?\d{4}"
)

# Never treated as an agent contact.
AGENT_EMAIL_BLOCKLIST = ("daft.ie", "distilled.ie", "amazonses.com", "google.com")


def extract_agent_email(text: str) -> str:
    for candidate in AGENT_EMAIL_RE.findall(text or ""):
        low = candidate.lower()
        if any(blocked in low for blocked in AGENT_EMAIL_BLOCKLIST):
            continue
        if low.startswith(("noreply", "no-reply", "donotreply")):
            continue
        return candidate
    return ""


def extract_agent_phone(text: str) -> str:
    match = AGENT_PHONE_RE.search(text or "")
    return match.group(0).strip() if match else ""


def _clean_address(text: str) -> str:
    """Collapse a Daft address into something a geocoder will accept.

    Daft's titles repeat the locality — "Blackrock , Co. Louth, Blackrock, Co.
    Louth, A91HW80" — which is pure noise to a geocoder. Drops repeated
    comma-separated segments, normalises spacing, and removes the space Daft
    leaves before a comma.
    """
    parts: list[str] = []
    seen: set[str] = set()
    for raw in (text or "").split(","):
        part = " ".join(raw.split()).strip()
        if not part or part.lower() in seen:
            continue
        seen.add(part.lower())
        parts.append(part)
    return ", ".join(parts)


def maps_url(rec: dict, cfg: dict) -> str:
    """One-tap driving directions from a listing to the office.

    The origin is the FULL POSTAL ADDRESS, not the Eircode.

    Verified 9 Oct 2026: Google Maps will search an Eircode typed by hand, but
    will not resolve a bare one passed as the `origin` parameter — the map then
    opens showing only the destination, with no route. The full address geocodes
    reliably, and Daft already appends the Eircode to it for precision.

    The destination is a text query rather than coordinates so it stays editable
    in config.toml without anyone looking up a latitude and longitude. Returns
    '' when there is nothing usable to navigate from.
    """
    commute = cfg.get("commute", {})
    destination = str(
        commute.get("destination_query") or commute.get("destination") or ""
    ).strip()
    origin = _clean_address(rec.get("title") or "")
    if not origin:
        origin = str(rec.get("eircode") or "").strip()
    if not origin or not destination:
        return ""
    return (
        "https://www.google.com/maps/dir/?api=1"
        f"&origin={urllib.parse.quote(origin)}"
        f"&destination={urllib.parse.quote(destination)}"
        "&travelmode=driving"
    )


def _stars(score: int) -> str:
    """Render 0-100 as five stars.

    Purely a glance aid: on a phone notification '★★★★☆' reads faster than
    '71/100'. The number stays alongside it because stars discard resolution —
    68 and 71 both round to the same stars, and the report sorts on the number.
    """
    filled = max(0, min(5, round(score / 20)))
    return "★" * filled + "☆" * (5 - filled)


def format_alert(rec: dict, score: int, reasons: list[str], flags: list[str],
                 cfg: dict) -> str:
    parts: list[str] = []
    if rec["beds"]:
        parts.append(f"{rec['beds']} bed")
    if rec["baths"]:
        parts.append(f"{rec['baths']} bath")
    if rec["property_type"]:
        parts.append(rec["property_type"])
    if rec.get("eircode"):
        parts.append(rec["eircode"])

    lines = [
        f"<b>🏠 {_stars(score)} {score}/100 · €{rec['price']:,}/mo</b>",
        html.escape((rec["title"] or "")[:140]),
    ]
    if parts:
        lines.append(" · ".join(html.escape(p) for p in parts))
    lines += [
        "",
        f"<a href=\"{rec['url']}\">Open on Daft →</a>",
    ]

    drive = maps_url(rec, cfg)
    if drive:
        lines.append(f"<a href=\"{drive}\">🚗 Drive from here to the office</a>")

    compose = enquiry_mailto(rec, cfg)
    if compose:
        label = "✉️ Ready-made enquiry"
        if rec.get("agent_email"):
            label += " (addressed to the agent)"
        lines.append(f"<a href=\"{compose}\">{label}</a>")
    contact = " · ".join(
        v for v in (rec.get("agent_phone"), rec.get("agent_email")) if v
    )
    if contact:
        lines.append("👤 " + html.escape(contact))

    if reasons:
        lines.append("<i>" + html.escape(" · ".join(reasons[:3])) + "</i>")
    if flags:
        lines.append("⚠️ " + html.escape("; ".join(flags[:2])))
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────

REPORT_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>daftcron — {count} matches</title>
<style>
 :root {{ color-scheme: light dark; --line:#8883; }}
 * {{ box-sizing:border-box; }}
 body {{ font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
        margin:0; padding:28px; max-width:1500px; }}
 h1 {{ font-size:20px; margin:0 0 4px; }}
 .meta {{ color:#888; font-size:13px; margin-bottom:18px; }}
 table {{ border-collapse:collapse; width:100%; font-size:14px; }}
 th,td {{ text-align:left; padding:9px 10px; border-bottom:1px solid var(--line);
          vertical-align:top; }}
 th {{ cursor:pointer; user-select:none; white-space:nowrap; position:sticky; top:0;
       background:Canvas; }}
 th:hover {{ color:#0a84ff; }}
 tr:hover td {{ background:#8881; }}
 a {{ color:#0a84ff; text-decoration:none; font-weight:600; }}
 a:hover {{ text-decoration:underline; }}
 .score {{ font-weight:700; font-variant-numeric:tabular-nums; }}
 .s80 {{ color:#28a745; }} .s65 {{ color:#5cb85c; }}
 .s50 {{ color:#d9a441; }} .s0 {{ color:#999; }}
 .price {{ font-variant-numeric:tabular-nums; white-space:nowrap; }}
 .why {{ color:#777; font-size:12.5px; }}
 .flag {{ color:#c0392b; font-size:12.5px; }}
 .new {{ background:#0a84ff22; padding:1px 5px; border-radius:3px; font-size:11px; }}
 .ber {{ display:inline-block; padding:1px 6px; border-radius:3px; background:#8882;
         font-size:12px; font-weight:600; }}
</style></head><body>
<h1>daftcron — {count} matches</h1>
<div class="meta">Generated {generated} · {total} listings tracked
 · <a href="#" onclick="window.print();return false">print</a></div>
<table id="t"><thead><tr>
 <th data-t="n">Score</th><th data-t="n">Price</th><th data-t="n">Total</th><th data-t="n">Beds</th>
 <th data-t="n">€/bed</th><th data-t="s">BER</th><th data-t="s">Area</th>
 <th data-t="s">Type</th><th data-t="s">Seller</th>
 <th data-t="n">Imgs</th><th data-t="n">Published</th><th>Listing</th>
</tr></thead><tbody>
{rows}
</tbody></table>
<script>
const t=document.getElementById('t');
t.querySelectorAll('th[data-t]').forEach((th,i)=>{{
  th.addEventListener('click',()=>{{
    const rows=[...t.tBodies[0].rows];
    const numeric=th.dataset.t==='n';
    const asc=!(th._asc=!th._asc);
    rows.sort((a,b)=>{{
      let x=a.cells[i].dataset.v??a.cells[i].innerText;
      let y=b.cells[i].dataset.v??b.cells[i].innerText;
      if(numeric){{x=parseFloat(x)||0;y=parseFloat(y)||0;}}
      return (x>y?1:x<y?-1:0)*(asc?1:-1);
    }});
    rows.forEach(r=>t.tBodies[0].appendChild(r));
  }});
}});
</script></body></html>
"""


def write_report(conn: sqlite3.Connection, cfg: dict) -> Path:
    top_n = cfg["report"]["top_n"]
    rows = conn.execute(
        """SELECT * FROM listings WHERE gone = 0
           ORDER BY score DESC, first_seen DESC LIMIT ?""",
        (top_n,),
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM listings WHERE gone = 0").fetchone()[0]

    body: list[str] = []
    for r in rows:
        score = r["score"]
        beds = r["beds"] or 0
        per_bed_value = r["price"] / beds if beds else 0
        per_bed = f"€{per_bed_value:,.0f}" if beds else "—"
        band = "s80" if score >= 80 else "s65" if score >= 65 else "s50" if score >= 50 else "s0"
        fresh = " <span class='new'>new</span>" if r["alerted"] else ""

        ber = r["ber"]
        ber_cell = f"<span class='ber'>{html.escape(ber)}</span>" if ber else "—"

        reasons = html.escape((r["reasons"] or "")[:150])
        flags = html.escape((r["flags"] or "")[:150])
        area = html.escape(r["area"] or "—")
        ptype = html.escape(r["property_type"] or "—")
        seller = html.escape(r["seller_type"] or "—")
        published = html.escape(str(r["first_publish"] or "—"))
        title = html.escape((r["title"] or "view")[:70])
        link = html.escape(r["url"] or "")

        extra = ""
        # Total cost, not just the headline rent. The two are not the same
        # number and the gap decides several listings a month — an all-inclusive
        # €1,380 beats a €1,300 where you pay every bill. '=' means the ad says
        # bills are included, '~' means the figure is rent plus our estimate.
        cost = effective_cost(dict(r), cfg) or 0
        exact = r["bills_included"] == 1
        total_label = f"€{cost:,}{'=' if exact else '~'}"
        drive = maps_url(dict(r), cfg)
        if drive:
            extra += (f" <a href='{html.escape(drive)}' target='_blank'"
                      f" rel='noopener'>🚗 directions</a>")
        if reasons:
            extra += f"<div class='why'>{reasons}</div>"
        if flags:
            extra += f"<div class='flag'>⚠ {flags}</div>"

        body.append(
            "<tr>"
            f"<td class='score {band}' data-v='{score}'>{score}</td>"
            f"<td class='price' data-v='{r['price']}'>€{r['price']:,}</td>"
            f"<td class='price' data-v='{cost}'>{total_label}</td>"
            f"<td data-v='{beds}'>{beds or '—'}</td>"
            f"<td class='price' data-v='{per_bed_value:.0f}'>{per_bed}</td>"
            f"<td data-v='{html.escape(ber or '')}'>{ber_cell}</td>"
            f"<td data-v='{area}'>{area}</td>"
            f"<td data-v='{ptype}'>{ptype}</td>"
            f"<td data-v='{seller}'>{seller}</td>"
            f"<td data-v='{r['images'] if r['images'] is not None else -1}'>"
            f"{r['images'] if r['images'] is not None else '—'}</td>"
            f"<td data-v='{published}'>{published}{fresh}</td>"
            f"<td><a href='{link}' target='_blank' rel='noopener'>{title}</a>{extra}</td>"
            "</tr>"
        )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        REPORT_TEMPLATE.format(
            count=len(rows),
            total=total,
            generated=datetime.now().strftime("%Y-%m-%d %H:%M"),
            rows="\n".join(body),
        ),
        encoding="utf-8",
    )
    return REPORT_PATH


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1/3/4 (alternate) — email ingest  —  THE COMPLIANT PATH
#
# daftcron reads the saved-search alert emails that Daft sends to the user.
# It never makes an automated request to daft.ie, so nothing here engages the
# terms-of-use prohibitions on automated retrieval or database population.
#
# Setup: on daft.ie, run your search, choose "Save this search", and turn on
# email alerts. Then either let them arrive in your inbox and export them as
# .eml, or add a mail rule that files them into the configured email_dir.
#
# ⚠ PROVISIONAL EXTRACTION. The field parsing below handles the common shapes
# (a listing link with an adjacent price and bed count) but has NOT been tuned
# against a real Daft alert email, because none was available. Before trusting
# it, run:
#     python3 daftcron.py dump-email path/to/alert.eml
# and check what it actually pulls out. Tune EMAIL_* patterns as needed.
# ─────────────────────────────────────────────────────────────────────────────

EMAIL_LISTING_RE = re.compile(
    r"https?://(?:www\.)?daft\.ie/(for-rent|share|for-sale)/([A-Za-z0-9\-\%]+?)/(\d{5,})",
    re.I,
)
EMAIL_PRICE_RE = re.compile(r"(?:&euro;|€|EUR)\s?([\d][\d,]{2,})")
EMAIL_BEDS_RE = re.compile(r"(\d{1,2})\s*(?:bed\b|bd\b|bedroom)", re.I)
EMAIL_BATHS_RE = re.compile(r"(\d{1,2})\s*(?:bath\b|bth\b|bathroom)", re.I)
EMAIL_BER_RE = re.compile(r"\bBER\s*[:\-]?\s*([A-G])\s?([1-4])?", re.I)
# ── Real Daft alert format, verified against a live email 4 Oct 2026 ──
#
#   one listing per email (subject names the property) — this is a fast feed
#   <p class="address-title-string">Kildoon, Nurney, Co. Kildare, R51P938</p>
#   <p ...>€1,300 per month</p>
#   <p ...>1 Bed · 1 Bath · Apartment</p>
#   <img alt="BER Rating SI_666" src=".../ber/SI_666.png"/>
#   <a href="https://www.daft.ie/for-rent/<slug>/<id>?utm_...">
#
# ⚠ BER is supplied ONLY as an image filename (`SI_666`), never as a letter.
#   The code-to-band mapping is not published, so BER cannot be scored from
#   the email route at all. Set scoring.w_ber = 0 rather than letting every
#   listing be flagged "no BER" and docked 20 points.
# Irish Eircodes: a routing key (letter + 2 digits) then a unique 4-character
# suffix. Property-level, so far more precise than a town name — the listing
# slug says "Ardee" but the Eircode says exactly where it is.
EIRCODE_RE = re.compile(
    r"\b([AC-FHKNPRTV-Y]\d{2})\s?([0-9AC-FHKNPRTV-Y]{4})\b", re.I
)


def extract_eircode(text: str) -> str:
    """Return a normalised Eircode ('A92 APK4'), or '' if none is present."""
    match = EIRCODE_RE.search(text or "")
    return f"{match.group(1).upper()} {match.group(2).upper()}" if match else ""


DAFT_ADDRESS_RE = re.compile(
    r'class="address-title-string"[^>]*>(.*?)</p>', re.S | re.I
)
DAFT_ALT_ADDRESS_RE = re.compile(r'<img[^>]+alt="([^"<>]{10,160})"', re.I)
DAFT_BER_IMAGE_RE = re.compile(r"/ber/([A-Za-z0-9_\-]+)\.png", re.I)
DAFT_SUMMARY_RE = re.compile(
    r"(\d+)\s*Bed\b[^\n]{0,60}?(\d+)\s*Bath\b", re.I | re.S
)

# Longest first, so 'semi-detached' wins over 'detached'.
PROPERTY_TYPE_WORDS = [
    "semi-detached", "end of terrace", "terraced", "detached", "townhouse",
    "penthouse", "maisonette", "bungalow", "apartment", "studio", "duplex",
    "flat", "house", "site",
]

# How much text after a listing link to search for its price / beds.
EMAIL_CONTEXT_CHARS = 900


def _strip_tags(fragment: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", fragment)).strip()


def _property_type_from(text: str) -> str:
    """Find a property type word in free text. '' when none is present."""
    lowered = (text or "").lower()
    for word in PROPERTY_TYPE_WORDS:
        if re.search(rf"\b{re.escape(word)}\b", lowered):
            return word
    return ""

IRISH_COUNTIES = [
    "antrim", "armagh", "carlow", "cavan", "clare", "cork", "derry", "donegal",
    "down", "dublin", "fermanagh", "galway", "kerry", "kildare", "kilkenny",
    "laois", "leitrim", "limerick", "longford", "louth", "mayo", "meath",
    "monaghan", "offaly", "roscommon", "sligo", "tipperary", "tyrone",
    "waterford", "westmeath", "wexford", "wicklow",
]


def _county_from(text: str) -> str:
    """Find an Irish county in a slug or title.

    Handles both the '-co-leitrim' form and the Dublin style, where the county
    leads a number ('...-artane-dublin-5') rather than being prefixed with 'co-'.
    """
    normalised = text.lower().replace("-", " ").replace("_", " ")
    words = set(re.split(r"\W+", normalised))
    for county in IRISH_COUNTIES:
        if county in words:
            return county.title()
    return ""


def blank_record(ad_id: int, url: str) -> dict:
    """A record with every field present but empty, for partial sources.

    Fields the source cannot supply are None rather than 0, so scoring can tell
    "unknown" from "zero". Scoring a missing image count as if it were
    "no photos" would penalise every record an email ever produced.
    """
    return {
        "id": ad_id, "url": url, "title": "", "price": 0, "beds": 0,
        "baths": 0, "property_type": "", "county": "", "area": "",
        "eircode": "", "ber": "", "agent_email": "", "agent_phone": "",
        "seller_type": "", "seller_name": "", "images": None, "views": None,
        "first_publish": "", "publish_date": "", "last_update": "",
        "back_on_market": 0, "lat": None, "lng": None,
        "description": "", "facilities": "",
    }


def _body_from_message(msg) -> str:
    """Extract the usable body from a message, preferring the HTML part."""
    html_parts: list[str] = []
    text_parts: list[str] = []

    walk = msg.walk() if msg.is_multipart() else [msg]
    for part in walk:
        if part.get_content_disposition() == "attachment":
            continue
        ctype = part.get_content_type()
        if ctype not in ("text/html", "text/plain"):
            continue
        try:
            content = part.get_content()
        except Exception:  # noqa: BLE001 - malformed encodings are common
            continue
        if not isinstance(content, str):
            continue
        (html_parts if ctype == "text/html" else text_parts).append(content)

    return "\n".join(html_parts) or "\n".join(text_parts)


def _msg_datetime(msg):
    """Full datetime from the message Date header, or None."""
    raw = msg.get("date")
    if not raw:
        return None
    try:
        parsed = email_utils.parsedate_to_datetime(str(raw))
    except (TypeError, ValueError):
        return None
    return parsed


def _msg_date(msg) -> str:
    """ISO date from the message Date header, or '' if unusable."""
    parsed = _msg_datetime(msg)
    return parsed.date().isoformat() if parsed else ""


def _msg_iso(msg) -> str:
    """Full ISO timestamp from the Date header, or ''.

    Includes the time, which cadence analysis needs. Callers that only want a
    date slice the first ten characters.
    """
    parsed = _msg_datetime(msg)
    return parsed.isoformat() if parsed else ""


def read_eml(path: Path) -> tuple[str, str, str]:
    """Return (body, subject, iso_timestamp) for an .eml file, preferring HTML."""
    with path.open("rb") as fh:
        msg = email.message_from_binary_file(fh, policy=email_policy.default)
    return _body_from_message(msg), str(msg.get("subject") or ""), _msg_iso(msg)


def _flatten(body: str) -> str:
    """Strip tags to a text projection, preserving some structure."""
    flat = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", body, flags=re.S | re.I)
    flat = re.sub(r"<br\s*/?>", "\n", flat, flags=re.I)
    flat = re.sub(r"</(tr|div|p|table)>\s*", "\n", flat, flags=re.I)
    flat = re.sub(r"<[^>]+>", " ", flat)
    flat = html.unescape(flat)
    flat = re.sub(r"[ \t\u00a0]+", " ", flat)
    flat = re.sub(r"\n\s*\n+", "\n", flat)
    return flat


def parse_alert_email(body: str, published_hint: str = "") -> list[dict]:
    """Extract listing records from one Daft alert email body.

    Provisional — see the warning above. Every listing yields a record even if
    price or beds could not be found; missing values show up as 0 so the
    problem is visible rather than silently wrong.

    Listing URLs are located in the RAW html, because href attributes do not
    survive tag-stripping. Flattened text is used only for the surrounding
    price / bed / BER context, which is rendered content.
    """
    found: dict[int, dict] = {}
    matches = list(EMAIL_LISTING_RE.finditer(body))

    for index, match in enumerate(matches):
        kind, slug, raw_id = match.group(1).lower(), match.group(2), match.group(3)
        if kind == "for-sale":
            continue
        ad_id = int(raw_id)
        if ad_id in found:
            continue

        # Bound the context to THIS listing's block. Without the next-link
        # boundary the window bleeds into the following listing and steals its
        # price, bed count and BER.
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        window_end = min(next_start, match.end() + EMAIL_CONTEXT_CHARS * 4)
        window_html = body[match.end(): window_end]
        window = _flatten(window_html)
        preceding = _flatten(body[max(0, match.start() - 2000): match.start()])

        price_m = EMAIL_PRICE_RE.search(window) or EMAIL_PRICE_RE.search(preceding[-400:])
        summary_m = DAFT_SUMMARY_RE.search(window)

        rec = blank_record(ad_id, f"{BASE_URL}/{kind}/{slug}/{ad_id}")
        rec["price"] = _to_int(price_m.group(1)) if price_m else 0

        # Some listings quote a weekly rent ("€260 per week"). Everything else
        # assumes monthly, so convert — otherwise a weekly listing looks absurdly
        # cheap and would dominate the value scoring rather than merely mislead.
        if rec["price"] and re.search(r"per\s*week", window, re.I):
            rec["price"] = round(rec["price"] * 52 / 12)

        # Beds, baths and type come from the "1 Bed · 1 Bath · Apartment" line,
        # which is the only place the type appears reliably.
        if summary_m:
            rec["beds"] = int(summary_m.group(1))
            rec["baths"] = int(summary_m.group(2))
            rec["property_type"] = _property_type_from(summary_m.group(0))
        if not rec["beds"]:
            beds_m = EMAIL_BEDS_RE.search(window)
            rec["beds"] = int(beds_m.group(1)) if beds_m else 0
        if not rec["baths"]:
            baths_m = EMAIL_BATHS_RE.search(window)
            rec["baths"] = int(baths_m.group(1)) if baths_m else 0
        if not rec["property_type"]:
            rec["property_type"] = _property_type_from(window)

        # Address: Daft puts it in its own paragraph. The listing photo's alt
        # attribute carries the same string, as a fallback.
        addr_m = DAFT_ADDRESS_RE.search(window_html)
        if addr_m:
            rec["title"] = _strip_tags(addr_m.group(1))
        if not rec["title"]:
            for alt in DAFT_ALT_ADDRESS_RE.findall(window_html):
                candidate = html.unescape(alt).strip()
                if "," in candidate and "daft" not in candidate.lower():
                    rec["title"] = candidate
                    break
        if not rec["title"]:
            # Last resort for layouts that put the address in anchor text.
            anchor_m = re.search(r">([^<>]{6,120})</a>", window_html)
            if anchor_m:
                rec["title"] = html.unescape(anchor_m.group(1)).strip()

        # The real Daft email ships BER only as an image filename, so this
        # normally stays empty. Recorded if present, but scoring.w_ber is 0
        # because it cannot be relied upon.
        ber_m = EMAIL_BER_RE.search(window)
        if ber_m:
            rec["ber"] = ber_m.group(1).upper()

        # County is usually in the slug; fall back to the visible address.
        rec["county"] = _county_from(slug) or _county_from(rec["title"])
        # The Eircode is the precise location — the town in the slug can be
        # misleading (a listing "in Ardee" that is minutes outside it).
        rec["eircode"] = extract_eircode(rec["title"]) or extract_eircode(slug)

        # Agent contact details, when this particular email carries them.
        rec["agent_email"] = extract_agent_email(window)
        rec["agent_phone"] = extract_agent_phone(window)
        # Area: the slug minus its trailing locality index.
        rec["area"] = slug.rsplit("-", 1)[0] if "-" in slug else slug
        # The email's own date stands in for a publish date, so freshness can
        # still score. Nothing else in the email carries one.
        if published_hint:
            rec["first_publish"] = published_hint[:10]
            rec["publish_date"] = published_hint[:10]
        found[ad_id] = rec

    return list(found.values())


# ─────────────────────────────────────────────────────────────────────────────
# IMAP ingest (optional) — reads alerts straight from the mailbox
#
# IMAP is a standard, documented, first-class way to access your own mail; it
# is how desktop mail clients work. Nothing here is a workaround, and reading
# your own mailbox over IMAP is not something Google objects to.
#
# What DOES draw Gmail's attention is not polling:
#   * repeated failed logins  -> temporary lockout / "suspicious sign-in"
#   * the account password instead of an App Password (no longer works at all)
#   * a sudden new country or datacenter IP -> verification prompt
# Polling every 30 minutes is unremarkable; mail clients do it all day.
#
# Why use it: on the folder path, .eml files only appear if Mail.app is open
# and its rule runs. Over IMAP the only requirement is that this machine wakes
# eventually — the mail server holds the alerts in the meantime.
# ─────────────────────────────────────────────────────────────────────────────

KEYCHAIN_SERVICE = "daftcron"


def _imap_password(cfg: dict | None = None) -> str | None:
    """Resolve the IMAP password.

    Precedence, highest first:
      1. DAFTCRON_IMAP_PASSWORD environment variable
      2. secrets.toml
      3. macOS Keychain (service "daftcron")

    Within secrets.toml the lookup is deliberately tolerant: the canonical key
    is [ingest] imap_password, but imap_password or app_password under any
    section will be accepted. Getting a credential into the right key of the
    right table is a silly thing to lose a round trip over.
    """
    from_env = os.environ.get("DAFTCRON_IMAP_PASSWORD")
    if from_env:
        return from_env.strip()

    if cfg:
        for section in (cfg.get("ingest"), cfg.get("notify"), cfg):
            if not isinstance(section, dict):
                continue
            for key in ("imap_password", "app_password"):
                value = section.get(key)
                if value and str(value).strip():
                    return str(value).strip()

    if shutil.which("security"):
        try:
            proc = subprocess.run(
                ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-w"],
                capture_output=True, text=True, timeout=10,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                return proc.stdout.strip()
        except Exception:  # noqa: BLE001
            pass
    return None


def install_run_watchdog(cfg) -> None:
    """Abort a run that overruns, so a stall can never wedge the scheduler.

    launchd's StartInterval does not start a second instance of a job whose
    previous instance is still alive. A process that hangs therefore does not
    merely miss one run — it stops the job permanently, and it does so
    silently, because a hang writes nothing to the log.

    That is not hypothetical. On 2026-10-11 a run opened its IMAP connection
    and then blocked forever on a socket read with no timeout. The process sat
    in state S for two and a half days, launchd declined to start another, and
    the feed went quiet for three days while the log recorded no error at all.
    The socket timeouts in imap_connect() make that unlikely; this makes it
    impossible, and it also covers stalls that are not socket reads — a wedged
    subprocess, a lock, a pathological parse.
    """
    import signal  # local: not needed on the non-ingest paths

    try:
        minutes = float(cfg.get("ingest", {}).get("run_timeout_minutes", 10) or 0)
    except (TypeError, ValueError):
        minutes = 10.0
    if minutes <= 0:
        return

    def _overran(_signum, _frame):
        log(f"!! run exceeded {minutes:g} min — aborting so the scheduler frees up")
        sys.exit(124)   # 124 = the conventional "timed out" exit status

    signal.signal(signal.SIGALRM, _overran)
    signal.alarm(int(minutes * 60))


def imap_connect(cfg, retries: int = 4):
    """Open, authenticate and select the mailbox. Caller logs out.

    The TCP connect is retried with backoff. launchd routinely starts a job at
    login or on wake before DNS and the network are ready, which surfaces as
    "nodename nor servname provided" — transient, not a fault. Without a retry
    that single race marks the job as failed until the next hourly run.

    Every socket operation is also given a deadline. See the note below: the
    default is to wait forever, which is how the job once hung for days.
    """
    import imaplib  # local: only needed on this path

    ic = cfg.get("ingest", {})
    host = ic.get("imap_host", "imap.gmail.com")
    port = int(ic.get("imap_port", 993))
    user = ic.get("imap_user", "")
    password = _imap_password(cfg)

    # ── the deadline ────────────────────────────────────────────────────────
    # A socket with no timeout blocks INDEFINITELY: a stalled read never raises,
    # it just waits. Laptops stall connections constantly — sleep, wifi
    # roaming, NAT tables expiring mid-session. Without this, one stalled read
    # stops the process, and because launchd will not start a second copy of a
    # job that is still running, that one stuck process stops every future run
    # as well. The failure mode is total silence, which is the worst kind.
    #
    # setdefaulttimeout covers anything else in the process that opens a
    # socket; the explicit timeout= below is what actually matters.
    timeout = float(ic.get("imap_timeout", 30) or 30)
    socket.setdefaulttimeout(timeout)

    if not user:
        sys.exit("ingest.imap_user is not set in config.toml")
    if not password:
        sys.exit(
            "No IMAP password found. Provide one by any of:\n"
            "  1. secrets.toml     [ingest] imap_password = \"...\"\n"
            "  2. environment      export DAFTCRON_IMAP_PASSWORD=...\n"
            "  3. macOS Keychain   security add-generic-password"
            " -s daftcron -a you@gmail.com -w\n"
            "Gmail requires an App Password, not your account password."
        )

    conn = None
    for attempt in range(1, retries + 1):
        try:
            conn = imaplib.IMAP4_SSL(host, port, timeout=timeout)
            break
        except Exception as exc:  # noqa: BLE001
            reason = f"{type(exc).__name__}: {exc}"
            if attempt == retries:
                sys.exit(
                    f"Could not reach {host}:{port} after {retries} attempts"
                    f" — {reason}\n"
                    "If this happens on EVERY run, the network or the host is"
                    " wrong.\nIf it only happens at login or on wake, the job is"
                    " starting before DNS is ready; the next hourly run will do"
                    " fine."
                )
            wait = 5 * attempt
            log(f"  ! {host} unreachable ({reason}) — retry in {wait}s")
            time.sleep(wait)

    try:
        conn.login(user, password)
    except imaplib.IMAP4.error as exc:
        sys.exit(
            f"IMAP login rejected for {user}: {exc}\n"
            "Usual causes: an account password instead of an App Password, "
            "2-Step Verification not enabled, or IMAP disabled on the account."
        )

    conn.select(ic.get("imap_mailbox", "INBOX"))
    return conn


def fetch_imap_messages(cfg, conn, limit: int = 200,
                        mark_seen: bool = True) -> list[tuple[str, str, str]]:
    """Return [(label, body, iso_timestamp)] for matching emails.

    mark_seen=False leaves read state alone, which is what analysis commands
    want — reading mail to count it should not alter the mailbox.
    """
    ic = cfg.get("ingest", {})
    criteria = ic.get("imap_search", 'FROM "daft.ie"')

    # A mailbox can hold years of alerts from previous searches. Those are stale:
    # they inflate the database and, worse, the area medians used for the value
    # component get computed from listings that are long gone. Restrict to a
    # recent window instead.
    since_days = int(ic.get("imap_since_days", 0) or 0)
    if since_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        criteria = f'{criteria} SINCE {cutoff.strftime("%d-%b-%Y")}'

    try:
        typ, data = conn.search(None, criteria)
    except Exception as exc:  # noqa: BLE001
        log(f"  ! IMAP search failed: {exc}")
        return []
    if typ != "OK":
        log(f"  ! IMAP search returned {typ}")
        return []

    ids = (data[0] or b"").split()
    if not ids:
        return []
    if len(ids) > limit:
        ids = ids[-limit:]

    messages: list[tuple[str, str, str]] = []
    for num in ids:
        try:
            # BODY.PEEK, not RFC822: plain RFC822 sets \Seen as an implicit side
            # effect of merely reading, which is rude for a --dry-run and
            # unnecessary when the search does not depend on read state.
            typ, payload = conn.fetch(num, "(BODY.PEEK[])")
        except Exception:  # noqa: BLE001
            continue
        if typ != "OK" or not payload or not isinstance(payload[0], tuple):
            continue
        msg = email.message_from_bytes(payload[0][1], policy=email_policy.default)
        body = _body_from_message(msg)
        if body:
            subject = str(msg.get("subject") or "(no subject)")
            messages.append((
                f"uid{num.decode(errors='replace')} — {subject}", body, _msg_iso(msg),
            ))

    if mark_seen:
        archive = ic.get("imap_archive_folder", "")
        for num in ids:
            try:
                if archive:
                    conn.copy(num, archive)
                conn.store(num, "+FLAGS", "\\Seen")
            except Exception:  # noqa: BLE001
                pass
    return messages


def enquiry_text(rec: dict, cfg: dict) -> str:
    """Compose the message to send an agent about one listing.

    Generated on demand and never sent automatically. The recipient is
    deliberately absent: a real Daft alert email carries NO agent contact
    details — no name, no address, no phone — and the only way to obtain them
    would be to fetch the listing page, which the Terms of Use prohibit. Daft
    agents are normally contacted through Daft's own form anyway.

    Returns '' when the enquiry block is disabled.
    """
    enq = cfg.get("enquiry", {})
    if not enq.get("enabled"):
        return ""

    address = _clean_address(rec.get("title") or "") or "the property listed"
    price = f"€{rec['price']:,} per month" if rec.get("price") else ""
    intro = f"I'd like to arrange a viewing of {address}"
    if price:
        intro += f" — {price}"

    # Who I am, in one sentence. The age line is optional and off at 0.
    who_bits = []
    age = int(enq.get("age", 0) or 0)
    if age:
        who_bits.append(str(age))
    occupation = (enq.get("occupation") or "").strip()
    employer = (enq.get("employer") or "").strip()
    if occupation and employer:
        who_bits.append(f"a {occupation} at {employer}")
    elif occupation:
        who_bits.append(f"a {occupation}")
    elif employer:
        who_bits.append(f"at {employer}")

    lines = ["Hello,", "", intro + "."]

    available = (enq.get("available_from") or "").strip()
    about = (enq.get("about") or "").strip()
    lease_ends = (enq.get("lease_ends") or "").strip()

    if about:
        # Your own wording, verbatim. The generated alternative describes you in
        # the flat register of a form ("44, a software engineer at IBM"), which
        # is accurate but reads like a template — and an agent sees a lot of
        # those. Anything set here wins over occupation/employer/age below.
        lines += ["", about]
    else:
        who = ", ".join(who_bits)
        if who:
            lines += ["", f"I'm {who}."]

    if available:
        # Naming the actual end date turns an arbitrary-sounding "December" into
        # a documented reason, and offers flexibility in the same breath. An
        # agent holding an empty property cares when you can start; a bare month
        # leaves them guessing whether you are constrained or merely unsure.
        if lease_ends:
            lines += [
                "",
                f"I'm moving from my current rental onto a new lease from "
                f"{available} — my current lease runs to {lease_ends}, so I'm "
                "flexible on the exact date and ready to commit rather than "
                "just browsing.",
            ]
        else:
            lines += [
                "",
                f"I'm moving from my current rental onto a new lease from "
                f"{available} — so I'm ready to commit rather than just browsing.",
            ]

    extra = (enq.get("extra") or "").strip()
    if extra:
        lines += ["", extra]

    lines += ["", "Could you let me know the next step for arranging a viewing?", "", "Thanks,"]
    name = (enq.get("name") or "").strip()
    if name:
        lines.append(name)
    phone = (enq.get("phone") or "").strip()
    if phone:
        lines.append(phone)
    mail = (enq.get("email") or "").strip()
    if mail:
        lines.append(mail)

    return "\n".join(lines).strip()


def enquiry_mailto(rec: dict, cfg: dict) -> str:
    """A mailto: link that opens a mail client with the enquiry pre-filled.

    No recipient — see enquiry_text(). The address is left blank on purpose so
    this is a compose action, not a send.
    """
    body = enquiry_text(rec, cfg)
    if not body:
        return ""
    address = _clean_address(rec.get("title") or "") or "property enquiry"
    subject = f"Enquiry: {address}"
    recipient = str(rec.get("agent_email") or "").strip()
    return (
        "mailto:" + urllib.parse.quote(recipient)
        + "?subject=" + urllib.parse.quote(subject)
        + "&body=" + urllib.parse.quote(body)
    )


def cmd_enquiry(args, cfg) -> None:
    """Print the ready-to-send enquiry for one listing.

    With no id this uses the top-scoring listing. When an id IS given but not
    held, it says so rather than quietly substituting a different property —
    the id may simply have aged out of the ingest window, and pasting the wrong
    address into an agent enquiry is worse than an error message.
    """
    conn = open_db()

    row = None
    if args.id:
        row = conn.execute(
            "SELECT * FROM listings WHERE id = ?", (args.id,)
        ).fetchone()
        if row is None:
            print(f"\n(id {args.id} not held — showing the top listing instead)\n")

    if row is None:
        row = conn.execute(
            "SELECT * FROM listings ORDER BY score DESC LIMIT 1"
        ).fetchone()
        if row is None:
            sys.exit("No listings yet — run: daftcron.py ingest")

    print()
    print(f"  {row['title']}")
    print(f"  €{row['price']:,}/mo · score {row['score']} · {row['url']}")
    print()
    print("─" * 68)
    print(enquiry_text(dict(row), cfg))
    print("─" * 68)
    print()
    print("  Daft agents are normally contacted through Daft's own form on the")
    print("  listing page. Open it, choose Contact Agent, and paste the above.")
    print(f"  {row['url']}")
    print()
    conn.close()


def score_and_alert(conn, cfg, records, dry_run, source, fetched=0, harvested=0):
    """Shared tail for both ingest paths: store, score, alert, log the run."""
    medians = price_medians(conn)
    alerts: list[tuple[int, dict, list[str], list[str]]] = []

    for rec in records:
        ok, why = passes_filters(rec, cfg)
        if not ok:
            store(conn, rec)          # remember it so we never re-check it
            conn.execute("UPDATE listings SET score=0, reasons=? WHERE id=?", (why, rec["id"]))
            continue
        _, previous_price = store(conn, rec)
        value, reasons, flags = score_listing(rec, cfg, medians, previous_price)
        conn.execute(
            "UPDATE listings SET score=?, reasons=?, flags=? WHERE id=?",
            (value, " · ".join(reasons), " · ".join(flags), rec["id"]),
        )
        if value >= cfg["scoring"]["alert_threshold"]:
            alerts.append((value, rec, reasons, flags))

    conn.commit()
    refresh_price_stats(conn)

    alerts.sort(key=lambda a: -a[0])
    alerted = 0
    if alerts and not dry_run and cfg["notify"].get("telegram_enabled"):
        for value, rec, reasons, flags in alerts[: cfg["scoring"]["daily_digest_max"]]:
            if telegram_send(cfg, format_alert(rec, value, reasons, flags, cfg)):
                alerted += 1
                conn.execute("UPDATE listings SET alerted=1 WHERE id=?", (rec["id"],))
                time.sleep(0.6)
        conn.commit()

    conn.execute(
        "INSERT INTO runs (ts, harvested, fetched, new, alerted, mode) VALUES (?,?,?,?,?,?)",
        (now_iso(), harvested, fetched, len(records), alerted, source),
    )
    conn.commit()
    return alerts, alerted


def cmd_ingest(args, cfg) -> None:
    """Read Daft alert emails, score them, and alert.

    Source comes from ingest.source: "email" reads .eml files from a folder,
    "imap" reads them straight from the mailbox.
    """
    ingest_cfg = cfg.get("ingest", {})
    source = ingest_cfg.get("source", "email")

    messages: list[tuple[str, str, str]] = []
    mail_dir: Path | None = None
    imap_conn = None

    if source == "imap":
        imap_conn = imap_connect(cfg)
        log(f"ingest: {ingest_cfg.get('imap_user', '')} @ "
            f"{ingest_cfg.get('imap_host', 'imap.gmail.com')}")
        messages = fetch_imap_messages(cfg, imap_conn)
        log(f"ingest: {len(messages)} unprocessed alert email(s)")
    else:
        mail_dir = Path(ingest_cfg.get("email_dir", "mail"))
        if not mail_dir.is_absolute():
            mail_dir = ROOT / mail_dir
        if not mail_dir.exists():
            mail_dir.mkdir(parents=True, exist_ok=True)
            log(f"created {mail_dir}")
            log('Drop Daft alert .eml files there, or set ingest.source = "imap".')
            return
        files = sorted(p for p in mail_dir.glob("*.eml") if p.is_file())
        if not files:
            log(f"no .eml files in {mail_dir}")
            log('Save a Daft alert there as .eml, or set ingest.source = "imap".')
            log("Check what would be extracted:  daftcron.py dump-email <file>")
            return
        log(f"ingest: {len(files)} email(s) in {mail_dir}")
        for path in files:
            body, _, msg_date = read_eml(path)
            messages.append((path.name, body, msg_date))

    conn = open_db()
    known = existing_ids(conn)

    records: list[dict] = []
    seen_this_run: set[int] = set()

    for name, body, msg_date in messages:
        if not body:
            log(f"  ! no usable body in {name}")
            continue
        parsed = parse_alert_email(body, msg_date)
        if not parsed:
            log(f"  ! no listings found in {name}")
            continue
        new_here = [r for r in parsed
                    if r["id"] not in known and r["id"] not in seen_this_run]
        log(f"  {name}: {len(parsed)} listings, {len(new_here)} new")
        for rec in new_here:
            seen_this_run.add(rec["id"])
        records.extend(new_here)

    if not records:
        log("ingest: nothing new")
        conn.execute(
            "INSERT INTO runs (ts, harvested, fetched, new, alerted, mode) VALUES (?,?,?,?,?,?)",
            (now_iso(), len(messages), 0, 0, 0, "ingest"),
        )
        conn.commit()
        conn.close()
        if imap_conn:
            try:
                imap_conn.logout()
            except Exception:  # noqa: BLE001
                pass
        return

    alerts, alerted = score_and_alert(
        conn, cfg, records, args.dry_run, "ingest",
        fetched=len(records), harvested=len(messages),
    )
    log(f"ingest complete: {len(records)} new, {len(alerts)} above threshold, "
        f"{alerted} alerted")
    if args.dry_run:
        log("(dry run — nothing sent)")

    # Folder handling: file the processed .eml files away.
    if (mail_dir and ingest_cfg.get("keep_processed", True) and not args.dry_run):
        done_dir = mail_dir / "processed"
        done_dir.mkdir(exist_ok=True)
        for name, _, _ in messages:
            try:
                (mail_dir / name).rename(done_dir / name)
            except OSError:
                pass

    write_report(conn, cfg)
    conn.close()
    if imap_conn:
        try:
            imap_conn.logout()
        except Exception:  # noqa: BLE001
            pass


def cmd_check_imap(args, cfg) -> None:
    """Verify mailbox access without processing or marking anything."""
    ic = cfg.get("ingest", {})
    conn = imap_connect(cfg)
    print(f"\nconnected  {ic.get('imap_host', 'imap.gmail.com')}:"
          f"{ic.get('imap_port', 993)}")
    print(f"user       {ic.get('imap_user', '')}")
    mailbox = ic.get("imap_mailbox", "INBOX")
    print(f"mailbox    {mailbox}")

    typ, data = conn.select(mailbox, readonly=True)
    count = data[0].decode() if (typ == "OK" and data and data[0]) else "?"
    print(f"messages   {count}")

    criteria = ic.get("imap_search", 'UNSEEN FROM "daft.ie"')
    typ, data = conn.search(None, criteria)
    ids = (data[0] or b"").split() if typ == "OK" else []
    print(f"matching   {len(ids)}   {criteria}")
    if not ids:
        print("\nNothing matched. Send a test alert, or widen ingest.imap_search")
        print("to something like:  UNSEEN")
    print()
    try:
        conn.logout()
    except Exception:  # noqa: BLE001
        pass


def cmd_dump_email(args, cfg) -> None:
    """Show exactly what daftcron extracts from an .eml, for tuning."""
    path = Path(args.path).expanduser()
    if not path.exists():
        sys.exit(f"not found: {path}")
    body, subject, msg_date = read_eml(path)
    print(f"\nfile     {path}")
    print(f"subject  {subject}")
    print(f"date     {msg_date or '(unreadable)'}")
    print(f"body     {len(body):,} chars")
    records = parse_alert_email(body, msg_date)
    print(f"listings {len(records)}\n")
    if not records:
        print("Nothing extracted. The email layout probably differs from the")
        print("patterns in EMAIL_* — see the flatten output below to tune them.\n")
        print("--- flattened body (first 2500 chars) ---")
        print(_flatten(body)[:2500])
        return
    for rec in records:
        print(f"  id={rec['id']}  €{rec['price']:,}  {rec['beds']}bed  "
              f"ber={rec['ber'] or '?'}  county={rec['county'] or '?'}")
        print(f"    {(rec['title'] or '(no title found)')[:90]}")
        print(f"    {rec['url']}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────


def cmd_baseline(args, cfg) -> None:
    """Walk existing stock and record it WITHOUT alerting."""
    pages = args.pages or cfg["daft"].get("baseline_pages", 20)
    log(f"baseline: harvesting {pages} sitemap pages (~{pages * SITEMAP_PAGE_SIZE} ads)")

    ads = harvest(cfg, pages)
    known = existing_ids(open_db())
    fresh = {k: v for k, v in ads.items() if k not in known}
    log(f"baseline: {len(ads)} ads found, {len(fresh)} not yet in the database")

    conn = open_db()
    stored = 0
    for i, (ad_id, meta) in enumerate(fresh.items(), 1):
        rec = fetch_detail(cfg, ad_id, meta["url"])
        if rec:
            store(conn, rec)
            stored += 1
        if i % 25 == 0:
            conn.commit()
            log(f"  {i}/{len(fresh)} fetched, {stored} stored")
        time.sleep(cfg["daft"]["request_delay_seconds"])

    conn.commit()
    refresh_price_stats(conn)
    conn.execute(
        "INSERT INTO runs (ts, harvested, fetched, new, alerted, mode) VALUES (?,?,?,?,?,?)",
        (now_iso(), len(ads), stored, stored, 0, "baseline"),
    )
    conn.commit()
    log(f"baseline complete: {stored} listings recorded, no alerts sent")
    conn.close()


def cmd_scan(args, cfg) -> None:
    """Sitemap-based ingest.

    NOT COMPLIANT with daft.ie/legal/terms-of-use. robots.txt carries
    `Allow: /api/sitemap*`, but robots.txt is a crawler directive, not a
    licence. The Terms of Use separately prohibit automated retrieval, and
    clause (d) specifically prohibits doing so to populate a database.

    Kept only so the work is preserved. Use `ingest` (email-based) instead.
    """
    source = cfg.get("ingest", {}).get("source", "email")
    if source != "sitemap" or not args.force:
        log("=" * 74)
        log("scan is DISABLED. It uses daft.ie's sitemap, which conflicts with")
        log("daft.ie/legal/terms-of-use:")
        log("")
        log("  (c) ... no robot, spider, website search/retrieval application, or")
        log("      other automated device, process or means to access, retrieve")
        log("      or index any portion of the Website...")
        log("  (d) ... access, retrieve or index any portion of the Website...")
        log("      for purposes of constructing or populating any database;")
        log("")
        log("robots.txt allows the sitemap path, but robots.txt is not a licence.")
        log("")
        log("Use:   daftcron.py ingest        <- the compliant, email-based path")
        log("Or:    set ingest.source = \"sitemap\" and pass --force to override.")
        log("=" * 74)
        sys.exit(1)

    pages = args.pages or cfg["daft"].get("pages_per_scan", 2)
    log(f"scan: harvesting {pages} sitemap page(s) [--force given]")

    ads = harvest(cfg, pages)
    conn = open_db()
    known = existing_ids(conn)
    candidates = {k: v for k, v in ads.items() if k not in known}

    prefilter = cfg["daft"].get("prefilter_slug_contains") or []
    if prefilter:
        before = len(candidates)
        candidates = {
            k: v for k, v in candidates.items()
            if any(s.lower() in v["slug"].lower() for s in prefilter)
        }
        log(f"scan: prefilter {before} → {len(candidates)} candidates")

    log(f"scan: {len(ads)} ads in window, {len(candidates)} new")
    if not candidates:
        conn.execute(
            "INSERT INTO runs (ts, harvested, fetched, new, alerted, mode) VALUES (?,?,?,?,?,?)",
            (now_iso(), len(ads), 0, 0, 0, "scan"),
        )
        conn.commit()
        conn.close()
        return

    records: list[dict] = []
    for i, (ad_id, meta) in enumerate(candidates.items(), 1):
        rec = fetch_detail(cfg, ad_id, meta["url"])
        if rec:
            records.append(rec)
        if i % 20 == 0:
            log(f"  {i}/{len(candidates)} checked")
        time.sleep(cfg["daft"]["request_delay_seconds"])

    alerts, alerted = score_and_alert(
        conn, cfg, records, args.dry_run, "scan",
        fetched=len(records), harvested=len(ads),
    )

    log(f"scan complete: {len(records)} fetched, {len(alerts)} above threshold, "
        f"{alerted} alerted")
    if args.dry_run:
        log("(dry run — nothing sent)")

    path = write_report(conn, cfg)
    log(f"report: {path}")
    conn.close()


def cmd_mark(args, cfg) -> None:
    """Record whether a listing's rent covers the bills.

    This cannot be automated. The alert email carries no description — verified
    against real alerts, which run to six lines with no bills language at all —
    so the ad's "all inclusive" or "bills in addition to rent" never reaches
    daftcron. You read the ad before enquiring anyway, which is the moment the
    answer is in front of you, so this is one command at the moment you know.
    """
    conn = open_db()
    row = conn.execute(
        "SELECT id, title, price FROM listings WHERE id = ?", (args.id,)
    ).fetchone()
    if row is None:
        conn.close()
        sys.exit(f"No listing with id {args.id}.")

    if args.included:
        value = 1
    elif args.excluded:
        value = 0
    else:
        value = None

    conn.execute("UPDATE listings SET bills_included = ? WHERE id = ?",
                 (value, args.id))
    conn.commit()

    label = {1: "bills INCLUDED", 0: "bills NOT included"}.get(value, "unknown")
    cost = effective_cost({"price": row["price"], "bills_included": value}, cfg)
    print(f"\n  {row['id']}  {label}")
    print(f"  {(row['title'] or '')[:70]}")
    print(f"  rent €{row['price']:,} → total €{cost:,}/month")
    print("\n  run `daftcron.py rescore` to fold this into the ranking\n")
    conn.close()


def cmd_rescore(args, cfg) -> None:
    """Recompute every stored score without re-reading the mailbox.

    Scores are written at ingest time, and the ranking commands read that stored
    column — so changing [scoring], [commute.times] or a bills flag leaves every
    existing row holding a number computed from the OLD settings. Re-ingesting
    does not fix it either: a listing is only rescored when it next appears in
    an email, so in a quiet week a config fix never reaches the rows it was
    meant to fix. This applies one immediately.
    """
    conn = open_db()
    medians = price_medians(conn)
    rows = conn.execute("SELECT * FROM listings").fetchall()

    changed = 0
    queued: list[tuple[int, dict, list[str], list[str]]] = []
    for row in rows:
        rec = dict(row)
        before = row["score"] or 0

        # Reproduce store()'s notion of previous_price: the price before the
        # current one, and only when it actually moved. A single history entry
        # is not a price drop, so it must not score as one.
        history = conn.execute(
            "SELECT price FROM price_history WHERE id = ? ORDER BY seen_at",
            (rec["id"],),
        ).fetchall()
        previous = None
        if len(history) >= 2 and history[-1]["price"] != history[-2]["price"]:
            previous = history[-2]["price"]

        ok, why = passes_filters(rec, cfg)
        if not ok:
            conn.execute("UPDATE listings SET score = 0, reasons = ? WHERE id = ?",
                         (why, rec["id"]))
            after = 0
        else:
            value, reasons, flags = score_listing(rec, cfg, medians, previous)
            conn.execute(
                "UPDATE listings SET score = ?, reasons = ?, flags = ? WHERE id = ?",
                (value, " · ".join(reasons), " · ".join(flags), rec["id"]),
            )
            after = value
            # Qualifies now but has never been sent. Usually this is a listing
            # the OLD settings discarded — a ceiling that has since been raised
            # leaves them sitting here, ranked and unseen, with nothing to say
            # they exist. That silence is the whole reason this flag exists.
            if value >= cfg["scoring"]["alert_threshold"] and not row["alerted"]:
                queued.append((value, rec, reasons, flags))

        if after != before:
            changed += 1

    conn.commit()
    refresh_price_stats(conn)
    log(f"rescore: {len(rows)} rows scored, {changed} changed, {len(queued)} queued")
    print(f"\n  re-scored {len(rows)} listings — {changed} score(s) changed\n")

    if queued:
        queued.sort(key=lambda item: -item[0])
        print(f"  {len(queued)} now qualify but were never sent:")
        for value, rec, _reasons, _flags in queued:
            print(f"    score {value:>3}  {rec['county'] or '?':<11} "
                  f"€{rec['price']:,}  {(rec['title'] or '')[:44]}")
        print()

        if args.alert and cfg["notify"].get("telegram_enabled"):
            sent = 0
            for value, rec, reasons, flags in queued[: cfg["scoring"]["daily_digest_max"]]:
                if telegram_send(cfg, format_alert(rec, value, reasons, flags, cfg)):
                    sent += 1
                    conn.execute("UPDATE listings SET alerted = 1 WHERE id = ?",
                                 (rec["id"],))
                    time.sleep(0.6)
            conn.commit()
            log(f"rescore: {sent} alert(s) sent")
            print(f"  sent {sent} alert(s) to Telegram\n")
        else:
            print("  re-run with --alert to send these\n")

    conn.close()


def cmd_top(args, cfg) -> None:
    conn = open_db()
    limit = args.n or 20
    rows = conn.execute(
        """SELECT * FROM listings WHERE gone = 0 AND score > 0
           ORDER BY score DESC, first_seen DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    if not rows:
        print("No scored listings yet. Run:  daftcron.py ingest")
        return
    # TOTAL is what you would actually pay — rent plus the bills allowance,
    # unless the listing is known to include them. Two listings at the same
    # rent are not the same property, and this column is where that shows.
    print(f"\n{'SCORE':>5}  {'RENT':>7}  {'TOTAL':>8}  {'BED':>3}  {'AREA':<16}  TITLE")
    print("─" * 104)
    for r in rows:
        rec = dict(r)
        cost = effective_cost(rec, cfg)
        exact = rec.get("bills_included") == 1
        print(
            f"{r['score']:>5}  €{r['price']:>6,}  €{cost:>6,}{'=' if exact else '~'}  "
            f"{r['beds'] or '–':>3}  {(r['area'] or '–')[:16]:<16}  {(r['title'] or '')[:36]}"
        )
        if r["flags"]:
            print(f"{'':>5}  ⚠ {r['flags'][:92]}")
    print()
    print(f"  ~ = rent + €{bills_allowance(cfg)} assumed bills    "
          "= = bills included in the rent")
    print("  mark <id> --included | --excluded once you have read the ad\n")
    conn.close()


def cmd_report(args, cfg) -> None:
    conn = open_db()
    path = write_report(conn, cfg)
    log(f"report written: {path}")
    conn.close()


def cmd_stats(args, cfg) -> None:
    conn = open_db()
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    scored = conn.execute("SELECT COUNT(*) FROM listings WHERE score > 0").fetchone()[0]
    hot = conn.execute("SELECT COUNT(*) FROM listings WHERE score >= ?",
                       (cfg["scoring"]["alert_threshold"],)).fetchone()[0]
    buckets = conn.execute("SELECT COUNT(*) FROM price_stats").fetchone()[0]
    last = conn.execute("SELECT * FROM runs ORDER BY ts DESC LIMIT 1").fetchone()

    print("\ndaftcron stats")
    print("─" * 46)
    print(f"  listings tracked    {total:,}")
    print(f"  scored (passed filter) {scored:,}")
    print(f"  above alert threshold  {hot:,}")
    print(f"  median buckets      {buckets:,}")
    if last:
        print(f"\n  last run: {last['mode']} at {last['ts']}")
        print(f"    harvested {last['harvested']} · fetched {last['fetched']} "
              f"· new {last['new']} · alerted {last['alerted']}")

    # The price spread, which is the quickest way to tell whether the Daft saved
    # search is actually feeding the range you widened it to.
    #
    # This matters because daftcron's own max_price is NOT the binding limit —
    # the search's ceiling is. If every listing tops out at the same suspicious
    # round number, the saved search is the constraint and no amount of changing
    # config.toml will help: the emails carrying the higher rents are never sent.
    ceiling = cfg["filters"].get("max_price") or 0
    print("\n  by monthly rent:")
    for lo, hi in ((0, 900), (900, 1100), (1100, 1300), (1300, 1500),
                   (1500, 1700), (1700, 2000), (2000, 10 ** 9)):
        n = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE price > ? AND price <= ?",
            (lo, hi),
        ).fetchone()[0]
        label = f"€{lo:,}–{hi:,}" if hi < 10 ** 9 else f"€{lo:,}+"
        note = ""
        if ceiling:
            if lo >= ceiling:
                note = "  stored only, not scored"
            elif lo < ceiling < hi:
                note = "  partly over max_price"
        print(f"    {label:<14} {n:>4}  {'#' * min(n, 28)}{note}")

    print()
    top = conn.execute(
        """SELECT county, COUNT(*) c FROM listings
           GROUP BY county ORDER BY c DESC LIMIT 8"""
    ).fetchall()
    if top:
        print("  by county:")
        for row in top:
            print(f"    {(row['county'] or '?'):<20} {row['c']:>6,}")
    print()
    conn.close()


def cmd_export(args, cfg) -> None:
    conn = open_db()
    rows = conn.execute(
        "SELECT * FROM listings WHERE gone = 0 ORDER BY score DESC"
    ).fetchall()
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="", encoding="utf-8") as fh:
        if rows:
            writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))
    log(f"exported {len(rows)} rows → {CSV_PATH}")
    conn.close()


def _write_chat_id(chat_id: int) -> bool:
    """Set telegram_chat_id in secrets.toml, leaving the token untouched."""
    path = CONFIG_PATH.parent / "secrets.toml"
    if not path.exists():
        return False

    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[str] = []
    replaced = inserted = False
    in_notify = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("["):
            if in_notify and not (replaced or inserted):
                out.append(f'telegram_chat_id = "{chat_id}"')
                inserted = True
            in_notify = stripped == "[notify]"
            out.append(line)
            continue
        if in_notify and re.match(r"\s*telegram_chat_id\s*=", line):
            out.append(f'telegram_chat_id = "{chat_id}"')
            replaced = True
            continue
        out.append(line)

    if in_notify and not (replaced or inserted):
        out.append(f'telegram_chat_id = "{chat_id}"')
        inserted = True
    if not (replaced or inserted):
        return False

    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    return True


def cmd_telegram_id(args, cfg) -> None:
    """List the chat ids that have messaged this bot.

    Saves hand-building the getUpdates URL. Never prints the token.
    """
    token = (cfg["notify"].get("telegram_token") or "").strip()
    if not token:
        sys.exit("No token configured yet — put one in secrets.toml first.")

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        sys.exit(f"Telegram rejected the request: {_telegram_error(exc)}")
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Could not reach Telegram: {type(exc).__name__}: {exc}")

    if not payload.get("ok"):
        sys.exit(f"Telegram said: {payload.get('description', 'unknown error')}")

    updates = payload.get("result") or []
    if not updates:
        print("\nThe bot has received no messages yet.\n")
        print("  Open   t.me/<your-bot-username>   in Telegram and send it")
        print("  anything — even just 'hi'. A bot cannot message you until")
        print("  you have messaged it first, so nothing appears here until then.\n")
        return

    found: dict[int, str] = {}
    for update in updates:
        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat") or {}
        if chat.get("id") is not None:
            label = (chat.get("first_name") or chat.get("title")
                     or chat.get("username") or chat.get("type") or "")
            found[chat["id"]] = str(label)

    print(f"\nFound {len(found)} chat(s) that have messaged this bot:\n")
    for chat_id, label in found.items():
        print(f'  telegram_chat_id = "{chat_id}"     {label}')

    if getattr(args, "write", False):
        if len(found) == 1:
            only = next(iter(found))
            if _write_chat_id(only):
                print(f"\n✓ Written to secrets.toml. Next:  daftcron.py test-telegram\n")
            else:
                print("\n! Could not update secrets.toml automatically.\n")
        else:
            print("\n! More than one chat found — edit secrets.toml yourself.\n")
    else:
        print("\nPut that number in secrets.toml under [notify].")
        print("Or re-run with --write to have it done for you.\n")


def cmd_status(args, cfg) -> None:
    """One-page readiness check. Never prints a credential."""
    ok, warn, bad = "\u2713", "!", "\u2717"
    print("\ndaftcron status")
    print("=" * 62)

    # ── files ────────────────────────────────────────────────────────────────
    print("\nfiles")
    print(f"  {ok} config.toml")
    secrets_path = CONFIG_PATH.parent / "secrets.toml"
    print(f"  {ok if secrets_path.exists() else warn} secrets.toml"
          + ("" if secrets_path.exists() else "  (missing — cp secrets.example.toml)"))

    # ── telegram ─────────────────────────────────────────────────────────────
    print("\ntelegram")
    notify = cfg.get("notify", {})
    token = (notify.get("telegram_token") or "").strip()
    chat_id = str(notify.get("telegram_chat_id") or "").strip()
    if not notify.get("telegram_enabled"):
        print(f"  {warn} disabled in config.toml")
    elif not token or not chat_id:
        print(f"  {bad} credentials incomplete "
              f"(token {'set' if token else 'MISSING'}, "
              f"chat id {'set' if chat_id else 'MISSING'})")
    else:
        try:
            with urllib.request.urlopen(
                f"https://api.telegram.org/bot{token}/getMe", timeout=10
            ) as resp:
                payload = json.loads(resp.read().decode("utf-8", "replace"))
            username = (payload.get("result") or {}).get("username")
            print(f"  {ok} bot @{username} — token accepted")
            print(f"  {ok} chat id set")
        except urllib.error.HTTPError as exc:
            print(f"  {bad} token rejected — {_telegram_error(exc)}")
        except Exception as exc:  # noqa: BLE001
            print(f"  {bad} could not reach Telegram: {type(exc).__name__}")

    # ── ingest ───────────────────────────────────────────────────────────────
    print("\ningest")
    ingest_cfg = cfg.get("ingest", {})
    source = ingest_cfg.get("source", "email")
    if source == "imap":
        user = ingest_cfg.get("imap_user", "")
        print(f"  {ok} source: imap ({ingest_cfg.get('imap_host')})")
        print(f"  {ok if user else bad} imap_user "
              + (user if user else "MISSING"))
        have_pw = bool(_imap_password(cfg))
        print(f"  {ok if have_pw else bad} password "
              + ("found" if have_pw
                 else "MISSING (secrets.toml, env var, or Keychain)"))
    else:
        mail_dir = Path(ingest_cfg.get("email_dir", "mail"))
        if not mail_dir.is_absolute():
            mail_dir = CONFIG_PATH.parent / mail_dir
        waiting = len(list(mail_dir.glob("*.eml"))) if mail_dir.exists() else 0
        print(f"  {ok if waiting else warn} source: folder ({mail_dir.name}/)"
              f" — {waiting} .eml waiting")
        if not waiting:
            print("      nothing arrives automatically on this route;")
            print("      files must be dropped in, or switch to imap")

    # ── schedule ─────────────────────────────────────────────────────────────
    print("\nschedule")
    job = cfg.get("_launchd_label", "ie.atensai.daftcron")
    if not shutil.which("launchctl"):
        print(f"  {warn} launchctl not found — not macOS?")
    else:
        try:
            listing = subprocess.run(
                ["launchctl", "list"], capture_output=True, text=True, timeout=10
            ).stdout
        except Exception:  # noqa: BLE001
            listing = ""
        row = next((l for l in listing.splitlines() if job in l), None)
        if not row:
            print(f"  {warn} not loaded — nothing runs automatically")
            print("      launchctl load -w ~/Library/LaunchAgents/"
                  f"{job}.plist")
        else:
            cols = row.split()
            pid = cols[0] if cols and cols[0] != "-" else ""
            code = cols[1] if len(cols) > 1 else "?"
            if code == "0":
                print(f"  {ok} loaded — last run exited cleanly")
            elif code == "-":
                print(f"  {ok} loaded — has not run yet")
            else:
                print(f"  {bad} loaded — LAST RUN FAILED (exit {code})")
                print("      check:  cat out/daftcron.err")
            if pid:
                print(f"      running right now as pid {pid}")
            if DB_PATH.exists():
                conn = open_db()
                last = conn.execute(
                    "SELECT ts FROM runs WHERE mode = 'ingest' "
                    "ORDER BY ts DESC LIMIT 1"
                ).fetchone()
                conn.close()
                if last:
                    print(f"      last ingest: {str(last['ts'])[:19]}")
                else:
                    print(f"  {warn} no ingest has ever completed")

    # ── commute ──────────────────────────────────────────────────────────────
    print("\ncommute")
    commute_cfg = cfg.get("commute", {})
    times = commute_cfg.get("times") or {}
    if commute_cfg.get("enabled"):
        print(f"  {ok} enabled — {len(times)} area(s), "
              f"{commute_cfg.get('days_per_week')} day(s)/week")
        print(f"      to {commute_cfg.get('destination')}")
        if len(times) < 5:
            print(f"  {warn} only {len(times)} area(s) listed — others score "
                  f"neutrally and get flagged")
    else:
        print(f"  {warn} disabled")

    # ── filters ──────────────────────────────────────────────────────────────
    print("\nfilters")
    filters = cfg.get("filters", {})
    print(f"  max price  €{filters.get('max_price')}")
    print(f"  min beds   {filters.get('min_beds')}")
    print(f"  counties   {filters.get('counties') or 'any'}")
    print(f"  types      {filters.get('property_types') or 'any'}")

    # ── data ─────────────────────────────────────────────────────────────────
    print("\ndata")
    if not DB_PATH.exists():
        print(f"  {warn} no database yet — nothing ingested")
    else:
        conn = open_db()
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        scored = conn.execute("SELECT COUNT(*) FROM listings WHERE score > 0").fetchone()[0]
        areas = conn.execute("SELECT COUNT(*) FROM area_stats").fetchone()[0]
        last = conn.execute("SELECT * FROM runs ORDER BY ts DESC LIMIT 1").fetchone()
        print(f"  listings {total}   scored {scored}   area medians {areas}")
        if last:
            print(f"  last run   {last['mode']} at {str(last['ts'])[:16]}")
        if total and not areas:
            print(f"  {warn} no area medians yet — the value component stays "
                  f"dormant until an area has 3+ listings")
        conn.close()
    print()


def cmd_cadence(args, cfg) -> None:
    """Analyse the alert emails in mail/ and report how Daft actually sends them.

    Answers two questions at once: whether the feed is per-listing or a daily
    digest, and whether the parser copes with the real format. Both matter,
    and both are settled by the same few downloaded emails.
    """
    ingest_cfg = cfg.get("ingest", {})
    mail_dir = Path(ingest_cfg.get("email_dir", "mail"))
    if not mail_dir.is_absolute():
        mail_dir = CONFIG_PATH.parent / mail_dir

    # Include already-handled emails. Cadence is about the pattern over time, so
    # the archive is more informative than whatever happens to be pending — and
    # after an ingest the only files present are in the archive.
    candidates = [p for p in mail_dir.glob("*.eml") if p.is_file()]
    archive_dir = mail_dir / "processed"
    if archive_dir.exists():
        candidates += [p for p in archive_dir.glob("*.eml") if p.is_file()]
    files = sorted(candidates)

    if not files:
        print(f"\nNo .eml files in {mail_dir}\n")
        print("  In Gmail on a computer, open a Daft alert and use")
        print("  the three-dot menu -> Show original -> Download Original.")
        print("  Three or four gives a much clearer answer than one.\n")
        return

    rows = []
    for path in files:
        with path.open("rb") as fh:
            msg = email.message_from_binary_file(fh, policy=email_policy.default)
        stamp = _msg_datetime(msg)
        records = parse_alert_email(
            _body_from_message(msg),
            stamp.date().isoformat() if stamp else "",
        )
        rows.append({
            "stamp": stamp,
            "subject": str(msg.get("subject") or "(no subject)"),
            "count": len(records),
        })

    print(f"\ndaftcron — {len(rows)} email(s) in {mail_dir.name}/\n")
    print(f"  {'received':<20} {'listings':>8}  {'read':>5}  subject")
    for row in sorted(rows, key=lambda r: str(r["stamp"]), reverse=True):
        when = row["stamp"].strftime("%Y-%m-%d %H:%M") if row["stamp"] else "(no date)"
        state = "ok" if row["count"] else "ZERO"
        print(f"  {when:<20} {row['count']:>8}  {state:>5}  {row['subject'][:34]}")

    counts = [row["count"] for row in rows]
    blank = sum(1 for c in counts if c == 0)
    print()
    if blank:
        print(f"  ! {blank} of {len(rows)} produced NO listings. That is the parser")
        print( "    failing on the real format, not an empty email. Run")
        print( "    `dump-email` on one of those to see the layout.\n")

    stamps = [row["stamp"] for row in rows if row["stamp"]]
    if len(stamps) < 3:
        print("  Cadence: not enough dated emails to judge — 3+ needed.\n")
        return

    days = {}
    for stamp in stamps:
        days.setdefault(stamp.date(), []).append(stamp)
    per_day = [len(v) for v in days.values()]
    average = sum(counts) / len(counts)
    hours = {s.hour for s in stamps}

    print(f"  {len(stamps)} emails across {len(days)} day(s): "
          f"{min(per_day)}-{max(per_day)} per day")
    print(f"  average {average:.1f} listings per email")
    print(f"  {len(hours)} distinct hour(s) of arrival")
    print()

    if average >= 6 and max(per_day) <= 2:
        print("  Verdict: looks like a DAILY DIGEST.")
        print("  One or two emails a day carrying many listings. Nothing here is")
        print("  fast, so daftcron is not giving away speed — its value is ranking")
        print("  the day's batch. Run it once each morning and that is enough.")
    elif average <= 3.5 and len(hours) >= 3:
        print("  Verdict: looks like PER-LISTING alerts.")
        print("  Arrivals spread through the day, few listings each. Daft already")
        print("  gives you speed, so daftcron works as a judgment layer on top.")
    else:
        print("  Verdict: inconclusive.")
        print("  Mixed counts or clustered times. Judge from the table above.")
    print()


def cmd_test_telegram(args, cfg) -> None:
    notify = cfg["notify"]
    if not notify.get("telegram_enabled"):
        sys.exit("notify.telegram_enabled is false in config.toml")

    token = (notify.get("telegram_token") or "").strip()
    chat_id = (notify.get("telegram_chat_id") or "").strip()
    if not token or not chat_id:
        print("\nNo Telegram credentials found.\n")
        print("  cp secrets.example.toml secrets.toml")
        print("  then put your token and chat id in secrets.toml\n")
        print("  (or export DAFTCRON_TELEGRAM_TOKEN / DAFTCRON_TELEGRAM_CHAT)\n")
        print(f"  token    {f'set, {len(token)} chars' if token else 'MISSING'}")
        print(f"  chat id  {chat_id or 'MISSING'}\n")
        sys.exit(1)

    ok = telegram_send(
        cfg,
        "<b>daftcron</b> ✅\nCredentials work. Alerts will arrive here.",
    )
    print("Test message sent — check your phone." if ok
          else "Failed. See the reason above.")
    sys.exit(0 if ok else 1)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="daftcron",
        description="Watch daft.ie for new rental listings and alert on the best ones.",
    )
    parser.add_argument("--config", type=Path, default=CONFIG_PATH,
                        help="path to config.toml")
    sub = parser.add_subparsers(dest="command", required=True)

    p_base = sub.add_parser("baseline", help="seed existing stock without alerting")
    p_base.add_argument("--pages", type=int, help="sitemap pages to walk")

    p_scan = sub.add_parser("scan", help="[not compliant] sitemap-based ingest")
    p_scan.add_argument("--pages", type=int, help="sitemap pages to walk")
    p_scan.add_argument("--dry-run", action="store_true", help="do not send alerts")
    p_scan.add_argument("--force", action="store_true",
                        help="acknowledge the terms-of-use conflict and run anyway")

    p_ingest = sub.add_parser("ingest", help="parse Daft alert emails (recommended)")
    p_ingest.add_argument("--dry-run", action="store_true", help="do not send alerts")

    sub.add_parser("check-imap", help="verify mailbox access without processing")

    p_id = sub.add_parser("telegram-id", help="find your Telegram chat id")
    p_id.add_argument("--write", action="store_true",
                      help="write it into secrets.toml (only if exactly one chat)")

    sub.add_parser("status", help="readiness check: what is wired, what is not")

    sub.add_parser("cadence", help="how often Daft sends alerts, and can we read them")

    p_enq = sub.add_parser("enquiry", help="print the ready-made message for a listing")
    p_enq.add_argument("id", type=int, nargs="?", default=None,
                       help="listing id (default: the top-scoring listing)")

    p_dump = sub.add_parser("dump-email", help="show what daftcron extracts from an .eml")
    p_dump.add_argument("path", type=str, help="path to a saved alert email")

    p_top = sub.add_parser("top", help="print current best listings")
    p_top.add_argument("n", type=int, nargs="?", default=20)

    p_mark = sub.add_parser("mark", help="record whether the rent covers the bills")
    p_mark.add_argument("id", type=int, help="listing id")
    mark_group = p_mark.add_mutually_exclusive_group(required=True)
    mark_group.add_argument("--included", action="store_true",
                            help="the ad says all bills are included")
    mark_group.add_argument("--excluded", action="store_true",
                            help="the ad says bills are extra")
    mark_group.add_argument("--clear", action="store_true",
                            help="forget what was recorded")

    p_rescore = sub.add_parser("rescore",
                               help="recompute scores after a config change")
    p_rescore.add_argument("--alert", action="store_true",
                           help="also send any listing that now qualifies "
                                "but has never been alerted")

    sub.add_parser("report", help="regenerate the HTML report")
    sub.add_parser("stats", help="database summary")
    sub.add_parser("export", help="dump matches to CSV")
    sub.add_parser("test-telegram", help="verify Telegram credentials")

    args = parser.parse_args()
    cfg = load_config(args.config)

    # Belt to the socket timeout's braces. A launchd job that hangs is not
    # merely late — it blocks its own future runs. See install_run_watchdog().
    install_run_watchdog(cfg)

    {
        "baseline": cmd_baseline,
        "ingest": cmd_ingest,
        "check-imap": cmd_check_imap,
        "telegram-id": cmd_telegram_id,
        "status": cmd_status,
        "cadence": cmd_cadence,
        "enquiry": cmd_enquiry,
        "dump-email": cmd_dump_email,
        "scan": cmd_scan,
        "top": cmd_top,
        "mark": cmd_mark,
        "rescore": cmd_rescore,
        "report": cmd_report,
        "stats": cmd_stats,
        "export": cmd_export,
        "test-telegram": cmd_test_telegram,
    }[args.command](args, cfg)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)
