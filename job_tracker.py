#!/usr/bin/env python3
"""
2027 Internship Job Alert Tracker
---------------------------------
Polls the public job-board APIs that tech companies use (Greenhouse, Lever,
Ashby, Workday, Amazon) for new 2027 internship postings and pushes a
notification to your phone via ntfy.sh.

Scales to tens of thousands of companies: companies marked tier=hot are checked
every run; everyone else is split into rotating batches ("shards") so each run
stays fast.

Also watches extra sources (sources.json): the Simplify and CSCareers 2027
internship lists on GitHub, Adzuna, Hacker News "Who's Hiring", USAJobs and
Google Jobs (via SerpApi). A job found by several sources alerts only once.

Usage:
    python job_tracker.py                 # run one check (GitHub Actions / cron)
    python job_tracker.py --loop          # keep running, check every 30 minutes
    python job_tracker.py --check         # verify every company in companies.csv responds
    python job_tracker.py --check-sources # test every extra source in sources.json
    python job_tracker.py --test-notify   # send a test notification to your phone
"""

import argparse
import csv
import hashlib
import html
import json
import logging
import math
import os
import re
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from hubs import HUBS, build_city_index, location_ok

CITY_INDEX = build_city_index(HUBS)

# ----------------------------------------------------------------------------
# Configuration (all overridable with environment variables)
# ----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
COMPANIES_FILE = BASE_DIR / "companies.csv"
KEYWORDS_FILE = BASE_DIR / "keywords.json"
SEEN_FILE = BASE_DIR / "seen_jobs.json"
SOURCES_FILE = BASE_DIR / "sources.json"

NTFY_TOPIC = os.getenv("NTFY_TOPIC", "").strip()   # .strip() removes stray spaces/Enter
NTFY_SERVER = (os.getenv("NTFY_SERVER", "") or "https://ntfy.sh").strip().rstrip("/")
CHECK_INTERVAL_MIN = int(os.getenv("CHECK_INTERVAL_MIN", "30"))
STRICT_MODE = os.getenv("STRICT_MODE", "0") == "1"      # 1 = only postings that say 2027
# LOCATION_FILTER: "hubs" = US tech hubs + US-remote (default), "any" = anywhere
LOCATION_FILTER = os.getenv("LOCATION_FILTER", "hubs").lower()
# Only alert for postings published within this many days (older ones are recorded silently).
MAX_AGE_DAYS = float(os.getenv("MAX_AGE_DAYS", "7"))
# The same company + title within this many days counts as the same job (e.g. found by two sources).
TITLE_DEDUPE_DAYS = 14
MAX_COMPANIES_PER_RUN = int(os.getenv("MAX_COMPANIES_PER_RUN", "4000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "24"))
MAX_INDIVIDUAL_ALERTS = 10
TIMEOUT = 25
HEADERS = {"User-Agent": "Mozilla/5.0 (student internship tracker; educational project)",
           "Accept-Encoding": "gzip, deflate"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("tracker")

# ----------------------------------------------------------------------------
# HTTP with retries (thread-local sessions reuse connections)
# ----------------------------------------------------------------------------
_local = threading.local()


def http(method: str, url: str, **kw) -> requests.Response:
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.headers.update(HEADERS)
    kw.setdefault("timeout", TIMEOUT)
    resp = None
    for attempt in range(4):
        try:
            resp = _local.session.request(method, url, **kw)
        except (requests.ConnectionError, requests.Timeout):
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)
            continue
        if resp.status_code in (429, 500, 502, 503, 504):
            wait = resp.headers.get("Retry-After", "")
            time.sleep(float(wait) if wait.replace(".", "", 1).isdigit() else 2 ** (attempt + 1))
            continue
        return resp
    return resp


# ----------------------------------------------------------------------------
# Matching rules (loaded from keywords.json)
# ----------------------------------------------------------------------------
def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[-_/.,()|:&+]", " ", (s or "").lower())).strip()


def build_regex(terms: list[str]) -> re.Pattern:
    parts = sorted({normalize(t) for t in terms if t.strip()}, key=len, reverse=True)
    return re.compile(r"\b(" + "|".join(re.escape(p).replace(r"\ ", r"\s+") for p in parts) + r")\b")


def load_keywords():
    kw = json.loads(KEYWORDS_FILE.read_text(encoding="utf-8"))
    if "core_role_terms" not in kw:                      # older keywords.json format
        kw = {"intern_terms": kw["intern_terms"], "core_role_terms": kw["role_terms"],
              "general_role_terms": [], "other_field_terms": [],
              "always_exclude": kw.get("exclude_terms", [])}
    empty = re.compile(r"(?!x)x")                        # matches nothing
    rx = lambda key: build_regex(kw[key]) if kw.get(key) else empty
    return (rx("intern_terms"), rx("core_role_terms"), rx("general_role_terms"),
            rx("other_field_terms"), rx("always_exclude"))


INTERN_RE, CORE_RE, GENERAL_RE, OTHER_FIELD_RE, ALWAYS_EXCLUDE_RE = load_keywords()


def role_matches(title: str) -> bool:
    """Is this a SWE / SDE / ML / AI / DS role (ignoring the intern check)?"""
    t = normalize(title)
    if ALWAYS_EXCLUDE_RE.search(t):
        return False
    if CORE_RE.search(t):
        return True
    return bool(GENERAL_RE.search(t)) and not OTHER_FIELD_RE.search(t)


TARGET_YEAR = "2027"
OTHER_YEAR_RE = re.compile(r"\b(2023|2024|2025|2026)\b")
# "Graduating in 2027" appears in Summer 2026 postings, so the year in a
# description only counts when attached to a season or to "intern".
TARGET_IN_TEXT_RE = re.compile(
    r"((summer|fall|spring|winter|autumn)\s*(of\s*)?2027|2027\s*(summer|fall|spring|winter|"
    r"intern|internship|co-?op))", re.I)
OTHER_SEASON_RE = re.compile(r"(summer|fall|spring|winter|autumn)\s*(of\s*)?(2025|2026)", re.I)


def title_is_candidate(title: str) -> bool:
    """Cheap title-only check: intern + tech role, not excluded, not an old year."""
    if not INTERN_RE.search(normalize(title)) or not role_matches(title):
        return False
    return not (OTHER_YEAR_RE.search(title) and TARGET_YEAR not in title)


def classify(title: str, text: str = "") -> str | None:
    """Return 'confirmed' (explicitly 2027), 'possible' (no year stated), or None."""
    if not title or not title_is_candidate(title):
        return None
    if TARGET_YEAR in title or TARGET_IN_TEXT_RE.search(text or ""):
        return "confirmed"
    if OTHER_SEASON_RE.search(text or ""):
        return None
    return None if STRICT_MODE else "possible"


def strip_html(raw: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(raw or ""))).strip()


# ----------------------------------------------------------------------------
# Posting dates
# ----------------------------------------------------------------------------
def to_epoch(value) -> float:
    """Turn any date format the job sites use into epoch seconds (0 = unknown)."""
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return value / 1000 if value > 1e11 else float(value)
    s = str(value).strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        pass
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    t = s.lower()                                        # "Posted 3 Days Ago", "today", "30+ days"
    now = time.time()
    m = re.fullmatch(r"(\d+)\s*(h|hr|hrs|d|w|mo)", t)      # "5h", "2d", "1w", "3mo"
    if m:
        mult = {"h": 1 / 24, "hr": 1 / 24, "hrs": 1 / 24, "d": 1, "w": 7, "mo": 30}[m.group(2)]
        return now - int(m.group(1)) * mult * 86400
    for fmt in ("%b %d %Y", "%B %d %Y"):                 # "Sep 20" (no year) - add this year
        try:
            d = datetime.strptime(f"{s} {datetime.now().year}", fmt).replace(tzinfo=timezone.utc)
            if d.timestamp() > now + 86400:
                d = d.replace(year=d.year - 1)
            return d.timestamp()
        except ValueError:
            continue
    if any(w in t for w in ("today", "just posted", "hour", "minute")):
        return now
    if "yesterday" in t:
        return now - 86400
    m = re.search(r"(\d+)\s*(\+)?\s*(day|week|month)", t)
    if m:
        days = int(m.group(1)) * {"day": 1, "week": 7, "month": 30}[m.group(3)]
        return now - (days + (1 if m.group(2) else 0)) * 86400
    return 0.0


def age_text(days) -> str:
    if days is None:
        return "post date unknown"
    days += 0.01                                         # absorb sub-second timing drift
    if days < 1:
        return "posted today"
    if days < 2:
        return "posted yesterday"
    return f"posted {int(days)} days ago"


# ----------------------------------------------------------------------------
# Fetchers — each returns a list of dicts: id, title, location, url, text, posted
# ----------------------------------------------------------------------------
def fetch_greenhouse(slug: str) -> list[dict]:
    # Two stages: the job list without descriptions is small; only fetch the
    # description for titles that already look like tech internships.
    r = http("GET", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        post = {"id": str(j["id"]), "title": j.get("title", ""),
                "location": (j.get("location") or {}).get("name", ""),
                "url": j.get("absolute_url", ""), "text": "",
                "posted": to_epoch(j.get("first_published") or j.get("updated_at"))}
        if title_is_candidate(post["title"]):
            try:
                d = http("GET", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{j['id']}")
                if d.ok:
                    post["text"] = strip_html(d.json().get("content", ""))
            except requests.RequestException:
                pass
        out.append(post)
    return out


def fetch_lever(slug: str) -> list[dict]:
    r = http("GET", f"https://api.lever.co/v0/postings/{slug}", params={"mode": "json"})
    r.raise_for_status()
    return [{"id": j["id"], "title": j.get("text", ""),
             "location": (j.get("categories") or {}).get("location", ""),
             "url": j.get("hostedUrl", ""), "text": j.get("descriptionPlain", "") or "",
             "posted": to_epoch(j.get("createdAt"))}
            for j in r.json()]


def fetch_ashby(slug: str) -> list[dict]:
    r = http("GET", f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
    r.raise_for_status()
    return [{"id": j["id"], "title": j.get("title", ""), "location": j.get("location", ""),
             "url": j.get("jobUrl", ""), "text": j.get("descriptionPlain", "") or "",
             "posted": to_epoch(j.get("publishedAt"))}
            for j in r.json().get("jobs", [])]


WORKDAY_LIMIT = threading.Semaphore(int(os.getenv("WORKDAY_CONCURRENCY", "8")))
WORKDAY_PER_RUN = int(os.getenv("WORKDAY_PER_RUN", "0"))      # 0 = all (on your Mac)
WORKDAY_RETRIES = int(os.getenv("WORKDAY_RETRIES", "4"))


def workday_post(url: str, body: dict) -> dict:
    """POST to Workday, a few at a time, retrying if it sends back a non-data page."""
    for attempt in range(WORKDAY_RETRIES):
        with WORKDAY_LIMIT:
            r = http("POST", url, json=body)
        if r is not None and r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                pass                                     # got an HTML "slow down" page
        elif r is not None and r.status_code in (400, 404, 410):
            r.raise_for_status()                         # a real error: don't retry
        if attempt < WORKDAY_RETRIES - 1:
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("Workday kept refusing (rate-limited) - will retry next run")


def fetch_workday(slug: str) -> list[dict]:
    """slug format: host|tenant|site  e.g. nvidia.wd5.myworkdayjobs.com|nvidia|NVIDIAExternalCareerSite"""
    host, tenant, site = slug.split("|")
    url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    jobs, offset = [], 0
    while offset < 200:
        data = workday_post(url, {"appliedFacets": {}, "limit": 20, "offset": offset,
                                  "searchText": "intern"})
        page = data.get("jobPostings", [])
        for j in page:
            path = j.get("externalPath", "")
            jobs.append({"id": path, "title": j.get("title", ""),
                         "location": j.get("locationsText", ""),
                         "url": f"https://{host}/en-US/{site}{path}", "text": "",
                         "posted": to_epoch(j.get("postedOn"))})
        if len(page) < 20:
            break
        offset += 20
    return jobs


def fetch_amazon(_slug: str) -> list[dict]:
    """Amazon's own jobs site (best-effort; unofficial endpoint)."""
    jobs = {}
    for query in ("software development engineer intern", "machine learning intern",
                  "data intern", "applied scientist intern"):
        r = http("GET", "https://www.amazon.jobs/en/search.json",
                 params={"base_query": query, "result_limit": 100, "sort": "recent"})
        r.raise_for_status()
        for j in r.json().get("jobs", []):
            jid = str(j.get("id_icims") or j.get("id"))
            jobs[jid] = {"id": jid, "title": j.get("title", ""),
                         "location": j.get("normalized_location") or j.get("location", ""),
                         "url": "https://www.amazon.jobs" + j.get("job_path", ""),
                         "text": strip_html(j.get("description_short", "") + " "
                                            + j.get("basic_qualifications", "")),
                         "posted": to_epoch(j.get("posted_date") or j.get("updated_time"))}
    return list(jobs.values())


def fetch_smartrecruiters(slug: str) -> list[dict]:
    """SmartRecruiters public Posting API. slug = company identifier (e.g. Visa)."""
    out, seen = [], set()
    for q in ("intern", "co-op"):
        offset = 0
        while offset < 500:
            r = http("GET", f"https://api.smartrecruiters.com/v1/companies/{slug}/postings",
                     params={"limit": 100, "offset": offset, "q": q})
            r.raise_for_status()
            content = r.json().get("content", [])
            for j in content:
                jid = str(j.get("id"))
                if jid in seen:
                    continue
                seen.add(jid)
                loc = j.get("location") or {}
                where = ", ".join(filter(None, [loc.get("city"), loc.get("region"), loc.get("country")]))
                if loc.get("remote"):
                    where += "; Remote" + (" US" if (loc.get("country") or "").lower() in ("us", "usa") else "")
                out.append({"id": jid, "title": j.get("name", ""), "location": where,
                            "url": f"https://jobs.smartrecruiters.com/{slug}/{jid}", "text": "",
                            "posted": to_epoch(j.get("releasedDate"))})
            if len(content) < 100:
                break
            offset += 100
    return out


def fetch_workable(slug: str) -> list[dict]:
    """Workable public job list. slug = account name (apply.workable.com/<slug>)."""
    r = http("GET", f"https://apply.workable.com/api/v1/widget/accounts/{slug}")
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        where = ", ".join(filter(None, [j.get("city"), j.get("state"), j.get("country")]))
        if j.get("telecommuting"):
            where += "; Remote"
        out.append({"id": j.get("shortcode") or j.get("url", ""), "title": j.get("title", ""),
                    "location": where, "text": "",
                    "url": j.get("url") or f"https://apply.workable.com/{slug}/j/{j.get('shortcode')}/",
                    "posted": to_epoch(j.get("published_on") or j.get("created_at"))})
    return out


def fetch_oracle(slug: str) -> list[dict]:
    """Oracle Recruiting Cloud. slug = host|site  e.g. jpmc.fa.oraclecloud.com|CX_1001"""
    host, site = slug.split("|")
    out, seen = [], set()
    for q in ("intern", "co-op"):
        offset = 0
        while offset < 200:
            finder = (f"findReqs;siteNumber={site},keyword={q},limit=25,offset={offset},"
                      f"sortBy=POSTING_DATES_DESC")
            r = http("GET", f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions",
                     params={"onlyData": "true", "expand": "requisitionList.secondaryLocations",
                             "finder": finder})
            r.raise_for_status()
            items = (r.json().get("items") or [{}])[0].get("requisitionList", []) or []
            for j in items:
                jid = str(j.get("Id"))
                if jid in seen:
                    continue
                seen.add(jid)
                locs = [j.get("PrimaryLocation") or ""]
                locs += [x.get("Name", "") for x in j.get("secondaryLocations") or [] if isinstance(x, dict)]
                out.append({"id": jid, "title": j.get("Title", ""), "location": "; ".join(filter(None, locs)),
                            "url": f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{jid}",
                            "text": "", "posted": to_epoch(j.get("PostedDate"))})
            if len(items) < 25:
                break
            offset += 25
    return out


FETCHERS = {"greenhouse": fetch_greenhouse, "lever": fetch_lever, "ashby": fetch_ashby,
            "workday": fetch_workday, "amazon": fetch_amazon,
            "smartrecruiters": fetch_smartrecruiters, "workable": fetch_workable,
            "oracle": fetch_oracle}


# ----------------------------------------------------------------------------
# Extra sources (configured in sources.json)
# Each returns posts with: id, title, company, location, url, text, mode, terms
#   mode "ats"  -> normal rules (title must say intern)
#   mode "list" -> curated internship list (every row is an internship already)
#   mode "hn"   -> free-text Hacker News comment
# ----------------------------------------------------------------------------
def _short_id(raw: str) -> str:
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:16]


def _github_list(cfg: dict) -> list[dict]:
    last_err = None
    for url in cfg.get("urls", []):
        try:
            r = http("GET", url, timeout=120)   # big file: allow extra time
            if r.status_code != 200:
                last_err = f"HTTP {r.status_code} for {url}"
                continue
            data = r.json()
            if not isinstance(data, list):
                last_err = f"unexpected format at {url}"
                continue
        except (requests.RequestException, ValueError) as e:
            last_err = str(e)
            continue
        posts = []
        for j in data:
            if j.get("active") is False or j.get("is_visible") is False:
                continue
            terms = j.get("terms") or ([j["season"]] if j.get("season") else [])
            locs = j.get("locations") or []
            posts.append({
                "id": str(j.get("id") or _short_id(j.get("url", "") + j.get("title", ""))),
                "title": j.get("title", ""),
                "company": j.get("company_name", "") or "Unknown company",
                "location": ", ".join(locs[:3]) if isinstance(locs, list) else str(locs),
                "url": j.get("url", ""),
                "text": "", "mode": "list", "terms": [str(t) for t in terms],
                "posted": to_epoch(j.get("date_posted")),
            })
        return posts
    raise RuntimeError(last_err or "no urls configured - update sources.json")


def src_adzuna(cfg: dict) -> list[dict]:
    app_id, app_key = os.getenv("ADZUNA_APP_ID"), os.getenv("ADZUNA_APP_KEY")
    if not (app_id and app_key):
        raise MissingKey("ADZUNA_APP_ID / ADZUNA_APP_KEY")
    posts = {}
    for q in cfg.get("queries", []):
        r = http("GET", f"https://api.adzuna.com/v1/api/jobs/{cfg.get('country', 'us')}/search/1",
                 params={"app_id": app_id, "app_key": app_key, "what": q, "results_per_page": 50,
                         "sort_by": "date", "max_days_old": 7})
        r.raise_for_status()
        for j in r.json().get("results", []):
            jid = str(j.get("id"))
            posts[jid] = {"id": jid, "title": j.get("title", ""),
                          "company": (j.get("company") or {}).get("display_name", "Unknown company"),
                          "location": (j.get("location") or {}).get("display_name", ""),
                          "url": j.get("redirect_url", ""), "text": strip_html(j.get("description", "")),
                          "mode": "ats", "terms": [], "posted": to_epoch(j.get("created"))}
    return list(posts.values())


def src_hackernews(cfg: dict) -> list[dict]:
    r = http("GET", "https://hn.algolia.com/api/v1/search_by_date",
             params={"tags": "story,author_whoishiring", "hitsPerPage": 10})
    r.raise_for_status()
    story = next((h for h in r.json().get("hits", [])
                  if (h.get("title") or "").lower().startswith("ask hn: who is hiring")), None)
    if not story:
        return []
    sid = story["objectID"]
    r = http("GET", "https://hn.algolia.com/api/v1/search_by_date",
             params={"tags": f"comment,story_{sid}", "query": "intern", "hitsPerPage": 200})
    r.raise_for_status()
    posts = []
    for c in r.json().get("hits", []):
        if str(c.get("parent_id")) != str(sid):          # top-level job posts only
            continue
        raw = c.get("comment_text") or ""
        first_line = strip_html(raw.split("<p>")[0])[:150]
        posts.append({"id": c["objectID"], "title": first_line,
                      "company": first_line.split("|")[0].strip()[:60] or "HN startup",
                      "location": "", "url": f"https://news.ycombinator.com/item?id={c['objectID']}",
                      "text": strip_html(raw), "mode": "hn", "terms": [],
                      "posted": to_epoch(c.get("created_at_i"))})
    return posts


def src_usajobs(cfg: dict) -> list[dict]:
    key, email = os.getenv("USAJOBS_API_KEY"), os.getenv("USAJOBS_EMAIL")
    if not (key and email):
        raise MissingKey("USAJOBS_API_KEY / USAJOBS_EMAIL")
    posts = {}
    for q in cfg.get("queries", []):
        r = http("GET", "https://data.usajobs.gov/api/search",
                 params={"Keyword": q, "ResultsPerPage": 100, "DatePosted": 14},
                 headers={"Host": "data.usajobs.gov", "User-Agent": email, "Authorization-Key": key})
        r.raise_for_status()
        for item in r.json().get("SearchResult", {}).get("SearchResultItems", []):
            d = item.get("MatchedObjectDescriptor", {})
            jid = str(item.get("MatchedObjectId") or d.get("PositionID"))
            summary = ((d.get("UserArea") or {}).get("Details") or {}).get("JobSummary", "")
            posts[jid] = {"id": jid, "title": d.get("PositionTitle", ""),
                          "company": d.get("OrganizationName", "US Government"),
                          "location": d.get("PositionLocationDisplay", ""),
                          "url": d.get("PositionURI", ""), "text": strip_html(summary),
                          "mode": "ats", "terms": [],
                          "posted": to_epoch(d.get("PublicationStartDate"))}
    return list(posts.values())


def src_serpapi(cfg: dict) -> list[dict]:
    key = os.getenv("SERPAPI_KEY")
    if not key:
        raise MissingKey("SERPAPI_KEY")
    posts = {}
    for q in cfg.get("queries", []):
        r = http("GET", "https://serpapi.com/search.json",
                 params={"engine": "google_jobs", "q": q, "hl": "en", "gl": "us", "api_key": key})
        r.raise_for_status()
        for j in r.json().get("jobs_results", []):
            jid = _short_id(j.get("job_id") or (j.get("title", "") + j.get("company_name", "")))
            apply = j.get("apply_options") or []
            posts[jid] = {"id": jid, "title": j.get("title", ""),
                          "company": j.get("company_name", "Unknown company"),
                          "location": j.get("location", ""),
                          "url": (apply[0].get("link") if apply else "") or j.get("share_link", ""),
                          "text": j.get("description", "") or "", "mode": "ats", "terms": [],
                          "posted": to_epoch((j.get("detected_extensions") or {}).get("posted_at"))}
    return list(posts.values())


# ----------------------------------------------------------------------------
# GitHub README internship lists (markdown or HTML tables)
# ----------------------------------------------------------------------------
URL_RE = re.compile(r'(?:href="|\]\()(\s*https?://[^")\s]+)')
SKIP_LINK = ("simplify.jobs/p/", "shields.io", "imgur.com", ".png", ".svg", ".gif", ".jpg",
             "github.com/", "githubusercontent", "i.imgur", "linkedin.com/company")
MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def _cell_text(cell: str) -> str:
    cell = re.sub(r"!\[[^\]]*\]\([^)]*\)", " ", cell)            # images
    cell = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", cell)        # [text](link) -> text
    cell = re.sub(r"<[^>]+>", " ", cell)                        # html tags
    cell = html.unescape(cell).replace("**", "").replace("__", "")
    return re.sub(r"\s+", " ", cell).strip()


def _list_date(text: str) -> float:
    t = (text or "").strip().lower()
    now = time.time()
    if not t:
        return 0.0
    if t in ("today", "new", "0d", "now") or re.fullmatch(r"\d+\s*(h|hr|hrs|hours?|m|mins?|minutes?)", t):
        return now
    m = re.fullmatch(r"(\d+)\s*(d|day|days)", t)
    if m:
        return now - int(m.group(1)) * 86400
    m = re.fullmatch(r"(\d+)\s*(mo|month|months)", t)
    if m:
        return now - int(m.group(1)) * 30 * 86400
    m = re.match(r"([a-z]{3})[a-z]*\.?\s+(\d{1,2})(?:,?\s*(\d{4}))?$", t)
    if m and m.group(1) in MONTHS:
        year = int(m.group(3)) if m.group(3) else datetime.now(timezone.utc).year
        ts = datetime(year, MONTHS[m.group(1)], int(m.group(2)), tzinfo=timezone.utc).timestamp()
        if not m.group(3) and ts > now + 2 * 86400:            # "Dec 30" seen in January
            ts = datetime(year - 1, MONTHS[m.group(1)], int(m.group(2)), tzinfo=timezone.utc).timestamp()
        return ts
    return to_epoch(text)


def parse_readme_tables(doc: str) -> list[dict]:
    """Pull job rows out of markdown/HTML tables, remembering the section heading above each."""
    rows = []
    # split into sections at headings, keeping the heading text
    parts = re.split(r"(?im)^(#{1,6}[^\n]*|\s*<h[1-6][^>]*>.*?</h[1-6]>|\s*<summary>.*?</summary>)\s*$", doc)
    heading = ""
    for part in parts:
        if re.match(r"(?is)\s*(#{1,6}|<h[1-6]|<summary>)", part or ""):
            heading = _cell_text(part.lstrip("# "))
            continue
        tables = []
        # markdown tables
        lines = (part or "").splitlines()
        i = 0
        while i < len(lines) - 1:
            if "|" in lines[i] and re.match(r"^\s*\|?\s*:?-{2,}", lines[i + 1]):
                header = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                body = []
                i += 2
                while i < len(lines) and "|" in lines[i]:
                    body.append([c for c in lines[i].strip().strip("|").split("|")])
                    i += 1
                tables.append((header, body))
            else:
                i += 1
        # html tables
        for table in re.findall(r"(?is)<table.*?</table>", part or ""):
            trs = re.findall(r"(?is)<tr.*?</tr>", table)
            if not trs:
                continue
            header = [_cell_text(c) for c in re.findall(r"(?is)<t[hd][^>]*>(.*?)</t[hd]>", trs[0])]
            body = [re.findall(r"(?is)<td[^>]*>(.*?)</td>", tr) for tr in trs[1:]]
            tables.append((header, [b for b in body if b]))
        for header, body in tables:
            h = [x.lower() for x in (_cell_text(c) for c in header)]
            col = lambda *names: next((k for k, name in enumerate(h) if any(n in name for n in names)), None)
            ci, ti = col("company", "employer"), col("role", "title", "position", "job")
            li, di = col("location"), col("date", "posted", "age")
            if ci is None or ti is None:
                continue
            last_company = ""
            for cells in body:
                if len(cells) <= max(ci, ti):
                    continue
                company = _cell_text(cells[ci])
                if company in ("↳", "") or company.startswith("↳"):
                    company = last_company
                last_company = company
                links = [u.strip() for u in URL_RE.findall(" ".join(cells))]
                good = [u for u in links if not any(s in u for s in SKIP_LINK)]
                title = _cell_text(cells[ti])
                if not company or not title or "🔒" in " ".join(cells):   # 🔒 = closed on these lists
                    continue
                rows.append({"company": company, "title": title,
                             "location": _cell_text(cells[li]) if li is not None and li < len(cells) else "",
                             "posted": _list_date(_cell_text(cells[di])) if di is not None and di < len(cells) else 0.0,
                             "url": (good or links or [""])[0], "section": heading})
    return rows


def src_readme_lists(cfg: dict) -> list[dict]:
    posts, worked, failed = [], 0, []
    for entry in cfg.get("repos", []):
        repo = entry["repo"] if isinstance(entry, dict) else entry
        files = (entry.get("files") if isinstance(entry, dict) else None) or ["README.md"]
        got = False
        for branch in ("main", "master", "dev"):
            texts = []
            for f in files:
                try:
                    r = http("GET", f"https://raw.githubusercontent.com/{repo}/{branch}/{f}", timeout=120)
                    if r is not None and r.status_code == 200:
                        texts.append(r.text)
                except requests.RequestException:
                    pass
            if not texts:
                continue
            got = True
            n = 0
            for row in (x for t in texts for x in parse_readme_tables(t)):
                sec = row["section"].lower()
                if re.search(r"new[\s-]?grad|full[\s-]?time|entry[\s-]?level", sec) \
                        and not re.search(r"intern|co-?op", sec):
                    continue                                   # skip new-grad sections
                n += 1
                posts.append({"id": _short_id(repo + row["company"] + row["title"] + row["url"]),
                              "title": row["title"], "company": row["company"],
                              "location": row["location"], "url": row["url"], "text": "",
                              "mode": "readme", "terms": [], "posted": row["posted"],
                              "intern_section": bool(re.search(r"intern|co-?op", sec))})
            log.info("  %s: %d listings", repo, n)
            break
        if got:
            worked += 1
        else:
            failed.append(repo)
    if failed:
        log.warning("GitHub lists not found (moved or renamed?): %s", ", ".join(failed))
    if not worked:
        raise RuntimeError("none of the GitHub lists could be downloaded")
    return posts


class MissingKey(Exception):
    """Raised when a source needs an API key that isn't set; the source is skipped quietly."""


SOURCES = {"simplify": _github_list, "vansh": _github_list, "readme_lists": src_readme_lists,
           "adzuna": src_adzuna,
           "hackernews": src_hackernews, "usajobs": src_usajobs, "serpapi": src_serpapi}


def load_sources() -> dict:
    if not SOURCES_FILE.exists():
        return {}
    cfg = json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    return {k: v for k, v in cfg.items() if k in SOURCES and isinstance(v, dict) and v.get("enabled")}


def classify_post(p: dict) -> str | None:
    mode = p.get("mode", "ats")
    if mode == "ats":
        return classify(p["title"], p["text"])
    if mode == "list":                                   # already a curated internship list
        if not role_matches(p["title"]):
            return None
        terms = " ".join(p.get("terms") or [])
        if terms and TARGET_YEAR not in terms and TARGET_YEAR not in p["title"]:
            return None                                  # e.g. an off-season "Fall 2026" row
        return "confirmed"
    if mode == "readme":
        t = normalize(p["title"])
        if not role_matches(p["title"]):
            return None
        if OTHER_YEAR_RE.search(p["title"]) and TARGET_YEAR not in p["title"]:
            return None
        if not (p.get("intern_section") or INTERN_RE.search(t)):
            return None                                        # not clearly an internship
        return "confirmed" if TARGET_YEAR in p["title"] else ("possible" if not STRICT_MODE else None)
    if mode == "hn" and re.search(r"full[\s-]?time", p["title"], re.I) \
            and not INTERN_RE.search(normalize(p["title"])):
        return None                                      # full-time post that only mentions interns in passing
    if mode == "hn":
        t = normalize(p["text"])
        if not INTERN_RE.search(t) or not (CORE_RE.search(t) or GENERAL_RE.search(t)):
            return None
        if TARGET_YEAR in p["text"]:
            return "confirmed"
        if OTHER_SEASON_RE.search(p["text"]):
            return None
        return None if STRICT_MODE else "possible"
    return None


def norm_url(u: str) -> str:
    u = (u or "").split("?")[0].split("#")[0].rstrip("/").lower()
    return re.sub(r"/(apply|application)$", "", u)


def title_key(company: str, title: str) -> str:
    """Same company + same job title = same job, even if different sites word it slightly differently."""
    c = (company or "").lower()
    c = re.sub(r"\(.*?\)", "", c)
    c = re.sub(r"\b(inc|llc|ltd|corp|corporation|co|company|the|group|holdings|technologies|"
               r"technology|labs|plc|gmbh)\b", "", c)
    t = (title or "").lower()
    t = re.sub(r"\binternships?\b", "intern", t)
    t = re.sub(r"\b(co-?op)s?\b", "coop", t)
    t = re.sub(r"\b(summer|fall|spring|winter|autumn)\b|\b20\d\d\b|\b(start|program)\b", "", t)
    return re.sub(r"[^a-z0-9]", "", c) + "|" + re.sub(r"[^a-z0-9]", "", t)


def canon_id(url: str) -> str:
    """The real job ID behind a link, so the same job linked from different sites matches."""
    low = (url or "").strip().lower()
    m = re.search(r"[?&]gh_jid=(\d+)", low) or re.search(r"greenhouse\.io/[^/]+/jobs/(\d+)", low)
    if m:
        return "gh:" + m.group(1)
    m = re.search(r"lever\.co/[^/]+/([0-9a-f-]{36})", low)
    if m:
        return "lv:" + m.group(1)
    m = re.search(r"ashbyhq\.com/[^/]+/([0-9a-f-]{36})", low)
    if m:
        return "ab:" + m.group(1)
    m = re.search(r"([a-z0-9-]+)\.wd\d+\.myworkdayjobs\.com/.*_([a-z0-9]+(?:-[a-z0-9]+)*?)(?:-\d)?/?(?:[?#]|$)", low)
    if m:
        return f"wd:{m.group(1)}:{m.group(2)}"
    m = re.search(r"smartrecruiters\.com/[^/]+/(\d{9,})", low)
    if m:
        return "sr:" + m.group(1)
    m = re.search(r"amazon\.jobs/(?:[a-z-]+/)?jobs/(\d+)", low)
    if m:
        return "az:" + m.group(1)
    m = re.search(r"workable\.com/(?:[^/]+/)?j/([a-z0-9]+)", low)
    if m:
        return "wk:" + m.group(1)
    return ""

def in_area(location: str) -> bool:
    return LOCATION_FILTER == "any" or location_ok(location, CITY_INDEX)


# ----------------------------------------------------------------------------
# Storage
# ----------------------------------------------------------------------------
def load_companies() -> list[dict]:
    with open(COMPANIES_FILE, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("name") and not r["name"].startswith("#")]
    seen_keys, out = set(), []
    for r in rows:
        ats = (r.get("ats") or "").strip().lower()
        key = (ats, (r.get("slug") or "").strip().lower())
        if ats in FETCHERS and key not in seen_keys:
            seen_keys.add(key)
            out.append({"name": r["name"].strip(), "ats": ats, "slug": r["slug"].strip(),
                        "tier": (r.get("tier") or "").strip().lower()})
    return out


def load_state() -> dict:
    if not SEEN_FILE.exists():
        return {"jobs": {}, "companies": [], "title_keys": {}}
    data = json.loads(SEEN_FILE.read_text(encoding="utf-8"))
    if "jobs" not in data:                               # old flat format
        data = {"jobs": data, "companies": sorted({k.split(":")[0] for k in data})}
    if "title_keys" not in data:                         # build from what we already have
        data["title_keys"] = sorted({title_key(v.get("company") or k.split(":")[0], v.get("title", ""))
                                     for k, v in data["jobs"].items() if not k.startswith("src:")})
    if isinstance(data["title_keys"], list):             # now stored as {title key: when seen}
        stamp = time.time()
        data["title_keys"] = {t: stamp for t in data["title_keys"]}
    return data


def save_state(state: dict) -> None:
    state["companies"] = sorted(set(state["companies"]))
    live = {v.get("url") for v in state["jobs"].values()}
    state["wd_locations"] = {u: l for u, l in WD_LOC_CACHE.items() if u in live or len(WD_LOC_CACHE) < 20000}
    cutoff = time.time() - TITLE_DEDUPE_DAYS * 86400     # forget old titles so reposts alert again
    state["title_keys"] = {t: ts for t, ts in state.get("title_keys", {}).items() if ts >= cutoff}
    SEEN_FILE.write_text(json.dumps(state, indent=0, sort_keys=True), encoding="utf-8")


# ----------------------------------------------------------------------------
# Notifications (ntfy.sh)
# ----------------------------------------------------------------------------
def _header_safe(s: str) -> str:
    return s.encode("latin-1", "ignore").decode("latin-1")[:200]


def notify(title: str, message: str, url: str = "", priority: str = "default",
           tags: str = "briefcase") -> bool:
    """Send a push notification. Returns True if ntfy accepted it."""
    if not NTFY_TOPIC:
        log.warning("NTFY_TOPIC not set - would have sent: %s | %s", title, message)
        return False
    headers = {"Title": _header_safe(title), "Priority": priority, "Tags": tags}
    if url:
        headers["Click"] = url
    for attempt in range(6):
        try:
            r = requests.post(f"{NTFY_SERVER}/{NTFY_TOPIC}", data=message.encode("utf-8"),
                              headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:                     # sending too fast - wait and retry
                time.sleep(6 * (attempt + 1))
                continue
            r.raise_for_status()
            return True
        except requests.RequestException as e:
            log.error("Notification failed: %s", e)
            return False
    log.error("Notification gave up after retries: %s", title)
    return False


# ----------------------------------------------------------------------------
# Core
# ----------------------------------------------------------------------------
def pick_batch(companies: list[dict]) -> tuple[list[dict], int]:
    """priority = every run, hot = every run if room (else rotated), rest = rotated shards."""
    slot = int(time.time() // (CHECK_INTERVAL_MIN * 60))
    crc = lambda c: zlib.crc32(c["name"].lower().encode())

    def rotate(group, cap):
        shards = max(1, math.ceil(len(group) / max(1, cap)))
        return [c for c in group if crc(c) % shards == slot % shards], shards

    # Workday limits how many requests it accepts from one server, so on GitHub we check
    # WORKDAY_PER_RUN Workday companies per run and rotate through the rest.
    workday = [c for c in companies if c["ats"] == "workday"] if WORKDAY_PER_RUN else []
    wd_batch, wd_shards = rotate(workday, WORKDAY_PER_RUN) if workday else ([], 1)
    others = [c for c in companies if not (WORKDAY_PER_RUN and c["ats"] == "workday")]
    priority = [c for c in others if c["tier"] == "priority"]
    hot = [c for c in others if c["tier"] == "hot"]
    rest = [c for c in others if c["tier"] not in ("priority", "hot")]
    priority, p_shards = rotate(priority, int(MAX_COMPANIES_PER_RUN * 0.6))
    room = MAX_COMPANIES_PER_RUN - len(priority)
    hot, h_shards = rotate(hot, int(room * 0.7))
    room = max(1, MAX_COMPANIES_PER_RUN - len(priority) - len(hot))
    batch, shards = rotate(rest, room)
    log.info("Tiers: priority every ~%d min, hot every ~%d min, others every ~%d min.",
             p_shards * CHECK_INTERVAL_MIN, h_shards * CHECK_INTERVAL_MIN, shards * CHECK_INTERVAL_MIN)
    if workday:
        log.info("Workday: %d of %d companies this run (each checked every ~%d min).",
                 len(wd_batch), len(workday), wd_shards * CHECK_INTERVAL_MIN)
    return priority + hot + batch + wd_batch, shards


WD_LOC_CACHE: dict = {}                                  # Workday "2 Locations" -> real cities
MULTI_LOC_RE = re.compile(r"^\s*\d+\s+locations?\s*$", re.I)


def workday_locations(slug: str, url: str) -> str:
    """Look up the real cities behind Workday's '3 Locations' label (cached)."""
    if url in WD_LOC_CACHE:
        return WD_LOC_CACHE[url]
    host, tenant, site = slug.split("|")
    path = url.split(f"/{site}", 1)[-1]
    locs = ""
    try:
        with WORKDAY_LIMIT:
            r = http("GET", f"https://{host}/wday/cxs/{tenant}/{site}{path}")
        if r is not None and r.status_code == 200:
            info = r.json().get("jobPostingInfo") or {}
            parts = [info.get("location") or ""] + list(info.get("additionalLocations") or [])
            locs = "; ".join(p for p in parts if p)
    except Exception:
        pass
    WD_LOC_CACHE[url] = locs
    return locs


def scan_company(company: dict) -> tuple[str, list[dict], str | None]:
    name = company["name"]
    try:
        postings = FETCHERS[company["ats"]](company["slug"])
    except Exception as e:                               # one bad company shouldn't stop the run
        return name, [], f"{type(e).__name__}: {e}"
    matches = []
    for p in postings:
        label = classify(p["title"], p["text"])
        if label and company["ats"] == "workday" and MULTI_LOC_RE.match(p.get("location") or ""):
            real = workday_locations(company["slug"], p["url"])
            if real:
                p["location"] = real                     # "2 Locations" -> "Seattle, WA; Austin, TX"
        if label and in_area(p.get("location", "")):
            matches.append({**p, "company": name, "label": label, "source": "career site",
                            "key": f"{name}:{p['id']}"})
    return name, matches, None


def scan_source(src: str, cfg: dict) -> tuple[str, list[dict], str | None]:
    unit = f"src:{src}"
    try:
        posts = SOURCES[src](cfg)
    except MissingKey as e:
        log.info("Source %s skipped (set %s to enable it).", src, e)
        return unit, [], "skip"
    except Exception as e:
        return unit, [], f"{type(e).__name__}: {e}"
    matches = []
    for p in posts:
        label = classify_post(p)
        if label and in_area(p.get("location", "")):
            matches.append({**p, "label": label, "source": cfg.get("label", src),
                            "key": f"{unit}:{p['id']}"})
    return unit, matches, None


def run_once() -> None:
    companies = load_companies()
    batch, shards = pick_batch(companies)
    slot = int(time.time() // (CHECK_INTERVAL_MIN * 60))
    sources_due = {k: v for k, v in load_sources().items()
                   if slot % max(1, int(v.get("every_n_runs", 1))) == 0}
    state = load_state()
    WD_LOC_CACHE.update(state.get("wd_locations", {}))
    known = set(state["companies"])
    seen_urls = {norm_url(v.get("url")) for v in state["jobs"].values() if v.get("url")}
    seen_urls |= {canon_id(v.get("url")) for v in state["jobs"].values() if canon_id(v.get("url"))}
    title_times = state.get("title_keys", {})
    seen_titles = {t for t, ts in title_times.items() if ts >= time.time() - TITLE_DEDUPE_DAYS * 86400}
    log.info("%d companies total -> scanning %d this run. Extra sources this run: %s",
             len(companies), len(batch), ", ".join(sources_due) or "none")

    new, errors, baselined, total, stale = [], [], 0, 0, 0
    run_start = time.time()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        futs = [pool.submit(scan_company, c) for c in batch]
        futs += [pool.submit(scan_source, k, v) for k, v in sources_due.items()]
        for fut in as_completed(futs):
            unit, matches, err = fut.result()
            if err == "skip":
                continue
            if err:
                errors.append(f"{unit}: {err}")
                continue
            total += len(matches)
            first_time = unit not in known
            for m in matches:
                if m["key"] in state["jobs"]:
                    continue
                state["jobs"][m["key"]] = {"title": m["title"], "url": m["url"], "first_seen": now,
                                           "company": m["company"], "posted": m.get("posted") or 0}
                u, cid = norm_url(m["url"]), canon_id(m["url"])
                tk = title_key(m["company"], m["title"])
                duplicate = (u and u in seen_urls) or (cid and cid in seen_urls) or tk in seen_titles
                seen_urls |= {x for x in (u, cid) if x}
                if duplicate:
                    continue                             # same job already found by another source

                def remember_title():
                    seen_titles.add(tk)
                    title_times[tk] = time.time()

                if first_time:
                    baselined += 1                       # already open before we started watching
                    remember_title()
                    log.info("EXISTING [%s] %s - %s (%s) %s", m["label"], m["company"],
                             m["title"], m["source"], m["url"])
                    continue
                posted = m.get("posted") or 0
                m["age_days"] = max(0.0, (run_start - posted) / 86400) if posted else None
                if m["age_days"] is not None and m["age_days"] > MAX_AGE_DAYS:
                    stale += 1                           # new to us, but posted too long ago
                    log.info("OLD (%s, not alerted) %s - %s", age_text(m["age_days"]),
                             m["company"], m["title"])
                    continue                             # old posts don't block newer same-title ones
                remember_title()
                m["tk"] = tk
                new.append(m)
            state["companies"].append(unit)

    for m in new:
        log.info("NEW [%s] %s - %s (%s, via %s, %s) %s", m["label"], m["company"], m["title"],
                 m["location"], m["source"], age_text(m.get("age_days")), m["url"])
    if errors:
        kinds = {}
        for e in errors:
            kinds.setdefault(e.split(": ", 1)[-1].split(":")[0][:60], []).append(e.split(": ")[0])
        for kind, names in sorted(kinds.items(), key=lambda kv: -len(kv[1])):
            log.warning("%d skipped (%s), e.g. %s", len(names), kind, ", ".join(names[:5]))

    if not known and baselined:
        notify("Intern tracker is live",
               f"Watching {len(companies)} companies + {len(load_sources())} extra sources. "
               f"{baselined} matching roles were already open (see the run log). "
               f"You'll be alerted for anything new.", tags="rocket")
    if new:
        ordered = sorted(new, key=lambda m: (m["label"] != "confirmed", m["company"]))
        failed = []
        for m in ordered[:MAX_INDIVIDUAL_ALERTS]:
            tag = "2027" if m["label"] == "confirmed" else "year not stated"
            ok = notify(f"{m['company']}: {m['title']}"[:180],
                   f"{m['location'] or 'Location N/A'}  ({tag})\n"
                   f"{age_text(m.get('age_days')).capitalize()} - via {m['source']}. Tap to open.",
                   url=m["url"], priority="high" if m["label"] == "confirmed" else "default")
            if not ok:
                failed.append(m)
        for m in failed:                                 # not delivered -> try again next run
            state["jobs"].pop(m["key"], None)
            title_times.pop(m.get("tk"), None)
        if failed:
            log.error("%d alert(s) could not be sent - they will be retried next run.", len(failed))
        if len(new) > MAX_INDIVIDUAL_ALERTS:
            rest = ordered[MAX_INDIVIDUAL_ALERTS:]
            notify(f"+{len(rest)} more new intern roles",
                   ", ".join(sorted({m["company"] for m in rest}))[:500])

    state["title_keys"] = title_times
    save_state(state)
    log.info("Done: %d matches in batch, %d new (alerted), %d older than %g days (not alerted), "
             "%d baselined, %d failed.", total, len(new), stale, MAX_AGE_DAYS, baselined, len(errors))


def check_sources() -> None:
    cfg = load_sources()
    if not cfg:
        print("No enabled sources in sources.json.")
    for k, v in cfg.items():
        unit, matches, err = scan_source(k, v)
        if err == "skip":
            print(f"  SKIP {k:<12} (API key not set)")
        elif err:
            print(f"  FAIL {k:<12} {err}")
        else:
            print(f"  OK   {k:<12} {len(matches)} matching internships right now")


def check_companies() -> None:
    ok, bad = 0, []
    companies = load_companies()
    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        futs = {pool.submit(FETCHERS[c["ats"]], c["slug"]): c for c in companies}
        for fut in as_completed(futs):
            c = futs[fut]
            try:
                n = len(fut.result())
                ok += 1
                print(f"  OK   {c['name']:<28} {c['ats']:<11} {n} postings")
            except Exception as e:
                bad.append(c["name"])
                print(f"  FAIL {c['name']:<28} {c['ats']:<11} {type(e).__name__}: {e}")
    print(f"\n{ok} working, {len(bad)} failing: {', '.join(bad[:100]) or 'none'}")


def replay(n: int) -> None:
    """Forget the n most recently recorded jobs so the next run alerts on them (for testing)."""
    state = load_state()
    jobs = state["jobs"]
    cutoff = time.time() - MAX_AGE_DAYS * 86400
    fresh = lambda v: 1 if (v.get("posted") or 0) >= cutoff else 0      # prefer recent postings
    newest = sorted(jobs.items(), key=lambda kv: (fresh(kv[1]), kv[1].get("posted") or 0,
                                                  kv[1].get("first_seen", "")), reverse=True)
    picked = []
    for k, v in newest:
        if v.get("url") and v.get("title") and k.split(":")[0] != "src":
            picked.append((k, v))
        if len(picked) >= n:
            break
    if not picked:
        print("Nothing to replay yet - run the tracker once first.")
        return
    urls = {norm_url(v["url"]) for _, v in picked}
    titles = {re.sub(r"[^a-z0-9]", "", re.sub(r"\binternships?\b", "intern", v["title"].lower()))
              for _, v in picked}
    removed = [k for k, v in jobs.items()
               if norm_url(v.get("url")) in urls
               or re.sub(r"[^a-z0-9]", "", re.sub(r"\binternships?\b", "intern", v.get("title", "").lower())) in titles]
    for k in removed:
        jobs.pop(k, None)
    state["title_keys"] = {t: ts for t, ts in state.get("title_keys", {}).items()
                           if t.split("|", 1)[-1] not in titles}
    save_state(state)
    print(f"Forgot {len(picked)} recent job(s):")
    for k, v in picked:
        print(f"  - {k.split(':')[0]}: {v['title']}")
    print("Now run:  python3 job_tracker.py")
    print("You should get an alert for each one that was posted in the last "
          f"{MAX_AGE_DAYS:g} days and is in your cities.")


def recent(days: float, send: bool, skip: float = 0) -> None:
    """List (and optionally send) every matching job posted in the last `days` days."""
    companies = load_companies()
    sources = load_sources()
    cutoff = time.time() - days * 86400
    newest = time.time() - skip * 86400                  # --skip: leave out the most recent days
    window = f"between {skip:g} and {days:g} days ago" if skip else f"in the last {days:g} day(s)"
    print(f"Checking all {len(companies)} companies + {len(sources)} extra sources "
          f"for jobs posted {window}... (takes a few minutes)")
    found, seen_urls, seen_titles = [], set(), set()
    if SEEN_FILE.exists():
        WD_LOC_CACHE.update(load_state().get("wd_locations", {}))
    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        futs = [pool.submit(scan_company, c) for c in companies]
        futs += [pool.submit(scan_source, k, v) for k, v in sources.items()]
        for fut in as_completed(futs):
            _, matches, err = fut.result()
            if err:
                continue
            for m in matches:
                if not m.get("posted") or m["posted"] < cutoff or m["posted"] > newest:
                    continue
                u, cid, tk = norm_url(m["url"]), canon_id(m["url"]), title_key(m["company"], m["title"])
                if (u and u in seen_urls) or (cid and cid in seen_urls) or tk in seen_titles:
                    continue
                seen_urls |= {x for x in (u, cid) if x}
                seen_titles.add(tk)
                m["age_days"] = max(0.0, (time.time() - m["posted"]) / 86400)
                found.append(m)

    found.sort(key=lambda m: -m["posted"])
    out = BASE_DIR / "recent_jobs.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["posted", "company", "title", "location", "label", "source", "url"])
        for m in found:
            w.writerow([age_text(m["age_days"]), m["company"], m["title"], m["location"],
                        m["label"], m["source"], m["url"]])
    print(f"\n{len(found)} matching job(s) posted {window}:\n")
    for m in found:
        print(f"  [{age_text(m['age_days'])}] {m['company']} - {m['title']} ({m['location'] or 'N/A'})")
        print(f"      {m['url']}")
    print(f"\nSaved to {out.name} (opens in Excel/Numbers).")

    if send and found:
        notify(f"{len(found)} jobs posted {window}",
               "Sending each one now - tap any notification to open that job.", tags="inbox_tray")
        print(f"Sending {len(found)} notifications (one per job, spaced out so none get dropped)...")
        for i, m in enumerate(found, 1):
            tag = "2027" if m["label"] == "confirmed" else "year not stated"
            notify(f"{m['company']}: {m['title']}"[:180],
                   f"{m['location'] or 'Location N/A'}  ({tag})\n"
                   f"{age_text(m['age_days']).capitalize()} - via {m['source']}. Tap to open.",
                   url=m["url"], priority="high" if m["label"] == "confirmed" else "default")
            time.sleep(1 if i < 50 else 5)                # ntfy allows short bursts, then ~1 per 5s
            if i % 10 == 0:
                print(f"  {i}/{len(found)} sent")
        print("Done - all jobs sent to your phone.")

def main() -> None:
    ap = argparse.ArgumentParser(description="2027 internship alert tracker")
    ap.add_argument("--loop", action="store_true", help=f"run every {CHECK_INTERVAL_MIN} minutes")
    ap.add_argument("--check", action="store_true", help="validate companies.csv")
    ap.add_argument("--check-sources", action="store_true", help="test sources.json")
    ap.add_argument("--recent", type=float, metavar="DAYS",
                    help="list every matching job posted in the last DAYS days (1 = today)")
    ap.add_argument("--send", action="store_true", help="with --recent: also send them to your phone")
    ap.add_argument("--skip", type=float, default=0, metavar="DAYS",
                    help="with --recent: leave out jobs from the most recent DAYS days")
    ap.add_argument("--replay", type=int, metavar="N",
                    help="testing: forget the N newest jobs so the next run alerts on them")
    ap.add_argument("--test-notify", action="store_true", help="send a test phone notification")
    args = ap.parse_args()

    if args.recent:
        recent(args.recent, args.send, args.skip)
    elif args.replay:
        replay(args.replay)
    elif args.test_notify:
        notify("Test alert", "If you see this, your phone notifications work!", tags="tada")
    elif args.check:
        check_companies()
    elif args.check_sources:
        check_sources()
    elif args.loop:
        while True:
            try:
                run_once()
            except Exception:
                log.exception("Run failed; will retry next cycle")
            log.info("Sleeping %d minutes...", CHECK_INTERVAL_MIN)
            time.sleep(CHECK_INTERVAL_MIN * 60)
    else:
        run_once()


if __name__ == "__main__":
    main()
