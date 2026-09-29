#!/usr/bin/env python3
"""
Scan every public job board we can find, and list each company that posted
SWE / SDE / ML / AI / DS roles in top US tech hubs in the last N days.

Step 1 - harvest (run once, ~10-30 min):
    python3 scan_job_boards.py --harvest
  Finds Greenhouse, Ashby and Workday boards in the free Common Crawl index,
  plus Lever/other boards already known from your companies.csv and the
  internship-history files. Saves them to board_list.csv.

Step 2 - scan (~15-60 min depending on board count):
    python3 scan_job_boards.py --scan
  Reads each board's public job API, keeps postings that match keywords.json,
  are in the hubs from recent_hiring_companies.py, and were posted in the last
  90 days. Also adds companies from the Simplify/CSCareers lists, Hacker News and
  Adzuna (catches custom career sites like Google). Groups every company by its most
  recent post: last 7 days / 8-30 / 31-60 / 61-90 days. Saves:
     hiring_companies_90d.csv  (spreadsheet)
     hiring_report.md          (readable list by time window and city)

Optional:
    python3 scan_job_boards.py --scan --days 60 --hubs "SF Bay Area,Seattle,Texas"
    python3 scan_job_boards.py --scan --skip-workday     # much faster
    python3 scan_job_boards.py --add                     # add results to your tracker

Uses only public job-board APIs that companies publish for anyone to read.
Requests are rate-limited per platform to stay polite.
"""

import argparse
import csv
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import job_tracker as jt
from build_company_list import board_from_url
from recent_hiring_companies import HUBS, build_city_index, hubs_in, norm_company
from hubs import us_ok

BASE_DIR = Path(__file__).resolve().parent
BOARD_FILE = BASE_DIR / "board_list.csv"
OUT_FILE = BASE_DIR / "hiring_companies_90d.csv"
REPORT_FILE = BASE_DIR / "hiring_report.md"
COMPANIES_FILE = BASE_DIR / "companies.csv"
KNOWN_FILES = ["companies.csv", "history_companies.csv", "discovered_companies.csv"]

CC_COLLECTIONS = "https://index.commoncrawl.org/collinfo.json"
CC_PATTERNS = [                      # (label, url pattern)
    ("greenhouse", "boards.greenhouse.io/*"),
    ("greenhouse", "job-boards.greenhouse.io/*"),
    ("ashby", "jobs.ashbyhq.com/*"),
    ("workday", "*.myworkdayjobs.com"),
]
# polite concurrency per platform
LIMITS = {"greenhouse": threading.Semaphore(20), "lever": threading.Semaphore(15),
          "ashby": threading.Semaphore(5), "workday": threading.Semaphore(15)}
DEFAULT_LIMIT = threading.Semaphore(10)


# ---------------------------------------------------------------------------
# Step 1: harvest boards
# ---------------------------------------------------------------------------
def cc_get(url, params, tries=6):
    for i in range(tries):
        try:
            r = jt.http("GET", url, params=params, timeout=120)
            if r is not None and r.status_code == 200:
                return r
            if r is not None and r.status_code == 404:
                return None
        except Exception:
            pass
        time.sleep(5 * (i + 1))                          # Common Crawl asks for patience
    return None


def harvest(crawls: int, max_pages: int | None):
    boards = {}                                          # (ats, slug) -> source
    info = cc_get(CC_COLLECTIONS, {})
    if info is None:
        print("  ! Couldn't reach Common Crawl - using only your known boards.")
        collections = []
    else:
        collections = info.json()[:crawls]
    for coll in collections:
        api = coll["cdx-api"]
        print(f"\nCommon Crawl snapshot {coll['id']}")
        for ats, pattern in CC_PATTERNS:
            meta = cc_get(api, {"url": pattern, "output": "json", "showNumPages": "true"})
            try:
                pages = int(meta.json()["pages"]) if meta is not None else 0
            except Exception:
                pages = 0
            if max_pages:
                pages = min(pages, max_pages)
            before = len(boards)
            for page in range(pages):
                r = cc_get(api, {"url": pattern, "output": "json", "fl": "url", "page": page})
                if r is None:
                    continue
                for line in r.text.splitlines():
                    m = re.search(r'"url":\s*"([^"]+)"', line)
                    board = board_from_url(m.group(1)) if m else None
                    if board and board[0] == ats:
                        boards.setdefault(board, "common crawl")
                print(f"  {pattern:<30} page {page + 1}/{pages}  ->  {len(boards) - before:,} new boards",
                      end="\r")
                time.sleep(1)                            # be gentle with the free index
            print(f"  {pattern:<30} {pages} pages  ->  {len(boards) - before:,} new boards        ")

    # boards you already know about (includes Lever, which Common Crawl can't see)
    for name in KNOWN_FILES:
        path = BASE_DIR / name
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                ats, slug = (r.get("ats") or "").strip().lower(), (r.get("slug") or "").strip()
                if ats in ("greenhouse", "lever", "ashby", "workday", "smartrecruiters",
                           "workable", "oracle") and slug:
                    key = (ats, slug if ats in ("workday", "oracle", "smartrecruiters") else slug.lower())
                    boards.setdefault(key, name)
    with open(BOARD_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ats", "slug", "source"])
        for (ats, slug), src in sorted(boards.items()):
            w.writerow([ats, slug, src])
    counts = defaultdict(int)
    for ats, _ in boards:
        counts[ats] += 1
    print(f"\nSaved {len(boards):,} boards to {BOARD_FILE.name}: "
          + ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())))


# ---------------------------------------------------------------------------
# Step 2: read boards
# ---------------------------------------------------------------------------
def ts(value) -> float:
    """Parse ISO strings or epoch (s or ms) into epoch seconds; 0 if unknown."""
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return value / 1000 if value > 1e11 else float(value)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def workday_age_days(text: str):
    t = (text or "").lower()
    if "today" in t:
        return 0
    if "yesterday" in t:
        return 1
    m = re.search(r"(\d+)\+?\s*days?", t)
    if m:
        return int(m.group(1)) if "+" not in t else None      # "30+ days" = unknown exact age
    return None


def read_board(ats: str, slug: str) -> tuple[str, list[dict]]:
    """Return (company_name, postings) - postings have title, location, date, url."""
    if ats not in LIMITS:                                   # smartrecruiters, workable, oracle
        with DEFAULT_LIMIT:
            posts = jt.FETCHERS[ats](slug)
        return "", [{"title": p["title"], "location": p["location"], "date": p.get("posted") or 0,
                     "url": p["url"]} for p in posts]
    with LIMITS[ats]:
        if ats == "greenhouse":
            r = jt.http("GET", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            if r is None or r.status_code != 200:
                return "", []
            jobs = r.json().get("jobs", [])
            name = next((j.get("company_name") for j in jobs if j.get("company_name")), "")
            return name, [{"title": j.get("title", ""),
                           "location": (j.get("location") or {}).get("name", ""),
                           "date": ts(j.get("first_published") or j.get("updated_at")),
                           "url": j.get("absolute_url", "")} for j in jobs]
        if ats == "lever":
            r = jt.http("GET", f"https://api.lever.co/v0/postings/{slug}", params={"mode": "json"})
            if r is None or r.status_code != 200 or not isinstance(r.json(), list):
                return "", []
            out = []
            for j in r.json():
                cat = j.get("categories") or {}
                locs = [cat.get("location") or ""] + list(cat.get("allLocations") or [])
                out.append({"title": j.get("text", ""), "location": "; ".join(locs),
                            "date": ts(j.get("createdAt")), "url": j.get("hostedUrl", "")})
            return "", out
        if ats == "ashby":
            r = jt.http("GET", f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
            if r is None or r.status_code != 200:
                return "", []
            out = []
            for j in r.json().get("jobs", []):
                locs = [j.get("location") or ""]
                locs += [s.get("location", "") for s in j.get("secondaryLocations") or [] if isinstance(s, dict)]
                addr = ((j.get("address") or {}).get("postalAddress") or {})
                locs.append(", ".join(filter(None, [addr.get("addressLocality"), addr.get("addressRegion")])))
                out.append({"title": j.get("title", ""), "location": "; ".join(filter(None, locs)),
                            "date": ts(j.get("publishedAt") or j.get("updatedAt")),
                            "url": j.get("jobUrl", "")})
            return "", out
        if ats == "workday":
            host, tenant, site = slug.split("|")
            seen, out = set(), []
            for q in ("intern", "software engineer", "machine learning", "data"):
                for offset in (0, 20):
                    r = jt.http("POST", f"https://{host}/wday/cxs/{tenant}/{site}/jobs",
                                json={"appliedFacets": {}, "limit": 20, "offset": offset, "searchText": q})
                    if r is None or r.status_code != 200:
                        return "", out
                    page = r.json().get("jobPostings", [])
                    for j in page:
                        path = j.get("externalPath", "")
                        if path in seen:
                            continue
                        seen.add(path)
                        age = workday_age_days(j.get("postedOn", ""))
                        out.append({"title": j.get("title", ""), "location": j.get("locationsText", ""),
                                    "date": time.time() - age * 86400 if age is not None else -1,
                                    "url": f"https://{host}/en-US/{site}{path}"})
                    if len(page) < 20:
                        break
            return tenant, out
    return "", []


def workday_detail_date(slug: str, url: str) -> float:
    """Look up the exact posting date for a Workday job ('30+ days ago' in the list view)."""
    host, tenant, site = slug.split("|")
    path = url.split(f"/{site}", 1)[-1]
    with LIMITS["workday"]:
        try:
            r = jt.http("GET", f"https://{host}/wday/cxs/{tenant}/{site}{path}")
            if r is not None and r.status_code == 200:
                return ts((r.json().get("jobPostingInfo") or {}).get("startDate"))
        except Exception:
            pass
    return 0.0


def process_board(ats, slug, cutoff, city_index, include_undated):
    """Read one board and keep matching, in-hub, recent postings."""
    try:
        name, posts = read_board(ats, slug)
    except Exception:
        return "", []
    kept = []
    for p in posts:
        if not jt.role_matches(p["title"]):
            continue
        found = hubs_in(p["location"], city_index, False) or \
            ({"Other US"} if jt.LOCATION_FILTER == "us" and p["location"] and us_ok(p["location"], city_index) else set())
        if not found:
            continue
        date = p["date"]
        if date == -1 and ats == "workday":
            date = workday_detail_date(slug, p["url"]) or -1
        if date in (-1, 0, 0.0, None):
            if not include_undated:
                continue
            date = None
        elif date < cutoff:
            continue
        kept.append({"title": p["title"], "hubs": found, "date": date, "url": p["url"],
                     "intern": bool(jt.INTERN_RE.search(jt.normalize(p["title"])))})
    return name, kept


BUCKETS = [(7, "Last 7 days"), (30, "8-30 days ago"), (60, "31-60 days ago"), (90, "61-90 days ago")]


def bucket_for(days_ago):
    if days_ago is None:
        return "Date unknown (still open)"
    for limit, label in BUCKETS:
        if days_ago <= limit:
            return label
    return f"{BUCKETS[-1][0]}+ days ago"


def scan(days: int, hubs: dict, skip_workday: bool, include_undated: bool, extra_sources: bool):
    if not BOARD_FILE.exists():
        raise SystemExit("Run python3 scan_job_boards.py --harvest first.")
    with open(BOARD_FILE, newline="", encoding="utf-8") as f:
        boards = [(r["ats"], r["slug"]) for r in csv.DictReader(f)
                  if not (skip_workday and r["ats"] == "workday")]
    now = time.time()
    cutoff = now - days * 86400
    city_index = build_city_index(hubs)
    companies = {}

    def record(name, ats, slug, post, source):
        key = norm_company(name) or slug
        c = companies.setdefault(key, {"name": name, "ats": ats, "slug": slug, "hubs": set(),
                                       "dates": [], "undated": 0, "interns": 0, "titles": [],
                                       "sources": set(), "urls": set()})
        if post["url"] and post["url"] in c["urls"]:
            return
        c["urls"].add(post["url"])
        if not c["ats"] and ats:
            c["ats"], c["slug"] = ats, slug
        c["hubs"] |= post["hubs"]
        c["sources"].add(source)
        c["interns"] += post["intern"]
        if post["date"]:
            c["dates"].append(post["date"])
        else:
            c["undated"] += 1
        if len(c["titles"]) < 3 and post["title"][:90] not in c["titles"]:
            c["titles"].append(post["title"][:90])

    print(f"Scanning {len(boards):,} job boards for SWE/ML/AI/DS postings in {len(hubs)} hubs, "
          f"last {days} days...")
    done, start = 0, time.time()
    with ThreadPoolExecutor(60) as pool:
        futs = {pool.submit(process_board, ats, slug, cutoff, city_index, include_undated): (ats, slug)
                for ats, slug in boards}
        for fut in as_completed(futs):
            ats, slug = futs[fut]
            done += 1
            name, kept = fut.result()
            pretty = name or slug.split("|")[0].split(".")[0].replace("-", " ")
            if pretty.islower():
                pretty = pretty.title()
            for p in kept:
                record(pretty, ats, slug, p, "job board")
            if done % 250 == 0 or done == len(boards):
                rate = done / max(1, time.time() - start)
                print(f"  {done:,}/{len(boards):,} boards read, {len(companies):,} companies so far, "
                      f"~{(len(boards) - done) / max(rate, 0.1) / 60:.0f} min left")

    if extra_sources:
        print("Adding companies from the Simplify/CSCareers lists, Hacker News and Adzuna...")
        from recent_hiring_companies import src_adzuna, src_hackernews, src_lists
        for stream, label in ((src_lists(cutoff), "Simplify/CSCareers"),
                              (src_hackernews(cutoff), "Hacker News"),
                              (src_adzuna(cutoff, hubs, days), "Adzuna")):
            try:
                for p in stream:
                    if not p.get("date") or p["date"] < cutoff or not p.get("company"):
                        continue
                    if label == "Hacker News":
                        t = jt.normalize(p.get("full_text", ""))
                        if not (jt.CORE_RE.search(t) or jt.GENERAL_RE.search(jt.normalize(p["title"]))):
                            continue
                        is_intern = bool(jt.INTERN_RE.search(t))
                    else:
                        if not jt.role_matches(p["title"]):
                            continue
                        is_intern = bool(jt.INTERN_RE.search(jt.normalize(p["title"])))
                    found = hubs_in(p["location"], city_index, False) or \
            ({"Other US"} if jt.LOCATION_FILTER == "us" and p["location"] and us_ok(p["location"], city_index) else set())
                    if not found:
                        continue
                    board = board_from_url(p["url"]) or ("", "")
                    record(p["company"].strip(), board[0], board[1],
                           {"title": p["title"], "hubs": found, "date": p["date"],
                            "url": p["url"], "intern": is_intern}, label)
            except Exception as e:
                print(f"  ! {label} failed part-way: {type(e).__name__}: {e}")

    # ---- build rows with recency buckets ----
    rows = []
    for c in companies.values():
        latest = max(c["dates"]) if c["dates"] else None
        days_ago = int((now - latest) // 86400) if latest else None
        within = lambda n: sum(1 for d in c["dates"] if now - d <= n * 86400)
        rows.append({"bucket": bucket_for(days_ago), "company": c["name"],
                     "hubs": "; ".join(sorted(c["hubs"])),
                     "latest_post": datetime.fromtimestamp(latest, timezone.utc).strftime("%Y-%m-%d") if latest else "",
                     "days_ago": days_ago if days_ago is not None else "",
                     "roles_last_7d": within(7), "roles_last_30d": within(30),
                     "roles_last_60d": within(60), "roles_last_90d": within(90) + c["undated"],
                     "intern_roles": c["interns"], "example_titles": " | ".join(c["titles"]),
                     "sources": "; ".join(sorted(c["sources"])), "ats": c["ats"], "slug": c["slug"],
                     "_order": days_ago if days_ago is not None else 10_000})
    order = {label: i for i, (_, label) in enumerate(BUCKETS)}
    rows.sort(key=lambda r: (order.get(r["bucket"], 99), -r["roles_last_90d"], r["company"].lower()))

    fields = ["bucket", "company", "hubs", "latest_post", "days_ago", "roles_last_7d", "roles_last_30d",
              "roles_last_60d", "roles_last_90d", "intern_roles", "example_titles", "sources", "ats", "slug"]
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    write_report(rows, hubs, days)

    print(f"\n{len(rows):,} companies posted matching roles in the last {days} days")
    for _, label in BUCKETS + [(0, "Date unknown (still open)")]:
        n = sum(1 for r in rows if r["bucket"] == label)
        if n:
            print(f"  {label:<28} {n:,}")
    print(f"\nSpreadsheet: {OUT_FILE.name}\nReadable report: {REPORT_FILE.name}")


def write_report(rows, hubs, days):
    hubs = list(hubs) + ["Other US"]                      # jobs outside the 22 hubs but in the US
    lines = [f"# Companies hiring SWE / ML / AI / DS roles - last {days} days",
             "", f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}. "
             "Each company appears once, in the window of its most recent matching post.", ""]
    # summary table
    labels = [label for _, label in BUCKETS] + ["Date unknown (still open)"]
    lines += ["| Hub | " + " | ".join(labels) + " | Total |",
              "|---|" + "---|" * (len(labels) + 1)]
    for hub in hubs:
        counts = [sum(1 for r in rows if r["bucket"] == l and hub in r["hubs"].split("; ")) for l in labels]
        if sum(counts):
            lines.append(f"| {hub} | " + " | ".join(map(str, counts)) + f" | {sum(counts)} |")
    lines.append("")
    for label in labels:
        in_bucket = [r for r in rows if r["bucket"] == label]
        if not in_bucket:
            continue
        n = len(in_bucket)
        lines += [f"## {label} ({n} {'company' if n == 1 else 'companies'})", ""]
        for hub in hubs:
            here = [r for r in in_bucket if hub in r["hubs"].split("; ")]
            if not here:
                continue
            lines.append(f"### {hub} ({len(here)})")
            lines += [f"- **{r['company']}** - {r['roles_last_90d']} role(s)"
                      + (f", {r['intern_roles']} intern" if r["intern_roles"] else "")
                      + (f", latest {r['latest_post']}" if r["latest_post"] else "")
                      + f" - _{r['example_titles'].split(' | ')[0]}_" for r in here]
            lines.append("")
    REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")


def add_to_tracker(interns_only: bool):
    if not OUT_FILE.exists():
        raise SystemExit("Run --scan first.")
    with open(COMPANIES_FILE, newline="", encoding="utf-8") as f:
        existing = list(csv.DictReader(f))
    names = {norm_company(r["name"]) for r in existing}
    boards = {(r["ats"].lower(), r["slug"].lower()) for r in existing}
    added = no_board = 0
    with open(OUT_FILE, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            interns = int(r["intern_roles"] or 0)
            if interns_only and interns == 0:
                continue
            if not r["ats"]:
                no_board += 1                            # custom career site - Simplify covers it
                continue
            key = (r["ats"].lower(), r["slug"].lower())
            if key in boards or norm_company(r["company"]) in names:
                continue
            existing.append({"name": r["company"], "ats": r["ats"], "slug": r["slug"],
                             "tier": "hot" if interns else ""})
            boards.add(key)
            names.add(norm_company(r["company"]))
            added += 1
    with open(COMPANIES_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "ats", "slug", "tier"], extrasaction="ignore")
        w.writeheader()
        w.writerows(existing)
    print(f"Added {added:,} companies. companies.csv now has {len(existing):,}.")
    if no_board:
        print(f"({no_board:,} use custom career sites - the Simplify source already watches them.)")


def main():
    ap = argparse.ArgumentParser(description="Harvest and scan public job boards")
    ap.add_argument("--harvest", action="store_true", help="step 1: find boards")
    ap.add_argument("--scan", action="store_true", help="step 2: read boards")
    ap.add_argument("--add", action="store_true", help="add results to companies.csv")
    ap.add_argument("--add-all", action="store_true",
                    help="with --add: include companies with no intern roles too")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--hubs", help='e.g. "SF Bay Area,Seattle,Texas,Chicago,New York City"')
    ap.add_argument("--skip-workday", action="store_true")
    ap.add_argument("--include-undated", action="store_true",
                    help="keep open postings whose exact date can't be found")
    ap.add_argument("--boards-only", action="store_true",
                    help="skip the Simplify / Hacker News / Adzuna sources")
    ap.add_argument("--crawls", type=int, default=1, help="Common Crawl snapshots to search")
    ap.add_argument("--max-pages", type=int, help="limit index pages per pattern (for testing)")
    args = ap.parse_args()

    hubs = HUBS
    if args.hubs:
        wanted = [h.strip().lower() for h in args.hubs.split(",")]
        hubs = {h: c for h, c in HUBS.items() if h.lower() in wanted}
        if not hubs:
            raise SystemExit(f"No matching hubs. Choose from: {', '.join(HUBS)}")
    if not (args.harvest or args.scan or args.add):
        ap.print_help()
        return
    if args.harvest:
        harvest(args.crawls, args.max_pages)
    if args.scan:
        scan(args.days, hubs, args.skip_workday, args.include_undated, not args.boards_only)
    if args.add:
        add_to_tracker(interns_only=not args.add_all)


if __name__ == "__main__":
    main()
