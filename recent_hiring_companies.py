#!/usr/bin/env python3
"""
Which companies posted SWE / SDE / ML / AI / DS roles in top US tech hubs
in the last N days (default 90)?

Legitimate, free sources only (no scraping of LinkedIn / Indeed / Handshake):
  1. Simplify + CSCareers internship lists (2026 + 2027)   - no key needed
  2. Hacker News "Who's Hiring" threads (last ~3 months)     - no key needed
  3. Adzuna job search API (aggregates thousands of sites)  - free key (optional)

Role matching uses your keywords.json, so results match what the tracker alerts on.

Usage:
    python3 recent_hiring_companies.py                 # all hubs, last 90 days
    python3 recent_hiring_companies.py --days 60 --hubs "SF Bay Area,Seattle,Texas"
    python3 recent_hiring_companies.py --include-remote
    python3 recent_hiring_companies.py --add           # add results to your tracker
"""

import argparse
import csv
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import job_tracker as jt
from build_company_list import board_from_url

BASE_DIR = Path(__file__).resolve().parent
OUT_FILE = BASE_DIR / "recent_hiring_companies.csv"
DISCOVERY_FILE = BASE_DIR / "recent_companies_for_discovery.csv"
COMPANIES_FILE = BASE_DIR / "companies.csv"

# ---------------------------------------------------------------------------
# Top US tech hubs - edit freely. Each hub lists the city names to look for.
# ---------------------------------------------------------------------------
from hubs import HUBS, REMOTE_WORDS, build_city_index, hubs_in  # noqa: F401


def norm_company(name: str) -> str:
    n = re.sub(r"[^a-z0-9 ]", " ", (name or "").lower())
    n = re.sub(r"\b(inc|llc|ltd|corp|corporation|co|company|technologies|the)\b", " ", n)
    return re.sub(r"\s+", " ", n).strip()


# ---------------------------------------------------------------------------
# Sources - each yields dicts: company, title, location, date, url, source
# ---------------------------------------------------------------------------
LIST_REPOS = ["SimplifyJobs/Summer2027-Internships", "SimplifyJobs/Summer2026-Internships",
              "vanshb03/Summer2027-Internships", "vanshb03/Summer2026-Internships"]


def src_lists(cutoff: float):
    for repo in LIST_REPOS:
        data = []
        for branch in ("dev", "main"):
            try:
                r = jt.http("GET", f"https://raw.githubusercontent.com/{repo}/{branch}/"
                                   ".github/scripts/listings.json", timeout=120)
                if r.status_code == 200 and isinstance(r.json(), list):
                    data = r.json()
                    break
            except Exception:
                continue
        print(f"  {repo:<42} {len(data):>6} listings" + ("" if data else "  (skipped)"))
        for j in data:
            ts = j.get("date_posted") or j.get("date_updated") or 0
            if not ts or float(ts) < cutoff:
                continue
            locs = j.get("locations") or []
            yield {"company": j.get("company_name", ""), "title": j.get("title", ""),
                   "location": "; ".join(locs) if isinstance(locs, list) else str(locs),
                   "date": float(ts), "url": j.get("url", ""), "source": "Simplify/CSCareers"}


def src_hackernews(cutoff: float):
    r = jt.http("GET", "https://hn.algolia.com/api/v1/search_by_date",
                params={"tags": "story,author_whoishiring", "hitsPerPage": 20})
    stories = [h for h in r.json().get("hits", [])
               if (h.get("title") or "").lower().startswith("ask hn: who is hiring")
               and h.get("created_at_i", 0) >= cutoff - 40 * 86400]
    print(f"  {'Hacker News Who is Hiring':<42} {len(stories):>6} monthly threads")
    for s in stories:
        for page in range(3):
            r = jt.http("GET", "https://hn.algolia.com/api/v1/search_by_date",
                        params={"tags": f"comment,story_{s['objectID']}", "hitsPerPage": 1000,
                                "page": page})
            hits = r.json().get("hits", [])
            for c in hits:
                if str(c.get("parent_id")) != str(s["objectID"]):
                    continue
                text = jt.strip_html(c.get("comment_text") or "")
                first = jt.strip_html((c.get("comment_text") or "").split("<p>")[0])[:200]
                yield {"company": first.split("|")[0].strip()[:60], "title": first,
                       "location": first, "date": float(c.get("created_at_i") or 0),
                       "url": f"https://news.ycombinator.com/item?id={c['objectID']}",
                       "source": "Hacker News", "full_text": text}
            if len(hits) < 1000:
                break


def src_adzuna(cutoff: float, hubs: dict, days: int):
    app_id, app_key = os.getenv("ADZUNA_APP_ID"), os.getenv("ADZUNA_APP_KEY")
    if not (app_id and app_key):
        print(f"  {'Adzuna':<42}   skipped (set ADZUNA_APP_ID / ADZUNA_APP_KEY)")
        return
    queries = ["software engineer", "machine learning", "data scientist"]
    calls = 0
    for hub, cities in hubs.items():
        where = cities[0]
        for q in queries:
            for page in (1, 2):
                if calls >= 200:                        # stay under the free 250/day limit
                    print("  Adzuna: stopping at 200 calls (free daily limit)")
                    return
                calls += 1
                try:
                    r = jt.http("GET", f"https://api.adzuna.com/v1/api/jobs/us/search/{page}",
                                params={"app_id": app_id, "app_key": app_key, "what": q,
                                        "where": where, "max_days_old": min(days, 90),
                                        "results_per_page": 50, "category": "it-jobs"})
                    results = r.json().get("results", []) if r.status_code == 200 else []
                except Exception:
                    results = []
                for j in results:
                    try:
                        ts = datetime.fromisoformat(j.get("created", "").replace("Z", "+00:00")).timestamp()
                    except ValueError:
                        ts = time.time()
                    loc = j.get("location") or {}
                    yield {"company": (j.get("company") or {}).get("display_name", ""),
                           "title": j.get("title", ""),
                           "location": "; ".join([loc.get("display_name", "")] + list(loc.get("area") or [])),
                           "date": ts, "url": j.get("redirect_url", ""), "source": "Adzuna"}
                if len(results) < 50:
                    break
                time.sleep(1.2)                         # stay under 25 calls/minute
    print(f"  {'Adzuna':<42} {calls:>6} API calls")


# ---------------------------------------------------------------------------
def collect(days: int, hubs: dict, include_remote: bool, interns_only: bool):
    cutoff = time.time() - days * 86400
    city_index = build_city_index(hubs)
    companies = defaultdict(lambda: {"name": "", "hubs": set(), "titles": [], "count": 0,
                                     "sources": set(), "boards": defaultdict(int), "latest": 0})
    streams = [src_lists(cutoff), src_hackernews(cutoff), src_adzuna(cutoff, hubs, days)]
    seen_posts = set()
    for stream in streams:
        try:
            for p in stream:
                if p["date"] and p["date"] < cutoff:
                    continue
                post_id = jt.norm_url(p["url"]) or (p["company"] + p["title"])
                if post_id in seen_posts:
                    continue                            # same posting found twice
                seen_posts.add(post_id)
                if p["source"] == "Hacker News":
                    t = jt.normalize(p.get("full_text", ""))
                    if not (jt.CORE_RE.search(t) or jt.GENERAL_RE.search(jt.normalize(p["title"]))):
                        continue
                    if interns_only and not jt.INTERN_RE.search(t):
                        continue
                else:
                    if not jt.role_matches(p["title"]):
                        continue
                    if interns_only and p["source"] == "Adzuna" \
                            and not jt.INTERN_RE.search(jt.normalize(p["title"])):
                        continue
                found = hubs_in(p["location"], city_index, include_remote)
                if not found or not p["company"]:
                    continue
                key = norm_company(p["company"])
                if not key:
                    continue
                c = companies[key]
                c["name"] = c["name"] or p["company"].strip()
                c["hubs"] |= found
                c["count"] += 1
                c["sources"].add(p["source"])
                c["latest"] = max(c["latest"], p["date"] or 0)
                if len(c["titles"]) < 3 and p["title"] not in c["titles"]:
                    c["titles"].append(p["title"][:90])
                board = board_from_url(p["url"])
                if board:
                    c["boards"][board] += 1
        except Exception as e:
            print(f"  ! a source failed part-way: {type(e).__name__}: {e}")
    return companies


def save(companies: dict):
    rows = sorted(companies.values(), key=lambda c: (-c["count"], c["name"].lower()))
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company", "hubs", "matching_posts", "latest_post", "example_titles",
                    "sources", "ats", "slug"])
        for c in rows:
            ats, slug = max(c["boards"], key=c["boards"].get) if c["boards"] else ("", "")
            latest = datetime.fromtimestamp(c["latest"], timezone.utc).strftime("%Y-%m-%d") if c["latest"] else ""
            w.writerow([c["name"], "; ".join(sorted(c["hubs"])), c["count"], latest,
                        " | ".join(c["titles"]), "; ".join(sorted(c["sources"])), ats, slug])
    return rows


def add_to_tracker(rows):
    with open(COMPANIES_FILE, newline="", encoding="utf-8") as f:
        existing = list(csv.DictReader(f))
    names = {norm_company(r["name"]) for r in existing}
    boards = {(r["ats"].lower(), r["slug"].lower()) for r in existing}
    added, need_discovery = 0, []
    for c in rows:
        key = norm_company(c["name"])
        if key in names:
            continue
        if c["boards"]:
            ats, slug = max(c["boards"], key=c["boards"].get)
            if (ats, slug.lower()) in boards:
                continue
            existing.append({"name": c["name"], "ats": ats, "slug": slug, "tier": "hot"})
            boards.add((ats, slug.lower()))
            added += 1
        else:
            need_discovery.append(c["name"])
        names.add(key)
    with open(COMPANIES_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "ats", "slug", "tier"], extrasaction="ignore")
        w.writeheader()
        w.writerows(existing)
    with open(DISCOVERY_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name"])
        w.writerows([[n] for n in need_discovery])
    print(f"\nAdded {added} companies with known job boards to companies.csv (marked hot).")
    print(f"{len(need_discovery)} companies need their board found - run:")
    print(f"  python3 discover_companies.py {DISCOVERY_FILE.name}")
    print("  python3 discover_companies.py --merge")


def main():
    ap = argparse.ArgumentParser(description="Companies hiring for your roles in tech hubs")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--hubs", help='comma list, e.g. "SF Bay Area,Seattle,Texas" (default: all)')
    ap.add_argument("--include-remote", action="store_true")
    ap.add_argument("--all-levels", action="store_true",
                    help="include full-time roles too (default: internships only)")
    ap.add_argument("--add", action="store_true", help="add results to companies.csv")
    args = ap.parse_args()

    hubs = HUBS
    if args.hubs:
        wanted = [h.strip().lower() for h in args.hubs.split(",")]
        hubs = {h: c for h, c in HUBS.items() if h.lower() in wanted}
        if not hubs:
            raise SystemExit(f"No matching hubs. Choose from: {', '.join(HUBS)}")
    print(f"Looking for {'all' if args.all_levels else 'intern'} SWE/ML/AI/DS postings in the "
          f"last {args.days} days across {len(hubs)} hubs...")
    companies = collect(args.days, hubs, args.include_remote, not args.all_levels)
    rows = save(companies)

    per_hub = defaultdict(int)
    for c in rows:
        for h in c["hubs"]:
            per_hub[h] += 1
    print(f"\n{len(rows):,} companies saved to {OUT_FILE.name}")
    for h, n in sorted(per_hub.items(), key=lambda x: -x[1]):
        print(f"  {h:<18} {n:,}")
    if args.add:
        add_to_tracker(rows)
    else:
        print("\nTo add them to your tracker: python3 recent_hiring_companies.py --add")


if __name__ == "__main__":
    main()
