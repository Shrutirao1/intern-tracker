#!/usr/bin/env python3
"""
Build a company list from REAL internship history.

Reads the community internship lists on GitHub (Simplify + CSCareers, 2025-2027),
keeps companies that posted SWE / SDE / ML / AI / DS internships (using your
keywords.json), reads each application link to find the company's exact job
board (Greenhouse, Lever, Ashby, Workday), checks the board works, and saves
the result.

Usage:
    python3 build_company_list.py            # build + verify -> history_companies.csv
    python3 build_company_list.py --merge    # add history_companies.csv to companies.csv
"""

import argparse
import csv
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import job_tracker as jt

BASE_DIR = Path(__file__).resolve().parent
OUT_FILE = BASE_DIR / "history_companies.csv"
UNSUPPORTED_FILE = BASE_DIR / "unsupported_companies.txt"
COMPANIES_FILE = BASE_DIR / "companies.csv"

LIST_REPOS = [
    ("SimplifyJobs/Summer2027-Internships", 2027),
    ("SimplifyJobs/Summer2026-Internships", 2026),
    ("SimplifyJobs/Summer2025-Internships", 2025),
    ("vanshb03/Summer2027-Internships", 2027),
    ("vanshb03/Summer2026-Internships", 2026),
]
BRANCHES = ["dev", "main"]
LISTINGS_PATH = ".github/scripts/listings.json"


# ---------------------------------------------------------------------------
# Reading the lists
# ---------------------------------------------------------------------------
def download_list(repo: str) -> list[dict]:
    for branch in BRANCHES:
        url = f"https://raw.githubusercontent.com/{repo}/{branch}/{LISTINGS_PATH}"
        try:
            r = jt.http("GET", url, timeout=120)
            if r.status_code == 200 and isinstance(r.json(), list):
                return r.json()
        except Exception:
            continue
    return []


def board_from_url(url: str):
    """Turn an application link into (ats, slug), or None if it's a custom site."""
    try:
        u = urlparse(url or "")
    except ValueError:
        return None
    host = u.netloc.lower().split(":")[0]
    parts = [p for p in u.path.split("/") if p]
    query = parse_qs(u.query)

    bad = lambda p: "." in p or p.lower() in ("embed", "v1", "api", "static", "assets", "wday")
    if host.endswith("greenhouse.io") and host.split(".")[0] in ("boards", "job-boards", "www"):
        if "for" in query:                                   # embed/job_app?for=slug
            return "greenhouse", query["for"][0].lower()
        if parts and not bad(parts[0]):
            return "greenhouse", parts[0].lower()
    if host == "jobs.lever.co" and parts and not bad(parts[0]):
        return "lever", parts[0].lower()
    if host == "jobs.ashbyhq.com" and parts and not bad(parts[0]):
        return "ashby", parts[0].lower()
    m = re.fullmatch(r"([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com", host)
    if m and parts:
        site = parts[1] if re.fullmatch(r"[a-z]{2}-[A-Za-z]{2}", parts[0]) and len(parts) > 1 else parts[0]
        if site.lower() not in ("job", "details", "wday") and "." not in site:
            return "workday", f"{host}|{m.group(1)}|{site}"
    return None


# ---------------------------------------------------------------------------
# Checking boards
# ---------------------------------------------------------------------------
def probe_workday(slug: str):
    host, tenant, site = slug.split("|")
    r = jt.http("POST", f"https://{host}/wday/cxs/{tenant}/{site}/jobs",
                json={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""})
    if r is None or r.status_code != 200:
        return None
    return r.json().get("total", 1)


def probe(ats: str, slug: str):
    try:
        if ats == "greenhouse":
            r = jt.http("GET", f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            return len(r.json().get("jobs", [])) if r is not None and r.status_code == 200 else None
        if ats == "lever":
            r = jt.http("GET", f"https://api.lever.co/v0/postings/{slug}", params={"mode": "json"})
            return len(r.json()) if r is not None and r.status_code == 200 and isinstance(r.json(), list) else None
        if ats == "ashby":
            r = jt.http("GET", f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
            return len(r.json().get("jobs", [])) if r is not None and r.status_code == 200 else None
        if ats == "workday":
            return probe_workday(slug)
    except Exception:
        return None
    return None


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def build(verify: bool) -> None:
    boards = {}                                   # (ats, slug) -> info
    unsupported = Counter()
    for repo, year in LIST_REPOS:
        rows = download_list(repo)
        kept = 0
        for j in rows:
            title, company = j.get("title", ""), (j.get("company_name") or "").strip()
            if not company or not jt.role_matches(title):
                continue
            kept += 1
            board = board_from_url(j.get("url", ""))
            if not board:
                unsupported[company] += 1
                continue
            info = boards.setdefault(board, {"name": company, "years": set(), "roles": 0})
            info["years"].add(year)
            info["roles"] += 1
        print(f"  {repo:<40} {len(rows):>6} listings, {kept:>5} SWE/ML/AI/DS roles"
              + ("" if rows else "   (could not download - skipped)"))

    # one board per company name (keep the one with the most roles)
    by_name = defaultdict(list)
    for (ats, slug), info in boards.items():
        by_name[info["name"].lower()].append((info["roles"], ats, slug, info))
    candidates = [max(v) for v in by_name.values()]
    print(f"\nFound {len(candidates)} companies with a readable job board "
          f"({len(unsupported)} more use custom career sites).")

    results = []
    if verify:
        print("Checking each board works (takes a few minutes)...")
        with ThreadPoolExecutor(24) as pool:
            futs = {pool.submit(probe, ats, slug): (roles, ats, slug, info)
                    for roles, ats, slug, info in candidates}
            for i, fut in enumerate(as_completed(futs), 1):
                roles, ats, slug, info = futs[fut]
                count = fut.result()
                if count is not None:
                    results.append((info["name"], ats, slug, info, count))
                if i % 100 == 0:
                    print(f"  {i}/{len(candidates)} checked, {len(results)} working")
    else:
        results = [(info["name"], ats, slug, info, "") for _, ats, slug, info in candidates]

    results.sort(key=lambda r: (-max(r[3]["years"]), -r[3]["roles"], r[0].lower()))
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "ats", "slug", "tier", "years", "past_roles", "open_jobs"])
        for name, ats, slug, info, count in results:
            tier = "priority" if info["years"] & {2026, 2027} else "hot"
            w.writerow([name, ats, slug, tier, " ".join(map(str, sorted(info["years"]))),
                        info["roles"], count])
    UNSUPPORTED_FILE.write_text(
        "Companies that posted SWE/ML/AI/DS internships on their own custom career sites.\n"
        "The tracker can't read these directly, but the Simplify list still covers them.\n\n"
        + "\n".join(f"{n}  ({c} roles)" for n, c in unsupported.most_common()), encoding="utf-8")

    pri = sum(1 for r in results if r[3]["years"] & {2026, 2027})
    print(f"\nSaved {len(results)} working boards to {OUT_FILE.name} "
          f"({pri} marked priority: posted 2026/2027 internships).")
    print(f"Custom-site companies listed in {UNSUPPORTED_FILE.name}.")
    print("Next: python3 build_company_list.py --merge")


def merge() -> None:
    if not OUT_FILE.exists():
        raise SystemExit("Run python3 build_company_list.py first.")
    with open(COMPANIES_FILE, newline="", encoding="utf-8") as f:
        existing = list(csv.DictReader(f))
    names = {r["name"].strip().lower() for r in existing}
    boards = {(r["ats"].strip().lower(), r["slug"].strip().lower()) for r in existing}
    by_name = {r["name"].strip().lower(): r for r in existing}
    by_board = {(r["ats"].strip().lower(), r["slug"].strip().lower()): r for r in existing}
    added = upgraded = 0
    with open(OUT_FILE, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            key = (r["ats"].lower(), r["slug"].lower())
            if r["name"].strip().lower() in names or key in boards:
                old = by_board.get(key) or by_name.get(r["name"].strip().lower())
                if old is not None and r["tier"] == "priority" and old.get("tier") != "priority":
                    old["tier"] = "priority"             # upgrade companies you already track
                    upgraded += 1
                continue
            existing.append({"name": r["name"], "ats": r["ats"], "slug": r["slug"], "tier": r["tier"]})
            names.add(r["name"].strip().lower())
            boards.add(key)
            added += 1
    with open(COMPANIES_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "ats", "slug", "tier"], extrasaction="ignore")
        w.writeheader()
        w.writerows(existing)
    print(f"Added {added} companies, upgraded {upgraded} to priority. "
          f"companies.csv now has {len(existing)} "
          f"({sum(1 for r in existing if r.get('tier') == 'priority')} priority).")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a company list from internship history")
    ap.add_argument("--merge", action="store_true", help="add results to companies.csv")
    ap.add_argument("--no-verify", action="store_true", help="skip checking each board")
    args = ap.parse_args()
    merge() if args.merge else build(verify=not args.no_verify)


if __name__ == "__main__":
    main()
