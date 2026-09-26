#!/usr/bin/env python3
"""
Company discovery: turn a big list of company NAMES into job boards the
tracker can read.

A company name alone isn't enough: the tracker needs to know which job-board
system (Greenhouse / Lever / Ashby) the company uses and its board ID ("slug").
This script guesses likely slugs from each name/website and tests them.

Run it ONCE (it's slow on big lists, and it's resumable), review the results,
then merge them into companies.csv.

Examples:
  # From a big CSV (e.g. People Data Labs free company dataset), tech + 50+ employees only
  python discover_companies.py companies_big.csv --tech-only --min-employees 50

  # Test run on the first 2,000 rows
  python discover_companies.py companies_big.csv --limit 2000

  # Verify a ready-made list of slugs (one per line) for one platform
  python discover_companies.py --slug-list greenhouse_slugs.txt --ats greenhouse

  # Add everything found to companies.csv
  python discover_companies.py --merge
"""

import argparse
import csv
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
OUT_FILE = BASE_DIR / "discovered_companies.csv"
PROGRESS_FILE = BASE_DIR / ".discover_progress.txt"
COMPANIES_FILE = BASE_DIR / "companies.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (student internship tracker; educational project)",
           "Accept-Encoding": "gzip, deflate"}
INTERN_HINT = re.compile(r"\b(intern|internship|co-?op)\b", re.I)

TECH_INDUSTRIES = (
    "computer software", "internet", "information technology", "computer hardware",
    "semiconductors", "computer networking", "computer & network security", "telecommunications",
    "computer games", "financial services", "capital markets", "investment management",
    "consumer electronics", "e-learning", "online media", "wireless", "biotechnology",
    "defense & space", "aviation & aerospace", "automotive", "research",
    "venture capital", "banking", "insurance", "entertainment", "media production",
)
LEGAL_SUFFIXES = {"inc", "incorporated", "llc", "ltd", "limited", "corp", "corporation", "co",
                  "company", "plc", "gmbh", "ag", "sa", "bv", "nv", "pte", "pty", "srl",
                  "holdings", "group", "the"}
SOFT_SUFFIXES = {"technologies", "technology", "labs", "software", "systems", "ai", "hq", "app"}

_local = threading.local()
_write_lock = threading.Lock()


def session() -> requests.Session:
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
        _local.s.headers.update(HEADERS)
    return _local.s


def get(url: str, **kw):
    for attempt in range(4):
        try:
            r = session().get(url, timeout=15, **kw)
        except requests.RequestException:
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(2 ** (attempt + 1))
            continue
        return r
    return None


# ---- probes: return (job_count, has_intern_posting) or None -----------------
def probe_greenhouse(slug):
    r = get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    if r is None or r.status_code != 200:
        return None
    titles = [j.get("title", "") for j in r.json().get("jobs", [])]
    return len(titles), any(INTERN_HINT.search(t) for t in titles)


def probe_lever(slug):
    r = get(f"https://api.lever.co/v0/postings/{slug}", params={"mode": "json"})
    if r is None or r.status_code != 200 or not isinstance(r.json(), list):
        return None
    titles = [j.get("text", "") for j in r.json()]
    return len(titles), any(INTERN_HINT.search(t) for t in titles)


def probe_ashby(slug):
    r = get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
    if r is None or r.status_code != 200:
        return None
    titles = [j.get("title", "") for j in r.json().get("jobs", [])]
    return len(titles), any(INTERN_HINT.search(t) for t in titles)


PROBES = {"greenhouse": probe_greenhouse, "ashby": probe_ashby, "lever": probe_lever}


# ---- slug guessing ------------------------------------------------------------
def slug_candidates(name: str, domain: str = "", domain_only: bool = False) -> list[str]:
    cands = []
    if domain:
        d = re.sub(r"^https?://", "", domain.strip().lower())
        d = re.sub(r"^www\.", "", d).split("/")[0]
        label = d.split(".")[0]
        if label:
            cands.append(label)
    if not domain_only or not cands:
        words = re.sub(r"[^a-z0-9 ]", " ", (name or "").lower()).split()
        while words and words[-1] in LEGAL_SUFFIXES:
            words.pop()
        if len(words) > 1 and words[0] == "the":
            cands.append("".join(words))                    # e.g. thetradedesk
            words.pop(0)
        if words:
            cands += ["".join(words), "-".join(words)]
            if len(words) > 1 and words[-1] in SOFT_SUFFIXES:
                cands += ["".join(words[:-1]), "-".join(words[:-1])]
    out = []
    for c in cands:
        c = c.strip("-")
        if len(c) >= 3 and re.fullmatch(r"[a-z0-9-]+", c) and c not in out:
            out.append(c)
    return out


def discover_one(name, domain, platforms, min_jobs, domain_only):
    for slug in slug_candidates(name, domain, domain_only):
        for ats in platforms:
            res = PROBES[ats](slug)
            if res and res[0] >= min_jobs:
                count, has_intern = res
                return {"name": name, "ats": ats, "slug": slug,
                        "tier": "hot" if has_intern else "", "jobs": count,
                        "matched_from": "domain" if domain and slug in domain.lower() else "name"}
    return None


# ---- input reading --------------------------------------------------------------
def pick_col(fields, *options):
    lower = {f.lower(): f for f in fields}
    for o in options:
        if o in lower:
            return lower[o]
    return None


def min_size(size_str: str) -> int:
    m = re.match(r"\s*(\d[\d,]*)", size_str or "")
    return int(m.group(1).replace(",", "")) if m else 0


def read_rows(args):
    if args.slug_list:
        for line in open(args.slug_list, encoding="utf-8"):
            s = line.strip()
            if s and not s.startswith("#"):
                yield s, s                                  # name = slug; slug hint as "domain"
        return
    with open(args.input, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        name_c = pick_col(reader.fieldnames, "name", "company", "company_name")
        dom_c = pick_col(reader.fieldnames, "domain", "website", "url")
        ind_c = pick_col(reader.fieldnames, "industry")
        size_c = pick_col(reader.fieldnames, "size", "size_range", "employees",
                          "current_employee_estimate")
        if not name_c:
            sys.exit(f"Couldn't find a name column in {reader.fieldnames}")
        for row in reader:
            if args.tech_only and ind_c and not any(t in (row.get(ind_c) or "").lower()
                                                    for t in TECH_INDUSTRIES):
                continue
            if args.min_employees and size_c and min_size(row.get(size_c)) < args.min_employees:
                continue
            if args.country and "country" in {k.lower() for k in row} and \
                    (row.get("country") or "").lower() != args.country.lower():
                continue
            yield row[name_c], (row.get(dom_c) or "") if dom_c else ""


# ---- commands -----------------------------------------------------------------------
def run_discovery(args):
    platforms = [args.ats] if args.ats else list(PROBES)
    done = set(PROGRESS_FILE.read_text(encoding="utf-8").splitlines()) if PROGRESS_FILE.exists() else set()
    rows = [(n, d) for n, d in read_rows(args) if n and f"{n}|{d}" not in done]
    if args.limit:
        rows = rows[: args.limit]
    print(f"{len(rows):,} companies to check ({len(done):,} already done from earlier runs). "
          f"Platforms: {', '.join(platforms)}. Ctrl+C is safe - rerun to resume.")

    new_file = not OUT_FILE.exists()
    out = open(OUT_FILE, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out, fieldnames=["name", "ats", "slug", "tier", "jobs", "matched_from"])
    if new_file:
        writer.writeheader()
    prog = open(PROGRESS_FILE, "a", encoding="utf-8")

    found, start = 0, time.time()
    try:
        with ThreadPoolExecutor(args.workers) as pool:
            futs = {pool.submit(discover_one, n, d, platforms, args.min_jobs,
                                args.domain_only): (n, d) for n, d in rows}
            for i, fut in enumerate(as_completed(futs), 1):
                n, d = futs[fut]
                try:
                    hit = fut.result()
                except Exception:
                    hit = None
                with _write_lock:
                    if hit:
                        found += 1
                        writer.writerow(hit)
                        out.flush()
                    prog.write(f"{n}|{d}\n")
                if i % 500 == 0 or i == len(rows):
                    rate = i / max(1e-6, time.time() - start)
                    eta = (len(rows) - i) / max(rate, 1e-6) / 3600
                    print(f"  {i:,}/{len(rows):,} checked, {found:,} boards found, "
                          f"{rate:.1f}/s, ~{eta:.1f} h left")
    except KeyboardInterrupt:
        print("\nStopped. Progress saved - run the same command again to resume.")
        pool.shutdown(cancel_futures=True)
    finally:
        out.close()
        prog.close()
    print(f"\nFound {found:,} job boards -> {OUT_FILE.name}. Review it, then run --merge.")


def merge():
    if not OUT_FILE.exists():
        sys.exit("No discovered_companies.csv yet - run discovery first.")
    with open(COMPANIES_FILE, newline="", encoding="utf-8") as f:
        existing = list(csv.DictReader(f))
    keys = {(r["ats"].lower(), r["slug"].lower()) for r in existing}
    added = 0
    with open(OUT_FILE, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            k = (r["ats"].lower(), r["slug"].lower())
            if k not in keys:
                keys.add(k)
                existing.append({"name": r["name"], "ats": r["ats"], "slug": r["slug"],
                                 "tier": r.get("tier", "")})
                added += 1
    with open(COMPANIES_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "ats", "slug", "tier"], extrasaction="ignore")
        w.writeheader()
        w.writerows(existing)
    print(f"Added {added:,} companies. companies.csv now has {len(existing):,}.")


def main():
    ap = argparse.ArgumentParser(description="Find job boards for a list of companies")
    ap.add_argument("input", nargs="?", help="CSV with a name column (and ideally domain/website)")
    ap.add_argument("--slug-list", help="text file of slugs to verify, one per line")
    ap.add_argument("--ats", choices=list(PROBES), help="only check this platform")
    ap.add_argument("--tech-only", action="store_true", help="keep tech-ish industries only")
    ap.add_argument("--min-employees", type=int, default=0, help="skip companies smaller than this")
    ap.add_argument("--country", help='e.g. "united states"')
    ap.add_argument("--domain-only", action="store_true",
                    help="only guess slugs from the website (fewer false matches)")
    ap.add_argument("--min-jobs", type=int, default=1, help="ignore boards with fewer postings")
    ap.add_argument("--limit", type=int, help="only process this many rows (for testing)")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--merge", action="store_true", help="add discovered boards to companies.csv")
    args = ap.parse_args()

    if args.merge:
        merge()
    elif args.input or args.slug_list:
        run_discovery(args)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
