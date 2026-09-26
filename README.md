# 2027 Internship Alert Tracker

Checks the career boards of tech companies every 30 minutes and sends a push
notification to your phone when a new **2027 tech internship** appears.

## Files
| File | What it does |
|---|---|
| `job_tracker.py` | The tracker. Runs every 30 min and sends alerts. |
| `companies.csv` | Companies to watch: `name, ats, slug, tier`. |
| `keywords.json` | Job-title words to match. Edit this to add or remove titles. |
| `sources.json` | Extra job sources (GitHub lists, Adzuna, Hacker News, USAJobs, Google Jobs). |
| `discover_companies.py` | Turns a big list of company names into job boards (run once). |
| `.github/workflows/tracker.yml` | Runs the tracker on GitHub every 30 min for free. |

## Setup: getting it running
1. **Phone:** install the **ntfy** app and subscribe to a hard-to-guess topic name,
   e.g. `yourname-intern-alerts-7x9q`.
2. **Computer:** install Python 3.10+, unzip this folder, then in a terminal:
   ```bash
   cd intern-tracker
   pip install -r requirements.txt
   export NTFY_TOPIC=yourname-intern-alerts-7x9q     # Windows: set NTFY_TOPIC=...
   python job_tracker.py --test-notify                # your phone should buzz
   python job_tracker.py --check                      # which companies respond
   python job_tracker.py                              # first real run
   ```
3. **Run it 24/7 on GitHub (free):**
   - Create a new GitHub repo and upload everything in this folder, including the hidden `.github` folder.
   - Repo → Settings → Secrets and variables → Actions → **New repository secret**:
     name `NTFY_TOPIC`, value = your topic.
   - Repo → Settings → Actions → General → Workflow permissions → **Read and write**.
   - Actions tab → "Internship Tracker" → **Run workflow**. After that it runs every 30 min.

## Extra job sources (`sources.json`)
Besides company career sites, the tracker also watches:

| Source | What it adds | Key needed? | How often |
|---|---|---|---|
| Simplify 2027 list (GitHub) | Community + Simplify's hourly scraping; includes Google, Meta, Apple, Microsoft | No | Every run |
| CSCareers 2027 list (GitHub) | Second community list (US / Canada / remote) | No | Every run |
| Hacker News "Who's Hiring" | Startup internships posted in the monthly thread | No | Every 2 h |
| Adzuna | Job search engine covering thousands of sites | Free key | Every run |
| USAJobs | Government / national lab internships | Free key | Every 2 h |
| Google Jobs (SerpApi) | Google's job search (LinkedIn, Indeed, etc.) | Free key, small monthly limit | Once a day |

The same job found by several sources only alerts you once, and each alert says
which source found it ("via Simplify 2027 list"). Test sources with:
```bash
python job_tracker.py --check-sources
```

### Getting the optional API keys
Sources without keys are skipped quietly, so add only the ones you want.
1. **Adzuna:** sign up at developer.adzuna.com → copy your *Application ID* and *Application Key*.
2. **USAJobs:** request a key at developer.usajobs.gov (it's emailed to you). You'll also need the email address you signed up with.
3. **SerpApi:** sign up at serpapi.com → copy your API key from the dashboard.

Add each one as a GitHub secret (Settings → Secrets and variables → Actions):
`ADZUNA_APP_ID`, `ADZUNA_APP_KEY`, `USAJOBS_API_KEY`, `USAJOBS_EMAIL`, `SERPAPI_KEY`.
To test locally, `export` (Mac) or `set` (Windows) the same names before running.

### If a GitHub list source fails
The lists store their data in a `listings.json` file that the maintainers could move.
Open the repo on GitHub, find `listings.json` (usually in `.github/scripts/`), click
**Raw**, and paste that URL into the source's `urls` in `sources.json`.

### Turning sources on/off
In `sources.json`, set `"enabled": false`, change `every_n_runs` (1 = every 30 min,
4 = every 2 h, 48 = daily), or edit the search `queries`.

## Scaling to thousands of companies
Your tracker needs a job-board ID for each company, not just its name.
`discover_companies.py` finds those IDs:

```bash
# 1. Get a company dataset, e.g. People Data Labs' free company dataset (CSV with
#    name, website, industry, size). Filter it down, then test on a small sample:
python discover_companies.py free_company_dataset.csv --tech-only --min-employees 50 --limit 2000

# 2. If results look good, run the full thing (resumable; Ctrl+C and rerun anytime)
python discover_companies.py free_company_dataset.csv --tech-only --min-employees 50 --country "united states"

# 3. Review discovered_companies.csv, delete bad matches, then:
python discover_companies.py --merge
python job_tracker.py --check
```
Already have a list of board slugs (one per line)? Verify it with
`python discover_companies.py --slug-list slugs.txt --ats greenhouse`.

**How the tracker handles big lists:**
- `tier=hot` companies are checked every 30 min. Discovery marks a company hot automatically
  if it has any intern posting open.
- Everyone else is split into rotating batches of up to 4,000 (`MAX_COMPANIES_PER_RUN`).
  With 20,000 companies, each non-hot company is checked about every 2.5 hours.
- A company's existing postings are recorded silently the first time it's scanned,
  so adding thousands of companies won't flood your phone.

## Adding job titles
Edit `keywords.json`. A title matches if it contains one `intern_terms` word, one
`role_terms` word, and no `exclude_terms` word. Matching is case-insensitive.

## Options (environment variables)
- `STRICT_MODE=1`: only alert when the posting explicitly says 2027.
- `MAX_COMPANIES_PER_RUN=4000`, `MAX_WORKERS=24`, `CHECK_INTERVAL_MIN=30`

## Limitations
- Google, Meta, Microsoft and Apple have no public API; they're covered through the GitHub lists instead.
- LinkedIn, Handshake and Indeed have no student API. Use their own apps' job-alert push notifications.
- Slug guessing can occasionally match the wrong company when names are common words.
  Rows with `matched_from=domain` are the most reliable; `--domain-only` is stricter.
- Workday companies can't be auto-discovered; add them by hand (format in `companies.csv`).
