# Cambridge Police Crime Trends – Cloud Data Pipeline

Daily ETL that pulls the [Cambridge PD Daily Police Log](https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb) from the Socrata API, validates and cleans it, stores it in **Supabase (cloud PostgreSQL)**, and shows it on a **website hosted on Vercel** that opens from any PC.

```
Socrata API -> extract -> validate -> transform -> load -> Supabase Postgres -> Vercel website (public URL)
                 \_____ Prefect flow, run daily by GitHub Actions (cron 00:00 UTC) _____/
```

## 1. Goal and use
Monitor local crime trends (by hour, month, incident type, location). The log updates daily, so automating the pull is worthwhile. Cloud DB + cloud scheduler + cloud dashboard means the pipeline keeps running, and anyone can open the dashboard, without your laptop being on.

## 2. Tech stack
| Layer | Tool |
|---|---|
| Extract | `sodapy` (Socrata API, optional app token) |
| Validate / Transform | `pandas` |
| Orchestration | Prefect `@task` / `@flow` (run per-invocation); schedule = GitHub Actions cron |
| Storage (cloud) | Supabase Postgres (free tier), `SQLAlchemy` + `psycopg2` |
| Website (cloud) | Vercel: static page + Python API route, Plotly.js charts |

## 3. The data
Each row = one incident: `id, date_time, type, subtype, location, description, last_updated`. ~12k rows (Jan 2025 onward). Stored with extra `year, month, day, hour` columns.

## 4. What the pipeline does
| Stage | File | Behaviour |
|---|---|---|
| Extract | `src/extract.py` | all rows of dataset `3gki-wyrb`, 3 retries, 30 s timeout |
| Validate | `src/validate.py` | required columns present; `date_time` ISO-8601; no nulls in date_time/type/location (raise = fail loudly); non-numeric ids -> NaN |
| Transform | `src/transform.py` | drop NaN ids, dedupe on `id`, parse timestamps, add year/month/day/hour |
| Load | `src/load.py` | creates table (`src/schema.sql`) and **upserts** (`ON CONFLICT (id)`), so daily re-runs never duplicate |
| Flow | `src/etl_pipeline.py` | chains the four tasks |
| Dashboard | `src/dashboard.py` | KPIs, incidents by hour/month, top types, top locations, year filter |

## 5. Files
```
crime-pipeline/
  README.md  requirements.txt  .gitignore  .env.example
  src/        extract.py validate.py transform.py load.py etl_pipeline.py schema.sql
  dashboard/  public/index.html  api/data.py  dev.py  requirements.txt   (the Vercel website)
  notebooks/  Reference_Tutorial.ipynb   (the original tutorial this project is based on)
  docs/       (screenshots for the report)
  cache/ data/  (git-ignored scratch)
  .github/workflows/etl.yml   (daily scheduler)
```

## 6. Setup and deploy

### A. Supabase (database, cloud)
1. supabase.com -> New project (free). Save the **database password**.
2. Project -> Connect -> **Session pooler** -> copy the URI (works over IPv4) -> this is `DATABASE_URL`.
3. The pipeline creates the table itself on first run.

### B. Run on your PC
```bash
pip install -r requirements.txt
cp .env.example .env          # paste DATABASE_URL (+ optional SOCRATA_APP_TOKEN)
python src/etl_pipeline.py     # ETL once
python dashboard/dev.py        # website at http://localhost:8000
```

### C. Schedule in the cloud (GitHub Actions)
Push repo to GitHub -> Settings -> Secrets and variables -> Actions -> add `DATABASE_URL` (and `SOCRATA_APP_TOKEN`). The workflow runs daily; trigger manually from the Actions tab (`workflow_dispatch`) to test.

### D. Website on any PC (Vercel)
1. vercel.com -> Add New Project -> import your GitHub repo.
2. **Root Directory: `dashboard`** (keeps the heavy ETL dependencies out of the website deploy).
3. Environment Variables: add `DATABASE_URL` (the same Session pooler URI).
4. Deploy. Your site is `https://<project>.vercel.app`, open to anyone.
`dashboard/public/index.html` is the page, `dashboard/api/data.py` is the API that queries Supabase (cached 1 hour at the edge).

## 7. Verify
Supabase -> SQL editor: `select count(*), max(date_time) from cpd_incidents;`

## 8. Tested
Extract/validate/transform run on the live API (12,845 raw -> ~12.2k clean rows). Load upsert verified idempotent (second run adds 0 rows; missing timestamps become NULL). Dashboard renders with no errors (4 charts, 3 KPIs). The Supabase connection itself is tested only once you add your `DATABASE_URL`.

## 9. Limitations / next steps
- Full re-pull each day (~12k rows, fast); switch to incremental via `last_updated` if the data grows.
- Supabase free projects pause after ~1 week of inactivity; the daily job keeps it active.
- Dashboard cache is 1 hour (`ttl=3600`).
- Row Level Security is enabled with no policies, so Supabase's public REST API exposes nothing; the pipeline and dashboard use direct Postgres.
