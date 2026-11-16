# Cambridge Police Crime Trends – Cloud Data Pipeline

> A complete encyclopedia entry for this project: what it is, why it exists, how every part works, how to run and deploy it, and a section-by-section walkthrough of the reference notebook it grew out of.

**Contents**
1. [Overview](#1-overview)
2. [Background and motivation](#2-background-and-motivation)
3. [Glossary](#3-glossary)
4. [The data source](#4-the-data-source)
5. [Architecture](#5-architecture)
6. [Repository layout](#6-repository-layout)
7. [Component reference (code walkthrough)](#7-component-reference)
8. [Cloud infrastructure](#8-cloud-infrastructure)
9. [Configuration and secrets](#9-configuration-and-secrets)
10. [How to run](#10-how-to-run)
11. [Dashboard](#11-dashboard)
12. [Design decisions](#12-design-decisions)
13. [Testing and verification](#13-testing-and-verification)
14. [Known issues, limitations, security](#14-known-issues-limitations-security)
15. [Future work](#15-future-work)
16. [Notebook walkthrough, section by section](#16-notebook-walkthrough-section-by-section)
17. [How this maps to the course guidelines](#17-how-this-maps-to-the-course-guidelines)

---

## 1. Overview

**Cambridge Police Crime Trends – Cloud Data Pipeline** is an automated, cloud-hosted data-engineering project. Every day it downloads the Cambridge (Massachusetts) Police Department's public incident log, checks its quality, cleans it, stores it in a PostgreSQL database hosted on **Supabase**, and exposes the result through a **Vercel-hosted website** that anyone can open from any computer with a web link.

| | |
|---|---|
| Domain | Public safety analytics / data engineering |
| Pattern | ETL (Extract – Transform – Load) with a validation gate |
| Language | Python 3.10+ |
| Core libraries | pandas, sodapy, Prefect, SQLAlchemy, psycopg2; Plotly.js on the website |
| Cloud pieces | Supabase (database), GitHub Actions (scheduler), Vercel (website) |
| Data size | ~12,000 incident rows (January 2025 onward), growing daily |
| Course | PAI102 – Data Engineering (lab project) |

## 2. Background and motivation

Police departments publish incident logs, but raw logs are hard to read. A resident, journalist or analyst wants answers: *At what time of day are incidents most frequent? Which streets appear most often? Is activity rising or falling?* Answering those requires collecting the data repeatedly, cleaning it consistently and putting it somewhere queryable.

Doing this by hand (download a CSV, tidy it in a spreadsheet) works once, but the Cambridge dataset is **updated daily**, so a manual process would be repeated forever and would be error-prone. A data pipeline automates the repetition: the same steps run on a schedule, the same quality rules are applied every time, and the database always holds the current state.

The pipeline is deliberately **cloud-hosted** so it does not depend on one person's laptop. The database lives on Supabase, the scheduler runs on GitHub's servers and the dashboard runs on Streamlit's servers. Whoever clones the project, on any PC, connects to the same cloud database.

## 3. Glossary

| Term | Meaning |
|---|---|
| **ETL** | Extract, Transform, Load: pull data from a source, reshape it, store it in a destination. |
| **ELT** | Variant that loads raw data first and transforms inside the warehouse; preferred for huge data. Not needed here (~12k rows fit in memory). |
| **API** | Application Programming Interface: a web address that returns data on request. |
| **Socrata / SODA** | The open-data platform and its API used by the City of Cambridge. |
| **App token** | Optional key that identifies your application to Socrata so it is not throttled. |
| **Pandas DataFrame** | In-memory table used for cleaning data. |
| **Prefect** | Python workflow tool: `@task` marks a step, `@flow` marks the pipeline of steps. |
| **PostgreSQL** | Open-source relational database. |
| **Supabase** | Hosted PostgreSQL with a free tier, used as the cloud database. |
| **Session pooler** | Supabase's IPv4-friendly connection endpoint (host `*.pooler.supabase.com`). |
| **Upsert** | "Insert, or update if it already exists": `INSERT … ON CONFLICT (id) DO UPDATE`. Makes re-runs safe. |
| **Idempotent** | Running twice gives the same result as running once. |
| **Row Level Security (RLS)** | Postgres feature that restricts row access; enabled here with no public policies. |
| **GitHub Actions** | GitHub's built-in automation; here, a cron job that runs the ETL daily. |
| **Vercel** | Hosting platform for static pages and serverless functions; serves the website and its `/api/data` route. |
| **ISO 8601** | International timestamp format, e.g. `2025-03-06T01:07:00`. |
| **Cron** | Schedule syntax; `0 0 * * *` means every day at 00:00. |

## 4. The data source

**Dataset:** [Cambridge Police Department – Daily Police Log](https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb) (Socrata ID `3gki-wyrb`), published by the City of Cambridge as open data. Each row describes one incident reported to or by the department.

| Column | Type | Meaning |
|---|---|---|
| `id` | integer | Unique incident number (primary key) |
| `date_time` | timestamp | When the incident occurred |
| `type` | text | Broad category, e.g. `INCIDENT`, `ARREST` |
| `subtype` | text | Specific offense, e.g. `LARCENY FROM BUILDING C266 S20` (optional) |
| `location` | text | Street or intersection (generalised, not exact addresses) |
| `description` | text | Plain-language summary written by the department |
| `last_updated` | timestamp | When the record was last modified |

Columns the pipeline **adds**: `year`, `month`, `day`, `hour` (extracted from `date_time` so dashboard queries can group by period cheaply).

**Quirks observed on the live data:** about 12.8k rows are returned but ~0.5k are removed in cleaning (duplicate or non-numeric ids); `subtype` can be missing; the data runs from 2025-01-01 and is current to within days.

**Ethics note:** the description field may contain names from arrest reports. The dashboard intentionally does not display the description column in charts, and the project must not be used to profile individuals.

## 5. Architecture

```
┌────────────┐   ┌─────────┐   ┌──────────┐   ┌───────────┐   ┌────────┐   ┌──────────────────┐   ┌───────────┐
│ Socrata API│──▶│ Extract │──▶│ Validate │──▶│ Transform │──▶│  Load  │──▶│ Supabase Postgres│──▶│ Streamlit │
└────────────┘   └─────────┘   └──────────┘   └───────────┘   └────────┘   └──────────────────┘   │ dashboard │
                    └────────── Prefect flow "cpd_incident_etl" ─────────┘                           └───────────┘
                                         ▲
                       GitHub Actions cron (daily 00:00 UTC)
```

Data flow, step by step:
1. **GitHub Actions** wakes up daily and runs `python src/etl_pipeline.py`.
2. **Extract** downloads every row from Socrata (retrying on failure).
3. **Validate** stops the run if the data is structurally wrong.
4. **Transform** removes bad and duplicate rows and adds date parts.
5. **Load** upserts rows into the Supabase `cpd_incidents` table.
6. The **Vercel website** calls a small Python API that queries the table and draws the charts in the browser.

## 6. Repository layout

The layout follows the earlier Cassava Leaf Disease project (`src/`, `notebooks/`, `docs/`, `cache/`, `data/`, README, requirements, `.gitignore`).

```
crime-pipeline/
├── README.md              Quick-start documentation
├── Description.md         This document
├── requirements.txt       Python dependencies
├── .env                   Local secrets (git-ignored)
├── .env.example           Template for .env
├── .gitignore
├── src/
│   ├── extract.py         Socrata → DataFrame
│   ├── validate.py        Data-quality gate
│   ├── transform.py       Cleaning + date parts
│   ├── load.py            Upsert into Postgres; connection helper
│   ├── etl_pipeline.py    Prefect flow chaining the four stages
│   └── schema.sql         Table definition
├── dashboard/             Vercel website: public/index.html, api/data.py, dev.py
├── notebooks/
│   └── Reference_Tutorial.ipynb   Original tutorial the project is based on
├── docs/                  Screenshots/figures for reports
├── cache/  data/          Git-ignored scratch space
└── .github/workflows/etl.yml      Daily scheduler
```

## 7. Component reference

### 7.1 `src/extract.py` – Extract
- Creates a `sodapy.Socrata` client for `data.cambridgema.gov` with a 30-second timeout (the default 10 s sometimes fails for a full download).
- `client.get_all("3gki-wyrb")` pages through every record automatically and returns them as a generator; `pd.DataFrame.from_records` turns that into a table.
- Decorated `@task(retries=3, retry_delay_seconds=10)`: if the network hiccups, Prefect waits 10 s and tries again, up to three times.
- **Token handling:** the optional `SOCRATA_APP_TOKEN` is read from the environment. If Socrata answers *403 Invalid app_token*, the task prints a notice and retries anonymously, because the token only affects throttling, not access.

### 7.2 `src/validate.py` – Validate
A *gate*: it either returns a usable DataFrame or raises an error that fails the run, so bad data never reaches the database.
| Check | On failure |
|---|---|
| Required columns `date_time, id, type, location` exist | raise `ValueError` |
| Optional columns are re-indexed in (missing `subtype` becomes empty) | continue |
| `id` converted to numeric; non-numeric becomes `NaN` | marked for removal in Transform |
| No nulls in `date_time`, `type`, `location` | raise `ValueError` |
| Every `date_time` parses as ISO 8601 (`datetime.fromisoformat`) | raise `ValueError` |

Philosophy: *fail loudly* when the data cannot be trusted (schema, timestamps, nulls); *fail quietly* only where a row can safely be discarded (bad ids).

### 7.3 `src/transform.py` – Transform
1. Drop rows whose `id` is `NaN` (the invalid ids flagged earlier).
2. Drop duplicate `id`s, keeping the first.
3. Cast `id` to integer.
4. Convert `date_time` and `last_updated` to real timestamps.
5. Add `year`, `month`, `day`, `hour` columns.

### 7.4 `src/load.py` – Load
- `make_engine(url)` builds a SQLAlchemy engine. It splits the connection string on the **last `@`** and URL-encodes the password, so passwords containing `@`, `:`, `/` or `#` work (a plain URL parser would mistake part of the password for the host name).
- `load_into_postgres` first runs `schema.sql` (creates the table and index, enables RLS if missing), then **upserts** rows in chunks: `INSERT … ON CONFLICT (id) DO UPDATE SET …`. Existing incidents are refreshed, new ones inserted, nothing duplicated.
- pandas `NaT`/`NaN` values become SQL `NULL`; pandas Timestamps become plain Python datetimes so every database driver accepts them.

### 7.5 `src/etl_pipeline.py` – Orchestration
`@flow(name="cpd_incident_etl")` runs `extract → validate → transform → load` and prints the number of upserted rows. Prefect records each task's state and retries. The script performs **one run**; scheduling is delegated to GitHub Actions so no server has to stay on.

### 7.6 `src/schema.sql`
Defines `cpd_incidents` with `id` as primary key, an index on `date_time`, and `ENABLE ROW LEVEL SECURITY`. With RLS on and no policies, Supabase's public REST API exposes nothing; this project connects with direct Postgres credentials instead.

### 7.7 `dashboard/` (the website, deployed on Vercel)
- `public/index.html`: one static page (no build step). Floating navigation pill, hero with a live chart, a KPI strip, the five pipeline stages, a bento of live charts, the technology logos and a footer. Light and dark themes follow the system setting; motion respects `prefers-reduced-motion`.
- `api/data.py`: a Vercel Python function. `GET /api/data?year=2026` runs five aggregate SQL queries on Supabase and returns JSON (totals, by hour, by month, top offences, top locations). Responses are cached at Vercel's edge for one hour. Passwords with `@` work (same last-`@` split as `load.py`).
- `dev.py`: local preview server (`python dashboard/dev.py`, http://localhost:8000).
- `requirements.txt`: only `psycopg2-binary`, which is why Vercel's Root Directory is set to `dashboard` and the heavy ETL packages are not installed there.

### 7.8 `.github/workflows/etl.yml`
Runs on a cron (`0 0 * * *`) and on manual trigger: checkout → Python 3.12 → install requirements → run the pipeline with `DATABASE_URL` and `SOCRATA_APP_TOKEN` read from repository secrets.

## 8. Cloud infrastructure

| Piece | Service | Cost | Role |
|---|---|---|---|
| Database | Supabase project `cambridge-crime-pipeline` (ref `mnwgiosggxrslbaonrsb`, region `ap-south-1` Mumbai) | Free tier | Stores `cpd_incidents` |
| Scheduler | GitHub Actions | Free for public/small private repos | Daily ETL |
| Website | Vercel (Hobby tier) | Free | Public site and `/api/data` |

Why this answers "run on a different PC": none of the state lives on a laptop. Any machine with the `DATABASE_URL` reads and writes the same Supabase database, and the dashboard link works from any browser.

## 9. Configuration and secrets

| Variable | Required | Meaning |
|---|---|---|
| `DATABASE_URL` | yes | Supabase **Session pooler** URI: `postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres` |
| `SOCRATA_APP_TOKEN` | no | Throttling-avoidance token from data.cambridgema.gov |

Locally these live in `.env` (git-ignored). In GitHub they are Actions secrets; in Vercel they are project environment variables. **Never commit `.env`** or paste the database password into chat, tickets or screenshots; if it leaks, reset it in Supabase → Database settings.

## 10. How to run

```bash
pip install -r requirements.txt
# put DATABASE_URL in .env
python src/etl_pipeline.py        # ETL once
python dashboard/dev.py           # http://localhost:8000
```
Verify in Supabase SQL editor: `select count(*), max(date_time) from cpd_incidents;`

**Deploy:** push to GitHub, add the two secrets, and the daily job starts. For the website: Vercel → import the repo → **Root Directory `dashboard`** → add `DATABASE_URL` as an environment variable → Deploy.

## 11. The website

| Element | Answers |
|---|---|
| Hero chart: incidents by hour of day | When do incidents happen? (busiest hour is 4 PM) |
| KPI: incidents | How big is the selected slice? |
| KPI: latest record | How fresh is the data? |
| KPI: busiest hour | Peak time of day |
| Year selector | Filters every chart through `/api/data?year=` |
| Pipeline section | How the data moves: extract, validate, transform, load, serve |
| Bar: incidents per month | Is activity rising or falling? |
| Bar: most reported offences (top 8) | What kinds of incidents? |
| Bar: top 10 locations | Where? |
| Note card | Reported incidents are not total crime; descriptions are never shown |
| Logo row | Technologies used |

**Design:** a public-data project site in an editorial, data-forward style. One accent colour (cobalt, lighter in dark mode), Geist and Geist Mono type, a double-bezel card system with one radius scale, a floating glass navigation pill, an orchestrated load-in on the hero and soft scroll reveals. Each chart shows one measure (an incident count), so it uses the single accent hue, no legend and no rainbow colouring. Loading skeletons and an error state with a retry button are included. No em dashes appear anywhere in the copy.

## 12. Design decisions

- **ETL over ELT:** data is small (~12k rows); pandas makes cleaning simple.
- **Supabase over Docker/local Postgres:** free, cloud-hosted, shared across PCs, no container setup.
- **Vercel website over Metabase/Streamlit:** Metabase needs Docker or paid hosting, and Streamlit needs a long-running server that Vercel's serverless platform cannot host. A static page plus one Python API route deploys free from the repo and loads fast.
- **Prefect decorators kept** to follow the reference tutorial, while **scheduling moved to GitHub Actions**, which is free and needs no always-on server.
- **Upsert instead of `to_sql(if_exists="replace")`:** the tutorial's replace drops the table (and its primary key) each run; upsert preserves the schema and is safe to repeat.
- **Fail loudly vs. quietly** in validation, as in §7.2.
- **Full re-pull daily:** simplest correct approach at this size.

## 13. Testing and verification

| Test | Result |
|---|---|
| Extract + validate + transform on live API | 12,845 raw rows → ~12.2k clean rows, no duplicate ids |
| Load upsert against SQLite (same `ON CONFLICT` syntax) | Second run changed no rows; missing timestamp stored as NULL |
| Full Prefect flow end to end | Completed |
| Website against live Supabase (local server + headless browser screenshot) | API returns aggregates for all years and per year; page renders KPIs and four charts in light and dark |
| Real Supabase load | See the run log; the pooler URL needs the password fix in §7.4 |

Bugs found and fixed during testing: unordered Socrata paging returned duplicate rows and skipped others (fixed by ordering on `:id`); timestamp binding errors on some drivers; dashboard assuming datetime dtype; passwords containing `@` breaking the connection URL; an invalid Socrata token aborting the run.

## 14. Known issues, limitations, security

- Each run re-downloads everything (fine now; wasteful at millions of rows).
- Supabase free projects pause after about a week without activity; the daily job prevents that.
- The dashboard is public HTTP(S) read access to aggregated data; add Vercel password protection if it must be private.
- Free tiers have rate and size limits (Supabase 500 MB DB).
- The text `description` may contain personal names; keep it out of public views.
- Data is as published by the police and reflects reporting, not total crime.

## 15. Future work

Incremental loads using `last_updated`; data-quality alerts (email/Slack on failure); geocoding streets for a map; time-series forecasting; Metabase or Superset for ad-hoc analysis; unit tests and CI on pull requests; a read-only database role for the dashboard.

## 16. Notebook walkthrough, section by section

The reference notebook is `notebooks/Reference_Tutorial.ipynb` (40 cells): a tutorial that builds the pipeline in Docker + Metabase. Below, every section is explained and mapped to this project.

**Section 1 – Title and Background Knowledge (cells 0–1).** Names the project and lists prerequisite concepts: ETL, and Docker Compose's *services*, *networks* and *volumes*. ETL is the whole project's pattern. Docker concepts matter in the notebook because it hosts Postgres/Metabase in containers; in this project Supabase replaces them.

**Section 2 – Data of Interest (cells 2–4).** Describes the Cambridge Police log: each row is an incident with date/time, type, street and description, with a screenshot of the source page. The key argument: a *pipeline* is justified because the data updates **daily**; for an annual dataset you would simply download a CSV once a year.

**Section 3 – ETL Pipeline overview (cell 5).** Lists the four stages (extract with the API, transform, load into PostgreSQL, visualise in Metabase) with a flow diagram. Explains **ETL vs ELT**: transforming in memory is fine for <10k rows; for big data, load first and transform inside the database. Introduces `sodapy` as the Socrata Python client.

**Section 4 – Setup installs (cells 6–7).** `pip install sodapy` and `pip install -U prefect`: the only two third-party packages the extract step and orchestration need beyond pandas.

**Section 5 – Extract code and notes (cells 8–11 first half).** `extract_data()` is a Prefect task with 3 retries (10 s apart). It reads `SOCRATA_APP_TOKEN` from `.env` via `python-dotenv`, creates the client with a 30 s timeout, downloads everything with `get_all`, and returns a DataFrame. Cells 9–10 run it and show `head()`. The notes explain why: an unauthenticated client is throttled; the default timeout is too short; pandas is chosen because validation/transformation follow. → *Now `src/extract.py`.*

**Section 6 – Validate (cells 11 second half–13).** Lists four checks: expected columns, numeric ids, ISO-8601 datetimes, no missing required values (datetime, id, type, location). The code in cell 12 implements `check_valid_schema` (Counter comparison of columns), `check_numeric_id` (`pd.to_numeric(errors='coerce')`), `verify_datetime` (`datetime.fromisoformat`) and `check_missing_values`, combined in the `validate_data` task. Cell 13 discusses **fail loudly vs fail silently**: schema, datetime and missing-value failures raise errors; bad ids are tolerated and removed later. → *Now `src/validate.py`; the schema check was relaxed so an absent optional `subtype` does not abort the run.*

**Section 7 – Transform (cells 13–14).** Three transformations: remove duplicates by `id`, remove NaN-id rows, split `date_time` into year/month/day/hour/minute/second to make later group-by queries easy. Implemented in `transform_data`. → *Now `src/transform.py` (minute/second dropped as unused).*

**Section 8 – Load: setting up PostgreSQL (cells 15–27).** Explains that loading needs a Postgres instance, created via a Docker Compose file. Cell 16 lists installs (PostgreSQL, Docker). Cells 17–26 are Colab/Linux commands that install Postgres and Docker (`apt-get`), start the service, set the postgres password, test a `psycopg2` connection, install `docker-compose` and check versions. Cell 27 says "Complete upto this only" (the instructor's cut-off for the lab). → *Replaced by a Supabase project, so no local installation is needed.*

**Section 9 – Compose file and explanation (cells 28–29).** A `docker-compose.yml` with two services: `postgres_cpd` (Postgres 16, credentials from `.env`, port 5433→5432, named volume `pgdata_cpd` for persistence, restart policy) and `pgadmin` (a GUI on port 8081). The explanation covers `container_name`, `environment`, `ports` mapping, `volumes` for persistence, and the three-step pattern for any query: connect, execute, commit/close. → *Not needed with Supabase, which provides its own SQL editor in place of pgAdmin.*

**Section 10 – Load code (cells 30–31).** `create_postgres_table()` connects with psycopg2 and creates `cpd_incidents` with typed columns and `id` as primary key. `load_into_postgres` builds a SQLAlchemy engine and calls `df.to_sql(..., if_exists='replace')`. Note: `replace` drops and recreates the table, discarding the primary key just defined, and the f-string reuses double quotes (valid only on Python 3.12+). → *`src/load.py` uses a real upsert and a safe connection builder.*

**Section 11 – Defining the pipeline with Prefect (cells 31–33).** Compares Airflow (DAGs, heavier infrastructure) with Prefect (decorators, simple deployment, Prefect Cloud). Rules: `@task` on each step, `@flow` on the function chaining them. `etl()` calls extract → validate → transform → load with prints (shown in the Prefect UI via `log_prints=True`), and `etl.serve(cron="0 0 * * *")` creates a daily deployment. → *Kept the decorators and flow in `src/etl_pipeline.py`; scheduling moved to GitHub Actions.*

**Section 12 – Visualizing in Metabase (cells 34–39).** Compares Streamlit with BI tools (Tableau, Superset, Metabase) and picks Metabase for a quick proof of concept. The expanded compose file adds `metabase` (port 3000, application DB settings, healthcheck) and `postgres_mb` (Metabase's own Postgres), a shared bridge network `metanet1`, and two volumes. Cell 37 gives the connection string Metabase uses to reach `postgres_cpd`. Cell 39 lists dashboard findings: incidents peak mid-late afternoon, nearly all are type `INCIDENT`, and counts peaked around Aug–Oct 2025 then declined. Because Metabase queries the database live, the dashboard never shows stale data. → *Replaced by the Streamlit dashboard (same idea, free cloud hosting).*

**Notebook issues worth knowing:** `to_sql(replace)` drops the schema; Python 3.12-only f-string; healthcheck `interval: 15` lacks a unit; no `depends_on` for Metabase's database; cell 39 is prose in a code cell; cells 17–26 assume Colab/Linux; imports in cell 32 are commented out so it only works inside the notebook session.

## 17. How this maps to the course guidelines

| Guideline | Where it is met |
|---|---|
| 1. Practise and deploy the complete pipeline during lab hours | Run `python src/etl_pipeline.py`; the same repo deploys via GitHub Actions |
| 2. Documentation of the data pipeline | `README.md` (quick start) and this `Description.md` (full reference) |
| 3. Dashboard accessible through any PC | Vercel website (public URL) reading Supabase through `/api/data` |
| 4. Pipeline implemented on the cloud | Supabase (database), GitHub Actions (scheduler), Vercel (website) |
| 5. Implementation on your own system | Local run with `.env` + `python dashboard/dev.py` |
