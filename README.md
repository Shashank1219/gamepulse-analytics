<div align="center">

```
  ▄████  ▄▄▄       ███▄ ▄███▓▓█████  ██▓███   █    ██  ██▓      ██████ ▓█████ 
 ██▒ ▀█▒▒████▄    ▓██▒▀█▀ ██▒▓█   ▀ ▓██░  ██▒ ██  ▓██▒▓██▒    ▒██    ▒ ▓█   ▀ 
▒██░▄▄▄░▒██  ▀█▄  ▓██    ▓██░▒███   ▓██░ ██▓▒▓██  ▒██░▒██░    ░ ▓██▄   ▒███   
░▓█  ██▓░██▄▄▄▄██ ▒██    ▒██ ▒▓█  ▄ ▒██▄█▓▒ ▒▓▓█  ░██░▒██░      ▒   ██▒▒▓█  ▄ 
░▒▓███▀▒ ▓█   ▓██▒▒██▒   ░██▒░▒████▒▒██▒ ░  ░▒▒█████▓ ░██████▒▒██████▒▒░▒████▒
 ░▒   ▒  ▒▒   ▓▒█░░ ▒░   ░  ░░░ ▒░ ░▒▓▒░ ░  ░░▒▓▒ ▒ ▒ ░ ▒░▓  ░▒ ▒▓▒ ▒ ░░░ ▒░ ░
  ░   ░   ▒   ▒▒ ░░  ░      ░ ░ ░  ░░▒ ░     ░░▒░ ░ ░ ░ ░ ▒  ░░ ░▒  ░ ░ ░ ░  ░
░ ░   ░   ░   ▒   ░      ░      ░   ░░        ░░░ ░ ░   ░ ░   ░  ░  ░     ░   
      ░       ░  ░       ░      ░  ░            ░         ░  ░      ░     ░  ░
```

# 🎮 GamePulse Analytics Platform

### *Where player events become product intelligence*

[![Status](https://img.shields.io/badge/status-live-success)](https://github.com)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://python.org)
[![Databricks](https://img.shields.io/badge/Databricks-Delta_Lake-red)](https://databricks.com)
[![dbt](https://img.shields.io/badge/dbt-1.11-orange)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.9-teal)](https://airflow.apache.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**An end-to-end batch data pipeline modelling the analytics stack of a casual mobile gaming company**

[Architecture](#-architecture) · [Quickstart](#-quickstart) · [The Stack](#-the-stack) · [The Layers](#-the-seven-layers) · [Results](#-results)

</div>

---

## 🕹️ The Premise

Imagine you are sitting in the data team of a casual mobile game studio. Tens of millions of players tap, swipe, and spend across your titles every day. Every action fires an event. Every event needs to land somewhere reliable, get cleaned up, get modelled into something an analyst can use, and end up on a dashboard before the next standup.

**GamePulse is that pipeline, built from scratch.** It simulates 50,000 players over 210 days, generates **41 million events** across five event types, and pushes them through seven engineered layers ending in a Power BI dashboard.

It exists for one reason: to prove that the patterns used at real gaming companies (Databricks, Delta Lake, dbt, Airflow, S3) can be wielded end-to-end by one person, with real data and real tests.

---

## 🗺️ Architecture

```
        ┌────────────────────────────────────────────────────────┐
        │  🎲  LAYER 1  ·  Python Event Generator                │
        │      50,000 players · 5 event types · 210 days         │
        └─────────────────────────┬──────────────────────────────┘
                                  │  Parquet (Snappy)
                                  ▼
        ┌────────────────────────────────────────────────────────┐
        │  🪣  LAYER 2  ·  AWS S3 Raw Landing Zone               │
        │      s3://gamepulse-raw/events/date=YYYY-MM-DD/        │
        └─────────────────────────┬──────────────────────────────┘
                                  │  Unity Catalog · IAM role
                                  ▼
        ┌────────────────────────────────────────────────────────┐
        │  ⚡  LAYER 4  ·  Databricks + Delta Lake               │
        │      PySpark · schema validation · dedup · OPTIMIZE    │
        └─────────────────────────┬──────────────────────────────┘
                                  │
                                  ▼
        ┌────────────────────────────────────────────────────────┐
        │  🧪  LAYER 5  ·  dbt Transformation                    │
        │      Staging → Intermediate → Marts                    │
        │      9 models · 44 tests · 6 custom SQL assertions     │
        └────────┬──────────────────────────────────────┬────────┘
                 │                                      │
                 ▼                                      ▼
   ┌────────────────────────┐              ┌────────────────────────┐
   │  🎯 A/B Test Pipeline  │              │  📊 LAYER 6 · Power BI│
   │  Variant leakage check │              │  Operations · Product  │
   └────────────────────────┘              └────────────────────────┘

   ┌────────────────────────────────────────────────────────────────┐
   │  🛠️  LAYER 3 · Apache Airflow Orchestration  (runs the chain)  │
   │  🚀 LAYER 7 · OPTIMIZE + ZORDER performance write-up           │
   └────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ The Stack

| Layer | Tool | Why |
|---|---|---|
| 🎲 Generation | Python · Faker · PyArrow · boto3 | Realistic synthetic event data at scale |
| 🪣 Storage | AWS S3 (Parquet · Snappy) | Cheap, partitioned, queryable from any compute |
| 🛠️ Orchestration | Apache Airflow 2.9 (Docker) | Daily DAG: sense → validate → trigger → transform → test |
| ⚡ Compute | Databricks Serverless · PySpark | Industry standard for gaming-scale event processing |
| 💎 Storage Format | Delta Lake | ACID, schema enforcement, time travel, ZORDER clustering |
| 🧪 Transformation | dbt-databricks | Version-controlled SQL with built-in testing |
| 📊 BI | Power BI (DirectQuery) | Live dashboards on top of the warehouse |
| 🤖 Automation | GitHub Actions | Cloud scheduling that does not need a local machine |

---

## 🎮 The Seven Layers

### 🎲 Layer 1 · The Event Generator

A Python simulator that builds 50,000 player profiles once and evolves them across 210 days. Each player belongs to one of four segments (`casual`, `mid-core`, `whale`, `churned`) with different session probabilities and purchase patterns. Treatment group users carry a 20% conversion uplift, which means the A/B test mart shows a real, measurable lift downstream.

Five event types are generated, each with rich domain-specific fields:

| Event | What it represents |
|---|---|
| `session_start` | Player opens the game · acquisition + lifetime context |
| `level_complete` | Player finishes a level · attempts, time, stars, boosters |
| `purchase_made` | Real-money transaction · SKU, price, local currency, promo |
| `powerup_used` | In-game item activation · inventory delta, outcome |
| `ad_watched` | Ad impression · network, format, completion, revenue |

Every event carries 13 shared identity fields built by a single `build_base_event()` function. Specific fields are merged on top. No duplication, no drift.

> **🎯 Design note:** Schema design deliberately mirrors retail transaction patterns. `session_start` = store visit. `purchase_made` = retail transaction with SKU and campaign attribution. The analytical questions are the same; only the domain changed.

---

### 🪣 Layer 2 · The Landing Zone

Generated events are written directly to **`s3://gamepulse-raw/events/date=YYYY-MM-DD/`** as Parquet files with Snappy compression. Date partitioning lets Spark prune at read time. The bucket is accessed by Databricks via a Unity Catalog external location backed by an IAM role with `sts:AssumeRole` trust, not by hardcoded keys.

---

### 🛠️ Layer 3 · The Orchestrator

A single Airflow DAG (`gamepulse_daily`) runs every day at 17:00 CEST and chains the entire batch:

```
sense_s3_partition  →  validate_event_volume  →  notify_ingestion
                                              →  notify_dbt_models
                                              →  notify_dbt_tests
```

`validate_event_volume` compares today's partition size to yesterday's and raises a soft alert if the drop exceeds 30%. The Databricks ingestion notebook and dbt transformation run as manual operator steps. The DAG logs reminders for both and marks tasks as successful to keep the run history clean. In production on MWAA or Astronomer, both steps would execute automatically via the Jobs API and dbt-databricks respectively.

Airflow runs locally in Docker (Postgres backend, LocalExecutor). All credentials come from a Git-ignored `.env` file injected at runtime via `${VAR}` interpolation in `docker-compose.yml`.

> **📋 Daily operator routine:** The pipeline has two manual touchpoints by design. Run the Databricks ingestion notebook from the Databricks UI, then run `dbt run && dbt test` locally. Airflow logs both as task notifications so the run history stays complete and auditable.
---

### ⚡ Layer 4 · The Ingestion

The Databricks notebook `01_ingest_raw` reads Parquet from S3, applies schema enforcement, casts numeric types correctly, drops rows missing critical identity fields, deduplicates on `event_id`, normalises timestamps to UTC, derives an `event_date` partition column, and writes everything into one wide Delta table: **`raw.game_events`**, partitioned by `event_date` and `event_type`.

> **Why wide?** Five event types, one table, nullable specific columns. One ingestion job. One staging model. One set of tests. The alternative (one table per event type) means five of everything and makes cross-event joins painful.

**Real numbers:**
- 41,693,475 rows ingested
- 209 daily partitions  
- All 5 event types present
- Snappy-compressed Delta files on S3

---

### 🧪 Layer 5 · The Transformation

A dbt project with three layers and **44 tests** that actually catch things.

```
models/
├── staging/
│   └── stg_game_events.sql           ← cast, dedup, normalize
├── intermediate/
│   ├── int_sessions.sql              ← sessionisation via aggregations
│   └── int_experiment_assignments.sql ← A/B leakage detection
└── marts/
    ├── fct_daily_active_users.sql    ← DAU by date and country
    ├── fct_retention.sql             ← Day 0/1/7/14/30 retention curves
    ├── fct_revenue.sql               ← ARPU, ARPPU, new payers
    ├── fct_funnel.sql                ← Level drop-off and booster rates
    ├── fct_ab_test_results.sql       ← Conversion with leakage exclusion
    └── dim_users.sql                 ← One row per player profile
```

**Six custom SQL tests** that go beyond `not_null` and `unique`:

| Test | Technique |
|---|---|
| `assert_valid_purchase_prices` | Multi-currency price validation |
| `assert_currency_matches_country` | Country-to-currency consistency check |
| `assert_ad_reward_balance_consistency` | Reward should never decrease balance |
| `assert_session_duration_under_24hrs` | CTE with `MIN/MAX` window per session |
| `assert_first_purchase_flag_consistency` | `ROW_NUMBER() + SUM() OVER` window functions, two failure modes in one query |
| `assert_no_session_before_install` | Temporal logic via CTE join with `MIN() OVER` |

> **🐛 The bug the tests caught:** The window function test flagged 8,786 rows where `is_first_purchase` was set incorrectly. Root cause was the generator pre-seeding purchase counts. The test was doing its job; the data was lying.

---

### 📊 Layer 6 · The Dashboard

Power BI connects to Databricks via **DirectQuery** using the SQL warehouse HTTP path. Every chart queries the dbt mart tables in real time, so the dashboard always reflects the latest pipeline run.

Two views:

- **🛠️ Operations** · DAU trend, revenue by day, paid user ratio, session volume health  
- **🎯 Product** · Retention curves by cohort, funnel drop-off, A/B test conversion comparison

---

### 🚀 Layer 7 · The Optimisation

Documented in `03_optimization_writeup.py` with real before/after measurements from the Spark UI.

The original deduplication used `ROW_NUMBER() OVER (PARTITION BY event_id)`. UUIDs have extremely high cardinality. When Spark shuffles by a UUID, no executor has data locality, every executor talks to every other executor, shuffle volume explodes.

The fix:
1. **`dropDuplicates()`** instead of window function — hash-based, not sort-based
2. **`repartition(event_date, event_type)`** to match the Delta write layout
3. **`OPTIMIZE ... ZORDER BY (user_id, event_timestamp)`** to compact files and cluster data physically

`OPTIMIZE + ZORDER` compacted **4,224 files in 8.59 seconds**. Downstream queries that filter by `user_id` now scan a fraction of the files they used to.

---

## 🤖 Automation

Two GitHub Actions workflows in `.github/workflows/`:

| Workflow | Schedule | Job |
|---|---|---|
| `daily_generator.yml` | 13:00 UTC | Generate today's events, push to S3, verify partition |
| `daily_pipeline.yml` | 15:00 UTC | Confirm S3 partition exists, summarise the day |

All credentials live as encrypted GitHub Secrets. Workflow files contain only `${{ secrets.NAME }}` references and are safe to publish.

---

## 🚀 Quickstart

> **Requirements:** WSL2 (or Linux/macOS), Python 3.10+, Docker, AWS account, Databricks workspace

```bash
# 1. Clone
git clone https://github.com/Shashank1219/gamepulse-analytics.git
cd gamepulse-analytics

# 2. Python env
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# fill in AWS_*, DATABRICKS_* values

# 4. Generate the data
python3 data_generator/generate_events.py

# 5. Ingest in Databricks
# → import databricks/notebooks/01_ingest_raw.py and run all cells

# 6. Transform
cd dbt/gamepulse
dbt run && dbt test

# 7. Orchestrate (optional)
cd airflow
docker-compose up -d
# → http://localhost:8080 · admin / admin
```

---

## 📁 Project Layout

```
gamepulse-analytics/
├── 🎲 data_generator/
│   └── generate_events.py
├── 🪣 data/raw/events/                  ← local JSON for debugging (gitignored)
├── 🛠️ airflow/
│   ├── dags/gamepulse_daily.py
│   └── docker-compose.yml
├── ⚡ databricks/notebooks/
│   ├── 01_ingest_raw.py
│   └── 03_optimization_writeup.py
├── 🧪 dbt/gamepulse/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/                           ← 6 custom SQL assertions
├── 🤖 .github/workflows/
│   ├── daily_generator.yml
│   └── daily_pipeline.yml
└── 📖 README.md
```

---

## 🏆 Results

```
   ╔══════════════════════════════════════════════════════════╗
   ║                                                          ║
   ║   📦  41,693,475 events ingested across 209 days         ║
   ║   🎮  50,000 simulated players · 4 segments              ║
   ║   📊  9 dbt models · 44 tests · 6 custom assertions      ║
   ║   ⚡  4,224 Delta files compacted via OPTIMIZE           ║
   ║   🎯  A/B test pipeline with variant leakage detection   ║
   ║   🛠️   Fully orchestrated, partially cloud-automated      ║
   ║                                                          ║
   ╚══════════════════════════════════════════════════════════╝
```

**Retention curve produced by the pipeline:**

| Day | Retention |
|---:|:---|
| Day 0  | 100.00% |
| Day 1  | 49.54%  |
| Day 7  | 47.72%  |
| Day 14 | 46.99%  |
| Day 30 | 46.66%  |

The curve drops sharply between Day 0 and Day 1 then flattens, which is the shape every casual game PM stares at every Monday.

---

## 👤 Author

**Shashank Prakash** · Data & Analytics Engineer · Berlin 🇩🇪

3+ years building scalable pipelines and BI on dbt, Databricks, Snowflake, Power BI, and Looker. Previously at Lowe's India Services where retail transaction data taught me everything I needed to know about purchase events, just at a different scale.

[`linkedin`](https://linkedin.com/in/shashank-prakash) · [`portfolio`](https://shashank-prakash.vercel.app) · [`email`](mailto:shashank.prakash1997@outlook.com)

---

<div align="center">

*Built with curiosity and a lot of `dbt run`.* 🎮

</div>