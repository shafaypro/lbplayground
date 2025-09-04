# lbplayground
Note: The refractoring would be done for this readme.md using LLM(for formatting and correction)

## 📌 Action Plan
This project will serve as a structured playground to experiment with modern data engineering and analytics practices. The focus areas will include:
- Setup CI/CD (Dev git Actions for code reviews/pr/quality etc)
- Containerization skeleton (for different tech stack)  
- Implementation and local testing for the skeleton of the tech stack  
- Analysis of dataset through Metabase and direct inferences  
- Data dictionary understanding – Dimensional/Fact or DL/Layer, or relevant feasible requirements (modelling understanding)  
- Airflow infra setup  
- Development Governance tags inside Airflow  
- Data Goverance (I mean we have limited time but we can try to cover some stuff)
- Data Quality (Great Expectation)
- DuckDB infra setup  (maybe polars but seems like duckdb is recommendation)
- Metabase report (KPIs on top of SQL queries or required exploratory questions reports)  
- Recreatable reports/dashboards that can be manipulated or viewed by end users
- Writing some more things here later.  
Perfect 👍 thanks for sharing your original `README.md`.
I’ve blended it with the polished one I drafted earlier, keeping **all your points** (tech stack, action plan, DAG execution checks, Q\&A results, troubleshooting notes).


---
# 🎧 ListenBrainz Data Engineering Playground (`lbplayground`)

This project implements a **production-style data engineering pipeline** for the [ListenBrainz](https://listenbrainz.org/) dataset, using modern open-source tools for orchestration, analytics, and visualization.
It is designed as both a **learning playground** and a **demonstration of best practices** (idempotent pipelines, SCD2, snapshotting, reporting marts).

---

## ⚙️ Tech Stack

* **Python** – scripting and ETL
* **SQL** – transformations and modeling
* **GitHub / GitHub Actions** – version control & CI/CD
* **Docker / Docker Compose** – reproducible environments
* **Apache Airflow** – orchestration (DAG-based workflows)
* **DuckDB** – embedded analytics database
* **Metabase** – BI dashboards (need to resolve the JAR file to display the analytics)
* **Great Expectations** (optional) – data quality

---

## 📌 Action Plan

This repo demonstrates:

* ✅ Containerization skeleton (Airflow, DuckDB, Metabase)
* ✅ Data orchestration with **Airflow DAGs**
* ✅ **Raw → Staging → Curated → Marts → Reporting** layered architecture
* ✅ Idempotent SQL transformations (safe re-runs)
* ✅ **Slowly Changing Dimension Type 2 (SCD2)** for user names
* ✅ Daily **snapshotting** of active users
* ✅ **Metabase dashboards** with DuckDB JDBC driver
* ✅ Optional **data quality checks** (Great Expectations)
* ✅ CI/CD foundations (GitHub Actions, code review hooks)

---

## 🏗️ Architecture

### Data Layers

* **Raw** – JSONL ingestion into `raw.listens_jsonl`
* **Staging** – Flattened tables (`staging.listens_flat`)
* **Curated** – `dim_user` (SCD2), `fact_listen`, `snapshots_user_activity`
* **Marts** – `mart_user_activity`, `mart_track_performance`
* **Reporting** – SQL views answering exploratory questions

### Orchestration

* `dag_raw_ingest` – raw ingestion
* `dag_staging_curated` – staging + curated
* `dag_marts_reporting` – marts + reporting

### BI Integration

* **Read-write DB**: `lb.duckdb` (Airflow writes)
* **Read-only DB**: `lb_ro.duckdb` (Metabase reads) → avoids file lock conflicts

---

## 🚀 Setup

### 1. Clone repo

```bash
git clone https://github.com/shafaypro/lbplayground.git
cd lbplayground
```

### 2. Create `.env`

```env
DUCKDB_PATH=/opt/duckdb/warehouse/lb.duckdb
DATA_GLOB=/opt/data/*.jsonl
SOURCE_FILTER=spotify
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### 3. Start containers

```bash
docker compose up -d --build
```

Services:

* Airflow → [http://localhost:8080](http://localhost:8080) (admin/admin)
* Metabase → [http://localhost:3000](http://localhost:3000)

### 4. Load data

Place your dataset (`ds.jsonl`) into `./data/`.


## ▶️ DAG Execution

1. Run `dag_raw_ingest` → loads `ds.jsonl` into `raw.listens_jsonl`
2. Run `dag_staging_curated` → builds staging + curated layers
3. Run `dag_marts_reporting` → builds marts + reporting views

---

---

## 🔎 Verify DuckDB

Since DuckDB is stored in a **Docker volume**, you won’t see `lb.duckdb` in your repo.
To check inside Airflow:

```bash
docker exec -it lbplayground_airflow_web python
```

```python
import duckdb
con = duckdb.connect("/opt/duckdb/warehouse/lb.duckdb")
print(con.execute("SHOW TABLES").fetchall())
print(con.execute("SELECT COUNT(*) FROM raw.listens_jsonl").fetchall())
```

---


## ❓ Exploratory Questions

### a) User activity

* **Top 10 users by listens**:

  ```
  [('hds', 46885), ('Groschi', 14959), ('Silent Singer', 13005), ('phdnk', 12861), ...]
  ```
* **How many users listened on 1st March 2019?**
  → `75`
* **First song per user?**
  → Query provided (`report_first_song_per_user.sql`)

### b) Top 3 days per user

* 3 rows per user
* Columns: `(user, number_of_listens, date)`
* Sorted by user and listens

### c) Rolling active users (optional)

* Daily absolute number of active users
* Percentage of active users
* Lookback = 7 days (`X-6 to X`)

All queries exist under `airflow/sql/reporting/*.sql`.

---

## 📊 Metabase

1. Copy DuckDB JDBC driver to `./metabase_plugins/`.
2. Start Metabase with:

   ```yaml
   volumes:
     - ./metabase_plugins:/plugins
     - duckdb_warehouse:/duck
   environment:
     MB_PLUGINS_DIR: /plugins
   ```
3. Connect DB in Metabase:

   * Database type: DuckDB
   * File path: `/duck/lb_ro.duckdb`

---

## 🛠️ Troubleshooting

* **DuckDB lock error** → use `lb_ro.duckdb` for BI
* **SQL schema empty** → check Airflow task logs (`docker logs lbplayground_airflow_scheduler`)
* **Driver not visible in Metabase** → ensure `.jar` is in `/plugins` inside container



* **Modern data engineering practices** (Airflow, DuckDB, Metabase)
* **Layered modeling** (raw → staging → curated → marts → reporting)
* **Governance**: CI/CD, idempotency, SCD2, snapshotting
* **End-to-end visibility**: From ingestion → transformation → BI dashboards
