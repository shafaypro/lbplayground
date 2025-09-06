# 🎵 LBPlayGround

## 1. Data Modelling

We start with **raw event data** in **JSONL (newline-delimited JSON)** format, representing individual listens from users.
Each record contains user info, track metadata, and the timestamp of the listen.

Directly querying JSONL is inefficient and brittle.
So we designed a **multi-layered data model** to gradually refine raw data into analytics-ready tables:

* **Raw Layer (`raw`)**

  * Stores the ingested JSONL as-is.
  * Goal: preserve schema drift, ensure reproducibility.

* **Staging Layer (`staging`)**

  * Flattens nested JSON fields into relational columns (`user`, `track`, `artist`, `listened_at`, etc.).
  * Provides a **clean, normalized event table**.

* **Curated Layer (`production_curated`)**

  * Introduces **Slowly Changing Dimension (SCD2)** tables (e.g. `dim_user`) to track user renames.
  * Builds **fact tables** (`fact_listen`) that join staging data with current dimension attributes.
  * Purpose: consistent surrogate keys, historization, analytics-grade data.

* **Reporting Layer (`reporting`)**

  * Defines **views** to directly answer business questions.
  * Ensures separation of ETL (data prep) from analytics (querying).

This layered approach enables:
✅ Data quality & reproducibility
✅ Separation of concerns
✅ Historical accuracy (SCD2)
✅ Simple queries for analysts

---

## 2. SQL Queries

### a) Staging – Flattening JSON

**File:** `staging/listens_flat.sql`

* Extracts key fields: `user_id`, `user_name`, `track_id`, `artist_name`, `track_name`, `release_name`, `listened_at`, `listened_date`, `source`.
* Provides a **normalized base table** for listens.

---

### b) Dimension – User SCD2

**File:** `production_curated/dim_user_scd2.sql`

* Implements **Slowly Changing Dimension Type 2** for `dim_user`.
* Tracks changes in `user_name` over time while keeping a stable `user_id`.
* Ensures **historical consistency** when users rename themselves.

---

### c) Fact Table – Listens

**File:** `production_curated/fact_listen.sql`

* Joins `staging.listens_flat` with `dim_user`.
* Produces `fact_listen`, the **canonical listen fact table** at the grain of **one row per listen event**:

  * `user_id`, `current_user_name` (SCD2 surrogate key + attribute)
  * `track_id`, `track_name`, `artist_name`, `release_name`
  * `listened_ts`, `listened_date`, `source`

---

### d) Reporting – Business Questions

1. **Top 10 Users by Number of Songs Listened**
   **File:** `report_top10_users.sql`
   Aggregates `fact_listen` by `user_id`. Returns top 10 listeners.

2. **Users Who Listened on 2019-03-01**
   **File:** `report_users_on_2019_03_01.sql`
   Counts distinct users active on March 1, 2019.
   ✅ Example result: **75 users**

3. **First Song per User**
   **File:** `report_first_song_per_user.sql`
   Uses window functions to capture each user’s first listen.

4. **Top 3 Days per User by Listens**
   **File:** `report_top3_days_per_user.sql`
   For each user, finds the 3 days with the most listens.
   Returns `(user, number_of_listens, date)` sorted by user and listens.

5. **Daily Active Users (7-day Rolling Window)**
   **File:** `report_daily_active_users.sql`
   Defines “active” = listened ≥1 track in `[X-6, X]`.
   Outputs daily:

   * Absolute number of active users
   * % of active users among all users

---

## 3. Why This Approach?

* **Scalability**: JSON → staging → curated → reporting ensures queries stay performant.
* **Maintainability**: Analysts can write simple queries against reporting views without parsing JSON.
* **Accuracy**: SCD2 ensures renames don’t break history.
* **Flexibility**: Business logic (e.g., “active users”) lives in views, not hardcoded in ETL.
* **Safety**: Analysts query against a **read-only snapshot (`lb_ro.duckdb`)**, ensuring BI tools don’t conflict with ETL jobs.

---

## 4. Usage

1. Load raw JSONL files into DuckDB:

   ```sql
   CREATE OR REPLACE TABLE raw.listens_jsonl AS
   SELECT *
   FROM read_json_auto('/opt/data/*.jsonl', format='newline_delimited');
   ```

2. Build staging tables:

   ```sql
   CREATE OR REPLACE TABLE staging.listens_flat AS
   SELECT ... FROM raw.listens_jsonl;
   ```

3. Update dimensions (`dim_user_scd2.sql`).

4. Rebuild fact tables (`fact_listen.sql`).

5. Create reporting views to answer analytics questions.

6. At the end of the ETL pipeline, publish a **read-only copy**:

   ```sql
   PRAGMA database_list;
   -- then
   EXPORT DATABASE '/opt/duckdb/warehouse/lb_ro.duckdb';
   ```

   BI tools like Metabase connect to `lb_ro.duckdb`.
