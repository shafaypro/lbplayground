## Inspect what’s in the DB

Run this inside the container Python REPL again:

docker exec -it lbplayground_airflow_web python (run up the python shell inside the web airflow)

```python
import duckdb
con = duckdb.connect("/opt/duckdb/warehouse/lb.duckdb")

# schemas & tables

print(con.execute("SHOW TABLES FROM raw").fetchall())

# peek raw rows
print(con.execute("SELECT * FROM raw.listens_jsonl LIMIT 3").fetchdf())
```

## Run the next DAG (staging + curated)

In Airflow UI:

1. Unpause/trigger **`dag_staging_curated`**.
   This should create:

* `staging.listens_flat`
* `production_curated.dim_user` (SCD2)
* `production_curated.fact_listen`
* `production_curated.snapshots_user_activity` (append-only)

verify:

```python
print(con.execute("SELECT COUNT(*) FROM staging.listens_flat").fetchall())
print(con.execute("SELECT * FROM staging.listens_flat LIMIT 5").fetchdf())
print(con.execute("SELECT COUNT(*) FROM production_curated.fact_listen").fetchall())
```

## Then run marts + reporting

Trigger **`dag_marts_reporting`**. It builds:

* `production_marts.mart_user_activity`
* `production_marts.mart_track_performance`
* Views in `reporting.*` (top10 users, first song per user, top 3 days per user, DAU)

Quick checks:

```python
print(con.execute("SELECT * FROM reporting.report_top10_users").fetchdf())
print(con.execute("SELECT * FROM reporting.report_users_on_2019_03_01").fetchdf())
print(con.execute("SELECT * FROM reporting.report_first_song_per_user LIMIT 10").fetchdf())
print(con.execute("SELECT * FROM reporting.report_top3_days_per_user LIMIT 10").fetchdf())
print(con.execute("SELECT * FROM reporting.report_daily_active_users LIMIT 10").fetchdf())

print(con.execute("SELECT COUNT(*) FROM reporting.report_daily_active_users").fetchall())

print(con.execute("SELECT COUNT(*) FROM reporting.report_first_song_per_user").fetchall())

print(con.execute("SELECT COUNT(*) FROM reporting.report_top3_days_per_user").fetchall())

print(con.execute("SELECT COUNT(*) FROM reporting.report_top10_users").fetchall())

print(con.execute("SELECT COUNT(*) FROM reporting.report_users_on_2019_03_01").fetchall())

```

## Metabase
Its recommended to use the lb_ro onlyh one not the original duckdb file.
Point Metabase to `/duck/lb_ro.duckdb` and build a small dashboard from the `reporting` schema.
