import os
import pathlib

import duckdb

# Environment-driven config (populated from your .env via docker-compose)
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/duckdb/warehouse/lb.duckdb")
SQL_DIR = pathlib.Path("/opt/airflow/sql")

DATA_GLOB = os.getenv("DATA_GLOB", "/opt/data/ds.jsonl")
SOURCE_FILTER = os.getenv("SOURCE_FILTER", "spotify")


def run_sql_file(relpath: str) -> None:
    """
    Read a SQL file from /opt/airflow/sql/<relpath>, substitute simple placeholders,
    and execute it against the DuckDB file specified by DUCKDB_PATH.
    """
    path = SQL_DIR / relpath
    sql = path.read_text()
    sql = sql.replace("{{DATA_GLOB}}", DATA_GLOB).replace(
        "{{SOURCE_FILTER}}", SOURCE_FILTER
    )
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.execute("PRAGMA threads=4;")
        con.execute(sql)
    finally:
        con.close()
