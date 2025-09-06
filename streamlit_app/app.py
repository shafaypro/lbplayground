import streamlit as st
import duckdb
import pandas as pd
from typing import List, Optional

# -------------------------------
# Page Config
# -------------------------------
st.set_page_config(
    page_title="🎧 ListenBrainz Assignment Dashboard",
    layout="wide"
)

# -------------------------------
# DuckDB Connection (read-only copy)
# -------------------------------
DB_PATH = "/opt/duckdb/warehouse/lb_ro.duckdb"

@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_connection()

@st.cache_data(show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    try:
        return con.execute(query).df()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

# -------------------------------
# Flexible column pickers
# -------------------------------
def pick_col(
    df: pd.DataFrame,
    preferred: List[str],
    contains_any: Optional[List[str]] = None,
    require_numeric: bool = False,
) -> Optional[str]:
    if df.empty:
        return None

    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}

    for name in preferred:
        if name.lower() in lower_map:
            return lower_map[name.lower()]

    if contains_any:
        for c in cols:
            lc = c.lower()
            if any(tok in lc for tok in contains_any):
                if not require_numeric or pd.api.types.is_numeric_dtype(df[c]):
                    return c

    if require_numeric:
        for c in cols:
            if pd.api.types.is_numeric_dtype(df[c]):
                return c
        return None
    else:
        for c in cols:
            if df[c].dtype == 'object' or pd.api.types.is_string_dtype(df[c]):
                return c
        return cols[0] if cols else None

def show_cols_hint(df: pd.DataFrame):
    st.caption(f"Columns: `{', '.join(df.columns)}`")

# -------------------------------
# UI
# -------------------------------
st.title("🎧 LB Playground Dashboard")
st.markdown("This dashboard answers the challenge using the **reporting views** built by Airflow over DuckDB.")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Q1: Top 10 Users",
    "Q2: Users on 2019-03-01",
    "Q3: First Song per User",
    "Q4: Top 3 Days per User",
    "Q5: Daily Active Users (Optional)",
    "Production Setup",
    "All Reports + Monitoring"
])

# -------------------------------
# Q1: Top 10 Users
# -------------------------------
with tab1:
    st.subheader("Q1: Who are the top 10 users by number of songs listened?")
    df = run_query("SELECT * FROM reporting.report_top10_users")
    if df.empty:
        st.warning("No data returned. Make sure the reporting views are built.")
    else:
        user_col = pick_col(df, preferred=["user_id", "user_name", "user"], contains_any=["user"])
        listens_col = pick_col(df, preferred=["number_of_listens", "listens", "count"], contains_any=["listen", "count"], require_numeric=True)
        show_cols_hint(df)

        if user_col and listens_col:
            st.bar_chart(df.set_index(user_col)[listens_col])
            st.dataframe(df)
        else:
            st.error(f"Couldn't find appropriate user/listen columns. user_col={user_col}, listens_col={listens_col}")

# -------------------------------
# Q2: Users on 2019-03-01
# -------------------------------
with tab2:
    st.subheader("Q2: How many users listened on 1st March 2019?")
    df = run_query("SELECT * FROM reporting.report_users_on_2019_03_01")
    if df.empty:
        st.warning("No data returned.")
    else:
        show_cols_hint(df)
        count_col = pick_col(df, preferred=["users_who_listened"], require_numeric=True)
        if not count_col:
            count_col = pick_col(df, contains_any=["user"], require_numeric=True)
        if not count_col:
            count_col = pick_col(df, require_numeric=True)

        if count_col:
            val = int(df[count_col].iloc[0])
            st.metric("Users Active on 2019-03-01", val)
            st.dataframe(df)
        else:
            st.error("Couldn't detect a numeric count column for active users.")

# -------------------------------
# Q3: First Song per User
# -------------------------------
with tab3:
    st.subheader("Q3: For every user, what was the first song listened?")
    df = run_query("SELECT * FROM reporting.report_first_song_per_user")
    if df.empty:
        st.warning("No data returned.")
    else:
        show_cols_hint(df)
        user_col = pick_col(df, preferred=["user_id", "user_name", "user"], contains_any=["user"])
        if user_col:
            sel_user = st.selectbox("Filter by user", ["All"] + sorted(df[user_col].astype(str).unique().tolist()))
            if sel_user != "All":
                df = df[df[user_col].astype(str) == sel_user]
        st.dataframe(df, use_container_width=True)

# -------------------------------
# Q4: Top 3 Days per User
# -------------------------------
with tab4:
    st.subheader("Q4: Top 3 listening days per user (user, number_of_listens, date)")
    df = run_query("SELECT * FROM reporting.report_top3_days_per_user")
    if df.empty:
        st.warning("No data returned.")
    else:
        show_cols_hint(df)
        user_col = pick_col(df, preferred=["user_id", "user_name", "user"], contains_any=["user"])
        listens_col = pick_col(df, preferred=["number_of_listens", "listens", "count"], contains_any=["listen","count"], require_numeric=True)
        date_col = pick_col(df, preferred=["date", "listened_date", "dt"], contains_any=["date","day"])
        if user_col and listens_col and date_col:
            df_sorted = df.sort_values([user_col, listens_col], ascending=[True, False])
            pick = st.selectbox("Select user", sorted(df[user_col].astype(str).unique().tolist()))
            df_user = df_sorted[df_sorted[user_col].astype(str) == pick]
            st.bar_chart(df_user.set_index(date_col)[listens_col])
            st.dataframe(df_user)
        else:
            st.error(f"Couldn't find user/listens/date columns. user_col={user_col}, listens_col={listens_col}, date_col={date_col}")

# -------------------------------
# Q5: Daily Active Users (Optional)
# -------------------------------
with tab5:
    st.subheader("Q5 (Optional): Daily active users & percentage")
    df = run_query("SELECT * FROM reporting.report_daily_active_users")
    if df.empty:
        st.warning("No data returned.")
    else:
        show_cols_hint(df)
        date_col = pick_col(df, preferred=["date","dt","day"], contains_any=["date","day"])
        active_col = pick_col(df, preferred=["number_active_users","active_users"], contains_any=["active","users"], require_numeric=True)
        pct_col = pick_col(df, preferred=["percentage_active_users","pct_active"], contains_any=["percent","pct"], require_numeric=True)
        if date_col and active_col:
            chart_cols = [active_col] + ([pct_col] if pct_col else [])
            st.line_chart(df.set_index(date_col)[chart_cols])
            st.dataframe(df)
        else:
            st.error(f"Couldn't find date/active columns. date_col={date_col}, active_col={active_col}, pct_col={pct_col}")

# -------------------------------
# Production Setup
# -------------------------------
with tab6:
    st.subheader("🏗️ Production Setup")
    st.markdown("""
- **Orchestration**: Apache Airflow runs layered SQL:
  - Raw → Staging → Curated (SCD2) → Marts → Reporting (views)
- **Warehouse**: DuckDB (`lb.duckdb`) for writes; **read-only** replica (`lb_ro.duckdb`) for BI (this app).
- **Data refresh**:
  - Daily (or @once bootstrap) DAGs build reporting views used here.
  - Optional task at the end copies `lb.duckdb` → `lb_ro.duckdb` to avoid locks.
- **Quality**: Great Expectations / lightweight checks (row counts, nulls, referential integrity).
- **CI/CD**: GitHub Actions (lint DAGs/SQL, unit tests for Python transforms).
- **Scalability**: swap DuckDB with cloud DW (Snowflake/BigQuery) with minimal app changes.
    """)

# -------------------------------
# All Reports + Monitoring
# -------------------------------
with tab7:
    st.subheader("📊 All Reports in One Place")

    st.markdown("### Q1: Top 10 Users")
    st.dataframe(run_query("SELECT * FROM reporting.report_top10_users"))

    st.markdown("### Q2: Users on 2019-03-01")
    st.dataframe(run_query("SELECT * FROM reporting.report_users_on_2019_03_01"))

    st.markdown("### Q3: First Song per User")
    st.dataframe(run_query("SELECT * FROM reporting.report_first_song_per_user"))

    st.markdown("### Q4: Top 3 Days per User")
    st.dataframe(run_query("SELECT * FROM reporting.report_top3_days_per_user"))

    st.markdown("### Q5: Daily Active Users (7-day Rolling)")
    st.dataframe(run_query("SELECT * FROM reporting.report_daily_active_users"))

    st.markdown("---")
    st.subheader("🛡️ Monitoring: Data Quality Results")

    # exists = run_query("""
    # SELECT COUNT(*) AS cnt
    # FROM information_schema.tables
    # WHERE lower(table_schema) = 'monitoring'
    #   AND lower(table_name) = 'data_quality_results'
    # """)

    # if not exists.empty and exists["cnt"].iloc[0] > 0:
    dq = run_query("""
        SELECT *
        FROM monitoring.data_quality_results
        ORDER BY checked_at DESC
        LIMIT 50
    """)
    if dq.empty:
        st.warning("No DQ results found. Run dag_data_quality first.")
    else:
        st.dataframe(dq)
    # else:
    #     st.info("⚠️ Monitoring table does not exist yet. Run dag_data_quality once to initialize it.")

