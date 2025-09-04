CREATE SCHEMA IF NOT EXISTS reporting;

CREATE OR REPLACE VIEW reporting.report_top3_days_per_user AS
WITH daily AS (
  SELECT
    COALESCE(f.current_user_name, f.user_name) AS "user",
    f.listened_date AS "date",
    COUNT(*) AS number_of_listens
  FROM production_curated.fact_listen f
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    "user",
    "date",
    number_of_listens,
    ROW_NUMBER() OVER (PARTITION BY "user" ORDER BY number_of_listens DESC, "date" ASC) AS rn
  FROM daily
)
SELECT "user", number_of_listens, "date"
FROM ranked
WHERE rn <= 3
ORDER BY "user" ASC, number_of_listens DESC;
