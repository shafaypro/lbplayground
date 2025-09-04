CREATE SCHEMA IF NOT EXISTS reporting;

CREATE OR REPLACE VIEW reporting.report_top10_users AS
SELECT
  COALESCE(f.current_user_name, f.user_name) AS "user",
  COUNT(*) AS number_of_listens
FROM production_curated.fact_listen f
GROUP BY 1
ORDER BY number_of_listens DESC
LIMIT 10;
