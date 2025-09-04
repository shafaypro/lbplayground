CREATE SCHEMA IF NOT EXISTS reporting;

CREATE OR REPLACE VIEW reporting.report_users_on_2019_03_01 AS
SELECT COUNT(DISTINCT f.user_id) AS users_who_listened
FROM production_curated.fact_listen AS f
WHERE f.listened_date = DATE '2019-03-01';
