CREATE SCHEMA IF NOT EXISTS reporting;

CREATE OR REPLACE VIEW reporting.report_daily_active_users AS
WITH days AS (
    SELECT DISTINCT listened_date AS date
    FROM production_curated.fact_listen
),

total_users AS (
    SELECT COUNT(DISTINCT user_id) AS total_users
    FROM production_curated.fact_listen
),

active AS (
    SELECT
        d.date,
        f.user_id
    FROM days AS d
    INNER JOIN production_curated.fact_listen AS f
        ON f.listened_date BETWEEN d.date - INTERVAL 6 DAY AND d.date
    GROUP BY d.date, f.user_id
)

SELECT
    a.date,
    COUNT(*) AS number_active_users,
    ROUND(COUNT(*) * 100.0 / tu.total_users, 2) AS percentage_active_users
FROM active AS a
CROSS JOIN total_users AS tu
GROUP BY a.date, tu.total_users
ORDER BY a.date;
