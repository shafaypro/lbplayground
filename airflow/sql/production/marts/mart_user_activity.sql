CREATE SCHEMA IF NOT EXISTS production_marts;

CREATE OR REPLACE TABLE production_marts.mart_user_activity AS
WITH ranked AS (
    SELECT
        user_id,
        listened_date,
        COUNT(*) AS listens,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
    FROM staging.listens_flat
    GROUP BY user_id, listened_date
)

SELECT
    user_id,
    listened_date,
    listens
FROM ranked
WHERE rn <= 3
ORDER BY user_id ASC, listens DESC;
