CREATE SCHEMA IF NOT EXISTS production_curated;

-- Create table if missing
CREATE TABLE IF NOT EXISTS production_curated.snapshots_user_activity (
    listened_date DATE,
    daily_active_users BIGINT,
    total_listens BIGINT,
    snapshot_ts TIMESTAMP
);

-- Append snapshot
INSERT INTO production_curated.snapshots_user_activity
SELECT
    listened_date,
    COUNT(DISTINCT user_id) AS daily_active_users,
    COUNT(*) AS total_listens,
    NOW() AS snapshot_ts
FROM staging.listens_flat
GROUP BY listened_date
ORDER BY listened_date;
-- I changed it to append only (best practice for historical refernece and over time trail)