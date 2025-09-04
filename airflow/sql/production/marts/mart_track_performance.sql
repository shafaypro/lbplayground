CREATE SCHEMA IF NOT EXISTS production_marts;

-- Per-track, per-day performance (listens + unique listeners)
CREATE OR REPLACE TABLE production_marts.mart_track_performance AS
SELECT
    f.track_id,
    f.track_name,
    f.artist_name,
    f.listened_date,
    COUNT(*) AS listens,
    COUNT(DISTINCT f.user_id) AS unique_listeners
FROM production_curated.fact_listen AS f
GROUP BY
    f.track_id,
    f.track_name,
    f.artist_name,
    f.listened_date
ORDER BY
    listens DESC,
    unique_listeners DESC,
    listened_date DESC;
