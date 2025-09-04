CREATE SCHEMA IF NOT EXISTS reporting;

CREATE OR REPLACE VIEW reporting.report_first_song_per_user AS
WITH ranked AS (
  SELECT
    f.user_id,
    COALESCE(f.current_user_name, f.user_name) AS "user",
    f.track_name,
    f.listened_ts,
    ROW_NUMBER() OVER (PARTITION BY f.user_id ORDER BY f.listened_ts ASC) AS rn
  FROM production_curated.fact_listen f
)
SELECT
  "user",
  track_name AS first_track,
  listened_ts AS first_listen_time
FROM ranked
WHERE rn = 1;
