CREATE OR REPLACE VIEW production_marts.vw_top_tracks_all_time AS
SELECT
  track_id,
  track_name,
  artist_name,
  COUNT(*) AS listens,
  COUNT(DISTINCT user_id) AS unique_listeners
FROM production_curated.fact_listen
GROUP BY 1,2,3
ORDER BY listens DESC, unique_listeners DESC;
