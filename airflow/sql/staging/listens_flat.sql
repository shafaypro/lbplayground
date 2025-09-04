CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.listens_flat AS
SELECT
  user_name AS user_name,
  user_name AS user_id,
  recording_msid AS track_id,
  track_metadata.artist_name AS artist_name,
  track_metadata.track_name AS track_name,
  track_metadata.release_name AS release_name,
  listened_at,
  TO_TIMESTAMP(listened_at) AS listened_ts,
  CAST(TO_TIMESTAMP(listened_at) AS DATE) AS listened_date,
  COALESCE(track_metadata.additional_info ->> 'source', '') AS source
FROM raw.listens_jsonl;
