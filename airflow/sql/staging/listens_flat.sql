CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.listens_flat AS
SELECT
    listens_jsonl.user_name,
    listens_jsonl.user_name AS user_id,
    listens_jsonl.recording_msid AS track_id,
    track_metadata.artist_name,
    track_metadata.track_name,
    track_metadata.release_name,
    listens_jsonl.listened_at,
    TO_TIMESTAMP(listens_jsonl.listened_at) AS listened_ts,
    CAST(TO_TIMESTAMP(listens_jsonl.listened_at) AS DATE) AS listened_date,
    COALESCE(track_metadata.additional_info ->> 'source', '') AS source
FROM raw.listens_jsonl;
