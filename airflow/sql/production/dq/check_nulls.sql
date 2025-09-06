INSERT INTO monitoring.data_quality_results
SELECT
    'null_check',
    CASE WHEN null_user_ids = 0 AND null_track_ids = 0 AND null_timestamps = 0
         THEN 'PASS' ELSE 'FAIL' END,
    NOW()
FROM (
    SELECT
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_ids,
        SUM(CASE WHEN track_id IS NULL THEN 1 ELSE 0 END) AS null_track_ids,
        SUM(CASE WHEN listened_ts IS NULL THEN 1 ELSE 0 END) AS null_timestamps
    FROM staging.listens_flat
) t;
