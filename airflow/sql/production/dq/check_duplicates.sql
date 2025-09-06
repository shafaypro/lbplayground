INSERT INTO monitoring.data_quality_results
SELECT
    'duplicate_listens_check',
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
    NOW()
FROM (
    SELECT user_id, track_id, listened_ts, COUNT(*) AS c
    FROM production_curated.fact_listen
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
) t;
