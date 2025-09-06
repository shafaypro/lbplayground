INSERT INTO monitoring.data_quality_results
SELECT
    'future_date_check',
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
    NOW()
FROM production_curated.fact_listen
WHERE listened_ts > NOW();
