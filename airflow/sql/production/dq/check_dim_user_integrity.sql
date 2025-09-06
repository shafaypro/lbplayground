INSERT INTO monitoring.data_quality_results
SELECT
    'dim_user_scd2_overlap_check',
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
    NOW()
FROM (
    SELECT user_id, COUNT(*)
    FROM production_curated.dim_user
    WHERE is_current = TRUE
    GROUP BY user_id
    HAVING COUNT(*) > 1
) t;
