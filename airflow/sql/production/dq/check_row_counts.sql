-- Row counts across layers (raw -> staging -> fact)
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.data_quality_results (
    check_name VARCHAR,
    status VARCHAR,
    checked_at TIMESTAMP
);

INSERT INTO monitoring.data_quality_results
SELECT
    'row_count_check' AS check_name,
    CASE WHEN fact_count < staging_count THEN 'FAIL' ELSE 'PASS' END AS status,
    NOW() AS checked_at
FROM (
    SELECT
        (SELECT COUNT(*) FROM raw.listens_jsonl) AS raw_count,
        (SELECT COUNT(*) FROM staging.listens_flat) AS staging_count,
        (SELECT COUNT(*) FROM production_curated.fact_listen) AS fact_count
) t;
