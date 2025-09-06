CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.data_quality_results (
    check_name VARCHAR,
    status VARCHAR,
    checked_at TIMESTAMP
);
