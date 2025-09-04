CREATE SCHEMA IF NOT EXISTS raw;

-- Idempotent overwrite of the raw landing table
CREATE OR REPLACE TABLE raw.listens_jsonl AS
SELECT *
FROM read_json_auto(
    '{{DATA_GLOB}}',
    format='newline_delimited',   -- JSONL / NDJSON
    maximum_depth=3,
    union_by_name=true,           -- handle schema drift
    ignore_errors=true,           -- skip bad rows instead of failing the load
    convert_strings_to_integers=false  -- keep "03" as text, don't coerce
);
