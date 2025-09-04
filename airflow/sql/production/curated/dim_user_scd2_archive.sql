CREATE SCHEMA IF NOT EXISTS production_curated;

-- Define the SCD2 dimension table if not exists
CREATE TABLE IF NOT EXISTS production_curated.dim_user (
    user_id VARCHAR,
    user_name VARCHAR,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);

-- Merge new user data from staging into dim_user (SCD2 logic)
MERGE INTO production_curated.dim_user AS d
USING (
    SELECT DISTINCT
        user_id,
        user_name
    FROM staging.listens_flat
) AS s
    ON d.user_id = s.user_id AND d.is_current = TRUE

-- Case 1: user exists but name changed -> close old record
WHEN MATCHED AND d.user_name <> s.user_name THEN
    UPDATE SET valid_to = NOW(), is_current = FALSE

-- Case 2: new user or reopened due to name change -> insert new record
WHEN NOT MATCHED THEN
    INSERT (user_id, user_name, valid_from, valid_to, is_current)
    VALUES (s.user_id, s.user_name, NOW(), TIMESTAMP '9999-12-31', TRUE);
