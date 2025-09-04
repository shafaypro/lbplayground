BEGIN TRANSACTION;

CREATE SCHEMA IF NOT EXISTS production_curated;

-- SCD2 table (create once)
CREATE TABLE IF NOT EXISTS production_curated.dim_user (
  user_id    VARCHAR,
  user_name  VARCHAR,
  valid_from TIMESTAMP,
  valid_to   TIMESTAMP,
  is_current BOOLEAN
);

-- Close current rows where the name changed
UPDATE production_curated.dim_user d
SET valid_to = NOW(),
    is_current = FALSE
FROM (
    SELECT DISTINCT user_id, user_name
    FROM staging.listens_flat
) s
WHERE d.user_id = s.user_id
  AND d.is_current = TRUE
  AND d.user_name <> s.user_name;

-- Insert new users or new versions for renamed users
INSERT INTO production_curated.dim_user (user_id, user_name, valid_from, valid_to, is_current)
SELECT
    s.user_id,
    s.user_name,
    NOW() AS valid_from,
    TIMESTAMP '9999-12-31' AS valid_to,
    TRUE AS is_current
FROM (
    SELECT DISTINCT user_id, user_name
    FROM staging.listens_flat
) s
LEFT JOIN production_curated.dim_user d
  ON d.user_id = s.user_id
 AND d.is_current = TRUE
WHERE d.user_id IS NULL;

COMMIT;
