CREATE OR REPLACE TABLE production_curated.fact_listen AS
SELECT
    lf.user_id,
    lf.user_name,
    du.user_name AS current_user_name,  -- handles renamed users
    lf.track_id,
    lf.track_name,
    lf.artist_name,
    lf.release_name,
    lf.listened_ts,
    lf.listened_date,
    lf.source
FROM staging.listens_flat lf
LEFT JOIN production_curated.dim_user du
  ON lf.user_id = du.user_id
 AND du.is_current = TRUE;
