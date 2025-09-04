# Dag Structures of which one works first.



dag_raw_ingest runs daily (or manual trigger).

Once raw_load_listens succeeds, it emits RAW_DATASET.

dag_staging_curated is subscribed to RAW_DATASET → runs automatically.

Once it finishes, it emits CURATED_DATASET.

dag_marts_reporting is subscribed to CURATED_DATASET → runs automatically.