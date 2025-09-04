from airflow.datasets import Dataset

# When raw ingestion finishes
RAW_DATASET = Dataset("duckdb://lb/raw/listens_jsonl")

# When curated layer finishes
CURATED_DATASET = Dataset("duckdb://lb/curated/complete")
