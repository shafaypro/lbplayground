             ┌───────────────────────────┐
             │       raw.listens_jsonl   │
             │  (JSONL landing table)    │
             └───────────────┬───────────┘
                             │
                             ▼
             ┌───────────────────────────┐
             │     staging.listens_flat  │
             │ user_id                   │
             │ user_name                 │
             │ track_id                  │
             │ artist_name               │
             │ track_name                │
             │ release_name              │
             │ listened_ts               │
             │ listened_date             │
             │ source                    │
             └───────────────┬───────────┘
                             │
       ┌─────────────────────┴────────────────────────────┐
       │                                                  │
       ▼                                                  ▼
┌───────────────────────┐                   ┌──────────────────────────┐
│ production_curated.   │                   │ production_curated.      │
│      dim_user (SCD2)  │                   │      fact_listen         │
│ user_id (PK)          │◄──────────────────┤ user_id (FK → dim_user) │
│ user_name             │                   │ track_id                 │
│ valid_from            │                   │ track_name               │
│ valid_to              │                   │ artist_name              │
│ is_current            │                   │ release_name             │
└───────────────────────┘                   │ listened_ts              │
                                            │ listened_date            │
                                            │ source                   │
                                            └───────────┬──────────────┘
                                                        │
                           ┌────────────────────────────┴──────────────┐
                           │                                           │
                           ▼                                           ▼
        ┌──────────────────────────────┐               ┌───────────────────────────────┐
        │ production_curated.          │               │ production_marts.             │
        │ snapshots_user_activity      │               │ mart_user_activity            │
        └──────────────────────────────┘               │ mart_track_performance        │
                                                       └───────────┬───────────────────┘
                                                                   │
                                                                   ▼
                                            ┌───────────────────────────────────────────┐
                                            │             reporting views               │
                                            │ report_top10_users                        │
                                            │ report_users_on_2019_03_01                │
                                            │ report_first_song_per_user                │
                                            │ report_top3_days_per_user                 │
                                            │ report_daily_active_users                 │
                                            └───────────────────────────────────────────┘

                     ┌───────────────────────────────────────────┐
                     │     monitoring.data_quality_results       │
                     │ (results of 7 SQL DQ checks)              │
                     └───────────────────────────────────────────┘
