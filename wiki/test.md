Type "help", "copyright", "credits" or "license" for more information.
>>> import duckdb
>>> con = duckdb.connect("/opt/duckdb/warehouse/lb.duckdb")
>>> con.execute("SELECT * FROM raw.listens_jsonl LIMIT 1;").fetchall()
[({'additional_info': {'release_msid': '"34dbfc73-31e4-45c3-9d6e-04d8b6c5fd4a"', 'release_mbid': None, 'recording_mbid': None, 'release_group_mbid': None, 'artist_mbids': '[]', 'tags': '[]', 'work_mbids': '[]', 'isrc': None, 'spotify_id': None, 'tracknumber': None, 'track_mbid': None, 'artist_msid': '"f1d39567-27e7-40af-852a-abaed88ec838"', 'recording_msid': '"1e1b2aa0-b2db-42ed-a8ba-89c303499408"', 'dedup_tag': None, 'artist_names': None, 'discnumber': None, 'duration_ms': None, 'listening_from': None, 'release_artist_name': None, 'release_artist_names': None, 'spotify_album_artist_ids': None, 'spotify_album_id': None, 'spotify_artist_ids': None, 'rating': None, 'source': None, 'track_length': None, 'track_number': None}, 'artist_name': 'Withered Hand', 'track_name': 'Love In the Time of Ecstacy', 'release_name': 'Good News'}, 1555286560, UUID('1e1b2aa0-b2db-42ed-a8ba-89c303499408'), 'NichoBI')]
>>> con.execute("SELECT count(*) FROM staging.listens_flat").fetchall()
[(0,)]


con.execute("SELECT user_name, user_name, recording_msid, track_metadata.artist_name, track_metadata.track_name, track_metadata.release_name, listened_at, TO_TIMESTAMP(listened_at), CAST(TO_TIMESTAMP(listened_at) AS DATE), COALESCE(track_metadata.additional_info.source, "") FROM raw.listens_jsonl;").fetchall()

