#!/usr/bin/env python3
# usage: gen_fixture_1m.py OUT.parquet  (then: swath-replay sort-fixture --capture DIR --output DIR)
# Synthetic 1M-key S3 fixture for the 4a differential (labelled synthetic; this file is its generator config).
import duckdb, sys
out = sys.argv[1]
con = duckdb.connect()
con.execute(f"""
COPY (
 WITH k AS (
  SELECT i,
   CASE
    WHEN i % 10 < 4 THEN printf('logs/%s/dt=2026-%02d-%02d/hour=%02d/part-%05d.json.gz', ['alpha','beta','gamma','delta','epsilon'][1 + (i % 5)], 1 + (i // 7919) % 12, 1 + (i // 263) % 28, (i // 11) % 24, i)
    WHEN i % 10 < 6 THEN printf('images/%08d.jpg', i)
    WHEN i % 10 = 6 THEN printf('users/%s/docs/report %d (final+v%d) %d%%.pdf', ['jürgen','zoë','名前','ana maría','o''brien'][1 + (i % 5)], i % 97, i % 7, i)
    WHEN i % 10 = 7 THEN printf('data/%s/%s/%s/%07d.parquet', chr(CAST(97 + (i % 26) AS INTEGER)), chr(CAST(97 + ((i // 26) % 26) AS INTEGER)), chr(CAST(97 + ((i // 676) % 26) AS INTEGER)), i)
    WHEN i % 10 = 8 THEN printf('flat-%09d', i)
    ELSE printf('deep/%s/x%07d', repeat('dir/', CAST(1 + i % 20 AS INTEGER)), i)
   END AS key
  FROM range(1000000) t(i)
  UNION ALL
  SELECT 1000000 + i, printf('long/%s-%04d', repeat('k', 980), i) FROM range(20) t(i)
 )
 SELECT key,
  CAST(1000 + (i * 7919) % 5000000 AS BIGINT) AS size,
  TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_microseconds(CAST(i AS BIGINT) * 1000003) AS last_modified,
  md5(CAST(i AS VARCHAR)) AS etag, CASE WHEN i % 13 = 0 THEN 'STANDARD_IA' ELSE 'STANDARD' END AS storage_class,
  CAST(NULL AS VARCHAR) AS version_id, true AS is_latest, false AS is_delete_marker,
  CAST(NULL AS VARCHAR) AS owner_id, CAST(NULL AS VARCHAR) AS owner_display_name,
  CASE WHEN i % 3 = 0 THEN 'CRC32' ELSE CAST(NULL AS VARCHAR) END AS checksum_algorithm,
  CASE WHEN i % 3 = 0 THEN 'FULL_OBJECT' ELSE CAST(NULL AS VARCHAR) END AS checksum_type,
  'OBJECT' AS row_type
 FROM k
) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
""")
print(con.execute(f"select count(*), count(distinct key), max(strlen(key)) from '{out}'").fetchall())
