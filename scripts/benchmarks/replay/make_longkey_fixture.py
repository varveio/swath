#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Create a deterministic long-key OBJECT fixture for replay resource arms.

The output directory is durable benchmark data, not repository scratch. It contains a
canonical Parquet capture, a stamped sorted serving fixture, and a byte manifest.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import duckdb

from run_pair import clean_java_env


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", required=True, help="candidate installDist swath-replay launcher")
    parser.add_argument("--java-home", required=True, help="JDK 25 used by the replay launcher")
    parser.add_argument("--output", required=True, help="new durable fixture directory")
    parser.add_argument("--count", type=int, default=600_000)
    parser.add_argument("--key-bytes", type=int, default=1024)
    parser.add_argument("--escape-heavy", action="store_true",
                        help="use ASCII spaces so encoding-type=url expands each key byte threefold")
    args = parser.parse_args()
    if args.count < 10_000 or args.count > 1_000_000:
        raise ValueError("count must be between 10,000 and 1,000,000")
    if args.key_bytes < 128 or args.key_bytes > 1024:
        raise ValueError("key-bytes must be between 128 and the S3 limit of 1024")
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    capture = output / "capture"
    capture.mkdir()
    parquet = capture / "part-00000.parquet"
    escaped = str(parquet).replace("'", "''")
    # The 12-digit suffix establishes strict byte order even after the common long prefix.
    with duckdb.connect() as connection:
        fill = " " if args.escape_heavy else "x"
        connection.execute(f"""
            COPY (
              SELECT (repeat('{fill}', {args.key_bytes - 12}) ||
                      lpad(i::VARCHAR, 12, '0'))::VARCHAR AS key,
                     1::BIGINT AS size,
                     TIMESTAMPTZ '2026-01-01 00:00:00+00' AS last_modified,
                     NULL::VARCHAR AS etag,
                     'STANDARD'::VARCHAR AS storage_class,
                     NULL::VARCHAR AS version_id,
                     NULL::BOOLEAN AS is_latest,
                     false AS is_delete_marker,
                     NULL::VARCHAR AS owner_id,
                     NULL::VARCHAR AS owner_display_name,
                     NULL::VARCHAR AS checksum_algorithm,
                     NULL::VARCHAR AS checksum_type,
                     'OBJECT'::VARCHAR AS row_type
              FROM range({args.count}) AS rows(i)
              ORDER BY i
            ) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)
        """)
    sorted_root = output / "sorted"
    command = [str(Path(args.replay).resolve()), "sort-fixture", "--capture", str(capture),
               "--output", str(sorted_root)]
    environment, inherited_java_options = clean_java_env()
    environment["JAVA_HOME"] = args.java_home
    result = subprocess.run(command, env=environment, text=True, capture_output=True, check=False)
    (output / "sort.log").write_text(result.stdout + result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"sort-fixture failed; see {output / 'sort.log'}")
    sorted_files = sorted(sorted_root.rglob("*.parquet"))
    if not sorted_files:
        raise RuntimeError("sort-fixture produced no Parquet files")
    with duckdb.connect() as connection:
        connection.execute("PRAGMA threads=1")
        glob = str(sorted_root / "*.parquet")
        stats = connection.execute("SELECT count(*), count(DISTINCT key), "
                                   "min(octet_length(CAST(key AS BLOB))), "
                                   "max(octet_length(CAST(key AS BLOB))), "
                                   "count(*) FILTER (WHERE row_type <> 'OBJECT' OR is_latest IS NOT NULL) "
                                   "FROM read_parquet(?)", [glob]).fetchone()
        if stats != (args.count, args.count, args.key_bytes, args.key_bytes, 0):
            raise RuntimeError(f"sorted fixture count/key-length/row-type mismatch: {stats}")
        stamp_rows = connection.execute("SELECT file_name, key, value FROM parquet_kv_metadata(?)",
                                        [glob]).fetchall()
        stamps = {}
        for filename, key, value in stamp_rows:
            stamps.setdefault(filename, {})[key.decode("utf-8")] = value.decode("utf-8")
        required = {"swath.sort.mode": "objects", "swath.sort.format_version": "1",
                    "swath.sort.order": "key_bytes_unsigned,version_id_null_first,row_type_rank"}
        if len(stamps) != len(sorted_files) or any(
                any(values.get(key) != expected for key, expected in required.items())
                for values in stamps.values()):
            raise RuntimeError("sorted fixture has missing or incompatible ordering stamps")
        schema_rows = connection.execute("SELECT file_name, name, type, converted_type, repetition_type "
                                         "FROM parquet_schema(?) "
                                         "WHERE name IN ('key','is_delete_marker','row_type')",
                                         [glob]).fetchall()
        by_file = {}
        for filename, name, physical, converted, repetition in schema_rows:
            by_file.setdefault(filename, {})[name] = (physical, converted, repetition)
        if len(by_file) != len(sorted_files) or any(
                columns.get("key") != ("BYTE_ARRAY", "UTF8", "REQUIRED")
                or columns.get("is_delete_marker", (None, None, None))[2] != "REQUIRED"
                or columns.get("row_type", (None, None, None))[2] != "REQUIRED"
                for columns in by_file.values()):
            raise RuntimeError("sorted fixture lacks required key/delete-marker/row-type columns")
        # Scan physical output order in bounded batches, not a sorting query that could hide disorder.
        cursor = connection.execute("SELECT key FROM read_parquet(?)", [glob])
        previous = None
        while batch := cursor.fetchmany(8192):
            for (key,) in batch:
                if previous is not None and key <= previous:
                    raise RuntimeError("sorted fixture keys are not strictly increasing")
                previous = key
    files = [parquet, *(path for path in sorted_root.rglob("*") if path.is_file())]
    manifest = {"count": args.count, "key_bytes": args.key_bytes,
                "escape_heavy": args.escape_heavy, "replay_command": command,
                "duckdb_version": duckdb.__version__, "python_version": sys.version,
                "java_home": args.java_home,
                "removed_inherited_java_option_keys": sorted(inherited_java_options),
                "generator_sha256": sha256(Path(__file__)),
                "replay_launcher_sha256": sha256(Path(args.replay).resolve()),
                "validated_stats": stats, "required_stamp": required,
                "files": [{"path": str(path.relative_to(output)), "bytes": path.stat().st_size,
                           "sha256": sha256(path)} for path in sorted(files)]}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"sorted_fixture": str(sorted_root), "manifest": str(output / 'manifest.json')}))


if __name__ == "__main__":
    main()
