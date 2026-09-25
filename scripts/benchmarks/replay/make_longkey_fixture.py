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

import duckdb


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replay", required=True, help="candidate installDist swath-replay launcher")
    parser.add_argument("--output", required=True, help="new durable fixture directory")
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--key-bytes", type=int, default=1024)
    args = parser.parse_args()
    if args.count < 10_000 or args.count > 1_000_000:
        raise ValueError("count must be between 10,000 and 1,000,000")
    if args.key_bytes < 128 or args.key_bytes > 4096:
        raise ValueError("key-bytes must be between 128 and 4096")
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    capture = output / "capture"
    capture.mkdir()
    parquet = capture / "part-00000.parquet"
    escaped = str(parquet).replace("'", "''")
    # The 12-digit suffix establishes strict byte order even after the common long prefix.
    with duckdb.connect() as connection:
        connection.execute(f"""
            COPY (
              SELECT CAST(repeat('x', {args.key_bytes - 12}) ||
                          lpad(i::VARCHAR, 12, '0') AS BLOB) AS key,
                     1::BIGINT AS size,
                     TIMESTAMPTZ '2026-01-01 00:00:00+00' AS last_modified,
                     NULL::VARCHAR AS etag,
                     'STANDARD'::VARCHAR AS storage_class,
                     NULL::VARCHAR AS version_id,
                     true AS is_latest,
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
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    (output / "sort.log").write_text(result.stdout + result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"sort-fixture failed; see {output / 'sort.log'}")
    files = [parquet, *(path for path in sorted_root.rglob("*") if path.is_file())]
    manifest = {"count": args.count, "key_bytes": args.key_bytes, "replay_command": command,
                "files": [{"path": str(path.relative_to(output)), "bytes": path.stat().st_size,
                           "sha256": sha256(path)} for path in sorted(files)]}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"sorted_fixture": str(sorted_root), "manifest": str(output / 'manifest.json')}))


if __name__ == "__main__":
    main()
