#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Compare every canonical value through an independent DuckDB reader."""

import argparse
import json
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("parquet", type=Path)
    args = parser.parse_args()
    expected = [json.loads(line) for line in args.input.read_text().splitlines() if line]
    columns = (
        "key", "size", "last_modified", "etag", "storage_class", "version_id",
        "is_latest", "is_delete_marker", "owner_id", "owner_display_name",
        "checksum_algorithm", "checksum_type", "row_type",
    )
    projection = ", ".join(
        f'epoch_us("{name}") AS "{name}"' if name == "last_modified" else f'"{name}"'
        for name in columns
    )
    filename = str(args.parquet.resolve()).replace("'", "''")
    def query(predicate=""):
        sql = (
            "SET threads=1; SET preserve_insertion_order=true; "
            f"SELECT to_json(r) AS row_value FROM "
            f"(SELECT {projection} FROM read_parquet('{filename}') {predicate}) r;"
        )
        result = subprocess.run(
            ["duckdb", "-json", "-c", sql], check=True, text=True, capture_output=True
        )
        # The CLI's ordinary JSON renderer quotes BOOLEAN values as strings.
        # DuckDB's to_json preserves the actual SQL types, including nulls.
        return [row["row_value"] for row in json.loads(result.stdout or "[]")]

    def equal(wanted, actual, label):
        if len(actual) != len(wanted):
            raise SystemExit(f"{label} row count mismatch: {len(actual)} != {len(wanted)}")
        for index, (want, got) in enumerate(zip(wanted, actual)):
            for name in columns:
                if type(want.get(name)) is not type(got.get(name)) or want.get(name) != got.get(name):
                    raise SystemExit(f"{label} row {index}, {name}: {got.get(name)!r} != {want.get(name)!r}")

    equal(expected, query(), "full scan")
    filtered_checks = 0
    if expected:
        # Constant folded UTF-8 literals also cover NUL/control keys without
        # embedding those bytes in SQL. These reads exercise Parquet filtering.
        for index in sorted({0, len(expected) // 2, len(expected) - 1}):
            key = expected[index]["key"]
            literal = f"decode(from_hex('{key.encode('utf-8').hex()}'))"
            wanted = [row for row in expected if row["key"] == key]
            equal(wanted, query(f"WHERE key = {literal}"), "key filter")
            filtered_checks += 1
    print(json.dumps({"reader": "duckdb", "file": str(args.parquet), "rows": len(expected),
                      "equal": True, "filtered_key_checks": filtered_checks}))


if __name__ == "__main__":
    main()
