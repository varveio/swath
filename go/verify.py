#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Verify a swath dataset (format 2) with DuckDB, independently of the Go writer.

Dispatches on schema_id, refusing unknown schemas. Current keys are strict;
versions keys are nondecreasing within a part and strict across parts. Checks
S3 version nullability and GCS generation ordering, manifest row counts,
byte-exact bounds and footer stamps. Optional reference-dir compares ordered
whole-row sequences and multisets, including nulls and metadata, via diffds.
"""
import base64
import json
import os
import sys

from dataset_schema import schema_id, S3_CURRENT, S3_VERSIONS, GCS_CURRENT, GCS_VERSIONS, VERSION_IDS


def main(out, reference=None):
    import duckdb
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + msg)
        ok = ok and cond

    m = json.load(open(os.path.join(out, "manifest.json")))
    check(m.get("format") == "swath-dataset" and m.get("format_version") == 2, "manifest is format 2")
    for k in ("schema_id", "published_at", "source", "scope", "coverage", "parts"):
        check(k in m, f"manifest has {k}")
    cov = m.get("coverage", {})
    check(cov.get("snapshot") is False and isinstance(cov.get("exclusions"), list) and isinstance(cov.get("filters"), list),
          f"coverage complete={cov.get('complete')} skipped={cov.get('skipped_entries')} scope={m.get('scope')}")
    for gone in ("_SUCCESS", "symlink.txt"):
        check(not os.path.exists(os.path.join(out, gone)), f"no {gone} (the manifest is the commit)")
    try:
        sid = schema_id(m)
    except ValueError as err:
        check(False, str(err))
        return 1
    groups = sid in VERSION_IDS
    s3 = sid in (S3_CURRENT, S3_VERSIONS)
    check(m["source"]["kind"] == ("s3" if s3 else "gcs" if sid in (GCS_CURRENT, GCS_VERSIONS) else "local-fs"),
          "source kind agrees with schema id")
    report = "run.json"
    check(os.path.exists(os.path.join(out, report)), f"{report} present (operational report)")
    files = [f["path"] for f in m["parts"]]
    paths = [os.path.join(out, k) for k in files]
    on_disk = sorted("data/" + p for p in os.listdir(os.path.join(out, "data")))
    check(on_disk == files, f"data/ holds exactly the {len(files)} manifest parts, in name order")
    rows = sum(f["rows"] for f in m["parts"])
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    glob = os.path.join(out, "data", "*.parquet")
    total = con.execute(f"SELECT count(*) FROM read_parquet('{glob}', union_by_name = true)").fetchone()[0]
    check(total == rows, f"DuckDB count {total} == sum of manifest rows {rows}")

    if s3:
        # The current-object schema (swath/s3-objects/v1):
        # name, physical type, repetition, logical type (None = unannotated).
        contract = [
            ("key", "BYTE_ARRAY", "REQUIRED", "String"), ("size", "INT64", "OPTIONAL", "IntType("),
            ("last_modified", "INT64", "OPTIONAL", "Timestamp"), ("etag", "BYTE_ARRAY", "OPTIONAL", "String"),
            ("storage_class", "BYTE_ARRAY", "OPTIONAL", "String"),
            ("owner_id", "BYTE_ARRAY", "OPTIONAL", "String"), ("owner_display_name", "BYTE_ARRAY", "OPTIONAL", "String"),
            ("checksum_algorithm", "BYTE_ARRAY", "OPTIONAL", "String"), ("checksum_type", "BYTE_ARRAY", "OPTIONAL", "String"),
        ]
        if sid == S3_VERSIONS:
            contract += [("version_id", "BYTE_ARRAY", "OPTIONAL", "String"),
                         ("is_latest", "BOOLEAN", "REQUIRED", None),
                         ("is_delete_marker", "BOOLEAN", "REQUIRED", None)]
        got = con.execute(f"SELECT name, type, repetition_type, logical_type FROM parquet_schema('{paths[0]}') WHERE name <> 'swath_listing'").fetchall()
        print("\nColumn-by-column against the selected S3 schema:")
        for i, want in enumerate(contract):
            g = got[i] if i < len(got) else (None, None, None, None)
            logical = None if g[3] is None else str(g[3])
            good = g[0] == want[0] and g[1] == want[1] and g[2] == want[2] and (
                logical is None if want[3] is None else (logical or "").startswith(want[3]))
            if want[0] == "last_modified":
                good = good and "isAdjustedToUTC=1" in (logical or "") and "MICROS=MicroSeconds" in (logical or "")
            check(good, f"  {want[0]}: {g[1]} {g[2]} {logical}")
        check(len(got) == len(contract), f"  {len(got)} columns, contract has {len(contract)}")
        if sid == S3_CURRENT:
            invalid = 'size IS NULL OR last_modified IS NULL OR etag LIKE \'"%\''
        else:
            invalid = """last_modified IS NULL OR is_latest IS NULL OR is_delete_marker IS NULL
                OR (is_delete_marker AND (size IS NOT NULL OR etag IS NOT NULL OR storage_class IS NOT NULL
                    OR checksum_algorithm IS NOT NULL OR checksum_type IS NOT NULL))
                OR (NOT is_delete_marker AND size IS NULL) OR etag LIKE '"%'"""
        bad = con.execute(f"SELECT count(*) FROM read_parquet('{glob}', union_by_name=true) WHERE {invalid}").fetchone()[0]
        check(bad == 0, "every row agrees with the selected S3 schema and nullability")

    if sid == GCS_VERSIONS:
        have = dict((r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true)").fetchall())
        check(have.get("generation") == "BIGINT" and "time_deleted" in have,
              "generations have exact int64 generation and nullable time_deleted")

    print("\nSchema as DuckDB sees it (DESCRIBE):")
    for name, typ, null, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name = true)").fetchall():
        print(f"  {name:20s} {typ:28s} null={null}")
    print("\nParquet physical schema (part 0):")
    if paths:
        for row in con.execute(
            f"SELECT name, type, repetition_type, converted_type, logical_type FROM parquet_schema('{paths[0]}')"
        ).fetchall():
            print("  " + " | ".join(str(x) for x in row))

    # S3 keys are STRING (VARCHAR); filesystem keys are unannotated BYTE_ARRAY (BLOB).
    key_type = dict((r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name = true)").fetchall())["key"]
    kb = "key" if key_type == "BLOB" else "encode(key)"
    print("\nPer part:")
    prev_max = None
    for i, (f, p) in enumerate(zip(m["parts"], paths)):
        key_violation = ">" if groups else ">="
        # Strict ascending within the file, comparing raw key bytes (BLOB compare is unsigned bytewise).
        r = con.execute(f"""
            WITH t AS (SELECT {kb}::BLOB AS b, file_row_number AS n
                       FROM read_parquet('{p}', file_row_number = true))
            SELECT count(*), min(b), max(b), first(b ORDER BY n), last(b ORDER BY n),
                   count(*) FILTER (WHERE prev IS NOT NULL AND prev {key_violation} b)
            FROM (SELECT b, n, lag(b) OVER (ORDER BY n) AS prev FROM t)
        """).fetchone()
        cnt, bmin, bmax, first, last, bad = r
        bmin, bmax, first, last = bytes(bmin), bytes(bmax), bytes(first), bytes(last)
        if sid == GCS_VERSIONS:
            bad_generations = con.execute(f"""SELECT count(*) FROM (
                SELECT key, generation, lag(key) OVER (ORDER BY file_row_number) AS pk,
                lag(generation) OVER (ORDER BY file_row_number) AS pg
                FROM read_parquet('{p}', file_row_number=true))
                WHERE generation IS NULL OR (key = pk AND generation <= pg)""").fetchone()[0]
            check(bad_generations == 0, "  generations strictly ascend within each name")
        lo, hi = base64.b64decode(f["min_key_base64"]), base64.b64decode(f["max_key_base64"])
        print(f"  {f['path']}: rows={cnt} bytes={f['bytes']} min_key={first!r} max_key={last!r}")
        check(cnt == f["rows"], f"  rows {f['rows']}")
        check(bad == 0, "  keys nondecreasing within part" if groups else "  keys strictly ascending within part")
        check(first == lo and last == hi, "  first/last key == manifest min_key_base64/max_key_base64 (decoded bytes)")
        check(bmin == first and bmax == last, "  first/last are byte min/max")
        if prev_max is not None:
            check(prev_max < first, "  previous part max key < this min key (unsigned bytes)")
        prev_max = last
        kv = dict(
            (k.decode() if isinstance(k, (bytes, bytearray)) else k, v.decode() if isinstance(v, (bytes, bytearray)) else v)
            for k, v in con.execute(f"SELECT key, value FROM parquet_kv_metadata('{p}')").fetchall()
        )
        want = {"swath.profile": "swath-dataset", "swath.profile_version": "2", "swath.schema_id": m["schema_id"],
                "swath.index_bound_bytes": "1024"}
        check(all(kv.get(k) == v for k, v in want.items()), "  footer stamps: profile, version, schema id, index bound")
        check(not [k for k in kv if k.startswith("swath.") and k not in want], "  no other swath.* stamps")
    if reference is not None:
        from bench.diffds import compare
        result = compare(out, reference)
        check(result["identical"], "whole-row ordered sequence and multiset match reference: " + json.dumps(result))
    print("\nRESULT:", "OK" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit("usage: verify.py <output-dir> [reference-dir]")
    sys.exit(main(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else None))
