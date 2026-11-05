#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Verify a swath dataset (format 2) with DuckDB, independently of the Go writer.

Checks: manifest.json is format 2 (format, format_version, schema_id, source, scope, coverage, parts) and
is the commit (no _SUCCESS, no symlink.txt); data/ holds exactly the manifest's parts; the run report
exists (_swath_summary.json for S3, _swath_local_summary.json otherwise); for S3, every column against the
current-object schema and every row a current object; per part: DuckDB row count == manifest rows,
first/last key == manifest min_key/max_key (exact), keys strictly ascending (unsigned bytes) within and
across parts; footer stamps (profile, profile version, schema id, index bound) and nothing else under
swath.*. Prints the schema as DuckDB sees it and min/max key per part.
"""
import json
import os
import sys

import duckdb


def main(out):
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
    s3 = m["source"]["kind"] == "s3"
    report = "_swath_summary.json" if s3 else "_swath_local_summary.json"
    check(os.path.exists(os.path.join(out, report)), f"{report} present (operational report)")
    files = [f["path"] for f in m["parts"]]
    paths = [os.path.join(out, k) for k in files]
    on_disk = sorted("data/" + p for p in os.listdir(os.path.join(out, "data")))
    check(on_disk == files, f"data/ holds exactly the {len(files)} manifest parts, in name order")
    rows = sum(f["rows"] for f in m["parts"])
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    glob = os.path.join(out, "data", "*.parquet")
    total = con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
    check(total == rows, f"DuckDB count {total} == sum of manifest rows {rows}")

    if s3:
        # The current-object schema (swath/s3-objects/v1):
        # name, physical type, repetition, logical type (None = unannotated).
        contract = [
            ("key", "BYTE_ARRAY", "REQUIRED", "String"), ("size", "INT64", "OPTIONAL", None),
            ("last_modified", "INT64", "OPTIONAL", "Timestamp"), ("etag", "BYTE_ARRAY", "OPTIONAL", "String"),
            ("storage_class", "BYTE_ARRAY", "OPTIONAL", "String"),
            ("owner_id", "BYTE_ARRAY", "OPTIONAL", "String"), ("owner_display_name", "BYTE_ARRAY", "OPTIONAL", "String"),
            ("checksum_algorithm", "BYTE_ARRAY", "OPTIONAL", "String"), ("checksum_type", "BYTE_ARRAY", "OPTIONAL", "String"),
        ]
        got = con.execute(f"SELECT name, type, repetition_type, logical_type FROM parquet_schema('{paths[0]}') WHERE name <> 'swath_listing'").fetchall()
        print("\nColumn-by-column against the current-object schema:")
        for i, want in enumerate(contract):
            g = got[i] if i < len(got) else (None, None, None, None)
            logical = None if g[3] is None else str(g[3])
            ok = g[0] == want[0] and g[1] == want[1] and g[2] == want[2] and (
                logical is None if want[3] is None else (logical or "").startswith(want[3]))
            if want[0] == "last_modified":
                ok = ok and "isAdjustedToUTC=1" in (logical or "") and "MICROS=MicroSeconds" in (logical or "")
            check(ok, f"  {want[0]}: {g[1]} {g[2]} {logical}")
        check(len(got) == len(contract), f"  {len(got)} columns, contract has {len(contract)}")
        bad = con.execute(f"""SELECT count(*) FROM read_parquet('{glob}') WHERE size IS NULL OR last_modified IS NULL
                              OR etag LIKE '"%'""").fetchone()[0]
        check(bad == 0, "every row is a current object: size and mtime set, etag unquoted")

    print("\nSchema as DuckDB sees it (DESCRIBE):")
    for name, typ, null, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall():
        print(f"  {name:20s} {typ:28s} null={null}")
    print("\nParquet physical schema (part 0):")
    if paths:
        for row in con.execute(
            f"SELECT name, type, repetition_type, converted_type, logical_type FROM parquet_schema('{paths[0]}')"
        ).fetchall():
            print("  " + " | ".join(str(x) for x in row))

    print("\nPer part:")
    prev_max = None
    for i, (f, p) in enumerate(zip(m["parts"], paths)):
        # Strict ascending within the file, comparing raw key bytes (BLOB compare is unsigned bytewise).
        r = con.execute(f"""
            WITH t AS (SELECT key, encode(key)::BLOB AS b, file_row_number AS n
                       FROM read_parquet('{p}', file_row_number = true))
            SELECT count(*), min(b), max(b), first(key ORDER BY n), last(key ORDER BY n),
                   count(*) FILTER (WHERE prev IS NOT NULL AND prev >= b)
            FROM (SELECT key, b, n, lag(b) OVER (ORDER BY n) AS prev FROM t)
        """).fetchone()
        cnt, bmin, bmax, first, last, bad = r
        print(f"  {f['path']}: rows={cnt} bytes={f['bytes']} min_key={first!r} max_key={last!r}")
        check(cnt == f["rows"], f"  rows {f['rows']}")
        check(bad == 0, "  keys strictly ascending within part (file row order)")
        check(first == f["min_key"] and last == f["max_key"], "  first/last key == manifest min_key/max_key")
        check(bytes(bmin) == first.encode() and bytes(bmax) == last.encode(), "  first/last are byte min/max")
        if prev_max is not None:
            check(prev_max < first.encode(), "  previous part max_key < this min_key (unsigned bytes)")
        prev_max = last.encode()
        kv = dict(
            (k.decode() if isinstance(k, (bytes, bytearray)) else k, v.decode() if isinstance(v, (bytes, bytearray)) else v)
            for k, v in con.execute(f"SELECT key, value FROM parquet_kv_metadata('{p}')").fetchall()
        )
        want = {"swath.profile": "swath-dataset", "swath.profile_version": "2", "swath.schema_id": m["schema_id"],
                "swath.index_bound_bytes": "1024"}
        check(all(kv.get(k) == v for k, v in want.items()), "  footer stamps: profile, version, schema id, index bound")
        check(not [k for k in kv if k.startswith("swath.") and k not in want], "  no other swath.* stamps")
    print("\nRESULT:", "OK" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: verify.py <output-dir>")
    sys.exit(main(sys.argv[1]))
