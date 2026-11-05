#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Verify a swath-local dataset with DuckDB (independent of the Go writer).

Checks: _SUCCESS present; run report named _swath_local_summary.json (not Swath's
reserved _swath_summary.json); manifest totalRows == sum(rowCount) == DuckDB count;
per-file DuckDB row count == manifest rowCount and min/max key == manifest
minKey/maxKey; keys strictly ascending (unsigned bytes) within and across parts
in file order; Parquet footer stamps (file_index 1..N, file_final only on N).
Prints the schema as DuckDB sees it and min/max key per part.
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

    check(os.path.exists(os.path.join(out, "_SUCCESS")), "_SUCCESS present")
    check(os.path.exists(os.path.join(out, "_swath_local_summary.json")), "_swath_local_summary.json present")
    check(not os.path.exists(os.path.join(out, "_swath_summary.json")),
          "no _swath_summary.json (Swath's reserved run-summary name, parsed by Varve)")
    m = json.load(open(os.path.join(out, "manifest.json")))
    files = [f["key"] for f in m["files"]]
    paths = [os.path.join(out, k) for k in files]
    on_disk = sorted("data/" + p for p in os.listdir(os.path.join(out, "data")))
    check(on_disk == files, f"data/ holds exactly the {len(files)} manifest parts, in name order")
    check(m["totalRows"] == sum(f["rowCount"] for f in m["files"]), "manifest totalRows == sum(rowCount)")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    glob = os.path.join(out, "data", "*.parquet")
    total = con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
    check(total == m["totalRows"], f"DuckDB count {total} == manifest totalRows {m['totalRows']}")

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
    for i, (f, p) in enumerate(zip(m["files"], paths)):
        # Strict ascending within the file, comparing raw key bytes (BLOB compare is unsigned bytewise).
        r = con.execute(f"""
            WITH t AS (SELECT key, encode(key)::BLOB AS b, file_row_number AS n
                       FROM read_parquet('{p}', file_row_number = true))
            SELECT count(*), min(b), max(b), first(key ORDER BY n), last(key ORDER BY n),
                   count(*) FILTER (WHERE prev IS NOT NULL AND prev >= b)
            FROM (SELECT key, b, n, lag(b) OVER (ORDER BY n) AS prev FROM t)
        """).fetchone()
        cnt, bmin, bmax, first, last, bad = r
        print(f"  {f['key']}: rows={cnt} bytes={f['size']} min_key={first!r} max_key={last!r}")
        check(cnt == f["rowCount"], f"  rowCount {f['rowCount']}")
        check(bad == 0, "  keys strictly ascending within part (file row order)")
        check(first == f["minKey"] and last == f["maxKey"], "  first/last key == manifest minKey/maxKey")
        check(bytes(bmin) == first.encode() and bytes(bmax) == last.encode(), "  first/last are byte min/max")
        if prev_max is not None:
            check(prev_max < first.encode(), "  previous part maxKey < this minKey (unsigned bytes)")
        prev_max = last.encode()
        kv = dict(
            (k.decode() if isinstance(k, (bytes, bytearray)) else k, v.decode() if isinstance(v, (bytes, bytearray)) else v)
            for k, v in con.execute(f"SELECT key, value FROM parquet_kv_metadata('{p}')").fetchall()
        )
        final = kv.get("swath.sort.file_final")
        check(kv.get("swath.sort.file_index") == str(i + 1), f"  swath.sort.file_index == {i+1}")
        check((final == "true") == (i == len(paths) - 1) and final in (None, "true"), "  file_final only on last part")
        check(kv.get("swath.sort.order") == m["sortKey"] and kv.get("swath.sort.mode") == "objects"
              and kv.get("swath.sort.format_version") == "1", "  sort order/mode/format stamps")
    print("\nRESULT:", "OK" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: verify.py <output-dir>")
    sys.exit(main(sys.argv[1]))
