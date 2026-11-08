#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Compare swath datasets with DuckDB, dispatching on the manifest schema id.

usage: diffds.py A B [--ts-ms]

Versions compare whole-row multisets (EXCEPT ALL, nulls included) and ordered
row sequences in manifest part/file-row order. Keys may repeat within a part;
parts must remain strictly key-disjoint. Current S3 still projects the legacy
canonical values for Java comparisons. Unknown or incompatible ids are refused.
"""
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dataset_schema import (COLUMNS, S3_CURRENT, GCS_CURRENT, VERSION_IDS,
                            schema_id, sql_string)

COLS = ["key", "size", "last_modified", "etag", "storage_class", "version_id", "is_latest",
        "is_delete_marker", "owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"]
# The canonical values of the reserved columns for a current object.
CURRENT_OBJECT = {"version_id": "NULL::VARCHAR", "is_latest": "NULL::BOOLEAN", "is_delete_marker": "false",
                  "row_type": "'OBJECT'"}


def parts(d):
    m = json.load(open(os.path.join(d, "manifest.json")))
    files = [f for f in (m.get("files") or m.get("parts") or [])]
    paths = []
    for f in files:
        p = (f.get("key") or f.get("path")) if isinstance(f, dict) else f
        paths.append(os.path.join(d, p) if not os.path.isabs(p) else p)
    if not paths:
        dd = os.path.join(d, "data")
        paths = sorted(os.path.join(dd, x) for x in os.listdir(dd) if x.endswith(".parquet"))
    return m, paths


def size(d):
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(os.path.join(d, "data")) for f in fs if f.endswith(".parquet"))


def view(con, name, paths, ts_ms, sid, cols):
    lst = "[" + ",".join(sql_string(p) for p in paths) + "]"
    have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({lst}, union_by_name=true)").fetchall()}
    sel = []
    for c in cols:
        if c not in have:
            if sid == S3_CURRENT and c in CURRENT_OBJECT:
                sel.append(f"{CURRENT_OBJECT[c]} AS {c}")
            elif sid == GCS_CURRENT and c == "time_created":
                sel.append("NULL::TIMESTAMP AS time_created")
            else:
                raise ValueError(f"schema {sid} is missing column {c}")
        elif c == "last_modified" and sid not in VERSION_IDS:
            sel.append("date_trunc('millisecond', last_modified::TIMESTAMP) AS last_modified" if ts_ms else "last_modified::TIMESTAMP AS last_modified")
        elif c == "size":
            sel.append("size::BIGINT AS size")
        else:
            sel.append(c)
    con.execute(f"CREATE VIEW {name} AS SELECT {', '.join(sel)}, filename, file_row_number FROM read_parquet({lst}, filename=true, file_row_number=true, union_by_name=true)")
    con.execute(f"CREATE TEMP TABLE {name}_files(filename VARCHAR, idx INTEGER)")
    con.executemany(f"INSERT INTO {name}_files VALUES (?, ?)", [(p, i) for i, p in enumerate(paths)])
    con.execute(f"CREATE VIEW {name}_ordered AS SELECT {', '.join(cols)}, row_number() OVER (ORDER BY f.idx, file_row_number) AS ordinal FROM {name} JOIN {name}_files f USING (filename)")


def ascending(con, name, paths, groups):
    """Within-part ordering and strictly disjoint complete groups across parts."""
    op = ">" if groups else ">="
    bad = con.execute(f"""SELECT count(*) FROM (
        SELECT encode(key) AS k, lag(encode(key)) OVER (PARTITION BY filename ORDER BY file_row_number) AS pk
        FROM {name}) WHERE pk IS NOT NULL AND pk {op} k""").fetchone()[0]
    prev = None
    for p in paths:
        lo, hi = con.execute(f"SELECT min(encode(key)), max(encode(key)) FROM {name} WHERE filename = ?", [p]).fetchone()
        if lo is not None and prev is not None and prev >= lo:
            bad += 1
        if hi is not None:
            prev = hi
    lo, hi = con.execute(f"SELECT min(encode(key)), max(encode(key)) FROM {name}").fetchone()
    return bad, lo, hi


def compare(a, b, ts_ms=False):
    import duckdb
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    ma, pa = parts(a)
    mb, pb = parts(b)
    sa, sb = schema_id(ma), schema_id(mb)
    legacy = {"swath/canonical", S3_CURRENT}
    if sa != sb and not {sa, sb} <= legacy:
        raise ValueError(f"incompatible schema ids: {sa} and {sb}")
    if ts_ms and sa in VERSION_IDS:
        raise ValueError("--ts-ms would discard version metadata precision")
    columns = COLS if sa in legacy else COLUMNS.get(sa)
    if columns is None:
        raise ValueError(f"comparison is not supported for {sa}")
    view(con, "A", pa, ts_ms, sa, columns)
    view(con, "B", pb, ts_ms, sb, columns)
    cols = ", ".join(columns)
    na = con.execute("SELECT count(*) FROM A").fetchone()[0]
    nb = con.execute("SELECT count(*) FROM B").fetchone()[0]
    a_b = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM A EXCEPT ALL SELECT {cols} FROM B)").fetchone()[0]
    b_a = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM B EXCEPT ALL SELECT {cols} FROM A)").fetchone()[0]
    sequence = con.execute(f"SELECT count(*) FROM (SELECT ordinal, {cols} FROM A_ordered EXCEPT ALL SELECT ordinal, {cols} FROM B_ordered)").fetchone()[0]
    reverse = con.execute(f"SELECT count(*) FROM (SELECT ordinal, {cols} FROM B_ordered EXCEPT ALL SELECT ordinal, {cols} FROM A_ordered)").fetchone()[0]
    bad_a, mina, maxa = ascending(con, "A", pa, sa in VERSION_IDS)
    bad_b, minb, maxb = ascending(con, "B", pb, sb in VERSION_IDS)
    percol = {}
    if a_b or b_a or sequence or reverse:
        for c in columns:
            percol[c] = con.execute(f"SELECT count(*) FROM A_ordered A JOIN B_ordered B USING (ordinal) WHERE A.{c} IS DISTINCT FROM B.{c}").fetchone()[0]
    out = {
        "schema_id": [sa, sb], "rows": [na, nb], "only_in_a": a_b, "only_in_b": b_a,
        "sequence_differences": [sequence, reverse], "not_ascending": [bad_a, bad_b],
        "differing_columns_at_same_position": {k: v for k, v in percol.items() if v},
        "min_key_equal": mina == minb, "max_key_equal": maxa == maxb,
        "parts": [len(pa), len(pb)], "bytes": [size(a), size(b)],
        "size_ratio_b_over_a": round(size(b) / max(1, size(a)), 4),
        "identical": na == nb and a_b == 0 and b_a == 0 and sequence == 0 and reverse == 0 and bad_a == 0 and bad_b == 0,
    }
    con.close()
    return out


def main():
    if len(sys.argv) not in (3, 4) or (len(sys.argv) == 4 and sys.argv[3] != "--ts-ms"):
        sys.exit("usage: diffds.py A B [--ts-ms]")
    try:
        out = compare(sys.argv[1], sys.argv[2], "--ts-ms" in sys.argv)
    except ValueError as err:
        sys.exit(str(err))
    print(json.dumps(out))
    sys.exit(0 if out["identical"] else 1)


if __name__ == "__main__":
    main()
