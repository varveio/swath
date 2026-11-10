#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Compare swath datasets with DuckDB, dispatching on the manifest schema id.

usage: diffds.py A B [--ts-ms] [--stream | --sql]

Versions compare whole-row multisets (EXCEPT ALL, nulls included) and ordered
row sequences in manifest part/file-row order. Keys may repeat within a part;
parts must remain strictly key-disjoint. Current S3 still projects the legacy
canonical values for Java comparisons. Unknown or incompatible ids are refused.

Two methods give the same answer. The SQL method runs the comparisons as DuckDB
queries over both whole datasets; its EXCEPT ALL holds every row, so its memory
grows with the dataset (143 million rows exhausted 50 GiB). The streaming method
reads both datasets once, part by part in manifest order and each part in
file-row order (checked against file_row_number), and compares them row by row
in bounded memory: the ordered sequences, the key order and bounds, and the
columns that differ at the same position. Equal sequences are equal multisets;
when the sequences differ, a second pass counts the multiset differences
exactly by merging the two streams in key order (each key's rows compared as a
multiset): the file order when both sides are in key order, else each side
sorted by key by DuckDB, which spills to a temporary directory under the
working directory. Without --stream or --sql the streaming method is used when
the two datasets hold more than STREAM_ROWS rows together. DIFFDS_MEMORY sets
each streaming scan's DuckDB memory limit (default 4GB; two run at a time).
"""
from collections import Counter
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dataset_schema import (COLUMNS, S3_CURRENT, GCS_CURRENT, VERSION_IDS,
                            schema_id, sql_string)

COLS = ["key", "size", "last_modified", "etag", "storage_class", "version_id", "is_latest",
        "is_delete_marker", "owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"]
# The canonical values of the reserved columns for a current object.
CURRENT_OBJECT = {"version_id": "NULL::VARCHAR", "is_latest": "NULL::BOOLEAN", "is_delete_marker": "false",
                  "row_type": "'OBJECT'"}
# Above this many rows (both datasets together) the streaming method is the default.
STREAM_ROWS = 20_000_000
# Rows the streaming method fetches from DuckDB at a time, and its per-scan limits.
BATCH_ROWS = 65_536
STREAM_THREADS = 2
STREAM_MEMORY = os.environ.get("DIFFDS_MEMORY", "4GB")


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


def listing(paths):
    return "[" + ",".join(sql_string(p) for p in paths) + "]"


def columns_of(con, paths):
    return {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({listing(paths)}, union_by_name=true)").fetchall()}


def selection(have, ts_ms, sid, cols, union=None):
    """The projection of cols over files holding the columns have. Reading one file of several,
    union is all the files' columns: one only some of them hold is null in the others, as
    union_by_name reads it."""
    sel = []
    for c in cols:
        if c not in have:
            if sid == S3_CURRENT and c in CURRENT_OBJECT:
                sel.append(f"{CURRENT_OBJECT[c]} AS {c}")
            elif sid == GCS_CURRENT and c == "time_created":
                sel.append("NULL::TIMESTAMP AS time_created")
            elif union is not None and c in union:
                sel.append(f"NULL AS {c}")
            else:
                raise ValueError(f"schema {sid} is missing column {c}")
        elif c == "last_modified" and sid not in VERSION_IDS:
            sel.append("date_trunc('millisecond', last_modified::TIMESTAMP) AS last_modified" if ts_ms else "last_modified::TIMESTAMP AS last_modified")
        elif c == "size":
            sel.append("size::BIGINT AS size")
        else:
            sel.append(c)
    return sel


def view(con, name, paths, ts_ms, sid, cols):
    lst = listing(paths)
    sel = selection(columns_of(con, paths), ts_ms, sid, cols)
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


def connect(threads=None, memory=None, temp=None):
    import duckdb
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    if memory:
        con.execute(f"SET memory_limit = {sql_string(memory)}")
    if temp:
        con.execute(f"SET temp_directory = {sql_string(temp)}")
    return con


def compare(a, b, ts_ms=False, method="auto"):
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
    if method == "auto":
        con = connect()
        total = sum(con.execute(f"SELECT count(*) FROM read_parquet({listing(p)})").fetchone()[0] for p in (pa, pb))
        con.close()
        method = "stream" if total > STREAM_ROWS else "sql"
    if method == "stream":
        out = compare_stream(pa, pb, sa, sb, columns, ts_ms)
    elif method == "sql":
        out = compare_sql(pa, pb, sa, sb, columns, ts_ms)
    else:
        raise ValueError(f"unknown method {method}")
    out.update({"parts": [len(pa), len(pb)], "bytes": [size(a), size(b)],
                "size_ratio_b_over_a": round(size(b) / max(1, size(a)), 4), "method": method})
    return out


def verdict(sids, na, nb, a_b, b_a, sequence, reverse, bad_a, bad_b, mina, minb, maxa, maxb, percol):
    return {
        "schema_id": sids, "rows": [na, nb], "only_in_a": a_b, "only_in_b": b_a,
        "sequence_differences": [sequence, reverse], "not_ascending": [bad_a, bad_b],
        "differing_columns_at_same_position": {k: v for k, v in percol.items() if v},
        "min_key_equal": mina == minb, "max_key_equal": maxa == maxb,
        "identical": na == nb and a_b == 0 and b_a == 0 and sequence == 0 and reverse == 0 and bad_a == 0 and bad_b == 0,
    }


def sql_views(pa, pb, sa, sb, columns, ts_ms):
    con = connect()
    view(con, "A", pa, ts_ms, sa, columns)
    view(con, "B", pb, ts_ms, sb, columns)
    return con


def multiset_sql(con, columns):
    cols = ", ".join(columns)
    a_b = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM A EXCEPT ALL SELECT {cols} FROM B)").fetchone()[0]
    b_a = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM B EXCEPT ALL SELECT {cols} FROM A)").fetchone()[0]
    return a_b, b_a


def compare_sql(pa, pb, sa, sb, columns, ts_ms):
    con = sql_views(pa, pb, sa, sb, columns, ts_ms)
    cols = ", ".join(columns)
    na = con.execute("SELECT count(*) FROM A").fetchone()[0]
    nb = con.execute("SELECT count(*) FROM B").fetchone()[0]
    a_b, b_a = multiset_sql(con, columns)
    sequence = con.execute(f"SELECT count(*) FROM (SELECT ordinal, {cols} FROM A_ordered EXCEPT ALL SELECT ordinal, {cols} FROM B_ordered)").fetchone()[0]
    reverse = con.execute(f"SELECT count(*) FROM (SELECT ordinal, {cols} FROM B_ordered EXCEPT ALL SELECT ordinal, {cols} FROM A_ordered)").fetchone()[0]
    bad_a, mina, maxa = ascending(con, "A", pa, sa in VERSION_IDS)
    bad_b, minb, maxb = ascending(con, "B", pb, sb in VERSION_IDS)
    percol = {}
    if a_b or b_a or sequence or reverse:
        for c in columns:
            percol[c] = con.execute(f"SELECT count(*) FROM A_ordered A JOIN B_ordered B USING (ordinal) WHERE A.{c} IS DISTINCT FROM B.{c}").fetchone()[0]
    con.close()
    return verdict([sa, sb], na, nb, a_b, b_a, sequence, reverse, bad_a, bad_b, mina, minb, maxa, maxb, percol)


def key_bytes(k):
    if k is None:
        return None
    return k.encode() if isinstance(k, str) else bytes(k)


def stream(paths, ts_ms, sid, columns):
    """Yields (part index, row) in manifest part order and file-row order. Each part's
    file_row_number must run 0, 1, 2, ...: the order is the file's, not the scan's."""
    con = connect(threads=STREAM_THREADS, memory=STREAM_MEMORY)
    try:
        union = columns_of(con, paths)
        for i, p in enumerate(paths):
            sel = selection(columns_of(con, [p]), ts_ms, sid, columns, union)
            cur = con.execute(f"SELECT {', '.join(sel)}, file_row_number FROM read_parquet({sql_string(p)}, file_row_number=true)")
            n = 0
            while batch := cur.fetchmany(BATCH_ROWS):
                for row in batch:
                    if row[-1] != n:
                        raise ValueError(f"{p}: the scan returned file row {row[-1]} at position {n}")
                    n += 1
                    yield i, row[:-1]
    finally:
        con.close()


class Order:
    """The SQL method's ascending check one row at a time: within a part each key above the
    one before (at or above it for a versions schema); each part's smallest key above the
    largest key of the part before it that has keys; the dataset's smallest and largest key."""

    def __init__(self, groups):
        self.groups, self.bad = groups, 0
        self.part = self.last = self.part_lo = self.part_hi = self.prev_hi = None
        self.lo = self.hi = None

    def close_part(self):
        if self.part_lo is not None and self.prev_hi is not None and self.prev_hi >= self.part_lo:
            self.bad += 1
        if self.part_hi is not None:
            self.prev_hi = self.part_hi

    def add(self, part, key):
        if part != self.part:
            if self.part is not None:
                self.close_part()
            self.part, self.last, self.part_lo, self.part_hi = part, None, None, None
        k = key_bytes(key)
        if k is None:
            return
        if self.last is not None and (self.last > k if self.groups else self.last >= k):
            self.bad += 1
        self.last = k
        if self.part_lo is None or k < self.part_lo:
            self.part_lo = k
        if self.part_hi is None or k > self.part_hi:
            self.part_hi = k
        if self.lo is None or k < self.lo:
            self.lo = k
        if self.hi is None or k > self.hi:
            self.hi = k

    def finish(self):
        if self.part is not None:
            self.close_part()
        return self.bad, self.lo, self.hi


def groups_of(rows):
    """Consecutive rows of one key: (key bytes, Counter of the rows)."""
    key, group = None, Counter()
    for _, row in rows:
        k = key_bytes(row[0])
        if group and k != key:
            yield key, group
            group = Counter()
        key = k
        group[row] += 1
    if group:
        yield key, group


def sorted_stream(paths, ts_ms, sid, columns, temp):
    """Yields (0, row) for every row, sorted by key bytes (an external sort: DuckDB spills
    to temp past its memory limit)."""
    con = connect(threads=STREAM_THREADS, memory=STREAM_MEMORY, temp=temp)
    try:
        sel = selection(columns_of(con, paths), ts_ms, sid, columns)
        cur = con.execute(f"SELECT {', '.join(sel)} FROM read_parquet({listing(paths)}, union_by_name=true) ORDER BY encode(key)")
        while batch := cur.fetchmany(BATCH_ROWS):
            for row in batch:
                yield 0, row
    finally:
        con.close()


def multiset_merge(ra, rb):
    """Exact EXCEPT ALL counts of two streams in key order (each key's rows consecutive):
    merged by key, each key's rows compared as multisets."""
    a_b = b_a = 0
    ga, gb = groups_of(ra), groups_of(rb)
    x, y = next(ga, None), next(gb, None)
    while x is not None or y is not None:
        if y is None or (x is not None and x[0] < y[0]):
            a_b += sum(x[1].values())
            x = next(ga, None)
        elif x is None or y[0] < x[0]:
            b_a += sum(y[1].values())
            y = next(gb, None)
        else:
            a_b += sum((x[1] - y[1]).values())
            b_a += sum((y[1] - x[1]).values())
            x, y = next(ga, None), next(gb, None)
    return a_b, b_a


def compare_stream(pa, pb, sa, sb, columns, ts_ms):
    ia, ib = stream(pa, ts_ms, sa, columns), stream(pb, ts_ms, sb, columns)
    oa, ob = Order(sa in VERSION_IDS), Order(sb in VERSION_IDS)
    na = nb = sequence = reverse = 0
    percol = Counter()
    while True:
        x, y = next(ia, None), next(ib, None)
        if x is None and y is None:
            break
        if x is not None:
            na += 1
            oa.add(x[0], x[1][0])
        if y is not None:
            nb += 1
            ob.add(y[0], y[1][0])
        if x is not None and y is not None:
            if x[1] != y[1]:
                sequence += 1
                reverse += 1
                for c, u, v in zip(columns, x[1], y[1]):
                    if u != v:
                        percol[c] += 1
        elif x is not None:
            sequence += 1
        else:
            reverse += 1
    bad_a, mina, maxa = oa.finish()
    bad_b, minb, maxb = ob.finish()
    if sequence == 0 and reverse == 0:
        a_b = b_a = 0  # equal sequences are equal multisets
    elif bad_a == 0 and bad_b == 0:
        a_b, b_a = multiset_merge(stream(pa, ts_ms, sa, columns), stream(pb, ts_ms, sb, columns))
    else:
        temp = tempfile.mkdtemp(prefix=".diffds-", dir=os.getcwd())
        try:
            a_b, b_a = multiset_merge(sorted_stream(pa, ts_ms, sa, columns, os.path.join(temp, "a")),
                                      sorted_stream(pb, ts_ms, sb, columns, os.path.join(temp, "b")))
        finally:
            shutil.rmtree(temp, ignore_errors=True)
    return verdict([sa, sb], na, nb, a_b, b_a, sequence, reverse, bad_a, bad_b, mina, minb, maxa, maxb, percol)


def main():
    args = sys.argv[1:]
    flags = [x for x in args if x.startswith("--")]
    names = [x for x in args if not x.startswith("--")]
    if len(names) != 2 or any(f not in ("--ts-ms", "--stream", "--sql") for f in flags) or {"--stream", "--sql"} <= set(flags):
        sys.exit("usage: diffds.py A B [--ts-ms] [--stream | --sql]")
    method = "stream" if "--stream" in flags else "sql" if "--sql" in flags else "auto"
    try:
        out = compare(names[0], names[1], "--ts-ms" in flags, method)
    except ValueError as err:
        sys.exit(str(err))
    print(json.dumps(out))
    sys.exit(0 if out["identical"] else 1)


if __name__ == "__main__":
    main()
