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
reads each dataset as the SQL method's view reads it (every part, union_by_name,
the same projection and defaults), every column cast to the type both sides'
union takes (the type the SQL method's set operations compare in), in manifest
part and file-row order (checked against the part index and file_row_number),
(a timestamp or other value Python would not compare exactly as its VARCHAR
rendering) and compares the two row by row in bounded memory: the ordered sequences, the
key order (each key against its immediate predecessor, as the SQL method's lag)
and bounds, and the columns that differ at the same position. Equal sequences
are equal multisets. When the sequences differ, the multiset differences are
counted by the SQL method's EXCEPT ALL, bucket by bucket: both sides are written
to Parquet partitioned by a hash of the whole cast row (equal rows share a
bucket, so the buckets' counts add up to the whole one's) under a temporary
directory in the working directory, as many buckets as keep each to about one
row per BUCKET_ROW_BYTES of the memory limit, and each bucket is compared alone.
(Copies of one identical row all share a bucket.) Without
--stream or --sql the streaming method is used when the two datasets hold more
than STREAM_ROWS rows together. DIFFDS_MEMORY sets the DuckDB memory limit of
each streaming connection (default 4GB; two scans run at a time).
"""
from collections import Counter
import glob
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
# Rows the streaming method fetches from DuckDB at a time, and the streaming
# connections' limits. A multiset bucket holds BUCKET_ROWS rows on average, by
# default one per BUCKET_ROW_BYTES of the memory limit: DuckDB's EXCEPT ALL holds
# its input in memory (the blockchain pair failed at 50 GiB, over 175 bytes a row).
BATCH_ROWS = 65_536
BUCKET_ROWS = None
BUCKET_ROW_BYTES = 2048
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


def selection(have, ts_ms, sid, cols):
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


def projected(con, paths, ts_ms, sid, columns, extra=""):
    """The SQL method's view of a dataset, as a query: every part through union_by_name and
    the same projection (selection over the whole dataset's columns)."""
    sel = selection(columns_of(con, paths), ts_ms, sid, columns)
    return f"SELECT {', '.join(sel)}{extra} FROM read_parquet({listing(paths)}, union_by_name=true, filename=true, file_row_number=true)"


def common_types(pa, pb, sa, sb, columns, ts_ms):
    """Each column's type in the union of the two views: the type the SQL method's set
    operations compare the two sides in. A column null on both sides is VARCHAR."""
    con = connect()
    try:
        q = f"{projected(con, pa, ts_ms, sa, columns)} UNION ALL {projected(con, pb, ts_ms, sb, columns)}"
        types = [r[1] for r in con.execute(f"DESCRIBE {q}").fetchall()]
    finally:
        con.close()
    return ["VARCHAR" if t in ("NULL", '"NULL"') else t for t in types]


def cast(columns, types):
    return ", ".join(f"CAST({c} AS {t}) AS {c}" for c, t in zip(columns, types))


# Types whose Python values compare exactly as DuckDB compares them. A column of another type
# (timestamps, of any precision and zone; anything else) is streamed as the VARCHAR rendering
# of its common-type value, which is one string per value.
PYTHON_EXACT = {"VARCHAR", "BLOB", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
                "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"}


def stream_cast(columns, types):
    return ", ".join(f"CAST({c} AS {t}) AS {c}" if t in PYTHON_EXACT else f"CAST(CAST({c} AS {t}) AS VARCHAR) AS {c}"
                     for c, t in zip(columns, types))


def stream(paths, ts_ms, sid, columns, types):
    """Yields (part index, row) of the view, cast to types (stream_cast), in manifest part order and
    file-row order: each row's part must not go back, and its file_row_number must run
    0, 1, 2, ... within the part (an empty part yields nothing)."""
    con = connect(threads=STREAM_THREADS, memory=STREAM_MEMORY)
    try:
        files = "[" + ",".join(sql_string(p) for p in paths) + "]"
        inner = projected(con, paths, ts_ms, sid, columns, f", list_position({files}, filename) AS part_index, file_row_number")
        cur = con.execute(f"SELECT {stream_cast(columns, types)}, part_index, file_row_number FROM ({inner})")
        part, n = 0, 0
        while batch := cur.fetchmany(BATCH_ROWS):
            for row in batch:
                if row[-2] != part:
                    if row[-2] is None or row[-2] < part or row[-1] != 0:
                        raise ValueError(f"the scan of {paths[0]}... left manifest order at part {row[-2]} row {row[-1]}")
                    part, n = row[-2], 0
                elif row[-1] != n:
                    raise ValueError(f"the scan returned file row {row[-1]} of part {part} at position {n}")
                n += 1
                yield part, row[:-2]
    finally:
        con.close()


class Order:
    """The SQL method's ascending check one row at a time: within a part each key against its
    immediate predecessor (a pair with a null key is not compared, as lag's null predicate);
    each part's smallest non-null key above the largest of the part before it that has keys;
    the dataset's smallest and largest non-null key."""

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
        if k is not None and self.last is not None and (self.last > k if self.groups else self.last >= k):
            self.bad += 1
        self.last = k
        if k is None:
            return
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


def memory_bytes(limit):
    """DIFFDS_MEMORY in bytes: a number with KB, MB, GB, TB (powers of 1000) or KiB, MiB,
    GiB, TiB (powers of 1024), as DuckDB's memory_limit reads it."""
    units = {"KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12, "KIB": 2**10, "MIB": 2**20, "GIB": 2**30, "TIB": 2**40, "B": 1}
    text = limit.strip().upper().replace(" ", "")
    for unit in sorted(units, key=len, reverse=True):
        if text.endswith(unit):
            return int(float(text[:-len(unit)]) * units[unit])
    raise ValueError(f"DIFFDS_MEMORY {limit!r} is not a size such as 4GB")


def multiset_buckets(pa, pb, sa, sb, columns, ts_ms, types, rows):
    """The SQL method's two EXCEPT ALL counts, bucket by bucket: each side's cast rows written
    to Parquet partitioned by hash(row) % buckets, then each bucket's EXCEPT ALL in both
    directions. Equal rows hash alike, so no row's match is in another bucket. Memory is
    DuckDB's, bounded by DIFFDS_MEMORY, spilling under the temporary directory."""
    per_bucket = BUCKET_ROWS or max(1, memory_bytes(STREAM_MEMORY) // BUCKET_ROW_BYTES)
    buckets = max(1, -(-rows // per_bucket))
    cols = ", ".join(columns)
    temp = tempfile.mkdtemp(prefix=".diffds-", dir=os.getcwd())
    try:
        con = connect(threads=STREAM_THREADS, memory=STREAM_MEMORY, temp=os.path.join(temp, "spill"))
        con.execute("SET preserve_insertion_order = false")
        for side, paths, sid in (("a", pa, sa), ("b", pb, sb)):
            rows_cast = f"SELECT {cast(columns, types)} FROM ({projected(con, paths, ts_ms, sid, columns)})"
            con.execute(f"COPY (SELECT hash({cols}) % {buckets} AS bucket, {cols} FROM ({rows_cast})) "
                        f"TO {sql_string(os.path.join(temp, side))} (FORMAT parquet, PARTITION_BY (bucket))")
        a_b = b_a = 0
        for i in range(buckets):
            fa = sorted(glob.glob(os.path.join(temp, "a", f"bucket={i}", "*.parquet")))
            fb = sorted(glob.glob(os.path.join(temp, "b", f"bucket={i}", "*.parquet")))
            ra = f"SELECT {cols} FROM read_parquet({listing(fa)}, hive_partitioning=false)" if fa else None
            rb = f"SELECT {cols} FROM read_parquet({listing(fb)}, hive_partitioning=false)" if fb else None
            if ra and rb:
                a_b += con.execute(f"SELECT count(*) FROM ({ra} EXCEPT ALL {rb})").fetchone()[0]
                b_a += con.execute(f"SELECT count(*) FROM ({rb} EXCEPT ALL {ra})").fetchone()[0]
            elif ra:
                a_b += con.execute(f"SELECT count(*) FROM ({ra})").fetchone()[0]
            elif rb:
                b_a += con.execute(f"SELECT count(*) FROM ({rb})").fetchone()[0]
        con.close()
        return a_b, b_a
    finally:
        shutil.rmtree(temp, ignore_errors=True)


def row_count(paths):
    con = connect()
    try:
        return con.execute(f"SELECT count(*) FROM read_parquet({listing(paths)})").fetchone()[0]
    finally:
        con.close()


def compare_stream(pa, pb, sa, sb, columns, ts_ms):
    types = common_types(pa, pb, sa, sb, columns, ts_ms)
    ia, ib = stream(pa, ts_ms, sa, columns, types), stream(pb, ts_ms, sb, columns, types)
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
    for n, paths in ((na, pa), (nb, pb)):
        if n != row_count(paths):
            raise ValueError(f"the scan of {paths[0]}... returned {n} rows, the parts hold {row_count(paths)}")
    bad_a, mina, maxa = oa.finish()
    bad_b, minb, maxb = ob.finish()
    if sequence == 0 and reverse == 0:
        a_b = b_a = 0  # equal sequences are equal multisets
    else:
        a_b, b_a = multiset_buckets(pa, pb, sa, sb, columns, ts_ms, types, na + nb)
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
