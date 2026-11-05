#!/usr/bin/env python3
"""Compare two swath sorted datasets (e.g. Go vs Java) with DuckDB.

usage: diffds.py A B [--ts-ms]

Checks: identical row sets on the 13 canonical columns (EXCEPT ALL both ways), both strictly ascending by
key bytes within and across parts (so identical sets imply identical order), manifest min/max key of the
whole dataset, row counts. Prints part counts and dataset sizes side by side. --ts-ms truncates
last_modified to milliseconds on both sides (S3 routes that render milliseconds).
"""
import json, os, sys
import duckdb

COLS = ["key", "size", "last_modified", "etag", "storage_class", "version_id", "is_latest",
        "is_delete_marker", "owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"]


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


def view(con, name, paths, ts_ms):
    sel = []
    for c in COLS:
        if c == "last_modified":
            sel.append("date_trunc('millisecond', last_modified::TIMESTAMP) AS last_modified" if ts_ms else "last_modified::TIMESTAMP AS last_modified")
        elif c == "size":
            sel.append("size::BIGINT AS size")
        else:
            sel.append(c)
    lst = "[" + ",".join("'%s'" % p for p in paths) + "]"
    con.execute(f"CREATE VIEW {name} AS SELECT {', '.join(sel)}, filename, file_row_number FROM read_parquet({lst}, filename=true, file_row_number=true, union_by_name=true)")


def ascending(con, name, paths):
    """Counts adjacent pairs not strictly ascending by key bytes, in part then row order (in DuckDB)."""
    order = ",".join("('%s', %d)" % (p, i) for i, p in enumerate(paths))
    con.execute(f"CREATE TEMP TABLE {name}_files(filename VARCHAR, idx INTEGER)")
    con.execute(f"INSERT INTO {name}_files VALUES {order}")
    bad = con.execute(f"""SELECT count(*) FROM (
        SELECT encode(key) AS k, lag(encode(key)) OVER (ORDER BY f.idx, file_row_number) AS pk
        FROM {name} JOIN {name}_files f USING (filename)) WHERE pk IS NOT NULL AND NOT (pk < k)""").fetchone()[0]
    lo, hi = con.execute(f"SELECT min(encode(key)), max(encode(key)) FROM {name}").fetchone()
    return bad, lo, hi


def main():
    a, b = sys.argv[1], sys.argv[2]
    ts_ms = "--ts-ms" in sys.argv
    con = duckdb.connect()
    ma, pa = parts(a)
    mb, pb = parts(b)
    view(con, "A", pa, ts_ms)
    view(con, "B", pb, ts_ms)
    cols = ", ".join(COLS)
    na = con.execute("SELECT count(*) FROM A").fetchone()[0]
    nb = con.execute("SELECT count(*) FROM B").fetchone()[0]
    a_b = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM A EXCEPT ALL SELECT {cols} FROM B)").fetchone()[0]
    b_a = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM B EXCEPT ALL SELECT {cols} FROM A)").fetchone()[0]
    bad_a, mina, maxa = ascending(con, "A", pa)
    bad_b, minb, maxb = ascending(con, "B", pb)
    percol = {}
    if a_b or b_a:
        for c in COLS:
            percol[c] = con.execute(f"SELECT count(*) FROM A JOIN B USING (key) WHERE A.{c} IS DISTINCT FROM B.{c}").fetchone()[0]
    out = {
        "rows": [na, nb], "only_in_a": a_b, "only_in_b": b_a, "not_ascending": [bad_a, bad_b],
        "min_key_equal": mina == minb, "max_key_equal": maxa == maxb,
        "parts": [len(pa), len(pb)], "bytes": [size(a), size(b)],
        "size_ratio_b_over_a": round(size(b) / max(1, size(a)), 4),
        "differing_columns_on_shared_keys": {k: v for k, v in percol.items() if v},
        "identical": na == nb and a_b == 0 and b_a == 0 and bad_a == 0 and bad_b == 0,
    }
    print(json.dumps(out))
    sys.exit(0 if out["identical"] else 1)


if __name__ == "__main__":
    main()
