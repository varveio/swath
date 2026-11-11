# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Manual reader tier: python3 -m unittest -v test_dataset_readers (requires DuckDB).

The schema-only tests also run without DuckDB; SQL and verifier tests then skip.
"""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from dataset_schema import AZBLOB_ADDED, COLUMNS, VERSION_IDS, compare_rows, schema_id, sql_string
from bench.diffds import compare

FIXTURES = Path(__file__).parent / "testdata" / "readers"


def fixtures():
    return [json.loads(p.read_text()) for p in sorted(FIXTURES.glob("*.json"))]


class WholeRows(unittest.TestCase):
    def test_dispatch(self):
        for f in fixtures():
            self.assertEqual(schema_id(f), f["schema_id"])
            self.assertEqual(set(f["rows"][0]), set(COLUMNS[schema_id(f)]))
        for sid in (None, "swath/unknown/v1"):
            with self.assertRaises(ValueError):
                schema_id({"schema_id": sid})
        self.assertEqual(schema_id({"schema_id": "swath/local-entries/v1"}), "swath/local-entries/v1")

    def test_sequences_multiplicity_metadata_and_nulls(self):
        self.assertEqual({f["schema_id"] for f in fixtures()}, VERSION_IDS)
        for f in fixtures():
            sid, rows = f["schema_id"], f["rows"]
            with self.subTest(schema=sid):
                self.assertEqual(compare_rows(sid, rows, copy.deepcopy(rows)), (True, True))
                self.assertEqual(compare_rows(sid, rows, [rows[1], rows[0], *rows[2:]]), (False, True))
                self.assertEqual(compare_rows(sid, rows, [*rows, rows[0]]), (False, False))
                for column in COLUMNS[sid]:
                    changed = copy.deepcopy(rows)
                    value = changed[0][column]
                    changed[0][column] = 0 if value is None else None
                    self.assertEqual(compare_rows(sid, rows, changed), (False, False), column)
                # A SQL null is not the literal S3 ID "null" or a false boolean.
                changed = copy.deepcopy(rows)
                changed[1]["size"] = 0 if sid.endswith("s3-versions/v1") else None
                self.assertEqual(compare_rows(sid, rows, changed), (False, False))


@unittest.skipUnless(importlib.util.find_spec("duckdb"), "DuckDB is not installed")
class DuckDBComparison(unittest.TestCase):
    def dataset(self, directory, fixture, rows):
        import duckdb
        directory.mkdir()
        (directory / "data").mkdir()
        sid = fixture["schema_id"]
        # Exact schema types keep null-only string fields from being inferred as integers.
        strings = {"key", "etag", "storage_class", "owner_id", "owner_display_name",
                   "checksum_algorithm", "checksum_type", "version_id", "md5", "crc32c", "content_type",
                   "content_md5", "blob_type", "access_tier", "archive_status", "rehydrate_priority", "content_encoding",
                   "encryption_scope", "customer_provided_key_sha256", "lease_state", "metadata", "tags",
                   "immutability_policy_mode", "snapshot"}
        booleans = {"access_tier_inferred", "server_encrypted", "legal_hold"}
        cols = COLUMNS[sid]
        types = ["VARCHAR" if c in strings else "BOOLEAN" if c.startswith("is_") or c in booleans else "BIGINT" for c in cols]
        con = duckdb.connect()
        con.execute("CREATE TABLE rows(" + ",".join(f'"{c}" {t}' for c, t in zip(cols, types)) + ")")
        con.executemany("INSERT INTO rows VALUES (" + ",".join("?" for _ in cols) + ")",
                        [tuple(row[c] for c in cols) for row in rows])
        con.execute(f"COPY rows TO {sql_string(directory / 'data/part-00000.parquet')} (FORMAT parquet)")
        con.close()
        (directory / "manifest.json").write_text(json.dumps({"schema_id": sid, "parts": [{"path": "data/part-00000.parquet"}]}))

    def test_actual_sql_on_both_schemas(self):
        for fixture in fixtures():
            for mutation in ("same", "reorder", "unordered", "duplicate", "null", "metadata"):
                with self.subTest(schema=fixture["schema_id"], mutation=mutation), tempfile.TemporaryDirectory() as td:
                    rows = fixture["rows"]
                    changed = copy.deepcopy(rows)
                    if mutation == "reorder":
                        changed[0], changed[1] = changed[1], changed[0]
                    elif mutation == "unordered":
                        changed[2], changed[3] = changed[3], changed[2]
                    elif mutation == "duplicate":
                        changed.insert(0, changed[0].copy())
                    elif mutation == "null":
                        changed[0]["size"] = None
                    elif mutation == "metadata":
                        changed[0]["last_modified"] += 1
                    a, b = Path(td) / "a", Path(td) / "b"
                    self.dataset(a, fixture, rows)
                    self.dataset(b, fixture, changed)
                    result = compare(str(a), str(b), method="sql")
                    self.assertEqual(result["identical"], mutation == "same", result)
                    if mutation in ("reorder", "unordered"):
                        self.assertEqual(result["only_in_a"], 0)
                        self.assertGreater(result["sequence_differences"][0], 0)
                    if mutation == "unordered":
                        self.assertGreater(result["not_ascending"][1], 0)
                    # The streaming method gives the SQL method's answer, every count included.
                    streamed = compare(str(a), str(b), method="stream")
                    self.assertEqual(streamed.pop("method"), "stream")
                    result.pop("method")
                    self.assertEqual(streamed, result)


def s3_rows(keys, **columns):
    """A SELECT of S3 objects with these keys (None for a null key), one row each in order;
    columns overrides a column's SQL expression, None leaves the column out."""
    cols = {"key": "k", "size": "CAST(100 AS BIGINT)", "last_modified": "TIMESTAMP '2026-01-01 00:00:00.123456'",
            "etag": "'e'", "storage_class": "'STANDARD'", "owner_id": "NULL::VARCHAR", "owner_display_name": "NULL::VARCHAR",
            "checksum_algorithm": "NULL::VARCHAR", "checksum_type": "NULL::VARCHAR"}
    cols.update(columns)
    listed = "[" + ",".join("NULL" if k is None else sql_string(k) for k in keys) + "]::VARCHAR[]"
    return (f"SELECT {', '.join(f'{v} AS {c}' for c, v in cols.items() if v is not None)} "
            f"FROM (SELECT unnest({listed}) AS k, generate_subscripts({listed}, 1) AS i) ORDER BY i")


def write_dataset(directory, schema, part_queries):
    import duckdb
    (directory / "data").mkdir(parents=True)
    con = duckdb.connect()
    entries = []
    for i, query in enumerate(part_queries):
        path = f"data/part-{i:05d}.parquet"
        con.execute(f"COPY ({query}) TO {sql_string(directory / path)} (FORMAT parquet)")
        entries.append({"path": path})
    con.close()
    (directory / "manifest.json").write_text(json.dumps({"schema_id": schema, "parts": entries}))


@unittest.skipUnless(importlib.util.find_spec("duckdb"), "DuckDB is not installed")
class StreamingMatchesSQL(unittest.TestCase):
    """The streaming method's report is the SQL method's, every count included, on the shapes
    where a row-by-row reader could part from DuckDB's view: columns and types that differ
    across parts and sides, null keys, empty and several parts, canonical against current,
    --ts-ms, and (with tiny buckets) the bucketed multiset pass."""

    S3 = "swath/s3-objects/v1"
    CASES = {
        "a column some parts lack": ([s3_rows(["a"], is_delete_marker="false"), s3_rows(["b"])],
                                     [s3_rows(["a"], is_delete_marker="false"), s3_rows(["b"], is_delete_marker="false")]),
        "a column's type across parts": ([s3_rows(["a"], etag="CAST(1 AS BIGINT)"), s3_rows(["b"], etag="'1'")],
                                         [s3_rows(["a", "b"], etag="'1'")]),
        "a column's type across sides": ([s3_rows(["a", "b"], etag="CAST(1 AS BIGINT)")], [s3_rows(["a", "b"], etag="'1'")]),
        "null keys, same": ([s3_rows(["b", None, "a"])], [s3_rows(["b", None, "a"])]),
        "null keys, differing": ([s3_rows(["b", None, "a", None])], [s3_rows(["b", None, "c"])]),
        "empty and several parts": ([s3_rows(["a", "b"]), s3_rows([]), s3_rows(["c"])], [s3_rows(["a"]), s3_rows(["b", "c"])]),
        "overlapping parts": ([s3_rows(["a", "b"]), s3_rows(["c"])], [s3_rows(["a", "b"]), s3_rows(["b", "c"])]),
        "a changed value": ([s3_rows(["a", "b", "c"])], [s3_rows(["a", "b", "c"], size="CASE WHEN k = 'b' THEN 7 ELSE 100 END")]),
        "a duplicated row": ([s3_rows(["a", "b"])], [s3_rows(["a", "a", "b"])]),
        "signed zeros": ([s3_rows(["a", "b"], etag="-CAST(0.0 AS DOUBLE)")], [s3_rows(["a", "b"], etag="CAST(0.0 AS DOUBLE)")]),
        "NaN variants": ([s3_rows(["a", "b"], etag="CAST('NaN' AS DOUBLE)")], [s3_rows(["a", "b"], etag="-CAST('NaN' AS DOUBLE)")]),
        "a differing float": ([s3_rows(["a", "b"], etag="CAST(1.5 AS DOUBLE)")],
                              [s3_rows(["a", "b"], etag="CASE WHEN k = 'b' THEN CAST('NaN' AS DOUBLE) ELSE 1.5 END")]),
        "microseconds": ([s3_rows(["a", "b"])], [s3_rows(["a", "b"], last_modified="TIMESTAMP '2026-01-01 00:00:00.123'")]),
    }

    def check(self, a, b, ts_ms=False):
        from bench import diffds
        sql = compare(str(a), str(b), ts_ms, method="sql")
        sql.pop("method")
        for bucket_rows in (diffds.BUCKET_ROWS, 2):
            saved, diffds.BUCKET_ROWS = diffds.BUCKET_ROWS, bucket_rows
            try:
                streamed = compare(str(a), str(b), ts_ms, method="stream")
            finally:
                diffds.BUCKET_ROWS = saved
            streamed.pop("method")
            self.assertEqual(streamed, sql, f"bucket rows {bucket_rows}")
        return sql

    def test_cases(self):
        for name, (pa, pb) in self.CASES.items():
            for ts_ms in (False, True):
                with self.subTest(case=name, ts_ms=ts_ms), tempfile.TemporaryDirectory() as td:
                    a, b = Path(td) / "a", Path(td) / "b"
                    write_dataset(a, self.S3, pa)
                    write_dataset(b, self.S3, pb)
                    result = self.check(a, b, ts_ms)
                    if name == "a column some parts lack":
                        self.assertFalse(result["identical"])  # union_by_name: null, not the canonical false
                    if name in ("a column's type across parts", "a column's type across sides", "null keys, same",
                                "empty and several parts", "signed zeros", "NaN variants"):
                        self.assertTrue(result["identical"], result)
                    if name == "a differing float":
                        self.assertFalse(result["identical"], result)
                    if name == "microseconds":
                        self.assertEqual(result["identical"], ts_ms, result)

    def test_canonical_against_current(self):
        canonical = {"version_id": "NULL::VARCHAR", "is_latest": "NULL::BOOLEAN", "is_delete_marker": "false", "row_type": "'OBJECT'"}
        with tempfile.TemporaryDirectory() as td:
            a, b = Path(td) / "a", Path(td) / "b"
            write_dataset(a, "swath/canonical", [s3_rows(["a", "b"], **canonical)])
            write_dataset(b, self.S3, [s3_rows(["a", "b"])])
            self.assertTrue(self.check(a, b)["identical"])
            self.assertTrue(self.check(a, b, ts_ms=True)["identical"])

    def test_one_key_group_streams_in_bounded_memory(self):
        """Two million versions of one key, one differing: the multiset pass is DuckDB's,
        bounded by DIFFDS_MEMORY, not a Python group of the key's rows."""
        import resource
        import subprocess
        import sys
        version = ("SELECT 'k' AS key, CAST(i AS BIGINT) AS size, TIMESTAMP '2026-01-01' AS last_modified, md5(i::VARCHAR) AS etag, "
                   "'STANDARD' AS storage_class, NULL::VARCHAR AS owner_id, NULL::VARCHAR AS owner_display_name, "
                   "NULL::VARCHAR AS checksum_algorithm, NULL::VARCHAR AS checksum_type, {vid} AS version_id, i = 0 AS is_latest, "
                   "false AS is_delete_marker FROM range(2000000) t(i) ORDER BY i")
        with tempfile.TemporaryDirectory() as td:
            a, b = Path(td) / "a", Path(td) / "b"
            write_dataset(a, "swath/s3-versions/v1", [version.format(vid="'v' || i")])
            write_dataset(b, "swath/s3-versions/v1", [version.format(vid="CASE WHEN i = 1234567 THEN 'changed' ELSE 'v' || i END")])
            env = dict(__import__("os").environ, DIFFDS_MEMORY="300MB")
            proc = subprocess.run([sys.executable, str(Path(__file__).parent / "bench" / "diffds.py"), str(a), str(b), "--stream"],
                                  cwd=td, env=env, capture_output=True, text=True)
            peak_kib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
            self.assertTrue(proc.stdout, proc.stderr)
            result = json.loads(proc.stdout)
            self.assertEqual(proc.returncode, 1, proc.stderr)
            self.assertEqual((result["only_in_a"], result["only_in_b"], result["sequence_differences"]), (1, 1, [1, 1]), result)
            self.assertLess(peak_kib, 1_000_000, f"peak RSS {peak_kib} KiB")  # 0.4 GB; a Python group of the key would take 2 GB
            sql = compare(str(a), str(b), method="sql")
            sql.pop("method"), result.pop("method")
            self.assertEqual(result, sql)


@unittest.skipUnless(importlib.util.find_spec("duckdb"), "DuckDB is not installed")
class VerifierSchema(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import os
        import subprocess
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.base = Path(cls.temp.name)
        env = dict(os.environ, SWATH_VERIFIER_FIXTURES=str(cls.base), GOTOOLCHAIN="auto")
        subprocess.run(["go", "test", "./internal/listing", "-run", "^TestVerifierFixtures$", "-count=1"],
                       cwd=Path(__file__).parent, env=env, check=True, text=True)

    def verify(self, path):
        import contextlib
        import io
        from verify import main
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = main(str(path))
        return status, output.getvalue()

    def test_verifier_rejects_missing_generation_column(self):
        import subprocess
        import sys
        proc = subprocess.run([sys.executable, str(Path(__file__).parent / "verify.py"),
                               str(self.base / "generations/missing_size")], text=True, capture_output=True)
        self.assertNotEqual(proc.returncode, 0, proc.stdout)
        self.assertIn("FAIL   size:", proc.stdout)
        self.assertIn("RESULT: FAILED", proc.stdout)

    def test_verifier_runs_from_any_working_directory(self):
        import subprocess
        import sys
        dataset = self.base / "generations/valid"
        with tempfile.TemporaryDirectory() as cwd:
            proc = subprocess.run([sys.executable, str(Path(__file__).resolve().parent / "verify.py"),
                                   str(dataset), str(dataset)], cwd=cwd, text=True, capture_output=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("whole-row ordered sequence and multiset match reference", proc.stdout)

    def test_complete_object_contract_in_every_part(self):
        for schema in ("current", "generations", "azblob", "azblobversions"):
            for dataset in sorted((self.base / schema).iterdir()):
                with self.subTest(schema=schema, mutation=dataset.name):
                    status, output = self.verify(dataset)
                    allowed = (dataset.name == "valid" or (schema == "current" and dataset.name == "missing_time_created")
                               or (schema == "azblob" and dataset.name.removeprefix("missing_") in AZBLOB_ADDED))
                    self.assertEqual(status == 0, allowed, output)
                    if not allowed:
                        column = dataset.name.split("_", 1)[1].removeprefix("type_")
                        self.assertIn("FAIL   " + column + ":", output)

    def test_azure_source_kind_must_match(self):
        dataset = self.base / "azblob/valid"
        manifest = dataset / "manifest.json"
        original = manifest.read_text()
        try:
            changed = json.loads(original)
            changed["extensions"]["swath"]["source"]["kind"] = "local-fs"
            manifest.write_text(json.dumps(changed))
            status, output = self.verify(dataset)
            self.assertEqual(status, 1, output)
            self.assertIn("FAIL source kind agrees with schema id", output)
        finally:
            manifest.write_text(original)

    def test_azure_size_accepts_signed_int64(self):
        import duckdb
        from verify import check_object_schema
        from dataset_schema import AZBLOB_CURRENT
        source = self.base / "azblob/valid/data/part-00000.parquet"
        with tempfile.TemporaryDirectory() as td, duckdb.connect() as con:
            for typ in ("BIGINT", "UBIGINT", "INTEGER", "VARCHAR"):
                with self.subTest(type=typ):
                    part = Path(td) / (typ + ".parquet")
                    con.execute(f"COPY (SELECT * REPLACE (CAST(size AS {typ}) AS size) "
                                f"FROM read_parquet({sql_string(source)}) UNION ALL "
                                f"SELECT * REPLACE (CAST(NULL AS {typ}) AS size) "
                                f"FROM read_parquet({sql_string(source)})) TO {sql_string(part)} (FORMAT PARQUET)")
                    verdicts = {}
                    check_object_schema(con, part, AZBLOB_CURRENT,
                                        lambda good, message: verdicts.update({message.split(":")[0].strip(): good}))
                    self.assertEqual(verdicts["size"], typ == "BIGINT")

    def test_azure_timestamps_require_utc_microseconds(self):
        import duckdb
        from verify import check_object_schema
        from dataset_schema import AZBLOB_CURRENT
        source = self.base / "azblob/valid/data/part-00000.parquet"
        with tempfile.TemporaryDirectory() as td, duckdb.connect() as con:
            for column in ("last_modified", "creation_time"):
                for typ in ("TIMESTAMP", "TIMESTAMP_MS", "TIMESTAMP_NS", "TIMESTAMPTZ"):
                    with self.subTest(column=column, type=typ):
                        part = Path(td) / (typ + ".parquet")
                        con.execute(f"COPY (SELECT * REPLACE (CAST({column} AS {typ}) AS {column}) "
                                    f"FROM read_parquet({sql_string(source)}) UNION ALL "
                                    f"SELECT * REPLACE (CAST(NULL AS {typ}) AS {column}) "
                                    f"FROM read_parquet({sql_string(source)})) TO {sql_string(part)} (FORMAT PARQUET)")
                        verdicts = {}
                        check_object_schema(con, part, AZBLOB_CURRENT,
                                            lambda good, message: verdicts.update({message.split(":")[0].strip(): good}))
                        self.assertEqual(verdicts[column], typ == "TIMESTAMPTZ")

    def test_time_deleted_requires_utc_microseconds(self):
        import duckdb
        from verify import check_object_schema
        from dataset_schema import GCS_VERSIONS
        source = self.base / "generations/valid/data/part-00000.parquet"
        with tempfile.TemporaryDirectory() as td, duckdb.connect() as con:
            for typ in ("TIMESTAMP", "TIMESTAMP_MS", "TIMESTAMP_NS", "TIMESTAMPTZ"):
                with self.subTest(type=typ):
                    part = Path(td) / (typ + ".parquet")
                    con.execute(f"COPY (SELECT * REPLACE (CAST(time_deleted AS {typ}) AS time_deleted) "
                                f"FROM read_parquet({sql_string(source)}) UNION ALL "
                                f"SELECT * REPLACE (CAST(NULL AS {typ}) AS time_deleted) "
                                f"FROM read_parquet({sql_string(source)})) TO {sql_string(part)} (FORMAT PARQUET)")
                    verdicts = {}
                    check_object_schema(con, part, GCS_VERSIONS, lambda good, message: verdicts.update({message.split(":")[0].strip(): good}))
                    self.assertEqual(verdicts["time_deleted"], typ == "TIMESTAMPTZ")


if __name__ == "__main__":
    unittest.main()
