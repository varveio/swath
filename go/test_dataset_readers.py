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

from dataset_schema import COLUMNS, VERSION_IDS, compare_rows, schema_id, sql_string
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
                   "checksum_algorithm", "checksum_type", "version_id", "md5", "crc32c", "content_type"}
        cols = COLUMNS[sid]
        types = ["VARCHAR" if c in strings else "BOOLEAN" if c.startswith("is_") else "BIGINT" for c in cols]
        con = duckdb.connect()
        con.execute("CREATE TABLE rows(" + ",".join(f'"{c}" {t}' for c, t in zip(cols, types)) + ")")
        con.executemany("INSERT INTO rows VALUES (" + ",".join("?" for _ in cols) + ")",
                        [tuple(row[c] for c in cols) for row in rows])
        con.execute(f"COPY rows TO {sql_string(directory / 'data/part-00000.parquet')} (FORMAT parquet)")
        con.close()
        (directory / "manifest.json").write_text(json.dumps({"schema_id": sid, "parts": [{"path": "data/part-00000.parquet"}]}))

    def test_actual_sql_on_both_schemas(self):
        for fixture in fixtures():
            for mutation in ("same", "reorder", "duplicate", "null", "metadata"):
                with self.subTest(schema=fixture["schema_id"], mutation=mutation), tempfile.TemporaryDirectory() as td:
                    rows = fixture["rows"]
                    changed = copy.deepcopy(rows)
                    if mutation == "reorder":
                        changed[0], changed[1] = changed[1], changed[0]
                    elif mutation == "duplicate":
                        changed.insert(0, changed[0].copy())
                    elif mutation == "null":
                        changed[0]["size"] = None
                    elif mutation == "metadata":
                        changed[0]["last_modified"] += 1
                    a, b = Path(td) / "a", Path(td) / "b"
                    self.dataset(a, fixture, rows)
                    self.dataset(b, fixture, changed)
                    result = compare(str(a), str(b))
                    self.assertEqual(result["identical"], mutation == "same", result)
                    if mutation == "reorder":
                        self.assertEqual(result["only_in_a"], 0)
                        self.assertGreater(result["sequence_differences"][0], 0)


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

    def test_complete_gcs_contract_in_every_part(self):
        for schema in ("current", "generations"):
            for dataset in sorted((self.base / schema).iterdir()):
                with self.subTest(schema=schema, mutation=dataset.name):
                    status, output = self.verify(dataset)
                    allowed = dataset.name == "valid" or (schema == "current" and dataset.name == "missing_time_created")
                    self.assertEqual(status == 0, allowed, output)
                    if not allowed:
                        column = dataset.name.split("_", 1)[1].removeprefix("type_")
                        self.assertIn("FAIL   " + column + ":", output)

    def test_time_deleted_requires_utc_microseconds(self):
        import duckdb
        from verify import check_gcs_schema
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
                    check_gcs_schema(con, part, GCS_VERSIONS, lambda good, message: verdicts.update({message.split(":")[0].strip(): good}))
                    self.assertEqual(verdicts["time_deleted"], typ == "TIMESTAMPTZ")


if __name__ == "__main__":
    unittest.main()
