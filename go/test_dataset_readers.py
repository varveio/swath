# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Run with python3 -m unittest test_dataset_readers (DuckDB SQL tests optional)."""
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
        self.assertEqual(schema_id({"schema_id": "swath-local/v1"}), "swath-local/v1")

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


if __name__ == "__main__":
    unittest.main()
