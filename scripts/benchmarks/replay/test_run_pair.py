#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Fast deterministic checks for paired statistics and immutable pilot planning."""

import os
import unittest
from unittest.mock import patch

import run_pair


def receipts(base, candidate, metric="objects_per_s"):
    result = []
    for round_number in range(1, 13):
        result.append({"round": round_number, "arm": "baseline", "status": "passed",
                       "result": {metric: base[round_number - 1]}})
        result.append({"round": round_number, "arm": "candidate", "status": "passed",
                       "result": {metric: candidate[round_number - 1]}})
    return result


class PairRunnerTest(unittest.TestCase):
    def test_identity_pairs_have_exact_unit_interval(self):
        report = run_pair.summarize(receipts([100] * 12, [100] * 12),
                                    "objects_per_s", .95, "min")
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["geometric_mean"], 1.0)
        self.assertEqual(report["ci95"], [1.0, 1.0])

    def test_whole_interval_must_clear_budget(self):
        pairs = receipts([100] * 12, [100, 100, 100, 100, 100, 100,
                                      100, 100, 100, 100, 100, 40])
        report = run_pair.summarize(pairs, "objects_per_s", .95, "min")
        self.assertEqual(report["status"], "inconclusive_or_failed")
        self.assertLess(report["ci95"][0], .95)

    def test_failure_and_missing_pair_do_not_disappear(self):
        pairs = receipts([100] * 12, [100] * 12)
        pairs[3]["status"] = "failed"
        self.assertEqual(run_pair.summarize(pairs, "objects_per_s", .95, "min")["status"],
                         "failed_pair")
        pairs.pop()
        self.assertEqual(run_pair.summarize(pairs, "objects_per_s", .95, "min")["status"],
                         "failed_pair")
        pairs[3]["status"] = "passed"
        self.assertEqual(run_pair.summarize(pairs, "objects_per_s", .95, "min")["status"],
                         "incomplete")

    def test_zero_baseline_is_explicit(self):
        both_zero = run_pair.summarize(receipts([0] * 12, [0] * 12, "backing_rows_per_object"),
                                       "backing_rows_per_object", 1.1, "max")
        self.assertEqual(both_zero["status"], "passed")
        excess = run_pair.summarize(receipts([0] * 12, [0] * 11 + [1], "backing_rows_per_object"),
                                    "backing_rows_per_object", 1.1, "max")
        self.assertEqual(excess["status"], "failed_zero_baseline")

    def test_pilot_repetitions_multiply_existing_work(self):
        self.assertEqual(run_pair.suggest_repetitions(3, 50, 12.5), 12)
        with self.assertRaises(ValueError):
            run_pair.suggest_repetitions(1, 50, 0)

    def test_heap_and_inherited_java_options(self):
        self.assertEqual(run_pair.heap_bytes("-Xms1g -Xmx1536m"), 1536 * 1024 ** 2)
        with patch.dict(os.environ, {"JAVA_TOOL_OPTIONS": "-Dsecret=redacted",
                                     "JDK_JAVA_OPTIONS": "-Xmx7g", "_JAVA_OPTIONS": "-Xmx9g"}):
            clean, inherited = run_pair.clean_java_env()
        self.assertFalse(any(key in clean for key in inherited))
        self.assertEqual(set(inherited), {"JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"})


if __name__ == "__main__":
    unittest.main()
