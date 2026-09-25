#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Fast deterministic checks for paired statistics and immutable pilot planning."""

import os
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
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

    def test_six_pair_aa_interval_uses_its_own_degrees_of_freedom(self):
        pairs = receipts([100] * 12, [90, 100, 110, 90, 100, 110] * 2)[:12]
        report = run_pair.summarize(pairs, "objects_per_s", .95, "min", rounds=6)
        self.assertEqual(len(report["pairs"]), 6)
        self.assertLess(report["ci95"][0], 1)
        self.assertGreater(report["ci95"][1], 1)
        self.assertGreater(report["ci95_half_width_log"],
                           report["projected_12_pair_ci_half_width_log"])

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
                                     "JDK_JAVA_OPTIONS": "-Xmx7g", "_JAVA_OPTIONS": "-Xmx9g",
                                     "SWATH_REPLAY_OPTS": "-Xmx11g"},
                        clear=True):
            clean, inherited = run_pair.clean_java_env()
        self.assertFalse(any(key in clean for key in inherited))
        self.assertEqual(set(inherited), {"JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS",
                                          "SWATH_REPLAY_OPTS"})

    def test_false_health_body_times_out_without_spinning(self):
        server = SimpleNamespace(poll=lambda: None, returncode=None)
        with patch.object(run_pair, "is_healthy", return_value=False), \
             patch.object(run_pair.time, "monotonic", return_value=42), \
             patch.object(run_pair.time, "sleep") as sleeping:
            with self.assertRaisesRegex(TimeoutError, "did not become healthy"):
                run_pair.wait_for_health(server, "http://127.0.0.1/healthz", 0)
        sleeping.assert_not_called()

    def test_per_thread_cpu_headroom_uses_carrier_deltas(self):
        before = {12: {"name": "carrier-a", "ticks": 100},
                  13: {"name": "carrier-b", "ticks": 50}}
        after = {12: {"name": "carrier-a", "ticks": 150},
                 13: {"name": "carrier-b", "ticks": 60}}
        with patch.object(run_pair.os, "sysconf", return_value=100):
            threads = run_pair.thread_utilization(before, after, 1.0)
        self.assertEqual(threads[0]["name"], "carrier-a")
        self.assertEqual(threads[0]["utilization"], .5)
        self.assertEqual(threads[1]["utilization"], .1)

    def test_gc_log_region_and_measured_cause_window(self):
        with tempfile.TemporaryDirectory() as scratch:
            init = Path(scratch) / "gc-init.log"
            events = Path(scratch) / "gc.log"
            init.write_text("[0.003s][info][gc,init] Heap Region Size: 1M\n")
            events.write_text(
                "[2026-09-25T12:00:00+00:00][info][gc] GC(1) Pause Young "
                "(Concurrent Start) (G1 Humongous Allocation) 1M->1M(1536M) 3.000ms\n"
                "[2026-09-25T12:00:10+00:00][info][gc] GC(2) Pause Young "
                "(Normal) (G1 Evacuation Pause) 1M->1M(1536M) 4.000ms\n")
            start = datetime(2026, 9, 25, 11, 59, 59, tzinfo=timezone.utc).timestamp() * 1000
            end = datetime(2026, 9, 25, 12, 0, 5, tzinfo=timezone.utc).timestamp() * 1000
            summary = run_pair.gc_log_summary(init, events, start, end)
        self.assertEqual(summary["g1_region_bytes"], 1024 ** 2)
        self.assertEqual(summary["gc_pause_cause_counts"]["humongous"], 1)
        self.assertEqual(summary["gc_pause_cause_counts"]["evacuation"], 0)
        self.assertEqual(summary["gc_pause_cause_ms"]["humongous"], 3.0)

    def test_gc_log_numeric_offset_without_colon(self):
        with tempfile.TemporaryDirectory() as scratch:
            init = Path(scratch) / "gc-init.log"
            events = Path(scratch) / "gc.log"
            init.write_text("[0.003s][info][gc,init] Heap Region Size: 1M\n")
            events.write_text(
                "[2026-09-25T13:00:01+0100][info][gc] GC(1) Pause Young "
                "(Normal) (G1 Evacuation Pause) 1M->1M(1536M) 2.000ms\n")
            start = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
            end = datetime(2026, 9, 25, 12, 0, 2, tzinfo=timezone.utc).timestamp() * 1000
            summary = run_pair.gc_log_summary(init, events, start, end)
        self.assertEqual(summary["gc_pause_cause_counts"]["evacuation"], 1)
        self.assertEqual(summary["gc_pause_cause_ms"]["evacuation"], 2.0)


if __name__ == "__main__":
    unittest.main()
