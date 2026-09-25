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
from unittest.mock import Mock, patch

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
    def test_failed_rate_warmup_brackets_live_client_and_acks_before_failure(self):
        state = {}
        stdin = Mock()
        samples = iter((100, 111, 110, 118))
        threads = iter(({1: {"name": "carrier", "ticks": 10}},
                        {1: {"name": "carrier", "ticks": 14}}))
        with patch.object(run_pair, "fetch_json", side_effect=[{"schema_version": 2},
                                                         {"schema_version": 2}]), \
             patch.object(run_pair, "validate_metrics"), \
             patch.object(run_pair, "cpu_ticks", side_effect=samples), \
             patch.object(run_pair, "thread_cpu_ticks", side_effect=threads), \
             patch.object(run_pair, "cpu_busy_ticks", side_effect=[{0: 50}, {0: 60}]), \
             patch.object(run_pair, "rss_bytes", side_effect=[1000, 1100]), \
             patch.object(run_pair.time, "monotonic", side_effect=[10.0, 12.0]):
            run_pair.capture_rate_warmup_marker("RATE_WARMUP_START", state, "/metrics",
                                                1, 2, {0}, stdin)
            self.assertEqual(stdin.write.call_count, 1)
            self.assertNotIn("client_cpu_after", state)
            run_pair.capture_rate_warmup_marker("RATE_WARMUP_END", state, "/metrics",
                                                1, 2, {0}, stdin)
        self.assertEqual(stdin.write.call_count, 2)
        self.assertEqual(stdin.flush.call_count, 2)
        self.assertEqual(state["client_cpu_after"] - state["client_cpu_before"], 7)
        self.assertEqual(state["client_threads_after"][1]["ticks"], 14)
        self.assertEqual(state["metrics_after"], {"schema_version": 2})
        with self.assertRaisesRegex(RuntimeError, "without one start"):
            run_pair.capture_rate_warmup_marker("RATE_WARMUP_END", {}, "/metrics",
                                                1, 2, {0}, stdin)

    def test_missing_rate_warmup_end_metrics_still_acks_and_preserves_error(self):
        state = {}
        stdin = Mock()
        with patch.object(run_pair, "fetch_json", side_effect=[{"schema_version": 2},
                                                         OSError("metrics endpoint unavailable")]), \
             patch.object(run_pair, "validate_metrics"), \
             patch.object(run_pair, "cpu_ticks", side_effect=[100, 200, 110, 210]), \
             patch.object(run_pair, "thread_cpu_ticks", return_value={}), \
             patch.object(run_pair, "cpu_busy_ticks", return_value={0: 50}), \
             patch.object(run_pair, "rss_bytes", return_value=1000), \
             patch.object(run_pair.time, "monotonic", side_effect=[10.0, 12.0]):
            run_pair.capture_rate_warmup_marker("RATE_WARMUP_START", state, "/metrics",
                                                1, 2, {0}, stdin)
            run_pair.capture_rate_warmup_marker("RATE_WARMUP_END", state, "/metrics",
                                                1, 2, {0}, stdin)
        self.assertEqual(stdin.write.call_count, 2)
        self.assertEqual(stdin.flush.call_count, 2)
        self.assertEqual(state["snapshot_error"], "metrics endpoint unavailable")
        self.assertNotIn("metrics_after", state)
        self.assertEqual(state["client_cpu_after"] - state["client_cpu_before"], 10)

    def test_partial_rate_warmup_end_snapshot_cannot_mask_driver_failure(self):
        state = {}
        stdin = Mock()
        with patch.object(run_pair, "fetch_json", return_value={"schema_version": 2}), \
             patch.object(run_pair, "validate_metrics"), \
             patch.object(run_pair, "cpu_ticks", side_effect=[100, 200, 110, 210]), \
             patch.object(run_pair, "thread_cpu_ticks", return_value={}), \
             patch.object(run_pair, "cpu_busy_ticks", side_effect=[{0: 50},
                                                              OSError("host ticks unavailable")]), \
             patch.object(run_pair, "rss_bytes", return_value=1000), \
             patch.object(run_pair.time, "monotonic", side_effect=[10.0, 12.0]):
            run_pair.capture_rate_warmup_marker("RATE_WARMUP_START", state, "/metrics",
                                                1, 2, {0}, stdin)
            run_pair.capture_rate_warmup_marker("RATE_WARMUP_END", state, "/metrics",
                                                1, 2, {0}, stdin)
        receipt = run_pair.rate_warmup_receipt(state)
        self.assertEqual(stdin.write.call_count, 2)
        self.assertEqual(receipt["snapshot_error"], "host ticks unavailable")
        self.assertIsNone(receipt["metrics_after"])
        self.assertFalse(receipt["complete_snapshot"])
        self.assertEqual(receipt["client_cpu_seconds"], 10 / os.sysconf("SC_CLK_TCK"))

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

    def test_flat_driver_command_keeps_native_token_walk_cli(self):
        args = SimpleNamespace(java_home="/jdk", driver_java_opts="-Xmx2g", classpath="classes:lib/*",
                               port=19091, workload="flat", baseline_protocol="s3",
                               candidate_protocol="s3", bucket="bench", fixture_glob="fixture/*.parquet",
                               clients=16, page_size=1000, warmup=1,
                               inventory={"fixture_count": 100, "fixture_digest": "a" * 64},
                               partitioned=True, repetitions=7, end_ack=True)
        self.assertEqual(run_pair.build_driver_command(args, "baseline"),
                         ["/jdk/bin/java", "--enable-native-access=ALL-UNNAMED", "-Xmx2g",
                          "-cp", "classes:lib/*", "io.varve.swath.replay.bench.ReplayHttpBench",
                          "http://127.0.0.1:19091", "s3", "bench", "fixture/*.parquet",
                          "16", "1000", "1", "100:" + "a" * 64, "bracket",
                          "partitioned", "7", "end_ack"])

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
