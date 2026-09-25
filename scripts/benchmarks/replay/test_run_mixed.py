#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Pure plan, order and paired-CI checks for the fixed mixed benchmark."""

import hashlib
import json
import math
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

import run_mixed


class MixedPlanTest(unittest.TestCase):
    @staticmethod
    def synthetic_cohort():
        protocols = run_mixed.PROTOCOLS
        signatures = {p: (str(index + 1) * 64) for index, p in enumerate(protocols)}
        phase_pages = {"s3": 0, "gcs": 33, "azure": 66}
        time_phase = {"s3": 0.0, "gcs": .333333, "azure": .666667}
        serving = {"protocols": list(protocols)}
        plan = {"mixed_rate_each_rps": 100.0,
                "isolated_rates_rps": {p: 300.0 for p in protocols},
                "native_page_counts": {p: 100 for p in protocols},
                "native_page_plan_sha256": signatures,
                "mixed_cycles": 2, "isolated_cycles": {p: 6 for p in protocols},
                "minimum_actual_server_utilization": .55,
                "max_actual_utilization_gap": .05,
                "serving_configuration_expected": serving}
        receipts = []
        for number in range(1, 13):
            mixed_protocols = {}
            for protocol in protocols:
                mixed_protocols[protocol] = {
                    "objects": 100_000, "p99_ns": 110,
                    "native_pages": 100, "page_plan_sha256": signatures[protocol],
                    "phase_offset_pages": phase_pages[protocol],
                    "time_phase_fraction": time_phase[protocol],
                    "complete_inventory_cycles": 2, "partial_tail_pages": 0,
                    "delivered_fraction": 1.0, "drain_inclusive_rate_rps": 100.0}
                isolated_item = dict(mixed_protocols[protocol])
                isolated_item.update(p99_ns=100, complete_inventory_cycles=6,
                                     drain_inclusive_rate_rps=300.0)
                receipts.append({"round": number, "panel_arm": protocol, "status": "passed",
                                 "serving_configuration": serving,
                                 "server_cpu_utilization": .65,
                                 "server_cpu_ns_per_object": 1000.0,
                                 "result": {"offered_requests_each": 600,
                                            "unsent_requests": 0,
                                            "protocols": {protocol: isolated_item}}})
            receipts.append({"round": number, "panel_arm": "mixed", "status": "passed",
                             "serving_configuration": serving,
                             "server_cpu_utilization": .65,
                             "server_cpu_ns_per_object": 1020.0,
                             "result": {"objects": 300_000, "offered_requests_each": 200,
                                        "unsent_requests": 0, "protocols": mixed_protocols}})
        return plan, receipts

    def test_synthetic_48_receipts_pass_and_each_quality_mutation_fails(self):
        plan, receipts = self.synthetic_cohort()
        summary = run_mixed.summarize_rounds(plan, receipts)
        self.assertEqual(summary["status"], "passed")
        self.assertAlmostEqual(summary["metrics"]["weighted_server_cpu_per_object"]
                               ["geometric_mean"], 1.02)

        def changed(arm, mutate, reason):
            cohort = deepcopy(receipts)
            receipt = next(x for x in cohort if x["round"] == 1 and x["panel_arm"] == arm)
            mutate(receipt)
            result = run_mixed.summarize_rounds(plan, cohort)
            self.assertEqual(result["status"], "inconclusive_or_failed")
            self.assertIn(reason, {item["reason"] for item in result["issues"]})

        changed("mixed", lambda x: x["result"].update(offered_requests_each=199),
                "mixed_scheduled_ticket_accounting")
        changed("gcs", lambda x: x["result"]["protocols"]["gcs"].update(
                page_plan_sha256="f" * 64), "native_page_plan_mismatch")
        changed("azure", lambda x: x["result"]["protocols"]["azure"].update(
                phase_offset_pages=7), "canonical_phase_identity_changed")
        changed("mixed", lambda x: x["result"]["protocols"]["gcs"].update(
                drain_inclusive_rate_rps=98.0), "achieved_rate_below_offered_99pct")
        changed("mixed", lambda x: x.update(server_cpu_utilization=.54),
                "mixed_cpu_utilization_below_contention_floor")
        changed("mixed", lambda x: x.update(server_cpu_utilization=.71),
                "mixed_cpu_utilization_above_70pct")
        changed("s3", lambda x: x.update(server_cpu_utilization=.59),
                "aggregate_utilization_not_matched")
        changed("gcs", lambda x: x["result"]["protocols"].clear(),
                "isolated_protocol_scope_changed")
        changed("azure", lambda x: x["result"]["protocols"]["azure"].update(
                partial_tail_pages=1), "isolated_scheduled_ticket_accounting")
        changed("mixed", lambda x: x.update(serving_configuration={"protocols": ["s3"]}),
                "serving_configuration_changed")
        changed("mixed", lambda x: x["result"]["protocols"]["s3"].update(
                delivered_fraction=.98), "offered_rate_missed")

        cpu_fail = deepcopy(receipts)
        for receipt in cpu_fail:
            if receipt["panel_arm"] == "mixed":
                receipt["server_cpu_ns_per_object"] = 1060.0
        self.assertEqual(run_mixed.summarize_rounds(plan, cpu_fail)["metrics"]
                         ["weighted_server_cpu_per_object"]["status"], "inconclusive_or_failed")
        p99_fail = deepcopy(receipts)
        for receipt in p99_fail:
            if receipt["panel_arm"] == "mixed":
                receipt["result"]["protocols"]["azure"]["p99_ns"] = 121
        self.assertEqual(run_mixed.summarize_rounds(plan, p99_fail)["metrics"]
                         ["azure_p99"]["status"], "inconclusive_or_failed")

    def test_object_weighted_isolated_cpu_uses_mixed_object_shares(self):
        plan, receipts = self.synthetic_cohort()
        for receipt in receipts:
            if receipt["panel_arm"] == "s3":
                receipt["server_cpu_ns_per_object"] = 800.0
            elif receipt["panel_arm"] == "azure":
                receipt["server_cpu_ns_per_object"] = 1200.0
            elif receipt["panel_arm"] == "mixed":
                p = receipt["result"]["protocols"]
                p["s3"]["objects"] = 200_000
                p["gcs"]["objects"] = 50_000
                p["azure"]["objects"] = 50_000
                receipt["server_cpu_ns_per_object"] = 918.0
        summary = run_mixed.summarize_rounds(plan, receipts)
        self.assertEqual(summary["status"], "passed")
        self.assertAlmostEqual(summary["metrics"]["weighted_server_cpu_per_object"]
                               ["geometric_mean"], 1.02)

    def test_williams_order_balances_positions_and_directed_carryover(self):
        rows = [run_mixed.balanced_order(number) for number in range(1, 5)]
        for arm in run_mixed.ARM_ORDER:
            self.assertEqual(sorted(row.index(arm) for row in rows), list(range(4)))
        pairs = [(row[i], row[i + 1]) for row in rows for i in range(3)]
        self.assertEqual(len(pairs), 12)
        self.assertEqual(len(set(pairs)), 12)
        self.assertEqual(rows, [run_mixed.balanced_order(number) for number in range(5, 9)])

    def test_whole_interval_must_meet_threshold(self):
        self.assertEqual(run_mixed.interval([1.0] * 12, 1.05)["status"], "passed")
        self.assertEqual(run_mixed.interval([1.08] * 12, 1.05)["status"],
                         "inconclusive_or_failed")
        self.assertEqual(run_mixed.interval([1.0] * 11, 1.05)["status"],
                         "incomplete_or_invalid")

    def test_cost_preflight_and_exact_integer_cycles(self):
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch)
            protocols = run_mixed.PROTOCOLS
            fixture = [{"path": "part.parquet", "sha256": "f" * 64, "size": 1}]
            distribution = [{"path": "lib/swath-replay.jar", "sha256": "d" * 64, "size": 1}]
            expected_serving = {"protocols": list(protocols)}
            candidate = root / "candidate"
            pilot_plan_file = root / "plan.json"
            pilot_plan_file.write_text(json.dumps({"baseline_distribution": distribution,
                                                   "candidate_distribution": distribution}))
            pilot_plan_sha = hashlib.sha256(pilot_plan_file.read_bytes()).hexdigest()
            preflight = {"purpose": "native_page_plan_preflight", "fixture_count": 1000,
                         "fixture_digest": "a" * 64,
                         "protocols": {p: {"native_pages": 1000, "page_plan_sha256": "b" * 64}
                                       for p in protocols}}
            preflight_file = root / "page-plan.json"
            preflight_file.write_text(json.dumps(preflight))
            pilots = {}
            for protocol in protocols:
                receipt = {"status": "passed", "result": {"protocol": protocol,
                           "page_size": 1000, "objects": 1000, "repetitions": 1,
                           "fixture_digest": "a" * 64},
                           "fixture_fingerprints": fixture, "fixture_glob": "fixture/*.parquet",
                           "server_cpus": list(range(6)), "client_cpus": list(range(6, 16)),
                           "server_java_opts": "-Xmx1536m", "serving_configuration": expected_serving,
                           "server_command": [str(candidate / "bin/swath-replay"), "serve",
                                              "--serving-mode", "sorted",
                                              "--parquet-connections", "16",
                                              "--max-concurrent-requests", "512",
                                              "--protocols=s3,gcs,azure"],
                           "server_cpu_ns_per_request": 2_000_000.0}
                path = root / (protocol + ".json")
                path.write_text(json.dumps(receipt))
                pilots[protocol] = {"path": str(path),
                                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                    "plan_sha256": pilot_plan_sha}
            plan = {"rounds": 12, "page_size": 1000, "full_warmup_cycles": 1,
                    "cost_pilot_repetitions": 1,
                    "rate_warmup_cycles": 9, "max_outstanding": 512,
                    "max_client_utilization": .70, "max_foreign_cpu_utilization": .02,
                    "max_actual_utilization_gap": .05,
                    "max_client_send_p99_lag_ns": 5_000_000,
                    "minimum_actual_server_utilization": .55,
                    "target_predicted_cpu_utilization": .60,
                    "cpu_cost_ns_per_request": {p: 2_000_000.0 for p in protocols},
                    "cost_pilots": pilots, "isolated_rates_rps": {p: 1800.0 for p in protocols},
                    "isolated_cycles": {p: 56 for p in protocols},
                    "native_page_counts": {p: 1000 for p in protocols},
                    "native_page_plan_sha256": {p: "b" * 64 for p in protocols},
                    "page_plan_preflight": {"path": str(preflight_file),
                        "sha256": hashlib.sha256(preflight_file.read_bytes()).hexdigest()},
                    "mixed_cycles": 19, "mixed_rate_each_rps": 600.0,
                    "fixture_count": 1000, "fixture_digest": "a" * 64,
                    "fixture_fingerprints": fixture, "fixture_glob": "fixture/*.parquet",
                    "server_cpus": "0-5", "client_cpus": "6-15",
                    "server_java_opts": "-Xmx1536m",
                    "serving_configuration_expected": expected_serving,
                    "server_options": ["--protocols=s3,gcs,azure"],
                    "connections": 16, "max_concurrent_requests": 512,
                    "candidate_distribution": str(candidate),
                    "candidate_distribution_fingerprints": distribution}
            self.assertAlmostEqual(run_mixed.validate_plan(plan), .60)
            plan.update({"bucket": "bench", "fixture": "fixture", "classpath": "/bench:/lib/*",
                         "driver_java_opts": "-Xmx2g", "java_home": "/jdk",
                         "metrics_port": 19092, "port": 19091, "run_timeout": 3600})
            arm = run_mixed.arm_args(plan, {"fixture_count": 1000,
                                           "fixture_digest": "a" * 64}, fixture,
                                         "s3,gcs,azure", 600.0, 19000)
            self.assertEqual(run_mixed.run_pair.build_driver_command(arm, "baseline"),
                             ["/jdk/bin/java", "--enable-native-access=ALL-UNNAMED", "-Xmx2g",
                              "-cp", "/bench:/lib/*",
                              "io.varve.swath.replay.bench.ReplayMixedOpenLoopBench",
                              "http://127.0.0.1:19091", "bench", "fixture/*.parquet",
                              "1000:" + "a" * 64, "s3,gcs,azure", "600.0", "19000",
                              "1000", "1", "9", "512", "bracket", "end_ack"])
            caps = {name: 1_000_000 for name in
                    ("rss_bytes", "fd_count", "thread_count", "heap_bytes", "direct_bytes",
                     "charged_response_bytes", "active_responses", "cached_rows",
                     "cached_windows", "nmt_committed_bytes")}
            plan["resource_proof"] = {"server_java_opts":
                                      "-Xmx1536m -XX:NativeMemoryTracking=summary",
                                      "caps": caps}
            resource_arm = run_mixed.resource_arm_args(plan,
                    {"fixture_count": 1000, "fixture_digest": "a" * 64}, fixture)
            self.assertTrue(resource_arm.resource_observation)
            self.assertEqual(resource_arm.resource_caps, caps)
            self.assertIn("NativeMemoryTracking=summary", resource_arm.server_java_opts)
            self.assertEqual(run_mixed.run_pair.build_driver_command(resource_arm, "baseline"),
                             run_mixed.run_pair.build_driver_command(arm, "baseline"))
            plan["mixed_cycles"] = 1
            with self.assertRaisesRegex(ValueError, "complete native inventory"):
                run_mixed.validate_plan(plan)
            plan["mixed_cycles"] = 19
            plan["mixed_rate_each_rps"] = math.nan
            with self.assertRaisesRegex(ValueError, "invalid isolated cost"):
                run_mixed.validate_plan(plan)
            plan["mixed_rate_each_rps"] = 600.0
            plan["isolated_cycles"]["s3"] = 56.0
            with self.assertRaisesRegex(ValueError, "JSON integers"):
                run_mixed.validate_plan(plan)


if __name__ == "__main__":
    unittest.main()
