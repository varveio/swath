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
        serving = {"protocols": list(protocols)}
        plan = {"mixed_rate_per_group_rps": 100.0, "fixture_count": 1000,
                "fixture_digest": "a" * 64,
                "rate_warmup_cycles": 1,
                "isolated_group_rates_rps": {p: 100.0 for p in protocols},
                "native_page_counts": {p: 100 for p in protocols},
                "native_page_plan_sha256": signatures,
                "mixed_cycles": 2, "isolated_cycles": {p: 6 for p in protocols},
                "minimum_actual_server_utilization": .55,
                "max_actual_utilization_gap": .05,
                "serving_configuration_expected": serving}
        def groups(arm, cycles):
            return [{"group_id": group_id,
                     "protocol": protocols[group_id] if arm == "mixed" else arm,
                     "cycles": cycles, "tickets": 100 * cycles,
                     "objects": 1000 * cycles, "key_digest": "a" * 64,
                     "page_plan_sha": signatures[protocols[group_id]
                                                   if arm == "mixed" else arm],
                     "native_pages": 100, "native_warmup_requests": 100,
                     "native_warmup_objects": 1000,
                     "attempted_requests": 100 * cycles,
                     "successful_requests": 100 * cycles,
                     "delivered_fraction": 1.0,
                     "phase": group_id / 3}
                    for group_id in range(3)]
        def counts(cycles):
            return {"key_digest_basis": "fixture_oracle_certified_by_measured_page_validation",
                    "native_warmup_scheduled": 300, "native_warmup_completed": 300,
                    "target_warmup_scheduled": 300, "target_warmup_admitted": 300,
                    "target_warmup_completed": 300, "target_warmup_unsent": 0,
                    "warmup_attempted_requests": 600, "warmup_successful_requests": 600}
        receipts = []
        for number in range(1, 13):
            mixed_protocols = {}
            for protocol in protocols:
                mixed_protocols[protocol] = {
                    "objects": 2000, "p99_ns": 110,
                    "native_pages": 100, "page_plan_sha256": signatures[protocol],
                    "complete_inventory_cycles": 2, "partial_tail_pages": 0,
                    "delivered_fraction": 1.0, "drain_inclusive_rate_rps": 100.0}
                isolated_item = dict(mixed_protocols[protocol])
                isolated_item.update(objects=18_000, p99_ns=100,
                                     complete_inventory_cycles=18,
                                     drain_inclusive_rate_rps=300.0)
                receipts.append({"round": number, "panel_arm": protocol, "status": "passed",
                                 "serving_configuration": serving,
                                 "server_cpu_utilization": .65,
                                 "server_cpu_ns_per_object": 1000.0,
                                 "result": {"offered_requests_per_group": 600,
                                            **counts(6),
                                            "offered_requests": 1800,
                                            "attempted_requests": 1800,
                                            "successful_requests": 1800,
                                            "requests": 1800, "objects": 18_000,
                                            "groups": groups(protocol, 6),
                                            "unsent_requests": 0,
                                            "protocols": {protocol: isolated_item}}})
            receipts.append({"round": number, "panel_arm": "mixed", "status": "passed",
                             "serving_configuration": serving,
                             "server_cpu_utilization": .65,
                             "server_cpu_ns_per_object": 1020.0,
                             "result": {"objects": 6000, "offered_requests_per_group": 200,
                                        **counts(2),
                                        "offered_requests": 600, "attempted_requests": 600,
                                        "successful_requests": 600, "requests": 600,
                                        "groups": groups("mixed", 2),
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

        changed("mixed", lambda x: x["result"].update(offered_requests_per_group=199),
                "mixed_scheduled_ticket_accounting")
        changed("mixed", lambda x: x["result"]["groups"][2].update(group_id=1),
                "mixed_scheduled_ticket_accounting")
        changed("s3", lambda x: x["result"]["groups"][2].update(protocol="gcs"),
                "isolated_scheduled_ticket_accounting")
        changed("mixed", lambda x: x["result"].update(objects=5999),
                "mixed_scheduled_ticket_accounting")
        changed("gcs", lambda x: x["result"]["protocols"]["gcs"].update(
                page_plan_sha256="f" * 64), "native_page_plan_mismatch")
        changed("azure", lambda x: x["result"]["groups"][1].update(phase=.5),
                "isolated_scheduled_ticket_accounting")
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
        changed("azure", lambda x: x["result"]["groups"][1].update(tickets=599),
                "isolated_scheduled_ticket_accounting")
        changed("mixed", lambda x: x.update(serving_configuration={"protocols": ["s3"]}),
                "serving_configuration_changed")
        changed("mixed", lambda x: x["result"]["protocols"]["s3"].update(
                delivered_fraction=.98), "offered_rate_missed")

        incomplete = deepcopy(receipts)
        incomplete_gcs = next(x for x in incomplete if x["round"] == 1
                              and x["panel_arm"] == "gcs")
        incomplete_gcs["result"]["protocols"].clear()
        invalid = run_mixed.summarize_rounds(plan, incomplete)
        self.assertEqual(invalid["metrics"]["weighted_server_cpu_per_object"]["status"],
                         "incomplete_or_invalid")
        self.assertEqual(len(invalid["metrics"]["weighted_server_cpu_per_object"]["pairs"]), 11)

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

    def test_equal_full_inventory_groups_use_equal_weighted_cpu(self):
        plan, receipts = self.synthetic_cohort()
        for receipt in receipts:
            if receipt["panel_arm"] == "s3":
                receipt["server_cpu_ns_per_object"] = 800.0
            elif receipt["panel_arm"] == "azure":
                receipt["server_cpu_ns_per_object"] = 1200.0
            elif receipt["panel_arm"] == "mixed":
                receipt["server_cpu_ns_per_object"] = 1020.0
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
            preflight = {"purpose": "native_page_plan_preflight", "fixture_count": 10_000,
                         "fixture_digest": "a" * 64,
                         "protocols": {p: {"native_pages": 7000, "page_plan_sha256": "b" * 64}
                                       for p in protocols}}
            preflight_file = root / "page-plan.json"
            preflight_file.write_text(json.dumps(preflight))
            pilots = {}
            for protocol in protocols:
                pilot_root = root / protocol
                pilot_root.mkdir()
                pilot_plan_file = pilot_root / "plan.json"
                pilot_plan_file.write_text(json.dumps({
                    "baseline_distribution": distribution,
                    "candidate_distribution": distribution,
                    "harness_manifest_sha256": "h" * 64,
                    "driver_classes_fingerprints": [{"path": "ReplayMixed.class",
                                                     "sha256": "c" * 64, "size": 1}],
                    "driver_libraries_fingerprints": distribution,
                    "driver_java_opts": "-Xmx2g",
                    "protocol": protocol, "group_count": 3, "lanes_per_group": 16,
                    "rate_per_group_rps": 400.0, "cycles": 2,
                    "fixture_digest": "a" * 64, "fixture_fingerprints": fixture,
                    "server_cpus": "0-5", "client_cpus": "6-15",
                    "server_java_opts": "-Xmx1536m",
                    "server_options": ["--protocols=s3,gcs,azure"],
                    "connections": 16, "max_concurrent_requests": 512}))
                groups = [{"group_id": g, "protocol": protocol,
                           "cycles": 2, "tickets": 14_000, "objects": 20_000,
                           "key_digest": "a" * 64, "page_plan_sha": "b" * 64,
                           "native_pages": 7000, "native_warmup_requests": 7000,
                           "native_warmup_objects": 10_000,
                           "attempted_requests": 14_000,
                           "successful_requests": 14_000,
                           "delivered_fraction": 1.0,
                           "phase": g / 3} for g in range(3)]
                receipt = {"status": "passed", "result": {
                           "page_size": 1000, "fixture_digest": "a" * 64,
                           "elapsed_ns": 35_000_000_000,
                           "warmup_metadata_verified_objects": 30_000,
                           "key_digest_basis": "fixture_oracle_certified_by_measured_page_validation",
                           "native_warmup_scheduled": 21_000,
                           "native_warmup_completed": 21_000,
                           "target_warmup_scheduled": 21_000,
                           "target_warmup_admitted": 21_000,
                           "target_warmup_completed": 21_000,
                           "target_warmup_unsent": 0,
                           "warmup_attempted_requests": 42_000,
                           "warmup_successful_requests": 42_000,
                           "offered_rate_per_group_rps": 400.0,
                           "offered_requests_per_group": 14_000,
                           "offered_requests": 42_000, "attempted_requests": 42_000,
                           "successful_requests": 42_000, "requests": 42_000,
                           "objects": 60_000, "groups": groups,
                           "protocols": {protocol: {"objects": 60_000,
                                                    "complete_inventory_cycles": 6,
                                                    "page_plan_sha256": "b" * 64}}},
                           "fixture_fingerprints": fixture, "fixture_glob": "fixture/*.parquet",
                           "server_cpus": list(range(6)), "client_cpus": list(range(6, 16)),
                           "server_java_opts": "-Xmx1536m", "serving_configuration": expected_serving,
                           "server_command": [str(candidate / "bin/swath-replay"), "serve",
                                              "--serving-mode", "sorted",
                                              "--parquet-connections", "16",
                                              "--max-concurrent-requests", "512",
                                              "--protocols=s3,gcs,azure"],
                           "server_cpu_ns_per_request": 2_000_000.0}
                path = pilot_root / "pilot.json"
                path.write_text(json.dumps(receipt))
                pilots[protocol] = {"path": str(path),
                                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                    "plan_sha256": hashlib.sha256(pilot_plan_file.read_bytes()).hexdigest()}
            plan = {"rounds": 12, "page_size": 1000, "full_warmup_cycles": 1,
                    "group_count": 3, "lanes_per_group": 16,
                    "cost_pilot_cycles": 2, "cost_pilot_rate_per_group_rps": 400.0,
                    "harness_manifest_sha256": "h" * 64,
                    "driver_classes_fingerprints": [{"path": "ReplayMixed.class",
                                                     "sha256": "c" * 64, "size": 1}],
                    "driver_libraries_fingerprints": distribution,
                    "driver_java_opts": "-Xmx2g",
                    "rate_warmup_cycles": 1, "max_outstanding": 512,
                    "max_client_utilization": .70, "max_foreign_cpu_utilization": .02,
                    "max_actual_utilization_gap": .05,
                    "max_client_send_p99_lag_ns": 5_000_000,
                    "minimum_actual_server_utilization": .55,
                    "target_predicted_cpu_utilization": .65,
                    "cpu_cost_ns_per_request": {p: 2_000_000.0 for p in protocols},
                    "cost_pilots": pilots, "isolated_group_rates_rps": {p: 650.0 for p in protocols},
                    "isolated_cycles": {p: 3 for p in protocols},
                    "native_page_counts": {p: 7000 for p in protocols},
                    "native_page_plan_sha256": {p: "b" * 64 for p in protocols},
                    "page_plan_preflight": {"path": str(preflight_file),
                        "sha256": hashlib.sha256(preflight_file.read_bytes()).hexdigest()},
                    "mixed_cycles": 3, "mixed_rate_per_group_rps": 650.0,
                    "fixture_count": 10_000, "fixture_digest": "a" * 64,
                    "fixture_fingerprints": fixture, "fixture_glob": "fixture/*.parquet",
                    "server_cpus": "0-5", "client_cpus": "6-15",
                    "server_java_opts": "-Xmx1536m",
                    "serving_configuration_expected": expected_serving,
                    "server_options": ["--protocols=s3,gcs,azure"],
                    "connections": 16, "max_concurrent_requests": 512,
                    "candidate_distribution": str(candidate),
                    "candidate_distribution_fingerprints": distribution}
            self.assertAlmostEqual(run_mixed.validate_plan(plan), .65)
            plan.update({"bucket": "bench", "fixture": "fixture", "classpath": "/bench:/lib/*",
                         "driver_java_opts": "-Xmx2g", "java_home": "/jdk",
                         "metrics_port": 19092, "port": 19091, "run_timeout": 3600})
            arm = run_mixed.arm_args(plan, {"fixture_count": 10_000,
                                           "fixture_digest": "a" * 64}, fixture,
                                         "s3,gcs,azure", 650.0, 21000)
            self.assertEqual(run_mixed.run_pair.build_driver_command(arm, "baseline"),
                             ["/jdk/bin/java", "--enable-native-access=ALL-UNNAMED", "-Xmx2g",
                              "-cp", "/bench:/lib/*",
                              "io.varve.swath.replay.bench.ReplayMixedOpenLoopBench",
                              "http://127.0.0.1:19091", "bench", "fixture/*.parquet",
                              "10000:" + "a" * 64, "s3,gcs,azure", "650.0", "21000",
                              "1000", "1", "1", "512", "bracket", "end_ack"])
            caps = {name: 1_000_000 for name in
                    ("rss_bytes", "fd_count", "thread_count", "heap_bytes", "direct_bytes",
                     "charged_response_bytes", "active_responses", "cached_rows",
                     "cached_windows", "nmt_committed_bytes")}
            plan["resource_proof"] = {"server_java_opts":
                                      "-Xmx1536m -XX:NativeMemoryTracking=summary",
                                      "caps": caps}
            resource_arm = run_mixed.resource_arm_args(plan,
                    {"fixture_count": 10_000, "fixture_digest": "a" * 64}, fixture)
            self.assertTrue(resource_arm.resource_observation)
            self.assertEqual(resource_arm.resource_caps, caps)
            self.assertIn("NativeMemoryTracking=summary", resource_arm.server_java_opts)
            self.assertEqual(run_mixed.run_pair.build_driver_command(resource_arm, "baseline"),
                             run_mixed.run_pair.build_driver_command(arm, "baseline"))
            plan["mixed_cycles"] = 1
            with self.assertRaisesRegex(ValueError, "complete native inventory"):
                run_mixed.validate_plan(plan)
            plan["mixed_cycles"] = 3
            plan["mixed_rate_per_group_rps"] = math.nan
            with self.assertRaisesRegex(ValueError, "invalid isolated cost"):
                run_mixed.validate_plan(plan)
            plan["mixed_rate_per_group_rps"] = 650.0
            plan["isolated_cycles"]["s3"] = 3.0
            with self.assertRaisesRegex(ValueError, "JSON integers"):
                run_mixed.validate_plan(plan)


if __name__ == "__main__":
    unittest.main()
