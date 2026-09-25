#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Fixed 12-round, four-arm native-token open-loop replay comparison.

The plan is frozen before round one from separately retained isolated CPU-cost pilots.
Every arm uses run_pair.run_arm for process isolation, counters and failure receipts.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import socket
import statistics
import subprocess
from pathlib import Path
from types import SimpleNamespace

import run_pair

PROTOCOLS = ("s3", "gcs", "azure")
ARM_ORDER = (*PROTOCOLS, "mixed")
WILLIAMS = (("s3", "gcs", "mixed", "azure"),
            ("gcs", "azure", "s3", "mixed"),
            ("azure", "mixed", "gcs", "s3"),
            ("mixed", "s3", "azure", "gcs"))


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def balanced_order(round_number):
    return WILLIAMS[(round_number - 1) % len(WILLIAMS)]


def serving_matches(receipt, expected):
    serving = receipt.get("serving_configuration") or {}
    return all(serving.get(name) == value for name, value in expected.items())


def command_value(command, option):
    try:
        return command[command.index(option) + 1]
    except (ValueError, IndexError):
        return None


def exact_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def interval(ratios, ceiling):
    if len(ratios) != 12 or any(r <= 0 or not math.isfinite(r) for r in ratios):
        return {"status": "incomplete_or_invalid", "pairs": ratios}
    logs = [math.log(r) for r in ratios]
    mean = statistics.mean(logs)
    half = 2.201 * statistics.stdev(logs) / math.sqrt(12)
    ci = [math.exp(mean - half), math.exp(mean + half)]
    return {"status": "passed" if ci[1] <= ceiling else "inconclusive_or_failed",
            "pairs": ratios, "geometric_mean": math.exp(mean), "ci95": ci,
            "sample_log_ratio_sd": statistics.stdev(logs), "threshold": ceiling}


def validate_plan(plan):
    if (not all(exact_int(plan[name]) for name in
                ("rounds", "page_size", "full_warmup_cycles", "cost_pilot_repetitions",
                 "rate_warmup_cycles", "max_outstanding"))
            or plan["rounds"] != 12 or plan["page_size"] != 1000
            or plan["full_warmup_cycles"] != 1):
        raise ValueError("mixed gate requires 12 fixed rounds, 1k pages and one full native warmup")
    if plan["cost_pilot_repetitions"] < 1:
        raise ValueError("isolated CPU-cost pilots must have fixed full inventory repetitions")
    if not exact_int(plan["rate_warmup_cycles"]) or not 1 <= plan["rate_warmup_cycles"] <= 10:
        raise ValueError("target-rate warmup must cover complete native page-plan cycles")
    if plan["max_outstanding"] < 1 or plan["max_outstanding"] > 512:
        raise ValueError("invalid fixed outstanding cap")
    if plan["max_client_utilization"] != .70 or plan["max_foreign_cpu_utilization"] != .02:
        raise ValueError("client and host quality limits changed")
    if (plan["max_actual_utilization_gap"] <= 0 or plan["max_actual_utilization_gap"] > .05
            or plan["max_client_send_p99_lag_ns"] <= 0
            or not .55 <= plan["minimum_actual_server_utilization"] <= .70
            or not .60 <= plan["target_predicted_cpu_utilization"] <= .70):
        raise ValueError("matched-utilization or dispatch-lag quality limit missing")
    if any(not math.isfinite(plan[name]) for name in
           ("max_actual_utilization_gap", "max_client_send_p99_lag_ns",
            "minimum_actual_server_utilization", "target_predicted_cpu_utilization")):
        raise ValueError("nonfinite mixed quality limit")
    if set(plan["cpu_cost_ns_per_request"]) != set(PROTOCOLS) \
            or set(plan["cost_pilots"]) != set(PROTOCOLS) \
            or set(plan["isolated_rates_rps"]) != set(PROTOCOLS) \
            or set(plan["isolated_cycles"]) != set(PROTOCOLS) \
            or set(plan["native_page_counts"]) != set(PROTOCOLS) \
            or set(plan["native_page_plan_sha256"]) != set(PROTOCOLS):
        raise ValueError("all three isolated cost receipts and rates are required")
    costs = plan["cpu_cost_ns_per_request"]
    rate = plan["mixed_rate_each_rps"]
    if any(not math.isfinite(v) or v <= 0 for v in costs.values()) \
            or not math.isfinite(rate) or rate <= 0:
        raise ValueError("invalid isolated cost pilots or mixed rate")
    if (any(not exact_int(value) for value in plan["native_page_counts"].values())
            or not exact_int(plan["mixed_cycles"])
            or any(not exact_int(value) for value in plan["isolated_cycles"].values())):
        raise ValueError("native page counts and cycle counts must be JSON integers")
    if len(set(plan["native_page_counts"].values())) != 1:
        raise ValueError("equal-rate mixed arm requires one shared native page count")
    preflight = plan["page_plan_preflight"]
    if sha256(preflight["path"]) != preflight["sha256"]:
        raise ValueError("native page-plan preflight receipt changed")
    observed_plan = json.loads(Path(preflight["path"]).read_text())
    if (observed_plan.get("purpose") != "native_page_plan_preflight"
            or observed_plan.get("fixture_count") != plan["fixture_count"]
            or observed_plan.get("fixture_digest") != plan["fixture_digest"]
            or set(observed_plan.get("protocols", {})) != set(PROTOCOLS)):
        raise ValueError("native page-plan preflight scope differs from fixed fixture")
    for protocol in PROTOCOLS:
        item = observed_plan["protocols"][protocol]
        if (item["native_pages"] != plan["native_page_counts"][protocol]
                or item["page_plan_sha256"] != plan["native_page_plan_sha256"][protocol]):
            raise ValueError("native page-plan preflight count/hash changed")
    page_count = next(iter(plan["native_page_counts"].values()))
    if page_count < 1 or page_count > plan["fixture_count"] \
            or page_count * plan["rate_warmup_cycles"] > 1_000_000 \
            or plan["mixed_cycles"] < 1 \
            or page_count * plan["mixed_cycles"] > 1_000_000 \
            or (page_count * plan["mixed_cycles"] - 1) / plan["mixed_rate_each_rps"] < 31 \
            or page_count * plan["rate_warmup_cycles"] / plan["mixed_rate_each_rps"] < 5:
        raise ValueError("mixed arm must schedule exact complete native inventory cycles")
    for protocol in PROTOCOLS:
        pilot = plan["cost_pilots"][protocol]
        if sha256(pilot["path"]) != pilot["sha256"]:
            raise ValueError("isolated cost pilot receipt changed")
        pilot_plan_path = Path(pilot["path"]).with_name("plan.json")
        if sha256(pilot_plan_path) != pilot["plan_sha256"]:
            raise ValueError("isolated cost pilot build plan changed")
        pilot_plan = json.loads(pilot_plan_path.read_text())
        if (pilot_plan["baseline_distribution"] != plan["candidate_distribution_fingerprints"]
                or pilot_plan["candidate_distribution"] != plan["candidate_distribution_fingerprints"]):
            raise ValueError("isolated cost pilot used a different distribution build")
        receipt = json.loads(Path(pilot["path"]).read_text())
        if receipt["status"] != "passed" or receipt["result"]["protocol"] != protocol \
                or receipt["result"]["page_size"] != 1000 \
                or receipt["result"]["objects"] \
                   != plan["fixture_count"] * plan["cost_pilot_repetitions"] \
                or receipt["result"]["repetitions"] != plan["cost_pilot_repetitions"] \
                or receipt["result"]["fixture_digest"] != plan["fixture_digest"] \
                or receipt["fixture_fingerprints"] != plan["fixture_fingerprints"] \
                or receipt["fixture_glob"] != plan["fixture_glob"] \
                or receipt["server_cpus"] != sorted(run_pair.cpus(plan["server_cpus"])) \
                or receipt["client_cpus"] != sorted(run_pair.cpus(plan["client_cpus"])) \
                or receipt["server_java_opts"] != plan["server_java_opts"] \
                or sorted(receipt["serving_configuration"]["protocols"]) != sorted(PROTOCOLS) \
                or not serving_matches(receipt, plan["serving_configuration_expected"]) \
                or not set(plan["server_options"]) <= set(receipt["server_command"]) \
                or command_value(receipt["server_command"], "--parquet-connections") \
                   != str(plan["connections"]) \
                or command_value(receipt["server_command"], "--serving-mode") != "sorted" \
                or command_value(receipt["server_command"], "--max-concurrent-requests") \
                   != str(plan["max_concurrent_requests"]) \
                or Path(receipt["server_command"][0]).resolve() \
                   != Path(plan["candidate_distribution"]).resolve() / "bin/swath-replay" \
                or not math.isclose(receipt["server_cpu_ns_per_request"], costs[protocol],
                                    rel_tol=1e-9):
            raise ValueError("isolated cost pilot does not ground the fixed plan")
    predicted_cpu_util = rate * sum(costs.values()) / (len(run_pair.cpus(
            plan["server_cpus"])) * 1e9)
    if predicted_cpu_util > .70:
        raise ValueError("mixed offered rate exceeds 70% predicted CPU capacity")
    if abs(predicted_cpu_util - plan["target_predicted_cpu_utilization"]) > .01:
        raise ValueError("mixed rate misses the predeclared nontrivial CPU target")
    for protocol in PROTOCOLS:
        derived_rate = rate * sum(costs.values()) / costs[protocol]
        actual = plan["isolated_rates_rps"][protocol]
        if not math.isfinite(actual) or abs(actual / derived_rate - 1) > .01:
            raise ValueError("isolated rate does not match predeclared aggregate CPU target")
        if (plan["isolated_cycles"][protocol] < 1
                or page_count * plan["isolated_cycles"][protocol] > 1_000_000
                or (page_count * plan["isolated_cycles"][protocol] - 1) / actual < 31
                or page_count * plan["rate_warmup_cycles"] / actual < 5):
            raise ValueError(f"isolated {protocol} arm must schedule exact complete inventory cycles")
    return predicted_cpu_util


def arm_args(plan, inventory, fixture_fingerprints, protocol, rate, offered_each):
    candidate = Path(plan["candidate_distribution"]).resolve()
    return SimpleNamespace(
        baseline=candidate / "bin/swath-replay", candidate=candidate / "bin/swath-replay",
        baseline_option=plan["server_options"],
        candidate_option=plan["server_options"],
        baseline_protocol=protocol, candidate_protocol=protocol,
        bucket=plan["bucket"], fixture=plan["fixture"], fixture_glob=plan["fixture_glob"],
        fixture_fingerprints=fixture_fingerprints, inventory=inventory,
        classpath=plan["classpath"], clients=0, connections=plan["connections"],
        delimiter_prefix=None, driver_java_opts=plan["driver_java_opts"], end_ack=True,
        java_home=plan["java_home"], max_client_utilization=plan["max_client_utilization"],
        max_concurrent_requests=plan["max_concurrent_requests"],
        max_foreign_cpu_utilization=plan["max_foreign_cpu_utilization"],
        metrics_port=plan["metrics_port"], min_duration=30, mode="sorted", page_size=1000,
        partitioned=False, port=plan["port"], repetitions=1, run_timeout=plan["run_timeout"],
        server_java_opts=plan["server_java_opts"], start_timeout=60, warmup=1,
        workload="mixed_open_loop", mixed_protocols=protocol,
        mixed_rate=rate, mixed_offered_each=offered_each,
        mixed_rate_warmup_cycles=plan["rate_warmup_cycles"],
        mixed_max_outstanding=plan["max_outstanding"],
        mixed_max_client_send_p99_lag_ns=plan["max_client_send_p99_lag_ns"])


def resource_arm_args(plan, inventory, fixture_fingerprints):
    proof = plan["resource_proof"]
    offered = plan["native_page_counts"]["s3"] * plan["mixed_cycles"]
    args = arm_args(plan, inventory, fixture_fingerprints, "s3,gcs,azure",
                    plan["mixed_rate_each_rps"], offered)
    args.server_java_opts = proof["server_java_opts"]
    args.resource_observation = True
    args.resource_caps = proof["caps"]
    return args


def summarize_rounds(plan, rounds):
    cpu_ratios = []
    p99_ratios = {protocol: [] for protocol in PROTOCOLS}
    issues = []
    utilization = []
    for number in range(1, 13):
        by_arm = {receipt["panel_arm"]: receipt for receipt in rounds
                  if receipt["round"] == number}
        if set(by_arm) != set(ARM_ORDER) or any(x["status"] != "passed" for x in by_arm.values()):
            issues.append({"round": number, "reason": "incomplete_or_failed_arm"})
            continue
        mixed = by_arm["mixed"]
        mixed_result = mixed["result"]
        for arm, receipt in by_arm.items():
            if not serving_matches(receipt, plan["serving_configuration_expected"]):
                issues.append({"round": number, "reason": "serving_configuration_changed",
                               "arm": arm})
        if set(mixed_result["protocols"]) != set(PROTOCOLS):
            issues.append({"round": number, "reason": "mixed_protocol_set_changed"})
            continue
        weighted_cost = 0.0
        mixed_objects = mixed_result["objects"]
        expected_mixed_tickets = (plan["native_page_counts"]["s3"]
                                  * plan["mixed_cycles"])
        if mixed_result["offered_requests_each"] != expected_mixed_tickets \
                or mixed_result["unsent_requests"] != 0:
            issues.append({"round": number, "reason": "mixed_scheduled_ticket_accounting"})
        if mixed["server_cpu_utilization"] < plan["minimum_actual_server_utilization"]:
            issues.append({"round": number, "reason": "mixed_cpu_utilization_below_contention_floor"})
        if mixed["server_cpu_utilization"] > .70:
            issues.append({"round": number, "reason": "mixed_cpu_utilization_above_70pct"})
        utilization.append({"round": number,
                            "mixed_server_cpu_utilization": mixed["server_cpu_utilization"],
                            "isolated_server_cpu_utilization": {
                                p: by_arm[p]["server_cpu_utilization"] for p in PROTOCOLS}})
        for protocol in PROTOCOLS:
            isolated = by_arm[protocol]
            isolated_scope = isolated["result"].get("protocols", {})
            if set(isolated_scope) != {protocol}:
                issues.append({"round": number, "reason": "isolated_protocol_scope_changed",
                               "protocol": protocol})
                continue
            isolated_result = isolated_scope[protocol]
            mixed_protocol = mixed_result["protocols"][protocol]
            if isolated["server_cpu_utilization"] < plan["minimum_actual_server_utilization"]:
                issues.append({"round": number, "reason": "isolated_cpu_utilization_below_contention_floor",
                               "protocol": protocol})
            if isolated["server_cpu_utilization"] > .70:
                issues.append({"round": number, "reason": "isolated_cpu_utilization_above_70pct",
                               "protocol": protocol})
            expected_iso_tickets = (plan["native_page_counts"][protocol]
                                    * plan["isolated_cycles"][protocol])
            if isolated["result"]["offered_requests_each"] != expected_iso_tickets \
                    or isolated["result"]["unsent_requests"] != 0 \
                    or isolated_result["partial_tail_pages"] != 0 \
                    or isolated_result["complete_inventory_cycles"] != plan["isolated_cycles"][protocol] \
                    or mixed_protocol["partial_tail_pages"] != 0 \
                    or mixed_protocol["complete_inventory_cycles"] != plan["mixed_cycles"]:
                issues.append({"round": number, "reason": "isolated_scheduled_ticket_accounting",
                               "protocol": protocol})
            if isolated_result["page_plan_sha256"] != mixed_protocol["page_plan_sha256"]:
                issues.append({"round": number, "reason": "native_page_plan_mismatch",
                               "protocol": protocol})
            if (isolated_result["phase_offset_pages"] != mixed_protocol["phase_offset_pages"]
                    or isolated_result["time_phase_fraction"] != mixed_protocol["time_phase_fraction"]):
                issues.append({"round": number, "reason": "canonical_phase_identity_changed",
                               "protocol": protocol})
            if (isolated_result["page_plan_sha256"] != plan["native_page_plan_sha256"][protocol]
                    or isolated_result["native_pages"] != plan["native_page_counts"][protocol]
                    or mixed_protocol["native_pages"] != plan["native_page_counts"][protocol]):
                issues.append({"round": number, "reason": "predeclared_native_page_plan_changed",
                               "protocol": protocol})
            if isolated_result["delivered_fraction"] < .99 or mixed_protocol["delivered_fraction"] < .99:
                issues.append({"round": number, "reason": "offered_rate_missed",
                               "protocol": protocol})
            if (isolated_result["drain_inclusive_rate_rps"]
                    / plan["isolated_rates_rps"][protocol] < .99
                    or mixed_protocol["drain_inclusive_rate_rps"]
                    / plan["mixed_rate_each_rps"] < .99):
                issues.append({"round": number, "reason": "achieved_rate_below_offered_99pct",
                               "protocol": protocol})
            if abs(isolated["server_cpu_utilization"] - mixed["server_cpu_utilization"]) \
                    > plan["max_actual_utilization_gap"]:
                issues.append({"round": number, "reason": "aggregate_utilization_not_matched",
                               "protocol": protocol,
                               "isolated": isolated["server_cpu_utilization"],
                               "mixed": mixed["server_cpu_utilization"]})
            weighted_cost += (mixed_protocol["objects"] / mixed_objects
                              * isolated["server_cpu_ns_per_object"])
            p99_ratios[protocol].append(mixed_protocol["p99_ns"] / isolated_result["p99_ns"])
        if weighted_cost > 0:
            cpu_ratios.append(mixed["server_cpu_ns_per_object"] / weighted_cost)
    metrics = {"weighted_server_cpu_per_object": interval(cpu_ratios, 1.05)}
    metrics.update({protocol + "_p99": interval(p99_ratios[protocol], 1.20)
                    for protocol in PROTOCOLS})
    return {"rounds": 12, "purpose": "fixed_native_mixed_open_loop_gate",
            "issues": issues, "actual_server_utilization": utilization, "metrics": metrics,
            "status": "passed" if not issues and all(x["status"] == "passed"
                                                   for x in metrics.values())
                      else "inconclusive_or_failed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--resource-only", action="store_true",
                        help="one mixed arm with the dedicated bounded-resource observer, no CI verdict")
    options = parser.parse_args()
    plan = json.loads(options.plan.read_text())
    predicted = validate_plan(plan)
    if sha256(__file__) != plan["orchestrator_sha256"] \
            or sha256(run_pair.__file__) != plan["pair_runner_sha256"]:
        parser.error("mixed orchestration source differs from immutable plan")
    resource_proof = plan.get("resource_proof") if options.resource_only else None
    if options.resource_only and not isinstance(resource_proof, dict):
        parser.error("resource-only mode requires a fixed resource_proof plan")
    if options.resource_only:
        cap_fields = {"rss_bytes", "fd_count", "thread_count", "heap_bytes",
                      "direct_bytes", "charged_response_bytes", "active_responses",
                      "cached_rows", "cached_windows", "nmt_committed_bytes"}
        caps = resource_proof.get("caps", {})
        if (set(caps) != cap_fields or any(not isinstance(value, int) or value <= 0
                                           for value in caps.values())
                or "-XX:NativeMemoryTracking=summary" not in
                   resource_proof.get("server_java_opts", "")):
            parser.error("resource proof needs fixed positive caps and explicit NMT summary")
        observer_source = Path(run_pair.__file__).with_name("resource_observer.py")
        if sha256(observer_source) != resource_proof.get("observer_sha256"):
            parser.error("dedicated resource observer source differs from fixed plan")
    root = Path(resource_proof["output"] if options.resource_only else plan["output"])
    if root.exists():
        parser.error("output root already exists; all rounds use a fresh immutable plan")
    candidate = Path(plan["candidate_distribution"]).resolve()
    candidate_jar = Path(plan["candidate_jar_path"]).resolve()
    if candidate_jar.parent != candidate / "lib" \
            or sha256(candidate_jar) != plan["candidate_jar_sha256"]:
        parser.error("frozen candidate JAR differs from plan")
    if sha256(Path(plan["harness_source"]) / "ReplayMixedOpenLoopBench.java") \
            != plan["mixed_driver_sha256"]:
        parser.error("frozen mixed driver differs from plan")
    harness_classes = Path(plan["classpath"].split(":", 1)[0])
    driver_libraries = Path(plan["driver_library_root"]).resolve()
    if plan["classpath"].split(":", 1)[1] != str(driver_libraries) + "/*":
        parser.error("driver dependency classpath differs from the frozen plan")
    if (run_pair.fingerprints(candidate) != plan["candidate_distribution_fingerprints"]
            or run_pair.fingerprints(harness_classes) != plan["driver_classes_fingerprints"]
            or run_pair.fingerprints(driver_libraries) != plan["driver_libraries_fingerprints"]):
        parser.error("candidate, harness classes or dependency closure changed")
    baseline_env, inherited = run_pair.clean_java_env()
    inventory_cmd = [f"{plan['java_home']}/bin/java", "--enable-native-access=ALL-UNNAMED",
                     *plan["driver_java_opts"].split(), "-cp", plan["classpath"],
                     "io.varve.swath.replay.bench.ReplayHttpBench", "--inventory",
                     plan["fixture_glob"], "0"]
    inventory_run = subprocess.run(inventory_cmd, env=baseline_env,
                                   capture_output=True, text=True, check=True)
    inventory = json.loads(inventory_run.stdout.splitlines()[-1])
    fixture_fingerprints = run_pair.fingerprints(plan["fixture"])
    if inventory["fixture_count"] != plan["fixture_count"] \
            or inventory["fixture_digest"] != plan["fixture_digest"] \
            or fixture_fingerprints != plan["fixture_fingerprints"]:
        parser.error("large fixture changed since predeclared plan")
    if inventory["fixture_key_bytes"] <= run_pair.heap_bytes(plan["server_java_opts"]) \
            or inventory["fixture_count"] <= plan["window_rows"] * plan["max_windows"]:
        parser.error("large fixture no longer exceeds heap and cache capacity")
    server_cpus = run_pair.cpus(plan["server_cpus"])
    client_cpus = run_pair.cpus(plan["client_cpus"])
    if not server_cpus or not client_cpus or server_cpus & client_cpus \
            or not server_cpus | client_cpus <= os.sched_getaffinity(0):
        parser.error("reserved CPU sets are invalid on this host")
    for port in (plan["port"], plan["metrics_port"]):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                parser.error(f"mixed benchmark port {port} is already in use")
    git_status = subprocess.run(["git", "status", "--porcelain=v1"], text=True,
                                capture_output=True, check=True).stdout.splitlines()
    git_revision = subprocess.run(["git", "rev-parse", "HEAD"], text=True,
                                  capture_output=True, check=True).stdout.strip()
    java_version = subprocess.run([f"{plan['java_home']}/bin/java", "-version"],
                                  env=baseline_env, text=True, capture_output=True,
                                  check=True).stderr.strip()
    root.mkdir(parents=True)
    provenance = {"plan": plan, "plan_sha256": sha256(options.plan),
                  "predicted_mixed_server_cpu_utilization": predicted,
                  "removed_inherited_java_option_keys": sorted(inherited),
                  "git_revision": git_revision, "git_dirty": bool(git_status),
                  "git_status": git_status, "java_version": java_version,
                  "host": {"kernel": platform.release(), "machine": platform.machine()},
                  "candidate_distribution": run_pair.fingerprints(candidate),
                  "driver_classes": run_pair.fingerprints(harness_classes)}
    (root / "plan.json").write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    if options.resource_only:
        args = resource_arm_args(plan, inventory, fixture_fingerprints)
        receipt = run_pair.run_arm(args, "baseline", 0, root, server_cpus, client_cpus)
        receipt["purpose"] = "mixed_resource_only_no_throughput_ci"
        (root / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": receipt["status"], "purpose": receipt["purpose"],
                          "resource_observation": receipt.get("resource_observation")}, sort_keys=True))
        return 0 if receipt["status"] == "passed" else 1
    receipts = []
    for number in range(1, 13):
        for arm in balanced_order(number):
            rate = (plan["mixed_rate_each_rps"] if arm == "mixed"
                    else plan["isolated_rates_rps"][arm])
            offered_each = plan["native_page_counts"]["s3"] * (
                    plan["mixed_cycles"] if arm == "mixed" else plan["isolated_cycles"][arm])
            duration = offered_each / rate
            protocol = "s3,gcs,azure" if arm == "mixed" else arm
            args = arm_args(plan, inventory, fixture_fingerprints, protocol, rate, offered_each)
            directory = root / f"round-{number:02d}" / arm
            directory.mkdir(parents=True)
            receipt = run_pair.run_arm(args, "baseline", number, directory,
                                       server_cpus, client_cpus)
            receipt["panel_arm"] = arm
            receipt["offered_rate_each_rps"] = rate
            receipt["scheduled_duration_seconds"] = duration
            receipt["purpose"] = "native_mixed_open_loop_fixed_gate"
            (directory / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
            with (root / "rounds.jsonl").open("a") as out:
                out.write(json.dumps(receipt, sort_keys=True) + "\n")
            receipts.append(receipt)
            print(f"round={number} arm={arm} status={receipt['status']}", flush=True)
    summary = summarize_rounds(plan, receipts)
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
