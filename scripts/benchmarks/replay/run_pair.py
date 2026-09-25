#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Run fixed, interleaved replay comparisons with disjoint client/server CPU sets.

The server command is supplied as a full path to a frozen installDist launcher. Receipts
include every attempt, even a failed one; a failure makes the panel fail rather than
silently dropping a pair. No timing assertion is made until all 12 pairs finish.
"""

import argparse
from datetime import datetime
import glob
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import platform
import re
import urllib.error
import urllib.request


def cpus(value):
    result = set()
    for part in value.split(","):
        if "-" in part:
            lo, hi = map(int, part.split("-", 1))
            result.update(range(lo, hi + 1))
        else:
            result.add(int(part))
    return result


def affinity(cpu_set):
    def set_affinity():
        os.sched_setaffinity(0, cpu_set)
    return set_affinity


def clean_java_env():
    env = os.environ.copy()
    inherited = {key: env.pop(key) for key in
                 ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "JAVA_OPTS",
                  "SWATH_REPLAY_OPTS") if key in env}
    return env, inherited


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.load(response)


def is_healthy(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return response.status == 200 and response.read(16).strip() == b"ok"


def wait_for_health(server, url, start_timeout):
    deadline = time.monotonic() + start_timeout
    while True:
        if server.poll() is not None:
            raise RuntimeError(f"server exited with {server.returncode}")
        try:
            if is_healthy(url):
                return
        except (OSError, ValueError, urllib.error.HTTPError):
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("server did not become healthy")
        time.sleep(.2)


def meter(snapshot, name, tags=None):
    tags = tags or {}
    return sum((m.get("count", m.get("value", 0)) or 0) for m in snapshot["meters"]
               if m["name"] == name and all(m.get("tags", {}).get(k) == v for k, v in tags.items()))


def meter_field(snapshot, name, field, tags=None):
    tags = tags or {}
    return sum((m.get(field) or 0) for m in snapshot["meters"]
               if m["name"] == name and all(m.get("tags", {}).get(k) == v for k, v in tags.items()))


def validate_metrics(snapshot):
    if snapshot.get("schema_version") not in (1, 2):
        raise RuntimeError(f"unsupported metrics schema {snapshot.get('schema_version')}")
    if snapshot["schema_version"] == 2:
        serving = snapshot.get("serving")
        required = ("protocols", "fixture_identity", "max_concurrent_requests", "max_responses",
                    "read_permit_limit", "response_buffer_budget", "max_response_bytes")
        if not isinstance(serving, dict) or any(key not in serving for key in required):
            raise RuntimeError("metrics schema 2 omitted serving configuration")


def fingerprints(path):
    path = Path(path)
    if path.is_file():
        files = [path]
        base = path.parent
    else:
        files = sorted(p for p in path.rglob("*") if p.is_file())
        base = path
    result = []
    for file in files:
        digest = hashlib.sha256()
        with file.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        result.append({"path": str(file.relative_to(base)), "size": file.stat().st_size,
                       "sha256": digest.hexdigest()})
    return result


def heap_bytes(java_opts):
    match = re.search(r"(?:^|\s)-Xmx(\d+)([kKmMgG])(?:\s|$)", java_opts)
    if not match:
        raise ValueError("--server-java-opts must declare -Xmx with k/m/g suffix")
    return int(match.group(1)) * {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}[match.group(2).lower()]


def gc_log_summary(init_log, gc_log, start_epoch_ms, end_epoch_ms):
    region = re.search(r"Heap Region Size: (\d+)([KMG])", Path(init_log).read_text())
    if not region:
        raise RuntimeError("GC init log omitted G1 heap region size")
    region_bytes = int(region.group(1)) * {"K": 1024, "M": 1024 ** 2, "G": 1024 ** 3}[region.group(2)]
    counts = {name: 0 for name in ("humongous", "evacuation", "metadata", "other")}
    pause_ms = {name: 0.0 for name in counts}
    for line in Path(gc_log).read_text().splitlines():
        if " Pause " not in line or "GC(" not in line:
            continue
        stamp = re.match(r"\[([^]]+)\]", line)
        duration = re.search(r"(\d+(?:\.\d+)?)ms$", line)
        if not stamp or not duration:
            continue
        # JDK unified logging writes numeric offsets without a colon on some builds;
        # Python before 3.11 requires the ISO 8601 colon form.
        iso_stamp = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", stamp.group(1))
        epoch_ms = datetime.fromisoformat(iso_stamp).timestamp() * 1000
        if not start_epoch_ms <= epoch_ms <= end_epoch_ms:
            continue
        cause = ("humongous" if "G1 Humongous Allocation" in line else
                 "evacuation" if "G1 Evacuation Pause" in line else
                 "metadata" if "Metadata GC Threshold" in line else "other")
        counts[cause] += 1
        pause_ms[cause] += float(duration.group(1))
    return {"g1_region_bytes": region_bytes, "gc_pause_cause_counts": counts,
            "gc_pause_cause_ms": {key: round(value, 3) for key, value in pause_ms.items()}}


def cpu_ticks(pid):
    # comm may contain spaces and parentheses; everything after its final ')' is fixed-position.
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return int(fields[11]) + int(fields[12])


def thread_cpu_ticks(pid):
    """Snapshot JVM OS threads; virtual-thread CPU is charged to carrier threads."""
    result = {}
    for task in Path(f"/proc/{pid}/task").iterdir():
        try:
            fields = (task / "stat").read_text().rsplit(")", 1)[1].split()
            result[int(task.name)] = {
                "name": (task / "comm").read_text().strip(),
                "ticks": int(fields[11]) + int(fields[12])}
        except (FileNotFoundError, ProcessLookupError):
            # A short-lived JVM thread can disappear between task listing and its stat read.
            continue
    return result


def cpu_busy_ticks(cpu_set):
    """Per-CPU non-idle ticks, including kernel/interrupt work, from one /proc/stat sample."""
    selected = {}
    for line in Path("/proc/stat").read_text().splitlines():
        fields = line.split()
        if not fields or not fields[0].startswith("cpu") or not fields[0][3:].isdigit():
            continue
        cpu = int(fields[0][3:])
        if cpu in cpu_set:
            counters = [int(value) for value in fields[1:]]
            selected[cpu] = sum(counters[index] for index in (0, 1, 2, 5, 6, 7))
    if set(selected) != set(cpu_set):
        raise RuntimeError("/proc/stat did not report every reserved CPU")
    return selected


def thread_utilization(before, after, elapsed_seconds):
    hz = os.sysconf("SC_CLK_TCK")
    threads = []
    for tid, later in after.items():
        ticks_before = before.get(tid, {"ticks": 0})["ticks"]
        seconds = max(0, later["ticks"] - ticks_before) / hz
        threads.append({"tid": tid, "name": later["name"], "cpu_seconds": seconds,
                        "utilization": seconds / elapsed_seconds})
    return sorted(threads, key=lambda thread: thread["cpu_seconds"], reverse=True)


def rss_bytes(pid):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return None


def run_arm(args, label, round_number, output_dir, server_cpus, client_cpus):
    server_cmd = [str(args.baseline if label == "baseline" else args.candidate), "serve",
                  "--fixture", args.fixture, "--bucket", args.bucket, "--host", "127.0.0.1",
                  "--port", str(args.port), "--metrics-port", str(args.metrics_port),
                  "--serving-mode", args.mode, "--parquet-connections", str(args.connections),
                  "--max-concurrent-requests", str(args.max_concurrent_requests)]
    server_cmd += args.baseline_option if label == "baseline" else args.candidate_option
    env, _ = clean_java_env()
    env["JAVA_HOME"] = args.java_home
    prefix = f"{round_number:02d}-{label}"
    gc_init_log = output_dir / f"{prefix}-gc-init.log"
    gc_log = output_dir / f"{prefix}-gc.log"
    env["JAVA_OPTS"] = (args.server_java_opts
                        + f" -Xlog:gc+init=info:file={gc_init_log}:time,level,tags"
                        + f" -Xlog:gc=info:file={gc_log}:time,level,tags")
    driver_env = env.copy()
    driver_env.pop("JAVA_OPTS", None)
    log_path = output_dir / f"{round_number:02d}-{label}-server.log"
    receipt = {"round": round_number, "arm": label, "server_command": server_cmd,
               "server_log": str(log_path),
               "server_cpus": sorted(server_cpus), "client_cpus": sorted(client_cpus),
               "fixture": args.fixture, "fixture_glob": args.fixture_glob,
               "page_size": args.page_size, "clients": args.clients, "warmup_walks": args.warmup,
               "partitioned": args.partitioned, "repetitions": args.repetitions,
               "java_home": args.java_home, "server_java_opts": args.server_java_opts,
               "resolved_server_java_opts": env["JAVA_OPTS"],
               "gc_init_log": str(gc_init_log), "gc_log": str(gc_log),
               "fixture_fingerprints": args.fixture_fingerprints, "status": "failed"}
    with log_path.open("wb") as log:
        server = subprocess.Popen(server_cmd, env=env, stdout=log, stderr=subprocess.STDOUT,
                                  preexec_fn=affinity(server_cpus))
        try:
            wait_for_health(server, f"http://127.0.0.1:{args.metrics_port}/healthz", args.start_timeout)
            metrics_url = f"http://127.0.0.1:{args.metrics_port}/metrics"
            receipt["runtime_attestation"] = fetch_json(
                f"http://127.0.0.1:{args.metrics_port}/runtime-attestation")
            protocol = args.baseline_protocol if label == "baseline" else args.candidate_protocol
            driver_class = {
                "flat": "io.varve.swath.replay.bench.ReplayHttpBench",
                "gcs_narrow": "io.varve.swath.replay.bench.ReplayGcsNarrowBench",
                "seek": "io.varve.swath.replay.bench.ReplayShapeBench",
                "delimiter": "io.varve.swath.replay.bench.ReplayShapeBench",
            }[args.workload]
            command = [f"{args.java_home}/bin/java", "--enable-native-access=ALL-UNNAMED",
                       *shlex.split(args.driver_java_opts),
                       "-cp", args.classpath, driver_class,
                       f"http://127.0.0.1:{args.port}", protocol, args.bucket,
                       args.fixture_glob, str(args.clients), str(args.page_size), str(args.warmup)]
            command.append(f"{args.inventory['fixture_count']}:{args.inventory['fixture_digest']}")
            command.append("bracket")
            if args.workload != "flat":
                mode = ("unchanged" if label == "baseline" else "narrowed") \
                        if args.workload == "gcs_narrow" else args.workload
                command += [mode, str(args.repetitions)]
                if args.workload == "delimiter":
                    command.append(args.delimiter_prefix)
            elif args.partitioned:
                command += ["partitioned", str(args.repetitions)]
            if args.end_ack:
                command.append("end_ack")
            with (output_dir / f"{round_number:02d}-{label}-driver.err").open("w") as driver_error:
                driver = subprocess.Popen(command, env=driver_env, stdout=subprocess.PIPE,
                                          stdin=subprocess.PIPE,
                                          stderr=driver_error, text=True, bufsize=1,
                                          preexec_fn=affinity(client_cpus))
                try:
                    lines = queue.Queue()
                    def collect_stdout():
                        try:
                            for output_line in driver.stdout:
                                lines.put(output_line)
                        finally:
                            lines.put(None)
                    reader = threading.Thread(target=collect_stdout, name="benchmark-stdout", daemon=True)
                    reader.start()
                    before = after = None
                    cpu_before = cpu_after = client_cpu_before = client_cpu_after = None
                    client_threads_before = client_threads_after = None
                    host_busy_before = host_busy_after = None
                    rss_before = rss_after = peak_rss = None
                    output_lines = []
                    run_deadline = time.monotonic() + args.run_timeout
                    while True:
                        if time.monotonic() >= run_deadline:
                            raise TimeoutError("driver timed out")
                        if before is not None and after is None:
                            peak_rss = max(peak_rss, rss_bytes(server.pid) or 0)
                        try:
                            line = lines.get(timeout=.2)
                        except queue.Empty:
                            if driver.poll() is not None and not reader.is_alive():
                                break
                            continue
                        if line is None:
                            break
                        else:
                            output_lines.append(line)
                            if not line.lstrip().startswith("{"):
                                continue
                            message = json.loads(line)
                            if message.get("event") == "MEASURE_START":
                                before = fetch_json(metrics_url)
                                validate_metrics(before)
                                cpu_before = cpu_ticks(server.pid)
                                client_cpu_before = cpu_ticks(driver.pid)
                                client_threads_before = thread_cpu_ticks(driver.pid)
                                host_busy_before = cpu_busy_ticks(server_cpus | client_cpus)
                                rss_before = rss_bytes(server.pid)
                                peak_rss = rss_before or 0
                                driver.stdin.write("\n")
                                driver.stdin.flush()
                            elif message.get("event") == "MEASURE_END":
                                cpu_after = cpu_ticks(server.pid)
                                client_cpu_after = cpu_ticks(driver.pid)
                                client_threads_after = thread_cpu_ticks(driver.pid)
                                host_busy_after = cpu_busy_ticks(server_cpus | client_cpus)
                                rss_after = rss_bytes(server.pid)
                                after = fetch_json(metrics_url)
                                validate_metrics(after)
                                if args.end_ack:
                                    driver.stdin.write("\n")
                                    driver.stdin.flush()
                    driver.wait(timeout=10)
                finally:
                    if driver.poll() is None:
                        driver.kill()
                        driver.wait()
            driver_stderr = (output_dir / f"{round_number:02d}-{label}-driver.err").read_text()[-4096:]
            receipt.update({"driver_command": command, "driver_exit": driver.returncode,
                            "driver_stderr": driver_stderr[-4096:], "server_rss_before": rss_before,
                            "server_rss_after": rss_after, "server_cpu_seconds":
                            (cpu_after - cpu_before) / os.sysconf("SC_CLK_TCK") if after else None,
                            "client_cpu_seconds":
                            (client_cpu_after - client_cpu_before) / os.sysconf("SC_CLK_TCK") if after else None,
                            "server_rss_sampled_peak": peak_rss,
                            "metrics_before": before, "metrics_after": after})
            if before is not None:
                receipt["serving_configuration"] = before.get("serving") if before["schema_version"] == 2 else {
                    "source": "unmodified_69cb809_cli_and_source",
                    "protocols": ["s3"], "fixture": args.fixture,
                    "serving_mode": args.mode, "read_permit_limit": args.connections,
                    "max_concurrent_requests": args.max_concurrent_requests,
                    "response_buffer_budget": None, "max_response_bytes": None}
            if driver.returncode != 0:
                try:
                    receipt["failed_driver_result"] = json.loads(output_lines[-1])
                except (ValueError, IndexError):
                    pass
                raise RuntimeError(f"driver failed: {driver_stderr[-1000:]}")
            if before is None or after is None:
                raise RuntimeError("driver omitted a measurement marker")
            result = json.loads(next(line for line in reversed(output_lines) if line.lstrip().startswith("{")))
            if result["attempted_requests"] != result["successful_requests"]:
                raise RuntimeError("unintended refused or failed requests")
            expected_objects = (args.inventory["fixture_count"] *
                                (args.repetitions if args.partitioned else args.clients)
                                if args.workload in ("flat", "gcs_narrow") else
                                args.clients * args.repetitions if args.workload == "seek" else None)
            if (expected_objects is not None and result["objects"] != expected_objects
                    or result["emitted_objects"] < result["objects"]
                    or args.workload == "delimiter" and result.get("common_prefixes", 0) < 1):
                raise RuntimeError("completed inventory or emitted-row accounting mismatch")
            if args.workload in ("flat", "gcs_narrow"):
                expected_metadata = args.inventory["fixture_count"] * args.repetitions * args.warmup \
                        * (1 if args.partitioned else args.clients)
                if (result.get("metadata_profile") != "fixture_name_size_time"
                        or result.get("warmup_metadata_verified_objects") != expected_metadata):
                    raise RuntimeError("fixture-backed warmup metadata was not fully verified")
                if args.workload == "gcs_narrow" and (
                        result.get("mode") != ("unchanged" if label == "baseline" else "narrowed")
                        or result.get("overshoot_objects") != result["emitted_objects"] - result["objects"]
                        or label == "candidate" and result["overshoot_objects"] != 0):
                    raise RuntimeError("GCS narrowed mode or overshoot accounting mismatch")
            elif args.workload == "seek":
                expected_metadata = args.clients * args.repetitions * args.warmup
                if (result.get("metadata_profile") != "fixture_seek_name_size_time"
                        or result.get("warmup_metadata_verified_objects") != expected_metadata):
                    raise RuntimeError("seek warmup metadata was not fully verified")
            elif args.workload == "delimiter":
                if (result.get("metadata_profile") != "fixture_delimiter_direct_name_size_time"
                        or not isinstance(result.get("warmup_metadata_verified_objects"), int)):
                    raise RuntimeError("delimiter warmup metadata was not fully verified")
            receipt["result"] = result
            receipt["server_cpu_ns_per_object"] = receipt["server_cpu_seconds"] * 1e9 / result["objects"]
            receipt["server_cpu_ns_per_request"] = receipt["server_cpu_seconds"] * 1e9 / result["requests"]
            elapsed_seconds = result["elapsed_ns"] / 1e9
            receipt["client_cpu_utilization"] = receipt["client_cpu_seconds"] / (elapsed_seconds * len(client_cpus))
            receipt["server_cpu_utilization"] = receipt["server_cpu_seconds"] / (elapsed_seconds * len(server_cpus))
            hz = os.sysconf("SC_CLK_TCK")
            server_busy = sum(host_busy_after[cpu] - host_busy_before[cpu] for cpu in server_cpus) / hz
            client_busy = sum(host_busy_after[cpu] - host_busy_before[cpu] for cpu in client_cpus) / hz
            receipt["foreign_server_cpu_seconds_upper_bound"] = max(
                0.0, server_busy - receipt["server_cpu_seconds"])
            receipt["foreign_client_cpu_seconds_upper_bound"] = max(
                0.0, client_busy - receipt["client_cpu_seconds"])
            receipt["foreign_cpu_utilization_upper_bound"] = (
                receipt["foreign_server_cpu_seconds_upper_bound"]
                + receipt["foreign_client_cpu_seconds_upper_bound"]) / (
                    elapsed_seconds * (len(server_cpus) + len(client_cpus)))
            quality_failure = None
            if receipt["foreign_cpu_utilization_upper_bound"] > args.max_foreign_cpu_utilization:
                quality_failure = "host_noisy"
            driver_threads = thread_utilization(client_threads_before, client_threads_after, elapsed_seconds)
            receipt["client_max_thread_utilization"] = max(
                (thread["utilization"] for thread in driver_threads), default=0)
            receipt["client_thread_cpu_top"] = driver_threads[:16]
            receipt["client_os_threads_before"] = len(client_threads_before)
            receipt["client_os_threads_after"] = len(client_threads_after)
            if receipt["client_cpu_utilization"] > args.max_client_utilization:
                quality_failure = quality_failure or "client_limited"
            if receipt["client_max_thread_utilization"] > args.max_client_utilization:
                quality_failure = quality_failure or "client_hot_thread_limited"
            if elapsed_seconds < args.min_duration:
                quality_failure = quality_failure or "under_duration"
            server_requests = meter(after, "swath.replay.http.requests") - meter(before, "swath.replay.http.requests")
            server_errors = meter(after, "swath.replay.http.errors") - meter(before, "swath.replay.http.errors")
            server_refusals = meter(after, "swath.replay.serving.refused") - meter(before, "swath.replay.serving.refused")
            admission_refusals = (meter(after, "swath.replay.response.admission.refused")
                                  - meter(before, "swath.replay.response.admission.refused"))
            receipt["server_requests"] = server_requests
            receipt["server_errors"] = server_errors
            receipt["server_refusals"] = server_refusals
            receipt["admission_refusals"] = admission_refusals
            if server_requests != result["attempted_requests"] or server_errors or server_refusals \
                    or admission_refusals:
                raise RuntimeError("server/client request counts or outcomes disagree")
            backing_reads = (meter(after, "swath.replay.parquet.query.latency")
                             - meter(before, "swath.replay.parquet.query.latency"))
            backing_rows = (meter_field(after, "swath.replay.parquet.query.rows", "total")
                            - meter_field(before, "swath.replay.parquet.query.rows", "total"))
            receipt["backing_reads"] = backing_reads
            receipt["backing_reads_per_object"] = backing_reads / result["objects"]
            receipt["backing_rows"] = backing_rows
            receipt["backing_rows_per_object"] = backing_rows / result["objects"]
            if args.workload == "delimiter":
                decoded = (meter_field(after, "swath.replay.delimiter.skipscan.decoded_key_rows", "total")
                           - meter_field(before, "swath.replay.delimiter.skipscan.decoded_key_rows", "total"))
                reseeks = (meter_field(after, "swath.replay.delimiter.skipscan.page_reseeks", "total")
                           - meter_field(before, "swath.replay.delimiter.skipscan.page_reseeks", "total"))
                prefixes = result["common_prefixes"]
                receipt["decoded_key_rows_per_prefix"] = decoded / prefixes
                receipt["page_reseeks_per_prefix"] = reseeks / prefixes
            receipt["server_cpu_ns_per_encoded_byte"] = receipt["server_cpu_seconds"] * 1e9 / result["bytes"]
            receipt["gc_pause_ms"] = (meter_field(after, "jvm.gc.pause", "sum_ms")
                                      - meter_field(before, "jvm.gc.pause", "sum_ms"))
            promoted = (meter(after, "jvm.gc.memory.promoted")
                        - meter(before, "jvm.gc.memory.promoted"))
            receipt["gc_promoted_bytes_estimate"] = promoted
            receipt["gc_promoted_bytes_per_object_estimate"] = promoted / result["objects"]
            receipt.update(gc_log_summary(gc_init_log, gc_log,
                                          before["sampled_at_epoch_ms"], after["sampled_at_epoch_ms"]))
            receipt["heap_used_bytes_after"] = meter(after, "jvm.memory.used", {"area": "heap"})
            if after["schema_version"] == 2:
                receipt["serving_configuration_after"] = after["serving"]
            allocated = meter(after, "jvm.gc.memory.allocated") - meter(before, "jvm.gc.memory.allocated")
            receipt["server_gc_allocation_estimate_bytes_per_object"] = (
                allocated / result["objects"] if allocated > 0 else None)
            receipt["server_direct_bytes_before"] = meter(before, "jvm.buffer.memory.used", {"id": "direct"})
            receipt["server_direct_bytes_after"] = meter(after, "jvm.buffer.memory.used", {"id": "direct"})
            if quality_failure is not None:
                raise RuntimeError(quality_failure)
            receipt["status"] = "passed"
        except Exception as exc:
            receipt["error"] = str(exc)
            if "output_lines" in locals():
                receipt["driver_stdout_lines"] = output_lines[-8:]
        finally:
            if server.poll() is None:
                server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=12)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    return receipt


def summarize(receipts, metric, threshold, direction, rounds=12):
    pairs = []
    if rounds not in (6, 12):
        raise ValueError("only predeclared six-pair A/A and twelve-pair gates are supported")
    for index in range(1, rounds + 1):
        round_receipts = [r for r in receipts if r["round"] == index]
        if len(round_receipts) != 2:
            return {"metric": metric, "status": "incomplete", "round": index}
        a, b = round_receipts
        if a["arm"] == "candidate":
            a, b = b, a
        if a["status"] != "passed" or b["status"] != "passed":
            return {"metric": metric, "status": "failed_pair", "round": index}
        base = a["result"][metric] if metric in a["result"] else a[metric]
        candidate = b["result"][metric] if metric in b["result"] else b[metric]
        if base == 0:
            if candidate != 0:
                return {"metric": metric, "status": "failed_zero_baseline", "round": index,
                        "candidate": candidate}
            pairs.append(1.0)
        else:
            pairs.append(candidate / base)
    logs = [math.log(ratio) for ratio in pairs]
    mean = statistics.mean(logs)
    half = (2.571 if rounds == 6 else 2.201) * statistics.stdev(logs) / math.sqrt(rounds)
    interval = [math.exp(mean - half), math.exp(mean + half)]
    passed = interval[0] >= threshold if direction == "min" else interval[1] <= threshold
    return {"metric": metric, "pairs": pairs, "geometric_mean": math.exp(mean),
            "ci95": interval, "ci95_half_width_log": half,
            "sample_log_ratio_sd": statistics.stdev(logs),
            "projected_12_pair_ci_half_width_log": 2.201 * statistics.stdev(logs) / math.sqrt(12),
            "threshold": threshold, "direction": direction,
            "status": "passed" if passed else "inconclusive_or_failed"}


def suggest_repetitions(existing, target_seconds, observed_seconds):
    if existing < 1 or target_seconds <= 0 or observed_seconds <= 0:
        raise ValueError("pilot repetition and duration inputs must be positive")
    return existing * math.ceil(target_seconds / observed_seconds)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--fixture-glob", required=True)
    parser.add_argument("--bucket", default="bench")
    parser.add_argument("--baseline-protocol", choices=["s3", "gcs", "azure"], default="s3")
    parser.add_argument("--candidate-protocol", choices=["s3", "gcs", "azure"], default="s3")
    parser.add_argument("--workload", choices=["flat", "seek", "delimiter", "gcs_narrow"], default="flat")
    parser.add_argument("--delimiter-prefix")
    parser.add_argument("--mode", default="sorted")
    parser.add_argument("--clients", type=int, required=True)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--partitioned", action="store_true",
                        help="all clients cover disjoint contiguous portions of one inventory")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--pilot", action="store_true",
                        help="one baseline characterization arm to freeze repetitions before a 12-pair gate")
    parser.add_argument("--aa-control", action="store_true",
                        help="predeclared six-pair identical-binary noise control, not a release gate")
    parser.add_argument("--pilot-target-duration", type=float, default=50,
                        help="conservative measured seconds for the fixed repetitions suggested by a pilot")
    parser.add_argument("--connections", type=int, default=16)
    parser.add_argument("--max-concurrent-requests", type=int, default=512)
    parser.add_argument("--server-cpus", required=True)
    parser.add_argument("--client-cpus", required=True)
    parser.add_argument("--port", type=int, default=19091)
    parser.add_argument("--metrics-port", type=int, default=19092)
    parser.add_argument("--java-home", default=os.environ.get("JAVA_HOME"), required=not os.environ.get("JAVA_HOME"))
    parser.add_argument("--server-java-opts", required=True,
                        help="identical explicit heap/direct/JDK options for both arms")
    parser.add_argument("--driver-java-opts", default="-Xms512m -Xmx2g -Djdk.nio.maxCachedBufferSize=262144",
                        help="explicit JVM options for the isolated Java HTTP driver")
    parser.add_argument("--end-ack", action="store_true",
                        help="hold Java client threads after MEASURE_END until per-thread CPU snapshot completes")
    parser.add_argument("--window-rows", type=int, default=12500)
    parser.add_argument("--max-windows", type=int, default=96)
    parser.add_argument("--require-oversized-fixture", action="store_true")
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline-option", action="append", default=[])
    parser.add_argument("--candidate-option", action="append", default=[])
    parser.add_argument("--start-timeout", type=float, default=60)
    parser.add_argument("--run-timeout", type=float, default=3600)
    parser.add_argument("--throughput-floor", type=float)
    parser.add_argument("--cpu-ceiling", type=float)
    parser.add_argument("--p99-ceiling", type=float)
    parser.add_argument("--decoded-work-ceiling", type=float)
    parser.add_argument("--backing-work-ceiling", type=float)
    parser.add_argument("--max-client-utilization", type=float, default=.70)
    parser.add_argument("--max-foreign-cpu-utilization", type=float, default=.02,
                        help="conservative upper bound for other processes/kernel work on reserved CPUs")
    parser.add_argument("--min-duration", type=float, default=30)
    args = parser.parse_args()
    inventory_env, inherited_java_options = clean_java_env()
    args.removed_inherited_java_option_keys = sorted(inherited_java_options)
    if args.warmup < 1:
        parser.error("at least one full warmup walk is required to compute the inventory oracle")
    if args.repetitions < 1:
        parser.error("repetitions must be positive and fixed before round 1")
    if args.workload == "flat" and not args.partitioned and args.repetitions != 1:
        parser.error("flat repetitions require --partitioned")
    if args.workload == "seek" and (args.partitioned or args.page_size != 1 or args.delimiter_prefix):
        parser.error("seek workload requires page size 1 and no partition/delimiter options")
    if args.workload == "delimiter" and (args.partitioned or not args.delimiter_prefix):
        parser.error("delimiter workload requires --delimiter-prefix and no partitioning")
    if args.workload == "flat" and args.delimiter_prefix:
        parser.error("flat workload does not use --delimiter-prefix")
    if args.workload == "gcs_narrow" and (not args.partitioned or args.delimiter_prefix
            or args.baseline_protocol != "gcs" or args.candidate_protocol != "gcs"
            or args.page_size > 1000):
        parser.error("GCS narrowed comparison requires partitioning, GCS on both arms and <=1k pages")
    if args.pilot:
        args.min_duration = 0
    if args.pilot and args.aa_control:
        parser.error("pilot and A/A control are separate modes")
    server_cpus, client_cpus = cpus(args.server_cpus), cpus(args.client_cpus)
    available = os.sched_getaffinity(0)
    if not server_cpus or not client_cpus or server_cpus & client_cpus:
        parser.error("server and client CPU sets must be nonempty and disjoint")
    if not server_cpus | client_cpus <= available:
        parser.error(f"CPU sets must be within available set {sorted(available)}")
    same_protocol = args.baseline_protocol == args.candidate_protocol
    if args.throughput_floor is None:
        args.throughput_floor = .95 if args.workload == "flat" and same_protocol else .90
    if args.cpu_ceiling is None:
        args.cpu_ceiling = (1.05 if same_protocol else 1.20) if args.workload == "flat" \
                else 1.20 if args.workload == "seek" else None
    if args.p99_ceiling is None and same_protocol and args.workload == "flat":
        args.p99_ceiling = 1.10
    if args.decoded_work_ceiling is None and args.workload == "delimiter":
        args.decoded_work_ceiling = 1.10
    if args.backing_work_ceiling is None and args.workload == "gcs_narrow":
        args.backing_work_ceiling = 1.10
    fixture_files = sorted(glob.glob(args.fixture_glob))
    if not fixture_files:
        parser.error("fixture glob matched no files")
    args.fixture_fingerprints = fingerprints(args.fixture)
    inventory_cmd = [f"{args.java_home}/bin/java", "--enable-native-access=ALL-UNNAMED",
                     *shlex.split(args.driver_java_opts), "-cp",
                     args.classpath, "io.varve.swath.replay.bench.ReplayHttpBench", "--inventory",
                     args.fixture_glob, str(args.clients if args.partitioned else 0)]
    inventory_run = subprocess.run(inventory_cmd, env=inventory_env, capture_output=True, text=True, check=True)
    args.inventory = json.loads(inventory_run.stdout.splitlines()[-1])
    args.heap_bytes = heap_bytes(args.server_java_opts)
    args.cache_row_budget = args.window_rows * args.max_windows
    args.oversized_fixture = {
        "fixture_keys": args.inventory["fixture_count"],
        "fixture_key_bytes": args.inventory["fixture_key_bytes"],
        "heap_bytes": args.heap_bytes,
        "cache_row_budget": args.cache_row_budget,
        "key_bytes_exceed_heap": args.inventory["fixture_key_bytes"] > args.heap_bytes,
        "key_count_exceeds_cache": args.inventory["fixture_count"] > args.cache_row_budget}
    if args.require_oversized_fixture and not (
            args.oversized_fixture["key_bytes_exceed_heap"]
            and args.oversized_fixture["key_count_exceeds_cache"]):
        parser.error("fixture key bytes must exceed heap and key count must exceed cache-row budget")
    for port in (args.port, args.metrics_port):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                parser.error(f"port {port} is already in use")
    args.output.mkdir(parents=True, exist_ok=False)
    baseline_fingerprints = fingerprints(args.baseline.parent.parent)
    candidate_fingerprints = fingerprints(args.candidate.parent.parent)
    driver_fingerprints = fingerprints(args.classpath.split(":", 1)[0])
    if not driver_fingerprints or not any(item["path"].endswith(".jar") for item in baseline_fingerprints) \
            or not any(item["path"].endswith(".jar") for item in candidate_fingerprints):
        parser.error("benchmark classes and both runnable distributions must be nonempty")
    if args.aa_control and (baseline_fingerprints != candidate_fingerprints
                            or args.baseline_option != args.candidate_option
                            or args.baseline_protocol != args.candidate_protocol):
        parser.error("A/A control requires identical binary fingerprints, server options and protocol")
    git_status = subprocess.run(["git", "status", "--porcelain=v1"], text=True,
                                capture_output=True, check=True).stdout.splitlines()
    rounds = 6 if args.aa_control else 12
    plan = {"rounds": 0 if args.pilot else rounds,
            "purpose": "pilot_characterization" if args.pilot else
                    "six_pair_aa_noise_control" if args.aa_control else "fixed_paired_gate",
            "args": {key: str(value) if isinstance(value, Path) else value
                                  for key, value in vars(args).items() if key not in ("fixture_fingerprints", "inventory")},
            "fixture_fingerprints": args.fixture_fingerprints,
            "inventory": args.inventory,
            "oversized_fixture": args.oversized_fixture,
            "baseline_distribution": baseline_fingerprints,
            "candidate_distribution": candidate_fingerprints,
            "driver_source": fingerprints(Path(__file__).parent),
            "driver_classes": driver_fingerprints,
            "git_revision": subprocess.run(["git", "rev-parse", "HEAD"], text=True,
                                           capture_output=True, check=True).stdout.strip(),
            "git_dirty": bool(git_status), "git_status": git_status,
            "java_version": subprocess.run([f"{args.java_home}/bin/java", "-version"], text=True,
                                          env=inventory_env, capture_output=True, check=True).stderr.strip(),
            "host": {"kernel": platform.release(), "machine": platform.machine(),
                     "processor": platform.processor()},
            "created_epoch_ms": int(time.time() * 1000)}
    (args.output / "plan.json").write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    if args.pilot:
        pilot = run_arm(args, "baseline", 0, args.output, server_cpus, client_cpus)
        pilot["purpose"] = "pilot_characterization_not_gate"
        if pilot["status"] == "passed" and (args.partitioned or args.workload != "flat"):
            seconds = pilot["result"]["elapsed_ns"] / 1e9
            pilot["suggested_fixed_repetitions"] = suggest_repetitions(
                args.repetitions, args.pilot_target_duration, seconds)
            pilot["target_duration_seconds"] = args.pilot_target_duration
        (args.output / "pilot.json").write_text(json.dumps(pilot, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"status": pilot["status"], "elapsed_ns": pilot.get("result", {}).get("elapsed_ns"),
                          "suggested_fixed_repetitions":
                          pilot.get("suggested_fixed_repetitions")}, sort_keys=True))
        return 0 if pilot["status"] == "passed" else 1
    receipts = []
    receipt_file = args.output / "rounds.jsonl"
    aborted = False
    with receipt_file.open("x") as out:
        for round_number in range(1, rounds + 1):
            order = ("baseline", "candidate") if round_number % 2 else ("candidate", "baseline")
            for arm in order:
                receipt = run_arm(args, arm, round_number, args.output, server_cpus, client_cpus)
                receipts.append(receipt)
                out.write(json.dumps(receipt, sort_keys=True) + "\n")
                out.flush()
                print(f"round={round_number} arm={arm} status={receipt['status']}", file=sys.stderr)
                if receipt["status"] != "passed":
                    print(f"failure receipt: {receipt_file}", file=sys.stderr)
                    aborted = True
                    if not args.aa_control:
                        break
            if aborted and not args.aa_control:
                break
    throughput_metric = "objects_per_s" if args.workload in ("flat", "gcs_narrow") else "requests_per_s"
    summary = {"rounds": rounds, "order": "alternating baseline/candidate", "workload": args.workload,
               "purpose": "aa_control" if args.aa_control else "fixed_paired_gate",
               throughput_metric: summarize(receipts, throughput_metric, args.throughput_floor, "min", rounds)}
    gated_metrics = [throughput_metric]
    if args.cpu_ceiling is not None:
        cpu_metric = "server_cpu_ns_per_request" if args.workload == "seek" else "server_cpu_ns_per_object"
        summary[cpu_metric] = summarize(receipts, cpu_metric, args.cpu_ceiling, "max", rounds)
        gated_metrics.append(cpu_metric)
    if args.workload == "delimiter":
        for metric in ("decoded_key_rows_per_prefix", "page_reseeks_per_prefix"):
            summary[metric] = summarize(receipts, metric, args.decoded_work_ceiling, "max", rounds)
            gated_metrics.append(metric)
    if args.workload == "gcs_narrow":
        metric = "backing_rows_per_object"
        summary[metric] = summarize(receipts, metric, args.backing_work_ceiling, "max", rounds)
        gated_metrics.append(metric)
    if args.p99_ceiling is not None:
        summary["p99_ns"] = summarize(receipts, "p99_ns", args.p99_ceiling, "max", rounds)
    else:
        summary["p99_ns"] = {"status": "characterization", "reason": "no cross-protocol p99 budget"}
    if args.aa_control:
        control_metrics = gated_metrics + (["p99_ns"] if summary["p99_ns"]["status"] != "characterization" else [])
        if all("ci95" in summary[key] for key in control_metrics):
            summary["aa_ci_contains_one"] = {key: summary[key]["ci95"][0] <= 1 <= summary[key]["ci95"][1]
                                              for key in control_metrics}
            summary["projected_12_pair_ci_resolution"] = {
                key: [math.exp(-summary[key]["projected_12_pair_ci_half_width_log"]),
                      math.exp(summary[key]["projected_12_pair_ci_half_width_log"])]
                for key in control_metrics}
            summary["status"] = ("passed" if all(summary["aa_ci_contains_one"].values())
                                 else "biased_or_noisy")
        else:
            summary["status"] = "incomplete"
    else:
        summary["status"] = ("passed" if all(summary[key]["status"] == "passed" for key in gated_metrics)
                             and summary["p99_ns"]["status"] in ("passed", "characterization")
                             else "inconclusive_or_failed")
    summary["fixture_unchanged"] = fingerprints(args.fixture) == args.fixture_fingerprints
    if not summary["fixture_unchanged"]:
        summary["status"] = "fixture_changed"
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
