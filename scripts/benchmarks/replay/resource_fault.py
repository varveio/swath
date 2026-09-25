#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Opt-in response-budget and slow-client replay characterization.

The run is a resource/fault receipt, never a throughput comparison. The caller supplies a
fixture with long keys and an explicit body upper bound. Every result, including failure,
is written outside the source tree to --output before an exception is re-raised.
"""

import argparse
import glob
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import shlex
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import zipfile

from run_pair import clean_java_env


def peak_capacity_bound(body_upper, preflight_peak, chunk_bytes=None):
    """Conservative encoded-budget bound for the selected response allocator."""
    if body_upper <= 0 or preflight_peak <= 0:
        raise ValueError("body upper bound and measured preflight peak must be positive")
    if chunk_bytes is not None:
        if chunk_bytes <= 0:
            raise ValueError("reported output chunk size must be positive")
        return max(preflight_peak, body_upper + chunk_bytes)
    return max(preflight_peak, math.ceil(2.5 * body_upper))


def adequate_budget(clients, body_upper, preflight_peak, chunk_bytes=None):
    return clients * peak_capacity_bound(body_upper, preflight_peak, chunk_bytes)


def require_declared_chunk_size(serving, declared):
    reported = serving.get("output_chunk_bytes")
    if reported is not None and (declared is None or reported != declared):
        raise RuntimeError("server output chunk size differs from declared benchmark arm")
    if reported is None and declared is not None:
        raise RuntimeError("declared chunk size but server reports growable-array allocator")


def http_json(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def metric(snapshot, name, tags=None):
    tags = tags or {}
    return sum((item.get("count", item.get("value", 0)) or 0)
               for item in snapshot.get("meters", [])
               if item.get("name") == name
               and all(item.get("tags", {}).get(key) == value for key, value in tags.items()))


def refusal_reasons(before, after):
    def counts(snapshot):
        return {(item.get("tags", {}).get("protocol"), item.get("tags", {}).get("reason")):
                item.get("count", 0) for item in snapshot.get("meters", [])
                if item.get("name") == "swath.replay.response.admission.refused"}
    left, right = counts(before), counts(after)
    return {f"{protocol}:{reason}": right.get((protocol, reason), 0) - left.get((protocol, reason), 0)
            for protocol, reason in left.keys() | right.keys()}


def memory_bytes(value):
    match = re.fullmatch(r"(\d+)([kKmMgG])", value)
    if not match:
        raise ValueError("memory bound must be an integer with k/m/g suffix")
    return int(match.group(1)) * {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}[
        match.group(2).lower()]


def direct_meters(snapshot):
    return [item for item in snapshot.get("meters", [])
            if item.get("name", "").startswith("jvm.buffer")
            and item.get("tags", {}).get("id") == "direct"]


def diagnostic_connections(snapshot):
    prefix = "swath.replay.diagnostic."
    result = {name: metric(snapshot, prefix + meter_name) for name, meter_name in {
        "accepted": "connections.accepted",
        "active": "connections.active",
        "peak": "connections.peak",
        "sndbuf_min_bytes": "sndbuf.min.bytes",
        "sndbuf_max_bytes": "sndbuf.max.bytes",
        "sndbuf_samples": "sndbuf.samples",
        "sndbuf_errors": "sndbuf.errors"}.items()}
    if (result["accepted"] < 1 or result["sndbuf_samples"] != result["accepted"]
            or result["sndbuf_errors"] != 0
            or result["sndbuf_min_bytes"] <= 0):
        raise RuntimeError("diagnostic connector did not read effective accepted SO_SNDBUF")
    return result


def rss(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def jcmd(java_home, pid, *args, environment=None):
    try:
        command = [str(Path(java_home) / "bin" / "jcmd"), str(pid), *args]
        result = subprocess.run(command, text=True, capture_output=True, timeout=10,
                                env=environment,
                                check=False)
        return {"command": command, "exit": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"error": str(error)}


def full_get(url, headers=None):
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
            return {"status": response.status, "length": len(body),
                    "content_length": int(response.headers.get("Content-Length", "-1")),
                    "reason": response.headers.get("x-swath-replay-error"),
                    "sha256": hashlib.sha256(body).hexdigest()}
    except urllib.error.HTTPError as error:
        body = error.read()
        return {"status": error.code, "length": len(body),
                "content_length": int(error.headers.get("Content-Length", "-1")),
                "reason": error.headers.get("x-swath-replay-error"),
                "sha256": hashlib.sha256(body).hexdigest()}


def oracle_inventory(args, clients, environment):
    command = [str(Path(args.java_home) / "bin" / "java"), "--enable-native-access=ALL-UNNAMED",
               "-cp", args.driver_classpath, "io.varve.swath.replay.bench.ReplayHttpBench",
               "--inventory", args.fixture_glob, str(clients)]
    result = subprocess.run(command, text=True, capture_output=True, env=environment,
                            timeout=args.driver_timeout, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"oracle inventory failed: {result.stderr[-1000:]}")
    return json.loads(result.stdout.strip().splitlines()[-1])


def oracle_walk(args, clients, output, inventory, environment):
    command = [str(Path(args.java_home) / "bin" / "java"), "--enable-native-access=ALL-UNNAMED",
               "-cp", args.driver_classpath, "io.varve.swath.replay.bench.ReplayHttpBench",
               f"http://127.0.0.1:{args.port}", args.protocol, args.bucket, args.fixture_glob,
               str(clients), str(args.page_size), str(args.driver_warmup),
               f"{inventory['fixture_count']}:{inventory['fixture_digest']}", "bracket",
               "partitioned", "1"]
    stdout_lines = []
    marker_before = marker_after = False
    with (output / f"oracle-c{clients}.err").open("w") as errors:
        driver = subprocess.Popen(command, text=True, bufsize=1, stdout=subprocess.PIPE,
                                  stdin=subprocess.PIPE, stderr=errors, env=environment)
        try:
            deadline = time.monotonic() + args.driver_timeout
            while time.monotonic() < deadline:
                ready, _, _ = select.select([driver.stdout], [], [], .2)
                if ready:
                    line = driver.stdout.readline()
                    if not line:
                        break
                    stdout_lines.append(line)
                    if line.lstrip().startswith("{"):
                        event = json.loads(line)
                        if event.get("event") == "MEASURE_START":
                            marker_before = True
                            driver.stdin.write("\n")
                            driver.stdin.flush()
                        elif event.get("event") == "MEASURE_END":
                            marker_after = True
                elif driver.poll() is not None:
                    break
            else:
                raise TimeoutError("oracle driver exceeded declared timeout")
            driver.wait(timeout=10)
        finally:
            if driver.poll() is None:
                driver.kill()
                driver.wait()
    (output / f"oracle-c{clients}.log").write_text("".join(stdout_lines))
    if driver.returncode != 0 or not marker_before or not marker_after:
        raise RuntimeError(f"oracle driver failed or omitted markers; see oracle-c{clients}.err/log")
    result = json.loads(next(line for line in reversed(stdout_lines)
                             if line.lstrip().startswith("{") and '"status"' not in line))
    if result["attempted_requests"] != result["successful_requests"]:
        raise RuntimeError("oracle walk had failed/refused HTTP requests")
    if result["objects"] != inventory["fixture_count"]:
        raise RuntimeError("oracle walk inventory count mismatch")
    return {"command": command, "inventory": inventory, "result": result}


def require_oracle_request_accounting(driver, server_requests):
    measured = driver.get("attempted_requests")
    warmup = driver.get("warmup_attempted_requests")
    warmup_successful = driver.get("warmup_successful_requests")
    if (measured is None or warmup is None or warmup_successful is None
            or warmup != warmup_successful or server_requests != measured + warmup):
        raise RuntimeError("server requests differ from exact measured plus warmup driver attempts")
    return warmup


def open_slow(host, port, path, clients, headers=None):
    sockets = []
    try:
        for _ in range(clients):
            connection = socket.socket()
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            connection.settimeout(10)
            connection.connect((host, port))
            extra = "".join(f"{name}: {value}\r\n" for name, value in (headers or {}).items())
            connection.sendall((f"GET {path} HTTP/1.1\r\nHost: {host}\r\n"
                                f"Connection: keep-alive\r\n{extra}\r\n").encode("ascii"))
            sockets.append(connection)
        return sockets
    except Exception:
        for connection in sockets:
            connection.close()
        raise


def read_response_head(connection):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = connection.recv(4096)
        if not chunk:
            raise RuntimeError("connection closed before response headers/body")
        data.extend(chunk)
        if len(data) > 65536:
            raise RuntimeError("response header exceeded 64 KiB")
    head, body = bytes(data).split(b"\r\n\r\n", 1)
    lines = head.decode("iso-8859-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        name, value = line.split(":", 1)
        headers[name.lower()] = value.strip()
    return status, headers, body


def receive_trickle_chunk(connection, limit):
    try:
        chunk = connection.recv(limit)
    except socket.timeout:
        return None, False
    except ConnectionResetError:
        return b"", True
    return chunk, chunk == b""


def await_condition(predicate, seconds, interval=.05):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def sample_resources(pid, metrics_url, stop, samples, errors):
    try:
        while not stop.wait(.1):
            value = rss(pid)
            sample = {"rss": value, "at_ns": time.monotonic_ns()}
            try:
                snapshot = http_json(metrics_url)
                sample.update({"direct": metric(snapshot, "jvm.buffer.memory.used", {"id": "direct"}),
                               "heap": metric(snapshot, "jvm.memory.used", {"area": "heap"}),
                               "active_responses": snapshot.get("serving", {}).get("active_responses", 0),
                               "charged_bytes": snapshot.get("serving", {}).get("charged_response_bytes", 0)})
            except (OSError, KeyError, ValueError, urllib.error.HTTPError) as error:
                errors.append(repr(error))
            samples.append(sample)
    except Exception as error:
        errors.append("sampler stopped: " + repr(error))


def parse_server_opts(opts):
    flags = {}
    for key, pattern in {
        "heap": r"(?<!\S)-Xmx(\d+[kKmMgG])(?=\s|$)",
        "max_direct_memory": r"(?<!\S)-XX:MaxDirectMemorySize=(\d+[kKmMgG])(?=\s|$)",
        "nio_cached_buffer": r"(?<!\S)-Djdk\.nio\.maxCachedBufferSize=(\d+)(?=\s|$)",
        "native_memory_tracking": r"(?<!\S)-XX:NativeMemoryTracking=(summary|detail)(?=\s|$)",
    }.items():
        matches = re.findall(pattern, opts)
        if len(matches) != 1:
            raise ValueError(f"--server-java-opts must declare {key} exactly once")
        flags[key] = matches[0]
    if flags["nio_cached_buffer"] != "262144":
        raise ValueError("jdk.nio.maxCachedBufferSize must be 262144 for this panel")
    return flags


def require_effective_vm_flags(vm_flags, system_properties, declared):
    if vm_flags.get("exit") != 0 or system_properties.get("exit") != 0:
        raise RuntimeError("effective JVM flags/properties could not be read")
    for flag, value in (("MaxHeapSize", memory_bytes(declared["heap"])),
                        ("MaxDirectMemorySize", memory_bytes(declared["max_direct_memory"]))):
        if not re.search(rf"(?:^|\s)-XX:{flag}={value}(?:\s|$)", vm_flags["stdout"]):
            raise RuntimeError(f"server effective {flag} differs from declared JVM bound")
    property_line = "jdk.nio.maxCachedBufferSize=" + declared["nio_cached_buffer"]
    if property_line not in system_properties["stdout"].splitlines():
        raise RuntimeError("server effective NIO cached-buffer bound differs from declaration")


def provenance(args, environment):
    launcher = Path(args.server).resolve()
    java = Path(args.java_home) / "bin" / "java"
    version = subprocess.run([str(java), "-version"], text=True, capture_output=True,
                             env=environment,
                             timeout=10, check=False)
    libs = sorted(path for path in (launcher.parent.parent / "lib").glob("swath-replay-*.jar")
                  if not path.name.endswith("-conformance.jar"))
    if len(libs) != 1:
        raise ValueError("cannot identify exactly one swath-replay main jar")
    def read(path):
        try:
            return Path(path).read_text().strip()
        except OSError as error:
            return {"error": str(error)}
    host = os.uname()
    return {"launcher_sha256": sha256(launcher), "main_jar_sha256": sha256(libs[0]),
            "java_version": (version.stdout + version.stderr).strip(),
            "java_exit": version.returncode,
            "host": dict(zip(("sysname", "nodename", "release", "version", "machine"), host)),
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "meminfo": read("/proc/meminfo"),
            "cgroup_cpu_max": read("/sys/fs/cgroup/cpu.max"),
            "cgroup_memory_max": read("/sys/fs/cgroup/memory.max"),
            "cgroup_cpuset": read("/sys/fs/cgroup/cpuset.cpus.effective"),
            "tcp_rmem": read("/proc/sys/net/ipv4/tcp_rmem"),
            "tcp_wmem": read("/proc/sys/net/ipv4/tcp_wmem")}


def diagnostic_classpath_provenance(classpath, expected_main_jar_sha):
    entries = []
    for part in classpath.split(os.pathsep):
        entries.extend(Path(match).resolve() for match in glob.glob(part) if Path(match).exists())
    class_name = Path("io/varve/swath/replay/server/ResourceFaultServer.class")
    classes = [entry / class_name for entry in entries if entry.is_dir()
               and (entry / class_name).is_file()]
    main_jars = [entry for entry in entries if entry.is_file()
                 and entry.name.startswith("swath-replay-") and entry.suffix == ".jar"
                 and not entry.name.endswith("-conformance.jar")]
    jar_classes = []
    for entry in entries:
        if entry.is_file() and entry.suffix == ".jar" and zipfile.is_zipfile(entry):
            with zipfile.ZipFile(entry) as archive:
                if class_name.as_posix() in archive.namelist():
                    jar_classes.append(str(entry))
    if (len(classes) != 1 or len(main_jars) != 1 or jar_classes
            or sha256(main_jars[0]) != expected_main_jar_sha):
        raise ValueError("diagnostic classpath must contain one launcher class and pinned main jar")
    class_files = sorted(classes[0].parent.glob("ResourceFaultServer*.class"))
    return {"class_file": str(classes[0]), "class_sha256": sha256(classes[0]),
            "class_files": [{"path": str(path), "sha256": sha256(path)}
                            for path in class_files],
            "main_jar": str(main_jars[0]), "main_jar_sha256": sha256(main_jars[0])}


def require_free_port(port):
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", port))


def prove_slow_held(snapshot, min_held, body_length):
    serving = snapshot["serving"]
    if (serving["active_responses"] < min_held or
            serving["charged_response_bytes"] < min_held * body_length):
        raise RuntimeError("slow clients were counted during injection but did not hold writes")


def prove_slow_rendered(before, held, clients, protocol):
    stages = "swath.replay.request.stage.latency"
    for stage in ("page", "render", "delay"):
        observed = metric(held, stages, {"protocol": protocol, "stage": stage}) - metric(
            before, stages, {"protocol": protocol, "stage": stage})
        if observed != clients:
            raise RuntimeError(f"slow clients did not all finish {stage} before the hold proof")
    writes = metric(held, stages, {"protocol": protocol, "stage": "write"}) - metric(
        before, stages, {"protocol": protocol, "stage": "write"})
    overruns = metric(held, "swath.replay.inject.overrun") - metric(
        before, "swath.replay.inject.overrun")
    if writes or overruns:
        raise RuntimeError("slow hold mixed completed writes or injection overruns")


def prove_trickle(evidence, write_timeout, grace, epsilon):
    if (not evidence["body_started"] or not evidence["held_after_body_start"]
            or not evidence["callback_released"] or not evidence["deadline_counter_engaged"]
            or not evidence["eof_or_reset"] or not evidence["progressed_after_idle"]
            or evidence["received_bytes"] >= evidence["content_length"]
            or evidence["elapsed_s"] < write_timeout - epsilon
            or evidence["elapsed_s"] > write_timeout + grace):
        raise RuntimeError("trickle did not prove a total deadline after idle-surviving progress")


def duration_seconds(value):
    if value.endswith("ms"):
        return int(value[:-2]) / 1000
    if value.endswith("s"):
        return int(value[:-1])
    raise ValueError("duration must be an integer number of ms or s")


def injection_seconds(value):
    if not value:
        return 0
    parts = value.split(",")
    for part in parts:
        if part.startswith("worker_page="):
            return duration_seconds(part.split("=", 1)[1])
    raise ValueError("resource arm requires explicit worker_page injection or empty injection")


def run(args):
    output = Path(args.output).resolve()
    source_root = Path(__file__).resolve().parents[3]
    if output == source_root or source_root in output.parents:
        raise ValueError("resource receipts must be outside the source working tree")
    if args.port == args.metrics_port:
        raise ValueError("listing and metrics ports must differ")
    require_free_port(args.port)
    require_free_port(args.metrics_port)
    output.mkdir(parents=True, exist_ok=False)
    environment, inherited_java_options = clean_java_env()
    environment["JAVA_OPTS"] = args.server_java_opts
    environment["JAVA_HOME"] = args.java_home
    receipt = {"status": "failed", "mode": args.mode, "protocol": args.protocol,
               "fixture": str(Path(args.fixture).resolve()),
               "output": str(output), "clients": args.clients, "page_size": args.page_size,
               "body_upper": args.body_upper, "response_buffer_budget": args.response_buffer_budget,
               "max_response_bytes": args.max_response_bytes,
               "max_concurrent_requests": args.max_concurrent_requests,
               "write_timeout": args.write_timeout, "idle_timeout": args.idle_timeout,
               "min_held": args.min_held, "hold_seconds": args.hold_seconds,
               "expect_overload": args.expect_overload, "normal_walker": args.normal_walker,
               "driver_warmup": args.driver_warmup,
               "heap_headroom_bytes": args.heap_headroom,
               "cache_and_staging_headroom_bytes": args.cache_staging_headroom,
               "decoded_row_bytes_assumption": args.decoded_row_bytes,
               "accepted_send_buffer_bytes": args.accepted_send_buffer_bytes,
               "transport": ("diagnostic_accepted_sndbuf" if args.accepted_send_buffer_bytes
                             else "instrumented_default_sndbuf" if args.diagnostic_server_classpath
                             else "default"),
               "server_java_opts": args.server_java_opts,
               "server_flags": parse_server_opts(args.server_java_opts),
               "removed_inherited_java_option_keys": sorted(inherited_java_options),
               "provenance": provenance(args, environment)}
    if args.body_upper <= 0 or args.body_upper > args.max_response_bytes:
        raise ValueError("declared body upper bound must fit the per-response cap")
    if args.clients > 2 * args.max_concurrent_requests and args.mode == "normal512":
        raise ValueError("normal arm exceeds response-count ceiling")
    if args.mode == "normal512" and args.protocol != "s3":
        raise ValueError("normal512 inventory arm is declared for S3; use provider panels for Azure")
    if args.protocol == "azure" and args.inject_latency:
        raise ValueError("Azure replay has no latency injection; pass --inject-latency ''")
    if args.page_size < 1 or args.page_size > (1000 if args.protocol == "s3" else 5000):
        raise ValueError("page size exceeds the selected protocol limit")
    if args.mode == "slow" and args.write_timeout_seconds <= (
            args.injected_seconds + args.hold_seconds + args.timeout_grace + 1):
        raise ValueError("slow hold must end well before the total write deadline")
    if args.mode == "slow" and args.injected_seconds <= 0:
        raise ValueError("slow hold proof requires a positive worker_page injection")
    if args.mode == "trickle" and args.clients != 1:
        raise ValueError("trickle proof opens exactly one client; declare --clients 1")
    if args.min_held < 1 or args.min_held > args.clients:
        raise ValueError("min-held must be in 1..clients")
    if memory_bytes(receipt["server_flags"]["heap"]) < (
            args.response_buffer_budget + args.heap_headroom):
        raise ValueError("declared -Xmx lacks response-budget plus heap headroom")
    prepared_handlers = min(args.max_concurrent_requests,
                            args.clients + (1 if args.mode == "slow" else 0))
    prepared_rows = prepared_handlers * args.page_size
    prepared_bytes = prepared_rows * args.decoded_row_bytes
    receipt["prepared_row_headroom"] = {
        "concurrent_handlers_assumption": prepared_handlers,
        "page_rows_per_handler": args.page_size,
        "decoded_row_bytes_assumption": args.decoded_row_bytes,
        "prepared_rows": prepared_rows,
        "prepared_bytes": prepared_bytes,
        "cache_and_staging_headroom_bytes": args.cache_staging_headroom,
        "heap_headroom_bytes": args.heap_headroom}
    if (args.decoded_row_bytes <= 0 or args.cache_staging_headroom <= 0
            or prepared_bytes + args.cache_staging_headroom > args.heap_headroom):
        raise ValueError("decoded rows plus cache/staging must fit separate heap headroom")
    if args.mode == "trickle" and args.idle_timeout_seconds >= args.write_timeout_seconds:
        raise ValueError("trickle arm requires idle timeout below total write timeout")
    if args.mode == "slow" and not args.expect_overload and not args.normal_walker:
        raise ValueError("adequate slow arm requires a normal oracle walker alongside held clients")
    if args.accepted_send_buffer_bytes and not args.diagnostic_server_classpath:
        raise ValueError("diagnostic send-buffer arm requires --diagnostic-server-classpath")
    if args.mode == "normal512" and (not args.driver_classpath or not args.fixture_glob):
        raise ValueError("normal512 requires --driver-classpath and --fixture-glob")
    if args.protocol == "s3":
        path = f"/{args.bucket}?list-type=2&max-keys={args.page_size}&encoding-type=url"
        request_headers = {}
    else:
        path = (f"/{args.azure_account}/{args.bucket}?restype=container&comp=list"
                f"&maxresults={args.page_size}")
        request_headers = {"x-ms-version": "2026-06-06"}
    command = [str(Path(args.server).resolve()), "serve", "--fixture", args.fixture,
               "--bucket", args.bucket, "--host", "127.0.0.1", "--port", str(args.port),
               "--metrics-port", str(args.metrics_port), "--serving-mode", args.serving_mode,
               "--parquet-connections", str(args.parquet_connections),
               "--max-concurrent-requests", str(args.max_concurrent_requests),
               "--response-buffer-budget", str(args.response_buffer_budget),
               "--max-response-bytes", str(args.max_response_bytes),
               "--write-timeout", args.write_timeout, "--idle-timeout", args.idle_timeout]
    if args.protocol == "azure":
        command += ["--protocols", "azure", "--azure-account", args.azure_account]
    if args.inject_latency:
        command += ["--inject-latency", args.inject_latency]
    if args.diagnostic_server_classpath:
        receipt["diagnostic_classpath"] = diagnostic_classpath_provenance(
            args.diagnostic_server_classpath, receipt["provenance"]["main_jar_sha256"])
        command = [str(Path(args.java_home) / "bin" / "java"),
                   *shlex.split(args.server_java_opts), "--enable-native-access=ALL-UNNAMED",
                   "-cp", args.diagnostic_server_classpath,
                   "io.varve.swath.replay.server.ResourceFaultServer",
                   args.fixture, args.bucket, args.serving_mode, str(args.port),
                   str(args.metrics_port), str(args.parquet_connections),
                   str(args.max_concurrent_requests), str(args.response_buffer_budget),
                   str(args.max_response_bytes), str(int(args.write_timeout_seconds * 1000)),
                   str(int(args.idle_timeout_seconds * 1000)), args.protocol,
                   args.azure_account, str(args.accepted_send_buffer_bytes), args.inject_latency]
        receipt["diagnostic_server_source_sha256"] = sha256(
            Path(__file__).with_name("ResourceFaultServer.java"))
    receipt["server_command"] = command
    log = (output / "server.log").open("wb")
    server = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT)
    def target_jcmd(*command_args):
        return jcmd(args.java_home, server.pid, *command_args, environment=environment)

    sockets = []
    stop_samples = threading.Event()
    base = f"http://127.0.0.1:{args.port}"
    metrics_url = f"http://127.0.0.1:{args.metrics_port}/metrics"
    resource_samples = []
    sample_errors = []
    sampler = threading.Thread(target=sample_resources,
                               args=(server.pid, metrics_url, stop_samples, resource_samples,
                                     sample_errors), daemon=True)
    sampler_started = False
    try:
        def healthy():
            if server.poll() is not None:
                raise RuntimeError(f"server exited {server.returncode}; see {output / 'server.log'}")
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{args.metrics_port}/healthz",
                                            timeout=1) as response:
                    return response.status == 200
            except (OSError, urllib.error.HTTPError):
                return False
        if not await_condition(healthy, args.start_timeout):
            raise TimeoutError("replay server startup deadline exceeded")
        if args.diagnostic_server_classpath:
            log.flush()
            marker = f"transport={receipt['transport']}"
            if marker not in (output / "server.log").read_text(errors="replace"):
                raise RuntimeError("diagnostic server did not confirm declared socket transport")
        before = http_json(metrics_url)
        sampler.start()
        sampler_started = True
        receipt["vm_command_line"] = target_jcmd("VM.command_line")
        receipt["vm_flags"] = target_jcmd("VM.flags")
        receipt["vm_system_properties"] = target_jcmd("VM.system_properties")
        if receipt["vm_command_line"].get("exit") != 0:
            raise RuntimeError("server VM command line could not be read")
        require_effective_vm_flags(receipt["vm_flags"], receipt["vm_system_properties"],
                                   receipt["server_flags"])
        if before.get("schema_version") != 2:
            raise RuntimeError("resource arm requires metrics schema 2")
        serving = before["serving"]
        require_declared_chunk_size(serving, args.output_chunk_bytes)
        expected_settings = {"max_responses": 2 * args.max_concurrent_requests,
                             "response_buffer_budget": args.response_buffer_budget,
                             "max_response_bytes": args.max_response_bytes,
                             "idle_timeout_ms": int(args.idle_timeout_seconds * 1000),
                             "write_timeout_ms": int(args.write_timeout_seconds * 1000)}
        for name, value in expected_settings.items():
            if serving.get(name) != value:
                raise RuntimeError(f"server reported {name}={serving.get(name)} expected {value}")
        receipt["serving"] = serving
        preflight = full_get(base + path, request_headers)
        receipt["preflight"] = preflight
        if preflight["status"] != 200 or preflight["length"] != preflight["content_length"]:
            raise RuntimeError("preflight listing failed or length mismatch")
        if preflight["length"] > args.body_upper:
            raise RuntimeError("actual preflight body exceeds declared upper bound")
        if args.diagnostic_server_classpath:
            receipt["diagnostic_connections_preflight"] = diagnostic_connections(http_json(metrics_url))
        if args.mode in ("slow", "trickle") and preflight["length"] < args.min_body:
            raise RuntimeError("fixture page is too small to exercise slow socket writes")
        receipt["metrics_before"] = http_json(metrics_url)
        preflight_peak = receipt["metrics_before"]["serving"]["peak_charged_response_bytes"]
        chunk_bytes = receipt["metrics_before"]["serving"].get("output_chunk_bytes")
        receipt["preflight_peak_charged_bytes"] = preflight_peak
        receipt["capacity_model"] = "fixed_chunks" if chunk_bytes is not None else "growable_array"
        receipt["capacity_bound_per_response"] = peak_capacity_bound(
            args.body_upper, preflight_peak, chunk_bytes)
        receipt["adequate_budget_for_clients"] = adequate_budget(
            args.clients, args.body_upper, preflight_peak, chunk_bytes)
        if (args.mode == "normal512" and
                args.response_buffer_budget < receipt["adequate_budget_for_clients"]):
            raise ValueError("configured budget is below conservative no-refusal bound")
        receipt["nmt_before"] = target_jcmd("VM.native_memory", "summary")
        if receipt["nmt_before"].get("exit") != 0 or "Total:" not in receipt["nmt_before"].get("stdout", ""):
            raise RuntimeError("NMT summary unavailable despite declared tracking flag")
        if args.jfr:
            receipt["jfr_start"] = target_jcmd("JFR.start", "name=replay-resource",
                                        "settings=profile")
            if receipt["jfr_start"].get("exit") != 0:
                raise RuntimeError("JFR start failed")
        if args.mode == "normal512":
            inventory = oracle_inventory(args, args.clients, environment)
            if inventory["fixture_count"] < args.clients * args.page_size:
                raise RuntimeError("fixture cannot offer one full page to each partitioned client")
            driver_before = metric(http_json(metrics_url), "swath.replay.http.requests")
            accepted_before = (diagnostic_connections(http_json(metrics_url))["accepted"]
                               if args.diagnostic_server_classpath else None)
            receipt["oracle"] = oracle_walk(args, args.clients, output, inventory, environment)
            receipt["oracle"]["server_requests"] = (metric(http_json(metrics_url),
                    "swath.replay.http.requests") - driver_before)
            receipt["oracle"]["warmup_requests"] = require_oracle_request_accounting(
                receipt["oracle"]["result"], receipt["oracle"]["server_requests"])
            max_body = receipt["oracle"]["result"].get("max_response_bytes")
            if max_body is None or max_body > args.body_upper:
                raise RuntimeError("oracle driver did not prove every response fits declared M")
            client_peak = receipt["oracle"]["result"].get("peak_outstanding_requests")
            receipt["client_peak_outstanding_requests"] = client_peak
            if client_peak is None or client_peak < args.clients:
                raise RuntimeError("normal arm did not prove declared client-side outstanding requests")
            if args.diagnostic_server_classpath:
                accepted_after = diagnostic_connections(http_json(metrics_url))["accepted"]
                accepted_delta = accepted_after - accepted_before
                receipt["oracle"]["accepted_connections_before"] = accepted_before
                receipt["oracle"]["accepted_connections_after"] = accepted_after
                receipt["oracle"]["accepted_connections_delta"] = accepted_delta
                receipt["oracle"]["requests_per_new_accept"] = (
                    receipt["oracle"]["server_requests"] / accepted_delta
                    if accepted_delta > 0 else None)
                if (accepted_delta > args.clients
                        or receipt["oracle"]["server_requests"] <= accepted_delta):
                    raise RuntimeError("diagnostic connector did not prove bounded accepts and reuse")
                receipt["connection_reuse_proof"] = "at_least_one_reuse_with_accepts_at_most_clients"
            else:
                receipt["connection_reuse_proof"] = "pending_separate_transport_observation"
        elif args.mode == "slow":
            sockets = open_slow("127.0.0.1", args.port, path, args.clients, request_headers)
            held = await_condition(lambda: http_json(metrics_url)["serving"]["active_responses"]
                                   >= args.min_held, args.hold_timeout)
            receipt["held_reached"] = bool(held)
            if not held:
                raise RuntimeError("slow clients did not hold the required callbacks")
            # The first count may still be in paging or injected delay. Check after the
            # declared injection has ended and again before disconnecting any socket.
            time.sleep(args.injected_seconds + args.hold_seconds)
            receipt["held_metrics"] = http_json(metrics_url)
            prove_slow_held(receipt["held_metrics"], args.min_held, preflight["length"])
            prove_slow_rendered(receipt["metrics_before"], receipt["held_metrics"],
                                args.clients, args.protocol)
            receipt["nmt_held"] = target_jcmd("VM.native_memory", "summary")
            if receipt["nmt_held"].get("exit") != 0 or "Total:" not in receipt["nmt_held"].get("stdout", ""):
                raise RuntimeError("NMT held snapshot unavailable")
            if args.normal_walker:
                if not args.driver_classpath or not args.fixture_glob:
                    raise ValueError("normal walker requires --driver-classpath and --fixture-glob")
                walker_before = metric(http_json(metrics_url), "swath.replay.http.requests")
                receipt["normal_walker"] = oracle_walk(args, 1, output,
                                                         oracle_inventory(args, 1, environment),
                                                         environment)
                receipt["normal_walker"]["server_requests"] = (metric(http_json(metrics_url),
                        "swath.replay.http.requests") - walker_before)
                receipt["normal_walker"]["warmup_requests"] = require_oracle_request_accounting(
                    receipt["normal_walker"]["result"],
                    receipt["normal_walker"]["server_requests"])
                held_after_walker = http_json(metrics_url)["serving"]
                if held_after_walker["active_responses"] < args.min_held:
                    raise RuntimeError("slow responses were not held alongside the normal walker")
            probe = full_get(base + path, request_headers)
            receipt["overload_probe"] = probe
            if args.expect_overload and (probe["status"] != 503
                                         or probe["reason"] not in
                                         ("response_count_exhausted", "response_budget_exhausted")):
                raise RuntimeError("tight-budget arm did not produce labeled replay overload")
            if not args.expect_overload and probe["status"] != 200:
                raise RuntimeError("adequately budgeted slow arm refused normal traffic")
            receipt["pre_disconnect_metrics"] = http_json(metrics_url)
            prove_slow_held(receipt["pre_disconnect_metrics"], args.min_held,
                            preflight["length"])
        else:
            connection = open_slow("127.0.0.1", args.port, path, 1, request_headers)[0]
            sockets = [connection]
            connection.settimeout(args.request_timeout)
            status, headers, initial_body = read_response_head(connection)
            if status != 200 or int(headers.get("content-length", "-1")) != preflight["length"]:
                raise RuntimeError("trickle response status/length differs from preflight")
            body_length = int(headers["content-length"])
            received = len(initial_body)
            if received == 0:
                first = connection.recv(1)
                if not first:
                    raise RuntimeError("trickle response ended before first body byte")
                received = 1
            started = time.monotonic()  # write deadline starts at first socket write, near this byte.
            state = http_json(metrics_url)["serving"]
            held_after_body_start = state["active_responses"] >= 1 and (
                    state["charged_response_bytes"] >= body_length)
            if not held_after_body_start:
                raise RuntimeError("trickle response was not held after body delivery began")
            receipt["nmt_held"] = target_jcmd("VM.native_memory", "summary")
            if receipt["nmt_held"].get("exit") != 0 or "Total:" not in receipt["nmt_held"].get("stdout", ""):
                raise RuntimeError("NMT trickle snapshot unavailable")
            deadline_before = metric(receipt["metrics_before"], "swath.replay.response.write.deadline",
                                     {"protocol": args.protocol, "reason": "total_deadline"})
            receive_buffer = connection.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            reopen_bytes = max(1024, receive_buffer // 2)
            connection.settimeout(max(.05, args.idle_timeout_seconds / 8))
            progressed_after_idle = False
            callback_released = False
            eof_or_reset = False
            deadline_counter_engaged = False
            while time.monotonic() - started < args.write_timeout_seconds + args.timeout_grace:
                time.sleep(min(args.trickle_interval, args.idle_timeout_seconds / 4))
                chunk, closed = receive_trickle_chunk(connection, reopen_bytes)
                if closed:
                    eof_or_reset = True
                elif chunk:
                    received += len(chunk)
                    if time.monotonic() - started > args.idle_timeout_seconds:
                        progressed_after_idle = True
                snapshot = http_json(metrics_url)
                state = snapshot["serving"]
                expired = metric(snapshot, "swath.replay.response.write.deadline",
                                 {"protocol": args.protocol, "reason": "total_deadline"}) > deadline_before
                deadline_counter_engaged |= expired
                if expired and state["active_responses"] == 0 and state["charged_response_bytes"] == 0:
                    callback_released = True
                    break
                if eof_or_reset or received >= body_length:
                    break
            elapsed = time.monotonic() - started
            if eof_or_reset and not callback_released:
                def deadline_drain():
                    snapshot = http_json(metrics_url)
                    serving = snapshot["serving"]
                    expired = metric(snapshot, "swath.replay.response.write.deadline",
                                     {"protocol": args.protocol, "reason": "total_deadline"}) > deadline_before
                    return expired and serving["active_responses"] == 0 and (
                        serving["charged_response_bytes"] == 0)
                callback_released = bool(await_condition(deadline_drain, args.timeout_grace))
                deadline_counter_engaged |= callback_released
            if callback_released and not eof_or_reset:
                connection.settimeout(.2)
                eof_deadline = time.monotonic() + args.timeout_grace
                while time.monotonic() < eof_deadline:
                    try:
                        tail = connection.recv(65536)
                        if not tail:
                            eof_or_reset = True
                            break
                        received += len(tail)
                    except ConnectionResetError:
                        eof_or_reset = True
                        break
                    except socket.timeout:
                        continue
            receipt["trickle"] = {"received_bytes": received,
                                  "content_length": body_length, "elapsed_s": elapsed,
                                  "receive_buffer": receive_buffer,
                                  "reopen_bytes": reopen_bytes,
                                  "body_started": True,
                                  "held_after_body_start": held_after_body_start,
                                  "deadline_counter_engaged": deadline_counter_engaged,
                                  "progressed_after_idle": progressed_after_idle,
                                  "callback_released": callback_released,
                                  "eof_or_reset": eof_or_reset}
            prove_trickle(receipt["trickle"], args.write_timeout_seconds,
                          args.timeout_grace, args.deadline_epsilon)
        drain_started = time.monotonic()
        for connection in sockets:
            connection.close()
        sockets = []
        drained = await_condition(lambda: http_json(metrics_url)["serving"]["active_responses"] == 0
                                  and http_json(metrics_url)["serving"]["charged_response_bytes"] == 0,
                                  args.drain_timeout)
        receipt["drained"] = bool(drained)
        receipt["drain_elapsed_s"] = time.monotonic() - drain_started
        if not drained:
            raise RuntimeError("response permits/bytes did not recover after clients closed")
        receipt["recovery_probe"] = full_get(base + path, request_headers)
        if (receipt["recovery_probe"]["status"] != 200
                or receipt["recovery_probe"]["sha256"] != preflight["sha256"]):
            raise RuntimeError("healthy request did not recover after slow clients")
        after = http_json(metrics_url)
        receipt["metrics_after"] = after
        if args.diagnostic_server_classpath:
            receipt["diagnostic_connections_after"] = diagnostic_connections(after)
        refused_before = metric(receipt["metrics_before"],
                                "swath.replay.response.admission.refused")
        refused_after = metric(after, "swath.replay.response.admission.refused")
        receipt["refusals"] = refused_after - refused_before
        receipt["refusals_by_reason"] = refusal_reasons(receipt["metrics_before"], after)
        if args.mode == "slow":
            deadline_tags = {"protocol": args.protocol, "reason": "total_deadline"}
            receipt["write_deadline_delta"] = (metric(after, "swath.replay.response.write.deadline",
                                                      deadline_tags)
                    - metric(receipt["metrics_before"], "swath.replay.response.write.deadline",
                             deadline_tags))
            if receipt["write_deadline_delta"] != 0:
                raise RuntimeError("slow clients recovered by write deadline, not disconnect")
        if args.mode == "normal512" and receipt["refusals"] != 0:
            raise RuntimeError("normal arm recorded unintended admission refusals")
        if args.mode == "slow" and args.expect_overload and receipt["refusals"] < 1:
            raise RuntimeError("overload response was not metered")
        receipt["direct_meters_before"] = direct_meters(receipt["metrics_before"])
        receipt["direct_meters_after"] = direct_meters(after)
        if not receipt["direct_meters_after"]:
            raise RuntimeError("JVM direct-buffer meters are unavailable")
        receipt["nmt_after"] = target_jcmd("VM.native_memory", "summary")
        if receipt["nmt_after"].get("exit") != 0 or "Total:" not in receipt["nmt_after"].get("stdout", ""):
            raise RuntimeError("NMT summary unavailable after resource arm")
        if args.jfr:
            jfr_path = output / "resource.jfr"
            receipt["jfr_stop"] = target_jcmd("JFR.stop", "name=replay-resource",
                                        f"filename={jfr_path}")
            receipt["jfr_path"] = str(jfr_path)
            if receipt["jfr_stop"].get("exit") != 0 or not jfr_path.is_file():
                raise RuntimeError("JFR stop/dump failed")
        receipt["resource_samples"] = resource_samples
        receipt["resource_sample_errors"] = sample_errors
        receipt["rss_peak_sampled"] = max((sample["rss"] for sample in resource_samples
                                           if sample["rss"] is not None), default=None)
        receipt["heap_peak_sampled"] = max((sample.get("heap") for sample in resource_samples
                                            if "heap" in sample), default=None)
        receipt["direct_peak_sampled"] = max((sample.get("direct") for sample in resource_samples
                                              if "direct" in sample), default=None)
        receipt["active_responses_peak_sampled"] = max((sample.get("active_responses")
                for sample in resource_samples if "active_responses" in sample), default=None)
        receipt["charged_bytes_peak_sampled"] = max((sample.get("charged_bytes")
                for sample in resource_samples if "charged_bytes" in sample), default=None)
        if receipt["heap_peak_sampled"] is None or receipt["direct_peak_sampled"] is None:
            raise RuntimeError("heap/direct sampled resource observations are missing")
        if sample_errors:
            raise RuntimeError("resource sampler missed or failed a metrics scrape")
        direct_limit = memory_bytes(receipt["server_flags"]["max_direct_memory"])
        receipt["direct_limit_bytes"] = direct_limit
        if receipt["direct_peak_sampled"] > direct_limit:
            raise RuntimeError("sampled direct-buffer use exceeded declared MaxDirectMemorySize")
        potential_clients = args.clients + (1 if args.mode == "slow" else 0)
        receipt["encoded_peak_bound_clients"] = potential_clients
        if (after["serving"]["peak_charged_response_bytes"] >
                potential_clients * receipt["capacity_bound_per_response"]):
            raise RuntimeError("charged peak exceeds declared per-client growth bound")
        if after["serving"]["peak_charged_response_bytes"] > args.response_buffer_budget:
            raise RuntimeError("server peak charged response bytes exceeded configured budget")
        server_requests = metric(after, "swath.replay.http.requests") - metric(
            receipt["metrics_before"], "swath.replay.http.requests")
        if args.mode == "normal512":
            expected_requests = receipt["oracle"]["server_requests"] + 1
        elif args.mode == "slow":
            expected_requests = args.clients + 2 + (
                receipt.get("normal_walker", {}).get("server_requests", 0))
        else:
            expected_requests = 2
        receipt["request_accounting"] = {"server_delta": server_requests,
                                         "expected": expected_requests}
        if server_requests != expected_requests:
            raise RuntimeError("server HTTP request count disagrees with driver/socket attempts")
        receipt["claim_scope"] = ("diagnostic custom-send-buffer bounds and recovery only"
                if args.accepted_send_buffer_bytes else
                "instrumented default-socket resource bounds; at least one reuse and accepts <= clients"
                if args.diagnostic_server_classpath else
                "default-transport resource bounds; pooled-connection reuse requires separate proof")
        receipt["status"] = "passed"
    except Exception as error:
        receipt["error"] = repr(error)
        try:
            receipt["metrics_failure"] = http_json(metrics_url)
        except (OSError, urllib.error.HTTPError) as scrape_error:
            receipt["metrics_failure_error"] = repr(scrape_error)
        raise
    finally:
        for connection in sockets:
            connection.close()
        stop_samples.set()
        if sampler_started:
            sampler.join(timeout=2)
        receipt["resource_samples"] = resource_samples
        receipt["resource_sample_errors"] = sample_errors
        if args.jfr and receipt.get("jfr_start", {}).get("exit") == 0 and "jfr_stop" not in receipt:
            jfr_path = output / "resource.jfr"
            receipt["jfr_stop"] = target_jcmd("JFR.stop", "name=replay-resource",
                                        f"filename={jfr_path}")
            receipt["jfr_path"] = str(jfr_path)
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
        log.close()
        receipt["server_exit"] = server.returncode
        receipt["rss_peak_sampled"] = max((sample["rss"] for sample in resource_samples
                                           if sample["rss"] is not None), default=None)
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--java-home", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--fixture-glob", help="Parquet glob for ReplayHttpBench inventory oracle")
    parser.add_argument("--driver-classpath", help="compiled ReplayHttpBench classes and dependency jars")
    parser.add_argument("--diagnostic-server-classpath",
                        help="ResourceFaultServer class plus frozen replay distribution jars")
    parser.add_argument("--accepted-send-buffer-bytes", type=int, default=0,
                        help="diagnostic-only accepted TCP send buffer; 0 uses production launcher")
    parser.add_argument("--driver-warmup", type=int, default=0)
    parser.add_argument("--driver-timeout", type=float, default=3600)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--protocol", choices=("s3", "azure"), default="s3")
    parser.add_argument("--azure-account", default="replay")
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("normal512", "slow", "trickle"), required=True)
    parser.add_argument("--clients", type=int, default=512)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--body-upper", type=int, required=True)
    parser.add_argument("--min-body", type=int, default=1024 * 1024)
    parser.add_argument("--min-held", type=int)
    parser.add_argument("--response-buffer-budget", type=int, required=True)
    parser.add_argument("--max-response-bytes", type=int, required=True)
    parser.add_argument("--output-chunk-bytes", type=int,
                        help="required declared chunk size for chunked serving candidates")
    parser.add_argument("--max-concurrent-requests", type=int, default=512)
    parser.add_argument("--parquet-connections", type=int, default=16)
    parser.add_argument("--serving-mode", choices=("sorted", "duckdb"), default="sorted")
    parser.add_argument("--port", type=int, default=19111)
    parser.add_argument("--metrics-port", type=int, default=19112)
    parser.add_argument("--server-java-opts", required=True)
    parser.add_argument("--inject-latency", default="worker_page=1s")
    parser.add_argument("--write-timeout", default="10s")
    parser.add_argument("--idle-timeout", default="30s")
    parser.add_argument("--trickle-interval", type=float, default=.1)
    parser.add_argument("--timeout-grace", type=float, default=1.0)
    parser.add_argument("--hold-seconds", type=float, default=2.0)
    parser.add_argument("--hold-timeout", type=float, default=10.0)
    parser.add_argument("--drain-timeout", type=float, default=10.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--heap-headroom", type=int, default=1024 * 1024 * 1024)
    parser.add_argument("--decoded-row-bytes", type=int, required=True,
                        help="predeclared upper estimate per prepared fixture row, including key bytes")
    parser.add_argument("--cache-staging-headroom", type=int, default=256 * 1024 * 1024,
                        help="separate cache, serializer staging, and other heap allowance")
    parser.add_argument("--deadline-epsilon", type=float, default=.5)
    parser.add_argument("--start-timeout", type=float, default=60.0)
    parser.add_argument("--expect-overload", action="store_true")
    parser.add_argument("--normal-walker", action="store_true")
    parser.add_argument("--jfr", action="store_true")
    args = parser.parse_args()
    args.write_timeout_seconds = duration_seconds(args.write_timeout)
    args.idle_timeout_seconds = duration_seconds(args.idle_timeout)
    args.injected_seconds = injection_seconds(args.inject_latency)
    if args.min_held is None:
        args.min_held = args.clients
    try:
        run(args)
    except Exception as error:
        output = Path(args.output).resolve()
        source_root = Path(__file__).resolve().parents[3]
        if output == source_root or source_root in output.parents:
            raise
        output.mkdir(parents=True, exist_ok=True)
        receipt = output / "receipt.json"
        if not receipt.exists():
            receipt.write_text(json.dumps({"status": "failed", "error": repr(error),
                                           "mode": args.mode, "fixture": args.fixture}, indent=2) + "\n")
        raise


if __name__ == "__main__":
    main()
