#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Opt-in response-budget and slow-client replay characterization.

The run is a resource/fault receipt, never a throughput comparison. The caller supplies a
fixture with long keys and an explicit body upper bound. Every result, including failure,
is written outside the source tree to --output before an exception is re-raised.
"""

import argparse
import asyncio
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request


def s3_initial_capacity(page_size, cap):
    return min(cap, max(4096, 512 + min(page_size, 1000) * 320))


def peak_capacity_bound(body_upper, page_size, cap):
    """Conservative old+new capacity bound for BudgetedOutput's 1.5x growth."""
    if body_upper <= 0 or body_upper > cap:
        raise ValueError("declared body upper bound must fit the per-response cap")
    return max(s3_initial_capacity(page_size, cap), math.ceil(2.5 * body_upper))


def adequate_budget(clients, body_upper, page_size, cap):
    return clients * peak_capacity_bound(body_upper, page_size, cap)


def http_json(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def metric(snapshot, name, tags=None):
    tags = tags or {}
    return sum((item.get("count", item.get("value", 0)) or 0)
               for item in snapshot.get("meters", [])
               if item.get("name") == name
               and all(item.get("tags", {}).get(key) == value for key, value in tags.items()))


def direct_meters(snapshot):
    return [item for item in snapshot.get("meters", [])
            if item.get("name", "").startswith("jvm.buffer")
            and item.get("tags", {}).get("id") == "direct"]


def rss(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def jcmd(pid, *args):
    try:
        command = ["jcmd", str(pid), *args]
        result = subprocess.run(command, text=True, capture_output=True, timeout=10,
                                check=False)
        return {"command": command, "exit": result.returncode,
                "stdout": result.stdout, "stderr": result.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"error": str(error)}


def full_get(url):
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            body = response.read()
            return {"status": response.status, "length": len(body),
                    "content_length": int(response.headers.get("Content-Length", "-1")),
                    "reason": response.headers.get("x-swath-replay-error")}
    except urllib.error.HTTPError as error:
        body = error.read()
        return {"status": error.code, "length": len(body),
                "content_length": int(error.headers.get("Content-Length", "-1")),
                "reason": error.headers.get("x-swath-replay-error")}


async def one_full_read(host, port, path, timeout):
    started = time.monotonic_ns()
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    try:
        writer.write(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode("ascii"))
        await writer.drain()
        status_line = await asyncio.wait_for(reader.readline(), timeout)
        if not status_line.startswith(b"HTTP/1."):
            raise RuntimeError(f"invalid HTTP status line {status_line[:100]!r}")
        status = int(status_line.split()[1])
        headers = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout)
            if line in (b"\r\n", b"\n", b""):
                break
            name, value = line.decode("iso-8859-1").split(":", 1)
            headers[name.lower()] = value.strip()
        length = int(headers["content-length"])
        body = await asyncio.wait_for(reader.readexactly(length), timeout)
        return {"status": status, "length": len(body), "content_length": length,
                "reason": headers.get("x-swath-replay-error"),
                "elapsed_ns": time.monotonic_ns() - started}
    finally:
        writer.close()
        await writer.wait_closed()


async def full_batch(host, port, path, clients, timeout):
    return await asyncio.gather(*(one_full_read(host, port, path, timeout)
                                  for _ in range(clients)), return_exceptions=True)


def open_slow(host, port, path, clients):
    sockets = []
    try:
        for _ in range(clients):
            connection = socket.create_connection((host, port), timeout=10)
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            connection.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: keep-alive\r\n\r\n"
                               .encode("ascii"))
            sockets.append(connection)
        return sockets
    except Exception:
        for connection in sockets:
            connection.close()
        raise


def await_condition(predicate, seconds, interval=.05):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def sample_rss(pid, stop, samples):
    while not stop.wait(.05):
        value = rss(pid)
        if value is not None:
            samples.append(value)


def parse_server_opts(opts):
    flags = {}
    for key, pattern in {
        "heap": r"(?:^|\s)-Xmx(\d+[kKmMgG])(?:\s|$)",
        "max_direct_memory": r"(?:^|\s)-XX:MaxDirectMemorySize=(\d+[kKmMgG])(?:\s|$)",
        "nio_cached_buffer": r"(?:^|\s)-Djdk\.nio\.maxCachedBufferSize=(\d+)(?:\s|$)",
    }.items():
        match = re.search(pattern, opts)
        if not match:
            raise ValueError(f"--server-java-opts must declare {key}")
        flags[key] = match.group(1)
    if flags["nio_cached_buffer"] != "262144":
        raise ValueError("jdk.nio.maxCachedBufferSize must be 262144 for this panel")
    return flags


def duration_seconds(value):
    if value.endswith("ms"):
        return int(value[:-2]) / 1000
    if value.endswith("s"):
        return int(value[:-1])
    raise ValueError("write timeout must be an integer number of ms or s")


def run(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    receipt = {"status": "failed", "mode": args.mode, "fixture": str(Path(args.fixture).resolve()),
               "output": str(output), "clients": args.clients, "page_size": args.page_size,
               "body_upper": args.body_upper, "response_buffer_budget": args.response_buffer_budget,
               "max_response_bytes": args.max_response_bytes,
               "max_concurrent_requests": args.max_concurrent_requests,
               "write_timeout": args.write_timeout, "server_java_opts": args.server_java_opts,
               "server_flags": parse_server_opts(args.server_java_opts),
               "capacity_bound_per_response": peak_capacity_bound(
                   args.body_upper, args.page_size, args.max_response_bytes)}
    receipt["adequate_budget_for_clients"] = adequate_budget(
        args.clients, args.body_upper, args.page_size, args.max_response_bytes)
    if args.mode == "normal512" and args.response_buffer_budget < receipt["adequate_budget_for_clients"]:
        raise ValueError("configured budget is below conservative no-refusal bound")
    if args.clients > 2 * args.max_concurrent_requests and args.mode == "normal512":
        raise ValueError("normal arm exceeds response-count ceiling")
    path = f"/{args.bucket}?list-type=2&max-keys={args.page_size}&encoding-type=url"
    command = [str(Path(args.server).resolve()), "serve", "--fixture", args.fixture,
               "--bucket", args.bucket, "--host", "127.0.0.1", "--port", str(args.port),
               "--metrics-port", str(args.metrics_port), "--serving-mode", args.serving_mode,
               "--parquet-connections", str(args.parquet_connections),
               "--max-concurrent-requests", str(args.max_concurrent_requests),
               "--response-buffer-budget", str(args.response_buffer_budget),
               "--max-response-bytes", str(args.max_response_bytes),
               "--write-timeout", args.write_timeout]
    if args.inject_latency:
        command += ["--inject-latency", args.inject_latency]
    receipt["server_command"] = command
    environment = os.environ.copy()
    environment["JAVA_OPTS"] = args.server_java_opts
    log = (output / "server.log").open("wb")
    server = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT)
    sockets = []
    stop_samples = threading.Event()
    rss_samples = []
    sampler = threading.Thread(target=sample_rss, args=(server.pid, stop_samples, rss_samples),
                               daemon=True)
    sampler.start()
    base = f"http://127.0.0.1:{args.port}"
    metrics_url = f"http://127.0.0.1:{args.metrics_port}/metrics"
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
        before = http_json(metrics_url)
        if before.get("schema_version") != 2:
            raise RuntimeError("resource arm requires metrics schema 2")
        serving = before["serving"]
        expected_settings = {"max_responses": 2 * args.max_concurrent_requests,
                             "response_buffer_budget": args.response_buffer_budget,
                             "max_response_bytes": args.max_response_bytes}
        for name, value in expected_settings.items():
            if serving.get(name) != value:
                raise RuntimeError(f"server reported {name}={serving.get(name)} expected {value}")
        receipt["serving"] = serving
        preflight = full_get(base + path)
        receipt["preflight"] = preflight
        if preflight["status"] != 200 or preflight["length"] != preflight["content_length"]:
            raise RuntimeError("preflight listing failed or length mismatch")
        if preflight["length"] > args.body_upper:
            raise RuntimeError("actual preflight body exceeds declared upper bound")
        if args.mode in ("slow", "trickle") and preflight["length"] < args.min_body:
            raise RuntimeError("fixture page is too small to exercise slow socket writes")
        receipt["metrics_before"] = http_json(metrics_url)
        receipt["nmt_before"] = jcmd(server.pid, "VM.native_memory", "summary")
        if args.jfr:
            receipt["jfr_start"] = jcmd(server.pid, "JFR.start", "name=replay-resource",
                                        "settings=profile")
            if receipt["jfr_start"].get("exit") != 0:
                raise RuntimeError("JFR start failed")
        if args.mode == "normal512":
            results = asyncio.run(full_batch("127.0.0.1", args.port, path, args.clients,
                                             args.request_timeout))
            failures = [repr(result) for result in results if isinstance(result, Exception)
                        or result.get("status") != 200 or result.get("length") != result.get("content_length")
                        or result.get("length") > args.body_upper]
            receipt["responses"] = {"total": len(results), "failures": failures[:30],
                                    "max_body": max((result["length"] for result in results
                                                     if isinstance(result, dict)), default=0),
                                    "max_elapsed_ns": max((result["elapsed_ns"] for result in results
                                                           if isinstance(result, dict)), default=0)}
            if failures:
                raise RuntimeError(f"normal arm had {len(failures)} failed/refused responses")
        elif args.mode == "slow":
            sockets = open_slow("127.0.0.1", args.port, path, args.clients)
            held = await_condition(lambda: http_json(metrics_url)["serving"]["active_responses"]
                                   >= args.min_held, args.hold_timeout)
            receipt["held_reached"] = bool(held)
            receipt["held_metrics"] = http_json(metrics_url)
            if not held:
                raise RuntimeError("slow clients did not hold the required callbacks")
            time.sleep(args.hold_seconds)
            probe = full_get(base + path)
            receipt["overload_probe"] = probe
            if args.expect_overload and (probe["status"] != 503
                                         or probe["reason"] not in
                                         ("response_count_exhausted", "response_budget_exhausted")):
                raise RuntimeError("tight-budget arm did not produce labeled replay overload")
            if not args.expect_overload and probe["status"] != 200:
                raise RuntimeError("adequately budgeted slow arm refused normal traffic")
        else:
            connection = open_slow("127.0.0.1", args.port, path, 1)[0]
            sockets = [connection]
            connection.settimeout(.2)
            started = time.monotonic()
            received = 0
            callback_released = False
            while time.monotonic() - started < args.write_timeout_seconds + args.timeout_grace:
                try:
                    chunk = connection.recv(1)
                except socket.timeout:
                    chunk = b""
                if chunk:
                    received += len(chunk)
                state = http_json(metrics_url)["serving"]
                if state["active_responses"] == 0 and state["charged_response_bytes"] == 0:
                    callback_released = True
                    break
                time.sleep(args.trickle_interval)
            receipt["trickle"] = {"received_bytes": received,
                                  "elapsed_s": time.monotonic() - started,
                                  "callback_released": callback_released}
            if not callback_released:
                raise RuntimeError("trickling client retained its response past total write deadline")
        for connection in sockets:
            connection.close()
        sockets = []
        drained = await_condition(lambda: http_json(metrics_url)["serving"]["active_responses"] == 0
                                  and http_json(metrics_url)["serving"]["charged_response_bytes"] == 0,
                                  args.drain_timeout)
        receipt["drained"] = bool(drained)
        if not drained:
            raise RuntimeError("response permits/bytes did not recover after clients closed")
        receipt["recovery_probe"] = full_get(base + path)
        if receipt["recovery_probe"]["status"] != 200:
            raise RuntimeError("healthy request did not recover after slow clients")
        after = http_json(metrics_url)
        receipt["metrics_after"] = after
        refused_before = metric(receipt["metrics_before"],
                                "swath.replay.response.admission.refused")
        refused_after = metric(after, "swath.replay.response.admission.refused")
        receipt["refusals"] = refused_after - refused_before
        if args.mode == "normal512" and receipt["refusals"] != 0:
            raise RuntimeError("normal arm recorded unintended admission refusals")
        if args.mode == "slow" and args.expect_overload and receipt["refusals"] < 1:
            raise RuntimeError("overload response was not metered")
        receipt["direct_meters_before"] = direct_meters(receipt["metrics_before"])
        receipt["direct_meters_after"] = direct_meters(after)
        receipt["nmt_after"] = jcmd(server.pid, "VM.native_memory", "summary")
        if args.jfr:
            jfr_path = output / "resource.jfr"
            receipt["jfr_stop"] = jcmd(server.pid, "JFR.stop", "name=replay-resource",
                                        f"filename={jfr_path}")
            receipt["jfr_path"] = str(jfr_path)
        receipt["rss_peak_sampled"] = max(rss_samples, default=None)
        receipt["status"] = "passed"
    except Exception as error:
        receipt["error"] = repr(error)
        raise
    finally:
        for connection in sockets:
            connection.close()
        stop_samples.set()
        sampler.join(timeout=2)
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
        log.close()
        receipt["server_exit"] = server.returncode
        receipt["rss_peak_sampled"] = max(rss_samples, default=None)
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("normal512", "slow", "trickle"), required=True)
    parser.add_argument("--clients", type=int, default=512)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--body-upper", type=int, required=True)
    parser.add_argument("--min-body", type=int, default=1024 * 1024)
    parser.add_argument("--min-held", type=int, default=1)
    parser.add_argument("--response-buffer-budget", type=int, required=True)
    parser.add_argument("--max-response-bytes", type=int, required=True)
    parser.add_argument("--max-concurrent-requests", type=int, default=512)
    parser.add_argument("--parquet-connections", type=int, default=16)
    parser.add_argument("--serving-mode", choices=("sorted", "duckdb"), default="sorted")
    parser.add_argument("--port", type=int, default=19111)
    parser.add_argument("--metrics-port", type=int, default=19112)
    parser.add_argument("--server-java-opts", required=True)
    parser.add_argument("--inject-latency", default="worker_page=1s")
    parser.add_argument("--write-timeout", default="2s")
    parser.add_argument("--trickle-interval", type=float, default=.1)
    parser.add_argument("--timeout-grace", type=float, default=1.0)
    parser.add_argument("--hold-seconds", type=float, default=2.0)
    parser.add_argument("--hold-timeout", type=float, default=10.0)
    parser.add_argument("--drain-timeout", type=float, default=10.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--start-timeout", type=float, default=60.0)
    parser.add_argument("--expect-overload", action="store_true")
    parser.add_argument("--jfr", action="store_true")
    args = parser.parse_args()
    args.write_timeout_seconds = duration_seconds(args.write_timeout)
    try:
        run(args)
    except Exception as error:
        output = Path(args.output).resolve()
        output.mkdir(parents=True, exist_ok=True)
        receipt = output / "receipt.json"
        if not receipt.exists():
            receipt.write_text(json.dumps({"status": "failed", "error": repr(error),
                                           "mode": args.mode, "fixture": args.fixture}, indent=2) + "\n")
        raise


if __name__ == "__main__":
    main()
