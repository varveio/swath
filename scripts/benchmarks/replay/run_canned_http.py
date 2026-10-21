#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Isolated HTTP client ceiling against an immutable, fixture-captured canned server."""

import argparse
import hashlib
import json
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time
from urllib.request import urlopen

from run_pair import affinity, clean_java_env, cpu_ticks, cpus, thread_cpu_ticks, thread_utilization


def read_stdout(stream, lines):
    try:
        for line in stream:
            lines.put(line)
    finally:
        lines.put(None)


def counter(url):
    with urlopen(url + "/counters", timeout=2) as response:
        return int(response.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath", required=True)
    parser.add_argument("--java-home", required=True)
    parser.add_argument("--protocol", choices=["s3", "gcs", "azure"], required=True)
    parser.add_argument("--s3-body", type=Path, required=True)
    parser.add_argument("--gcs-body", type=Path, required=True)
    parser.add_argument("--azure-body", type=Path, required=True)
    parser.add_argument("--server-cpus", required=True)
    parser.add_argument("--client-cpus", required=True)
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--port", type=int, default=19091)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    server_cpus, client_cpus = cpus(args.server_cpus), cpus(args.client_cpus)
    if not server_cpus or not client_cpus or server_cpus & client_cpus:
        parser.error("nonempty server/client CPU sets must be disjoint")
    args.output.mkdir(parents=True, exist_ok=False)
    env, inherited = clean_java_env()
    env["JAVA_HOME"] = args.java_home
    java = f"{args.java_home}/bin/java"
    klass = "io.varve.swath.replay.bench.ReplayCannedHttpBench"
    bodies = {"s3": args.s3_body, "gcs": args.gcs_body, "azure": args.azure_body}
    chosen_body = bodies[args.protocol]
    receipt = {"kind": "canned_http", "status": "failed", "protocol": args.protocol,
               "clients": args.clients, "seconds": args.seconds,
               "server_cpus": sorted(server_cpus), "client_cpus": sorted(client_cpus),
               "removed_inherited_java_option_keys": sorted(inherited),
               "bodies": {name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                                  "bytes": path.stat().st_size} for name, path in bodies.items()}}
    endpoint = f"http://127.0.0.1:{args.port}"
    server_cmd = [java, "-Xms256m", "-Xmx512m", "-cp", args.classpath, klass, "server",
                  str(args.port), *(str(path) for path in bodies.values())]
    client_cmd = [java, "-Xms512m", "-Xmx2g", "-cp", args.classpath, klass, "client",
                  endpoint, args.protocol, str(chosen_body), str(args.clients), str(args.seconds), "1000"]
    receipt["server_command"] = server_cmd
    receipt["client_command"] = client_cmd
    with (args.output / "server.log").open("wb") as server_log:
        server = subprocess.Popen(server_cmd, env=env, stdout=server_log, stderr=subprocess.STDOUT,
                                  preexec_fn=affinity(server_cpus))
        try:
            deadline = time.monotonic() + 20
            while True:
                if server.poll() is not None:
                    raise RuntimeError(f"canned server exited with {server.returncode}")
                try:
                    counter(endpoint)
                    break
                except OSError:
                    if time.monotonic() > deadline: raise TimeoutError("canned server startup")
                    time.sleep(.1)
            with (args.output / "client.err").open("w") as client_error:
                client = subprocess.Popen(client_cmd, env=env, stdout=subprocess.PIPE,
                                          stdin=subprocess.PIPE, stderr=client_error,
                                          text=True, bufsize=1, preexec_fn=affinity(client_cpus))
                lines = queue.Queue()
                threading.Thread(target=read_stdout, args=(client.stdout, lines), daemon=True).start()
                start_ticks = end_ticks = None
                client_start = client_end = None
                server_start = server_end = None
                before_threads = after_threads = None
                output = []
                try:
                    deadline = time.monotonic() + args.seconds + 60
                    while True:
                        if time.monotonic() > deadline: raise TimeoutError("canned client")
                        try:
                            line = lines.get(timeout=.2)
                        except queue.Empty:
                            continue
                        if line is None: break
                        output.append(line)
                        if not line.lstrip().startswith("{"): continue
                        message = json.loads(line)
                        if message.get("event") == "MEASURE_START":
                            server_start = cpu_ticks(server.pid)
                            client_start = cpu_ticks(client.pid)
                            before_threads = thread_cpu_ticks(client.pid)
                            start_ticks = counter(endpoint)
                            client.stdin.write("\n"); client.stdin.flush()
                        elif message.get("event") == "MEASURE_END":
                            server_end = cpu_ticks(server.pid)
                            client_end = cpu_ticks(client.pid)
                            after_threads = thread_cpu_ticks(client.pid)
                            end_ticks = counter(endpoint)
                            client.stdin.write("\n"); client.stdin.flush()
                    client.wait(timeout=10)
                finally:
                    if client.poll() is None:
                        client.kill();client.wait()
                receipt["client_exit"] = client.returncode
                receipt["client_stderr"] = (args.output / "client.err").read_text()[-4000:]
                receipt["client_stdout_tail"] = output[-4:]
                if client.returncode != 0:
                    raise RuntimeError("canned client failed")
                if None in (server_start, server_end, client_start, client_end, start_ticks, end_ticks):
                    raise RuntimeError("missing measurement marker")
                result = json.loads(next(line for line in reversed(output) if line.lstrip().startswith("{")))
                if result.get("kind") != "canned_http" or result["requests"] != end_ticks - start_ticks:
                    raise RuntimeError("canned HTTP request accounting mismatch")
                hz = __import__("os").sysconf("SC_CLK_TCK")
                elapsed = result["elapsed_ns"] / 1e9
                receipt.update({"status": "passed", "result": result,
                                "server_requests": end_ticks - start_ticks,
                                "server_cpu_seconds": (server_end - server_start) / hz,
                                "client_cpu_seconds": (client_end - client_start) / hz,
                                "client_cpu_utilization": (client_end - client_start) / hz / elapsed / len(client_cpus),
                                "server_cpu_utilization": (server_end - server_start) / hz / elapsed / len(server_cpus),
                                "client_thread_cpu_top": thread_utilization(before_threads, after_threads, elapsed)[:16]})
        except Exception as exc:
            receipt["error"] = str(exc)
        finally:
            if server.poll() is None: server.send_signal(signal.SIGTERM)
            try: server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill();server.wait()
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": receipt["status"], "protocol": args.protocol,
                      "objects_per_s": receipt.get("result", {}).get("objects_per_s")}, sort_keys=True))
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
