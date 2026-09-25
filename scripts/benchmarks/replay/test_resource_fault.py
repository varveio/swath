#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Pure sizing and configuration checks for the opt-in resource driver."""

import unittest
import socket
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory

from resource_fault import (adequate_budget, diagnostic_classpath_provenance,
                            diagnostic_connections,
                            duration_seconds, parse_server_opts,
                            peak_capacity_bound, prove_slow_held, prove_slow_rendered,
                            prove_trickle, require_declared_chunk_size,
                            require_effective_vm_flags, require_oracle_request_accounting,
                            receive_trickle_chunk, sha256)


class ResourceFaultTest(unittest.TestCase):
    def test_initial_and_growth_bound_include_old_and_new_capacity(self):
        self.assertEqual(peak_capacity_bound(1_000_000, 320_512), 2_500_000)
        self.assertEqual(adequate_budget(512, 1_000_000, 320_512),
                         1_280_000_000)
        self.assertEqual(peak_capacity_bound(100, 320_512), 320_512)
        self.assertEqual(peak_capacity_bound(1_000_000, 320_512, 262_144), 1_262_144)
        with self.assertRaises(ValueError):
            peak_capacity_bound(100, 0)

    def test_growth_bound_covers_every_one_byte_growth_point(self):
        for initial in (1, 4, 4096, 320_512):
            for body in (initial, initial + 1, 100_000, 1_500_000):
                if body < initial:
                    continue
                capacity = initial
                peak = initial
                while capacity < body:
                    old = capacity
                    capacity = max(old + 1, old + (old >> 1))
                    peak = max(peak, old + capacity)
                self.assertLessEqual(peak, peak_capacity_bound(body, initial))

    def test_explicit_jvm_bounds_and_write_deadline_are_required(self):
        flags = parse_server_opts("-Xms4g -Xmx4g -XX:MaxDirectMemorySize=512m "
                                  "-Djdk.nio.maxCachedBufferSize=262144 "
                                  "-XX:NativeMemoryTracking=summary")
        self.assertEqual(flags["max_direct_memory"], "512m")
        with self.assertRaises(ValueError):
            parse_server_opts("-Xmx4g -Djdk.nio.maxCachedBufferSize=262144")
        with self.assertRaises(ValueError):
            parse_server_opts("-Xmx4g -Xmx2g -XX:MaxDirectMemorySize=512m "
                              "-Djdk.nio.maxCachedBufferSize=262144 "
                              "-XX:NativeMemoryTracking=summary")
        self.assertEqual(duration_seconds("250ms"), .25)
        self.assertEqual(duration_seconds("2s"), 2)

    def test_effective_jvm_bounds_must_match_declaration(self):
        declared = parse_server_opts("-Xmx4g -XX:MaxDirectMemorySize=512m "
                                     "-Djdk.nio.maxCachedBufferSize=262144 "
                                     "-XX:NativeMemoryTracking=summary")
        flags = {"exit": 0, "stdout": "-XX:MaxHeapSize=4294967296 "
                 "-XX:MaxDirectMemorySize=536870912"}
        properties = {"exit": 0, "stdout": "jdk.nio.maxCachedBufferSize=262144\n"}
        require_effective_vm_flags(flags, properties, declared)
        with self.assertRaises(RuntimeError):
            require_effective_vm_flags(flags | {"stdout": "-XX:MaxHeapSize=2147483648"},
                                       properties, declared)
        with self.assertRaises(RuntimeError):
            require_effective_vm_flags(flags, {"exit": 0, "stdout": ""}, declared)

    def test_chunked_arms_must_match_reported_server_allocator(self):
        require_declared_chunk_size({"output_chunk_bytes": 262_144}, 262_144)
        require_declared_chunk_size({}, None)
        for serving, declared in (({"output_chunk_bytes": 262_144}, None),
                                  ({"output_chunk_bytes": 262_144}, 131_072),
                                  ({}, 262_144)):
            with self.subTest(serving=serving, declared=declared), self.assertRaises(RuntimeError):
                require_declared_chunk_size(serving, declared)

    def test_diagnostic_classpath_binds_exact_class_and_main_jar(self):
        with TemporaryDirectory(prefix="resource-classpath-") as temporary:
            root = Path(temporary)
            class_file = root / "classes/io/varve/swath/replay/server/ResourceFaultServer.class"
            class_file.parent.mkdir(parents=True)
            class_file.write_bytes(b"diagnostic class")
            lib = root / "lib"
            lib.mkdir()
            jar = lib / "swath-replay-1.jar"
            jar.write_bytes(b"pinned replay jar")
            classpath = f"{root / 'classes'}:{lib / '*'}"
            proof = diagnostic_classpath_provenance(classpath, sha256(jar))
            self.assertEqual(proof["class_sha256"], sha256(class_file))
            with self.assertRaises(ValueError):
                diagnostic_classpath_provenance(classpath, "wrong")
            shadow = lib / "swath-replay-shadow.jar"
            shadow.write_bytes(b"wrong shadow jar")
            with self.assertRaises(ValueError):
                diagnostic_classpath_provenance(classpath, sha256(jar))
            shadow.unlink()
            shadow = lib / "unrelated.jar"
            with zipfile.ZipFile(shadow, "w") as archive:
                archive.writestr("io/varve/swath/replay/server/ResourceFaultServer.class",
                                 b"shadow launcher")
            with self.assertRaises(ValueError):
                diagnostic_classpath_provenance(classpath, sha256(jar))

    def test_diagnostic_connector_requires_effective_kernel_send_buffer(self):
        names = {"connections.accepted": 2, "connections.active": 1,
                 "connections.peak": 2, "sndbuf.min.bytes": 131_072,
                 "sndbuf.max.bytes": 131_072, "sndbuf.samples": 2,
                 "sndbuf.errors": 0}
        snapshot = {"meters": [{"name": "swath.replay.diagnostic." + key,
                                 "value": value, "tags": {}}
                                for key, value in names.items()]}
        self.assertEqual(diagnostic_connections(snapshot)["accepted"], 2)
        with self.assertRaises(RuntimeError):
            diagnostic_connections({"meters": []})

    def test_oracle_request_accounting_rejects_unexplained_surplus(self):
        driver = {"attempted_requests": 10, "warmup_attempted_requests": 3,
                  "warmup_successful_requests": 3}
        self.assertEqual(require_oracle_request_accounting(driver, 13), 3)
        with self.assertRaises(RuntimeError):
            require_oracle_request_accounting(driver, 14)
        with self.assertRaises(RuntimeError):
            require_oracle_request_accounting(driver | {"warmup_successful_requests": 2}, 13)
        with self.assertRaises(RuntimeError):
            require_oracle_request_accounting({"attempted_requests": 10}, 10)

    def test_trickle_requires_held_body_progress_timeout_and_actual_close(self):
        valid = {"body_started": True, "held_after_body_start": True,
                 "callback_released": True, "deadline_counter_engaged": True,
                 "eof_or_reset": True, "progressed_after_idle": True,
                 "received_bytes": 2000, "content_length": 3000, "elapsed_s": 5.1}
        prove_trickle(valid, 5, 1, .5)
        for change in ({"body_started": False}, {"held_after_body_start": False},
                       {"callback_released": False}, {"deadline_counter_engaged": False},
                       {"eof_or_reset": False}, {"progressed_after_idle": False},
                       {"received_bytes": 3000}, {"elapsed_s": 1.0}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                prove_trickle(valid | change, 5, 1, .5)

    def test_trickle_read_distinguishes_timeout_progress_and_eof(self):
        class FakeConnection:
            def __init__(self):
                self.results = [socket.timeout(), b"progress", b""]

            def recv(self, limit):
                result = self.results.pop(0)
                if isinstance(result, Exception):
                    raise result
                return result

        connection = FakeConnection()
        self.assertEqual(receive_trickle_chunk(connection, 1024), (None, False))
        self.assertEqual(receive_trickle_chunk(connection, 1024), (b"progress", False))
        self.assertEqual(receive_trickle_chunk(connection, 1024), (b"", True))

    def test_slow_hold_must_survive_injection_as_charged_response_bytes(self):
        prove_slow_held({"serving": {"active_responses": 4,
                                     "charged_response_bytes": 12_000}}, 4, 3000)
        with self.assertRaises(RuntimeError):
            prove_slow_held({"serving": {"active_responses": 4,
                                         "charged_response_bytes": 0}}, 4, 3000)

    def test_slow_hold_requires_all_rendered_delays_without_overrun_or_write(self):
        before = {"meters": []}
        stages = [{"name": "swath.replay.request.stage.latency", "count": 4,
                   "tags": {"protocol": "s3", "stage": stage}}
                  for stage in ("page", "render", "delay")]
        held = {"meters": stages}
        prove_slow_rendered(before, held, 4, "s3")
        for change in (stages[:2],
                       stages + [{"name": "swath.replay.request.stage.latency", "count": 1,
                                  "tags": {"protocol": "s3", "stage": "write"}}],
                       stages + [{"name": "swath.replay.inject.overrun", "count": 1,
                                  "tags": {"shape": "page"}}]):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                prove_slow_rendered(before, {"meters": change}, 4, "s3")


if __name__ == "__main__":
    unittest.main()
