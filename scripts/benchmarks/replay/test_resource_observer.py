#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Pure checks for the dedicated mixed/staggered replay resource observer."""

import unittest
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
from unittest.mock import patch

from resource_observer import (charged_peak_for_cap, check_bounds, nmt_committed_bytes,
                               observe, read_sample)


class ResourceObserverTest(unittest.TestCase):
    def test_nmt_total_is_read_in_bytes(self):
        self.assertEqual(nmt_committed_bytes("Total: reserved=4GB, committed=2274755KB"),
                         2274755 * 1024)
        with self.assertRaises(ValueError):
            nmt_committed_bytes("Native Memory Tracking disabled")

    def test_every_declared_cap_is_enforced(self):
        check_bounds({"fd_count": 9, "thread_count": 10},
                     {"fd_count": 9, "thread_count": 11})
        with self.assertRaisesRegex(RuntimeError, "fd_count"):
            check_bounds({"fd_count": 10, "thread_count": 10},
                         {"fd_count": 9, "thread_count": 11})
        self.assertEqual(charged_peak_for_cap([{"charged_response_bytes": 10,
                                                "server_peak_charged_response_bytes": 200}]), 200)
        with self.assertRaisesRegex(RuntimeError, "charged_response_bytes"):
            check_bounds({"charged_response_bytes": charged_peak_for_cap([
                {"charged_response_bytes": 10,
                 "server_peak_charged_response_bytes": 200}])},
                {"charged_response_bytes": 100})

    def test_missing_or_null_heap_direct_and_cache_gauges_fail_loudly(self):
        snapshot = {"schema_version": 2,
                    "serving": {"charged_response_bytes": 1,
                                "peak_charged_response_bytes": 2,
                                "active_responses": 1},
                    "meters": [
                        {"name": "jvm.memory.used", "value": 10,
                         "tags": {"area": "heap", "id": "one"}},
                        {"name": "jvm.memory.used", "value": 20,
                         "tags": {"area": "heap", "id": "two"}},
                        {"name": "jvm.buffer.memory.used", "value": 0,
                         "tags": {"id": "direct"}},
                        {"name": "swath.replay.prefetch.rows.live", "value": 0, "tags": {}},
                        {"name": "swath.replay.prefetch.windows.live", "value": 0, "tags": {}}]}
        with (patch("resource_observer.http_json", return_value=snapshot),
              patch("resource_observer.rss", return_value=100),
              patch("resource_observer.count_entries", return_value=1)):
            self.assertEqual(read_sample(1, "unused")["heap_bytes"], 30)
            self.assertEqual(read_sample(1, "unused")["cached_rows"], 0)
            for name in ("jvm.memory.used", "jvm.buffer.memory.used",
                         "swath.replay.prefetch.rows.live",
                         "swath.replay.prefetch.windows.live"):
                missing = snapshot | {"meters": [meter for meter in snapshot["meters"]
                                                 if meter["name"] != name]}
                with (self.subTest(name=name, case="missing"),
                      patch("resource_observer.http_json", return_value=missing),
                      self.assertRaisesRegex(RuntimeError, "required resource gauge")):
                    read_sample(1, "unused")
                null = snapshot | {"meters": [meter | {"value": None}
                                              if meter["name"] == name else meter
                                              for meter in snapshot["meters"]]}
                with (self.subTest(name=name, case="null"),
                      patch("resource_observer.http_json", return_value=null),
                      self.assertRaisesRegex(RuntimeError, "required resource gauge")):
                    read_sample(1, "unused")

    def test_ready_waits_for_first_nmt_and_sample_then_writes_receipt(self):
        stopped = threading.Event()
        order = []
        sample = {"rss_bytes": 1000, "fd_count": 3, "thread_count": 5,
                  "heap_bytes": 500, "direct_bytes": 20,
                  "charged_response_bytes": 10, "active_responses": 1,
                  "server_peak_charged_response_bytes": 10,
                  "cached_rows": 2, "cached_windows": 1}
        calls = 0
        def fake_sample(_pid, _url):
            nonlocal calls
            order.append("sample")
            calls += 1
            if calls == 2:
                stopped.set()
            return sample | {"at_ns": calls}
        nmt = {"exit": 0, "stdout": "Total: reserved=2KB, committed=1KB",
               "command": ["jcmd"]}
        def fake_nmt(*_args, **_kwargs):
            order.append("nmt")
            return nmt
        class ReadyCapture(io.StringIO):
            def write(self, value):
                if '"READY"' in value:
                    order.append("READY")
                return super().write(value)
        with TemporaryDirectory(prefix="resource-observer-") as temporary:
            output = Path(temporary) / "observer.json"
            args = SimpleNamespace(output=str(output), pid=1, metrics_url="http://unused",
                                   java_home="/jdk", sample_ms=10, nmt_interval_s=5,
                                   rss_cap=2000, fd_cap=10, thread_cap=10, heap_cap=1000,
                                   direct_cap=100, charged_cap=100, active_cap=10,
                                   cache_rows_cap=10, cache_windows_cap=10,
                                   nmt_committed_cap=2000)
            stdout = ReadyCapture()
            with (patch("resource_observer.jcmd", side_effect=fake_nmt),
                  patch("resource_observer.read_sample", side_effect=fake_sample),
                  redirect_stdout(stdout)):
                receipt = observe(args, stopped)
            self.assertEqual(order[:3], ["nmt", "sample", "READY"])
            self.assertEqual(json.loads(stdout.getvalue().strip()),
                             {"event": "READY", "pid": 1})
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["peaks"]["nmt_committed_bytes"], 1024)
            self.assertEqual(json.loads(output.read_text())["status"], "passed")


if __name__ == "__main__":
    unittest.main()
