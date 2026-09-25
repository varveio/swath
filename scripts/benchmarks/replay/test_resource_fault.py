#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Pure sizing and configuration checks for the opt-in resource driver."""

import unittest

from resource_fault import (adequate_budget, duration_seconds, parse_server_opts,
                            peak_capacity_bound, s3_initial_capacity)


class ResourceFaultTest(unittest.TestCase):
    def test_initial_and_growth_bound_include_old_and_new_capacity(self):
        self.assertEqual(s3_initial_capacity(1000, 64 * 1024 * 1024), 320_512)
        self.assertEqual(peak_capacity_bound(1_000_000, 1000, 64 * 1024 * 1024), 2_500_000)
        self.assertEqual(adequate_budget(512, 1_000_000, 1000, 64 * 1024 * 1024),
                         1_280_000_000)
        self.assertEqual(peak_capacity_bound(100, 1000, 64 * 1024 * 1024), 320_512)
        with self.assertRaises(ValueError):
            peak_capacity_bound(64 * 1024 * 1024 + 1, 1000, 64 * 1024 * 1024)

    def test_explicit_jvm_bounds_and_write_deadline_are_required(self):
        flags = parse_server_opts("-Xms4g -Xmx4g -XX:MaxDirectMemorySize=512m "
                                  "-Djdk.nio.maxCachedBufferSize=262144")
        self.assertEqual(flags["max_direct_memory"], "512m")
        with self.assertRaises(ValueError):
            parse_server_opts("-Xmx4g -Djdk.nio.maxCachedBufferSize=262144")
        self.assertEqual(duration_seconds("250ms"), .25)
        self.assertEqual(duration_seconds("2s"), 2)


if __name__ == "__main__":
    unittest.main()
