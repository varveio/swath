#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Baseline: os.walk + os.lstat on every non-directory name; prints the count."""
import os
import sys

n = 0
for d, _, files in os.walk(sys.argv[1]):
    for f in files:
        os.lstat(os.path.join(d, f))
        n += 1
print(n)
