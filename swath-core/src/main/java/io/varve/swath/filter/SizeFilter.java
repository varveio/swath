/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;

/**
 * Keeps objects whose size is in {@code [minBytes, maxBytes]} (inclusive). Rows
 * without a size (common prefixes, delete markers) always pass — size is an
 * object predicate.
 */
public record SizeFilter(long minBytes, long maxBytes) implements Filter {

    public static SizeFilter atLeast(long min) {
        return new SizeFilter(min, Long.MAX_VALUE);
    }

    public static SizeFilter atMost(long max) {
        return new SizeFilter(0, max);
    }

    @Override
    public boolean matches(ListEntry e) {
        if (e instanceof ObjectEntry o) {
            return o.size() >= minBytes && o.size() <= maxBytes;
        }
        return true;
    }

    @Override
    public int cost() {
        return 0;
    }
}
