/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/** Constructor-only adversarial page shaping. Production handlers use {@link #NONE}. */
public record PaginationTestProfile(int shortPageSize, boolean emptyFirstPage) {
    public static final PaginationTestProfile NONE = new PaginationTestProfile(0, false);

    public PaginationTestProfile {
        if (shortPageSize < 0) {
            throw new IllegalArgumentException("short page size must be nonnegative");
        }
    }

    public int effectivePageSize(int requested) {
        return shortPageSize == 0 ? requested : Math.min(requested, shortPageSize);
    }

    public String bindingName() {
        return "short=" + shortPageSize + ";empty-first=" + emptyFirstPage;
    }
}
