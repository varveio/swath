/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.Locale;

/** Policy for choosing raw-key split points for the parallel final merge. */
public enum MergeBoundaryPolicy {
    /** Existing evenly spaced selection over the bounded distinct-key candidate set. */
    DISTINCT("distinct"),
    /** Entry-mass quantiles derived from validated type-2 page indexes. */
    ROWS("rows");

    private final String configValue;

    MergeBoundaryPolicy(String configValue) {
        this.configValue = configValue;
    }

    public String configValue() {
        return configValue;
    }

    public static MergeBoundaryPolicy fromConfigValue(String property, String raw) {
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        for (MergeBoundaryPolicy policy : values()) {
            if (policy.configValue.equals(normalized)) {
                return policy;
            }
        }
        throw new IllegalArgumentException(property + " must be distinct or rows, got " + raw);
    }
}
