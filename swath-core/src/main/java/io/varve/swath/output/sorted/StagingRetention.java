/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

/** Post-publish ownership policy for original listing page-run staging segments. */
public enum StagingRetention {
    DELETE_AFTER_PUBLISH("off"),
    RETAIN_ORIGINALS("on");

    private final String tuneValue;

    StagingRetention(String tuneValue) {
        this.tuneValue = tuneValue;
    }

    public boolean retainsOriginals() {
        return this == RETAIN_ORIGINALS;
    }

    public String tuneValue() {
        return tuneValue;
    }

    public static StagingRetention fromEnabled(boolean enabled) {
        return enabled ? RETAIN_ORIGINALS : DELETE_AFTER_PUBLISH;
    }

    static StagingRetention fromProperty(String property, String value) {
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "on" -> RETAIN_ORIGINALS;
            case "off" -> DELETE_AFTER_PUBLISH;
            default -> throw new IllegalArgumentException(
                    property + " must be on or off, got " + value);
        };
    }

}
