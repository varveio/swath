/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.Locale;

/** The retained tune value for the sole finalization implementation. */
public enum SortFinalization {
    PIPELINE("pipeline");

    private final String configValue;

    SortFinalization(String configValue) {
        this.configValue = configValue;
    }

    public String configValue() {
        return configValue;
    }

    public static SortFinalization fromConfigValue(String key, String raw) {
        String normalized = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
        for (SortFinalization value : values()) {
            if (value.configValue.equals(normalized)) {
                return value;
            }
        }
        throw new IllegalArgumentException(key + ": expected pipeline, got '" + raw + "'");
    }
}
