/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

/** The {@code row_type} discriminator (contract §4). */
public enum RowType {
    OBJECT,
    COMMON_PREFIX,
    DELETE_MARKER;

    public static RowType of(ListEntry e) {
        return switch (e) {
            case ObjectEntry ignored -> OBJECT;
            case CommonPrefixEntry ignored -> COMMON_PREFIX;
            case DeleteMarkerEntry ignored -> DELETE_MARKER;
        };
    }
}
