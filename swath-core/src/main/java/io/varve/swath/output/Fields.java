/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

/** Shared field rendering for the text formatters. */
final class Fields {

    private Fields() {
    }

    static String orEmpty(String s) {
        return s == null ? "" : s;
    }
}
