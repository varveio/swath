/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

/** The single lowercase-hex digit table shared by this package's escapers. */
final class Hex {

    private static final char[] LOWER = "0123456789abcdef".toCharArray();

    private Hex() {
    }

    /** The lowercase hex digit for the low nibble of {@code v}. */
    static char digit(int v) {
        return LOWER[v & 0xF];
    }
}
