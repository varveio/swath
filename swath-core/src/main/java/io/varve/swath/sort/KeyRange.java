/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Inclusive lower and exclusive upper raw-key bounds; either bound may be unbounded. */
record KeyRange(byte[] lo, byte[] hi) {
}
