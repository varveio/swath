/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Policy for adjacent rows with the same raw key in the final sorted output. */
public enum EqualKeyPolicy {
    /** Preserve every row, including version or cross-row-type groups that share a raw key. */
    ALLOW,
    /** Reject the output before writing the second row of an equal raw-key pair. */
    REJECT
}
