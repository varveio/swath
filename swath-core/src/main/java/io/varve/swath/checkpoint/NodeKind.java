/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

/** Worklist node kind. RANGE is the only kind the S3 work-stealing engine drives; PREFIX/INVENTORY_FILE are not yet implemented. */
public enum NodeKind {
    RANGE,
    PREFIX,
    INVENTORY_FILE
}
