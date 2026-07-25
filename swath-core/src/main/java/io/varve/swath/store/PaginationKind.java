/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store;

/**
 * How a store paginates. {@code KEY}: pagination is purely {@code start_after =
 * last key} (S3 OBJECTS). {@code OPAQUE_MARKER}: an opaque continuation token
 * (some stores). S3 OBJECTS uses {@code KEY}.
 */
public enum PaginationKind {
    KEY,
    OPAQUE_MARKER
}
