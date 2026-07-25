/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

/** A common prefix (folder) row; {@code key} is the prefix. Emitted only in delimiter mode. */
public record CommonPrefixEntry(KeyBytes key) implements ListEntry {
}
