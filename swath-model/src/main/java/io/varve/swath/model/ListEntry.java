/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

/**
 * A listed row (contract §1.2). Sealed so formatters/sorters switch
 * exhaustively. Emission rules:
 * <ul>
 *   <li><b>Objects</b> are always emitted.</li>
 *   <li><b>Common prefixes</b> are emitted only in non-recursive ({@code delimiter})
 *       mode; in the recursive default they are an internal seed artifact.</li>
 *   <li><b>Delete markers</b> are emitted only with {@code --all-versions}, unless
 *       {@code --no-delete-markers}.</li>
 * </ul>
 */
public sealed interface ListEntry permits ObjectEntry, CommonPrefixEntry, DeleteMarkerEntry {
    KeyBytes key();
}
