/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Why a sort buffer was sealed for flush (drives {@code SORT.buffer_byte_gated}). */
enum SealTrigger {
    /** The estimated pre-encode byte gate ({@code segment-bytes}) fired — the key-size-independent path. */
    BYTE_GATE,
    /** The secondary entry-count cap ({@code segment-entries}) fired. */
    ENTRY_CAP,
    /** Neither gate fired — a partial buffer sealed at end-of-listing (drain). */
    DRAIN
}
