/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

/** Typed checkpoints in the replacement-publication tail. Package-private test seam. */
enum PublicationStep {
    AFTER_WORKING_SWEEP,
    AFTER_ALL_TMP_PARTS_DURABLE,
    AFTER_STALE_FINAL_SWEEP,
    AFTER_PART_RENAME,
    AFTER_OUTPUT_DIRECTORY_SYNC,
    AFTER_PUBLISH_LISTENER,
    AFTER_STAGING_COMPLETION
}
