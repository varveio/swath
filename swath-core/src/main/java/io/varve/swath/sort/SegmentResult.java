/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.nio.file.Path;
import java.util.Map;

/**
 * The outcome of flushing one sorted staging segment — the clean seam the checkpoint layer consumes
 * to record a {@code part_file} row and advance each node's {@code durable_cursor}.
 *
 * @param path           the finalized (footer-fsynced) segment file
 * @param rows           rows written to the segment
 * @param bytes          on-disk segment size in bytes
 * @param perNodeMaxKeys the max key each node contributed to this segment (accumulated at ingest)
 *                       — {@code durable_cursor} for a node advances to its value here
 */
public record SegmentResult(Path path, long rows, long bytes, Map<Long, byte[]> perNodeMaxKeys) {
}
