/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.sort.FinalPartMetadata;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A complete unpublished sorted replacement set. Every temporary part is durably closed, dense in
 * list order, cardinality-checked, and strictly adjacent under raw unsigned key order.
 *
 * <p>This value deliberately contains no consumer-visible path, manifest, success marker, run
 * state, symlink, or checkpoint phase. Publication code assigns final names only after receiving
 * the complete value.
 */
public final class PreparedSortedParts {
    private final List<Part> parts;
    private final long sourceRows;
    private final long outputRows;
    private final MergeStatistics mergeStatistics;
    private final CleanupToken cleanupToken;

    PreparedSortedParts(List<Part> parts, long sourceRows, long outputRows,
            MergeStatistics mergeStatistics, CleanupToken cleanupToken) {
        this.parts = List.copyOf(parts);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("prepared sorted parts must not be empty");
        }
        if (sourceRows < 0 || outputRows < 0) {
            throw new IllegalArgumentException("prepared sorted cardinalities must be non-negative");
        }
        this.sourceRows = sourceRows;
        this.outputRows = outputRows;
        this.mergeStatistics = Objects.requireNonNull(mergeStatistics, "mergeStatistics");
        this.cleanupToken = Objects.requireNonNull(cleanupToken, "cleanupToken");
    }

    public List<Part> parts() {
        return parts;
    }

    public long sourceRows() {
        return sourceRows;
    }

    public long outputRows() {
        return outputRows;
    }

    public MergeStatistics mergeStatistics() {
        return mergeStatistics;
    }

    public CleanupToken cleanupToken() {
        return cleanupToken;
    }

    /** Exact durable bytes across the ordered unpublished part set. */
    public long outputBytes() {
        long bytes = 0;
        for (Part part : parts) {
            bytes = Math.addExact(bytes, part.bytes());
        }
        return bytes;
    }

    /** One unpublished durable part and the exact facts used by the pre-publication proof. */
    public record Part(
            Path temporaryPath,
            long rows,
            long bytes,
            byte[] rawMinKey,
            byte[] rawMaxKey,
            Optional<FinalPartMetadata> publicationMetadata) {

        public Part {
            Objects.requireNonNull(temporaryPath, "temporaryPath");
            Objects.requireNonNull(publicationMetadata, "publicationMetadata");
            if (rows < 0 || bytes < 0) {
                throw new IllegalArgumentException("prepared part counts must be non-negative");
            }
            if ((rawMinKey == null) != (rawMaxKey == null)) {
                throw new IllegalArgumentException(
                        "prepared part raw bounds must both be absent or present");
            }
            if ((rows == 0) != (rawMinKey == null)) {
                throw new IllegalArgumentException(
                        "prepared empty parts have no bounds and non-empty parts require bounds");
            }
            rawMinKey = rawMinKey == null ? null : rawMinKey.clone();
            rawMaxKey = rawMaxKey == null ? null : rawMaxKey.clone();
        }

        @Override
        public byte[] rawMinKey() {
            return rawMinKey == null ? null : rawMinKey.clone();
        }

        @Override
        public byte[] rawMaxKey() {
            return rawMaxKey == null ? null : rawMaxKey.clone();
        }
    }

    /** Algorithm counters retained through publication without carrying publication policy. */
    public record MergeStatistics(
            long mergePasses,
            long cascadedPasses,
            long pagesForwarded,
            int finalizationParallelism) {

        public MergeStatistics {
            if (mergePasses < 0 || cascadedPasses < 0 || pagesForwarded < 0
                    || finalizationParallelism < 1) {
                throw new IllegalArgumentException("prepared merge statistics are out of range");
            }
        }
    }

    /** Opaque authority to remove only disposable intermediates after publication commits. */
    public record CleanupToken(List<Path> disposableIntermediates) {
        public CleanupToken {
            disposableIntermediates = List.copyOf(disposableIntermediates);
        }
    }
}
