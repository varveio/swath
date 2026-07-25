/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Resume-time output-dir reconciliation for the Parquet sink (algorithms.md §4.5,
 * I6). On resume, every {@code part-*.parquet} on disk that the checkpoint does
 * <b>not</b> record as finalized is discarded — it is either a part that never got
 * its footer (a hard-crash leftover) or a footered part whose {@code partFinalized}
 * commit didn't land (so its rows aren't durable and will be re-listed). Finalized
 * parts are never touched.
 */
public final class ParquetResume {

    private ParquetResume() {
    }

    /**
     * Resume sweep for the {@code --sort} staging dir: delete every staging segment /
     * cascade intermediate in {@code stagingDir} not recorded as a finalized segment. The sorter owns
     * the staging dir exclusively (it only ever creates content there), so matching its two staging
     * extensions is safe — {@code .pageseg} (the page-run format new runs write) and {@code
     * .parquet} (legacy columnar staging + the SORT-RESUME tests' planted debris). A leftover is either
     * a segment whose {@code partFinalized} commit never landed (non-durable) or a stale cascade
     * intermediate from a crashed merge; either way it is re-derived. Finalized segments are never touched.
     */
    public static void discardNonFinalizedSegments(Path stagingDir, Set<String> finalizedSegmentNames)
            throws IOException {
        if (!Files.isDirectory(stagingDir)) {
            return;
        }
        try (Stream<Path> entries = Files.list(stagingDir)) {
            List<Path> stale = entries.filter(p -> {
                String name = p.getFileName().toString();
                return (name.endsWith(".pageseg") || name.endsWith(".parquet"))
                        && !finalizedSegmentNames.contains(name);
            }).toList();
            for (Path p : stale) {
                Files.deleteIfExists(p);
            }
        }
    }

    /**
     * Delete every {@code part-*.parquet} under {@code <dir>/data/} (parts are nested in
     * the pure-parquet {@code data/} subdir) whose canonical {@code data/}-prefixed relative path is
     * not in {@code finalizedFileNames}. {@code dir} is the dataset root; {@code finalizedFileNames}
     * carry the same {@code data/}-prefixed form the checkpoint {@code part_file.path} stores, so the
     * comparison agrees across all three references (manifest key / checkpoint path / this sweep).
     */
    public static void discardNonFinalized(Path dir, Set<String> finalizedFileNames) throws IOException {
        Path dataDir = DatasetLayout.of(dir).dataDir();
        if (!Files.isDirectory(dataDir)) {
            return;
        }
        try (Stream<Path> entries = Files.list(dataDir)) {
            List<Path> stale = entries.filter(p -> {
                String name = p.getFileName().toString();
                String rel = DatasetLayout.key(name);
                return name.startsWith("part-") && name.endsWith(".parquet")
                        && !finalizedFileNames.contains(rel);
            }).toList();
            for (Path p : stale) {
                Files.deleteIfExists(p);
            }
        }
    }
}
