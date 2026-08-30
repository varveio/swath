/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.sort.StagingReconciliation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
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
     * Resume sweep for the {@code --sort} staging dir: retain checkpoint-finalized, bare {@code
     * .pageseg} names and delete every other immediate {@code .pageseg} or legacy {@code .parquet}
     * entry. The sorter owns those two staging extensions; unrelated immediate entries are left
     * untouched. Before deleting anything, the method refuses unsafe or duplicate retained names,
     * a missing directory with retained names, a symbolic-link/non-directory staging path, or a
     * retained segment that is missing or not an ordinary file. A deleted leftover is either a
     * segment whose {@code partFinalized} commit never landed or a stale cascade intermediate; both
     * are re-derived. Finalized segments are never touched.
     */
    public static void discardNonFinalizedSegments(Path stagingDir, Set<String> finalizedSegmentNames)
            throws IOException {
        StagingReconciliation.fromNames(finalizedSegmentNames);
        if (!Files.exists(stagingDir, LinkOption.NOFOLLOW_LINKS)) {
            if (finalizedSegmentNames.isEmpty()) {
                return;
            }
            throw new IOException("sort staging directory is missing: " + stagingDir);
        }
        if (!Files.isDirectory(stagingDir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("sort staging path is not an ordinary directory: " + stagingDir);
        }
        for (String finalized : finalizedSegmentNames) {
            Path segment = stagingDir.resolve(finalized);
            if (!Files.isRegularFile(segment, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("sort staging segment is missing or not an ordinary file: "
                        + segment);
            }
        }
        try (Stream<Path> entries = Files.list(stagingDir)) {
            List<Path> stale = entries.filter(path -> {
                String name = path.getFileName().toString();
                return (name.endsWith(".pageseg") || name.endsWith(".parquet"))
                        && !finalizedSegmentNames.contains(name);
            }).toList();
            for (Path path : stale) {
                Files.deleteIfExists(path);
            }
        }
    }

    /**
     * Delete every {@code part-*.parquet} under {@code <dir>/data/} (parts are nested in
     * the pure-parquet {@code data/} subdir) whose canonical {@code data/}-prefixed relative path is
     * not in {@code finalizedFileNames}. {@code dir} is the dataset root; {@code finalizedFileNames}
     * carry the same {@code data/}-prefixed form the checkpoint {@code part_file.path} stores, so the
     * comparison agrees across all three references (manifest key / checkpoint path / this sweep).
     * Before deleting anything, the method refuses a finalized key outside that exact two-component
     * namespace or a checkpoint-finalized part that is missing, symbolic, or not an ordinary file.
     */
    public static void discardNonFinalized(Path dir, Set<String> finalizedFileNames) throws IOException {
        DatasetLayout layout = DatasetLayout.of(dir);
        Path dataDir = layout.dataDir();
        if (Files.exists(dataDir, LinkOption.NOFOLLOW_LINKS)
                && !Files.isDirectory(dataDir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Parquet data path is not an ordinary directory: " + dataDir);
        }
        for (String finalized : finalizedFileNames) {
            Path part = requireSafeFinalizedPart(layout, finalized);
            if (!Files.isRegularFile(part, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("checkpoint-finalized Parquet part is missing or not an ordinary file: "
                        + part);
            }
        }
        if (!Files.isDirectory(dataDir, LinkOption.NOFOLLOW_LINKS)) {
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

    private static Path requireSafeFinalizedPart(DatasetLayout layout, String key) throws IOException {
        final Path parsed;
        try {
            parsed = Path.of(key);
        } catch (InvalidPathException | NullPointerException e) {
            throw new IOException("invalid checkpoint-finalized Parquet part path: " + key, e);
        }
        if (parsed.isAbsolute() || parsed.getNameCount() != 2
                || !Manifest.DATA_DIR.equals(parsed.getName(0).toString())
                || !key.equals(DatasetLayout.key(parsed.getFileName().toString()))) {
            throw new IOException("unsafe checkpoint-finalized Parquet part path: " + key);
        }
        return layout.resolveKey(key);
    }
}
