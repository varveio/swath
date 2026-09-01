/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import com.sun.management.OperatingSystemMXBean;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

/** Shared deterministic corpus and process helpers for sort measurement harnesses. */
public final class SortBenchCorpus {

    private static final String KEY_PREFIX = "corp-data-lake-logs";
    private static final String[] STORAGE_CLASSES =
            {"STANDARD", "STANDARD_IA", "INTELLIGENT_TIERING", "GLACIER"};

    private SortBenchCorpus() {
    }

    /** Summary of a generated page-run corpus. */
    public record Stats(int segments, long rows, long bytes) {
    }

    /**
     * Lazily generates one segment's block-interleaved, strictly key-ordered share of a corpus.
     * The global row-index space is split into {@code blockRows}-sized blocks and assigned round-robin
     * to segments; each segment emits its assigned blocks in increasing order. The generated keys are
     * therefore unique and increasing within a segment, while segments interleave across the full
     * keyspace at block granularity.
     */
    public static SortedEntryCursor generatedCursor(int segment, int numSegments, int blockRows, long totalRows,
                                               long rowsPerDay, LocalDate base) {
        if (numSegments <= 0) {
            throw new IllegalArgumentException("numSegments must be positive, got " + numSegments);
        }
        if (segment < 0 || segment >= numSegments) {
            throw new IllegalArgumentException("segment must be in [0, " + numSegments + "), got " + segment);
        }
        if (blockRows <= 0) {
            throw new IllegalArgumentException("blockRows must be positive, got " + blockRows);
        }
        return new GeneratedCursor(segment, numSegments, blockRows, totalRows, rowsPerDay, base);
    }

    /**
     * Materialize a benchmark arm by hard-linking an already-snapshotted catalog.
     *
     * <p>A physical copy can warm the source files and turn a cold-storage measurement into a
     * filesystem-copy benchmark. Refuse it rather than quietly changing the measurement arm.
     */
    public static List<Path> hardLinkCorpus(List<Path> files, Path target) throws IOException {
        List<Path> out = new ArrayList<>();
        for (Path file : files) {
            Path destination = target.resolve(file.getFileName().toString());
            if (!Files.getFileStore(file).equals(Files.getFileStore(target))) {
                throw new IOException("benchmark arm requires same-filesystem hard links; refusing a physical"
                        + " copy from " + file + " to " + target);
            }
            Files.createLink(destination, file);
            out.add(destination);
        }
        return out;
    }

    /** List page-run inputs in deterministic filename order without copying or opening them. */
    public static List<Path> pageRunSegments(Path directory) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(directory, "*.pageseg")) {
            ds.forEach(files::add);
        }
        files.sort(Comparator.comparing(path -> path.getFileName().toString()));
        return List.copyOf(files);
    }

    /** Process CPU time in nanoseconds, or {@code -1} when the platform cannot provide it. */
    public static long processCpuTimeNanos() {
        var os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof OperatingSystemMXBean sun) {
            long cpu = sun.getProcessCpuTime();
            return cpu >= 0 ? cpu : -1;
        }
        return -1;
    }

    /** Best-effort removal of a measurement temporary tree. */
    public static void deleteTree(Path root) {
        try {
            if (!Files.exists(root)) {
                return;
            }
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
        } catch (IOException ignored) {
            // Best-effort cleanup of a measurement temporary tree.
        }
    }

    private static final class GeneratedCursor implements SortedEntryCursor {
        private final int segment;
        private final int numSegments;
        private final int blockRows;
        private final long totalRows;
        private final long rowsPerDay;
        private final LocalDate base;

        private long currentBlock;
        private long currentI;
        private long blockEnd;

        private GeneratedCursor(int segment, int numSegments, int blockRows, long totalRows, long rowsPerDay,
                                LocalDate base) {
            this.segment = segment;
            this.numSegments = numSegments;
            this.blockRows = blockRows;
            this.totalRows = totalRows;
            this.rowsPerDay = rowsPerDay;
            this.base = base;
            currentBlock = segment - (long) numSegments;
        }

        private boolean advanceIfNeeded() {
            while (currentI >= blockEnd) {
                currentBlock += numSegments;
                long start = currentBlock * (long) blockRows;
                if (start >= totalRows) {
                    return false;
                }
                currentI = start;
                blockEnd = Math.min(start + blockRows, totalRows);
            }
            return true;
        }

        @Override
        public boolean hasNext() {
            return advanceIfNeeded();
        }

        @Override
        public ListEntry next() {
            if (!advanceIfNeeded()) {
                throw new NoSuchElementException();
            }
            ListEntry entry = entry(currentI, rowsPerDay, base);
            currentI++;
            return entry;
        }

        @Override
        public void close() {
            // In-memory generator — nothing to release.
        }
    }

    private static ObjectEntry entry(long index, long rowsPerDay, LocalDate base) {
        String key = key(index, rowsPerDay, base);
        long size = 1 + Math.floorMod(mix(index * 31 + 7), 5_000_000L);
        long day = index / rowsPerDay;
        long within = index % rowsPerDay;
        long lastModified = day * 86_400_000_000L + within * 137L;
        String etag = String.format("%016x%016x", mix(index + 999), mix(index + 7_777));
        String storageClass = STORAGE_CLASSES[(int) (index % STORAGE_CLASSES.length)];
        return new ObjectEntry(KeyBytes.ofUtf8(key), size, lastModified, etag, storageClass,
                null, false, null, null, null, null);
    }

    private static String key(long index, long rowsPerDay, LocalDate base) {
        long day = index / rowsPerDay;
        long within = index % rowsPerDay;
        LocalDate date = base.plusDays(day);
        long h1 = mix(index);
        long h2 = mix(index ^ 0x9E3779B97F4A7C15L);
        return String.format("%s/%04d/%02d/%02d/%08d-%016x%016x",
                KEY_PREFIX, date.getYear(), date.getMonthValue(), date.getDayOfMonth(), within, h1, h2);
    }

    private static long mix(long value) {
        value += 0x9E3779B97F4A7C15L;
        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }
}
