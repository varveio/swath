/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The <b>sort-fixture engine</b> (§0.7): turns a legacy/unsorted
 * swath capture (a directory of {@code *.parquet} parts, or a single part file) into a stamped,
 * globally sorted Parquet output — one file, or a range-disjoint {@code part-NNNNN.parquet} roll when
 * {@code final-file-bytes} is set — by driving the same staging-segment → {@link KWayMerge} →
 * {@link SortTransform} pipeline the {@code --sort} listing path uses. This is the only class the
 * replay module's {@code io.varve.swath.replay.fixture} package needs to call — it takes and returns
 * only {@link Path}s and library records, so no parquet/hadoop type ever needs to appear on the
 * replay module's <b>compile</b> classpath.
 *
 * <p><b>v1 is deliberately single-mode, not configurable (§0.6):</b> unlike swath
 * {@code --sort} (which never drops user entries and sorts versioned output fine), the
 * sort-fixture engine treats a legacy capture as necessarily <b>non-versioned</b> and
 * <b>key-unique</b> — anything else is a broken fixture, not a policy choice:
 * <ul>
 *   <li><b>Versioned fail-fast (§0.6).</b> Any row carrying a non-null {@code version_id} aborts
 *       the whole transform with {@link VersionedCaptureException}, detected while entries are
 *       streamed off the input parts (the transform's own pass — free, never a separate scan).
 *       There is no flag to opt back into versioned handling in v1 — that is deferred serving-side
 *       semantics, not a sort-fixture toggle.</li>
 *   <li><b>Adjacent-equal-key fail-fast (§0.5).</b> Once entries are in the total {@link
 *       ListEntryComparator} order, two adjacent entries comparing equal (same key, since v1 is
 *       non-versioned) throw {@link DuplicateKeyException} naming the offending key — a
 *       non-versioned fixture with duplicate keys is broken, and dedup-keep-first is a follow-up
 *       flag, not built now. The check fires both while a single staging chunk is locally sorted
 *       AND across the final cross-segment merge, so a duplicate split across two staging chunks is
 *       still caught.</li>
 *   <li><b>Cross-row-type key uniqueness (§0.5).</b> The {@link ListEntryComparator}-driven merge's
 *       own duplicate hook cannot see a same-raw-key clash across row types (see {@link
 *       DuplicateKeyCheckingWriterFactory}, which closes the gap inline in the one write pass
 *       {@link SortTransform} performs by wrapping the final {@link SortedFileWriter} this class
 *       hands {@link SortTransform}). This key-unique requirement is a sort-fixture-only policy,
 *       applied only via that wrapper — {@link SortTransform} itself, and so swath {@code --sort}
 *       (which drives it directly with an unwrapped {@link SortedFileWriterFactory}), never applies
 *       it (that path never drops or rejects user entries, §0.5).</li>
 * </ul>
 *
 * <p><b>Crash-safe and idempotent by re-run (like {@code --sort}'s own publish).</b> Each final file
 * is written to {@code part-NNNNN.parquet.tmp} in the staging dir, then atomically renamed into
 * {@code outputDir} in key order by {@link SortTransform}, which also sweeps stale {@code .tmp}s and
 * abandoned {@code part-*} finals from a prior crashed attempt at the START of the next invocation. A
 * crash mid-publish can leave tmp files, or — for a multi-file roll — a prefix of renamed finals
 * alongside the still-unrenamed tmps; that entry sweep clears both before re-merging, so a re-run is
 * clean. This engine passes {@link PublishListener#NO_OP}, so there is no manifest commit point over
 * the roll — each per-file rename is atomic, the multi-file dataset as a whole is not. This engine's own
 * staging directory ({@link #STAGING_DIR_NAME}, a fixed name under {@code outputDir}) is wiped at
 * the START of every call for the same reason: unlike the checkpoint-tracked {@code --sort}
 * pipeline, sort-fixture has no resume story — a crashed prior attempt's partial staging segments
 * are worthless and are discarded rather than reused. {@code outputDir} is treated as fully OWNED,
 * disposable content by this engine (like {@code --sort}'s own {@code data/}) — never point
 * {@code --capture} and {@code --output} at the same directory.
 */
public final class CaptureSorter {

    /**
     * Fixed staging subdirectory name under {@code outputDir}, wiped at the start of every
     * {@link #sort} — the visible {@code _staging} name, unified with
     * the {@code --sort} listing path's {@code ListCommand#SORT_STAGING_DIR}: this is a SEPARATE root
     * (the sort-fixture {@code outputDir}, never a live listing's dataset root), so sharing the name
     * carries no path-collision risk.
     */
    public static final String STAGING_DIR_NAME = "_staging";

    private final SortConfig config;
    private final SortMetrics metrics;
    private final SortedFileWriterFactory finalWriterDelegate;

    public CaptureSorter(SortConfig config) {
        this(config, SortMetrics.NO_OP);
    }

    public CaptureSorter(SortConfig config, SortMetrics metrics) {
        this(config, metrics, new SortedParquetWriterFactory(config, SortMode.OBJECTS));
    }

    /** Narrow test seam for forcing final-writer interleavings through the complete fixture pipeline. */
    CaptureSorter(SortConfig config, SortMetrics metrics,
                  SortedFileWriterFactory finalWriterDelegate) {
        this.config = config;
        this.metrics = metrics;
        this.finalWriterDelegate = finalWriterDelegate;
    }

    /**
     * Sort every {@code *.parquet} part found in {@code captureDir} (or {@code captureDir} itself,
     * if it names a single file) into one stamped sorted Parquet file under {@code outputDir}
     * (created if absent). Mirrors the replay module's own {@code DuckDbListingStore} file-or-
     * directory convention so callers can point this at anything the replay server already accepts
     * as a fixture.
     */
    public SortTransformResult sort(Path captureDir, Path outputDir) throws IOException {
        return sort(resolveParts(captureDir), outputDir);
    }

    /** As {@link #sort(Path, Path)}, with the input parts already resolved by the caller. */
    public SortTransformResult sort(List<Path> inputParts, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        Path stagingDir = outputDir.resolve(STAGING_DIR_NAME);
        wipe(stagingDir);
        Files.createDirectories(stagingDir);

        Comparator<ListEntry> comparator = new ListEntryComparator();
        DuplicateHook hook = CaptureSorter::failOnDuplicate;
        List<Path> segments = stageSegments(inputParts, stagingDir, comparator, hook);

        SortedFileWriterFactory finalWriterFactory = new DuplicateKeyCheckingWriterFactory(
                finalWriterDelegate, metrics);
        SortTransform transform = new SortTransform(new SortRun(config, comparator, hook, metrics, finalWriterFactory));
        return transform.transform(segments, outputDir, stagingDir, PublishListener.NO_OP);
    }

    /**
     * Stream every input part's entries (via {@link SegmentReader} — the same low-level,
     * one-row-group-at-a-time reader {@code --sort} uses for its own staging segments, since a
     * plain unsorted swath part shares the canonical schema), scanning for a version_id as they go
     * (§0.6), and chunk them into locally-sorted staging segments gated by the same
     * {@link SortConfig#segmentBytes()}/{@link SortConfig#segmentEntries()} knobs the listing-time
     * sort lane uses.
     */
    private List<Path> stageSegments(List<Path> inputParts, Path stagingDir,
                                     Comparator<ListEntry> comparator, DuplicateHook hook) throws IOException {
        SegmentWriter segmentWriter = new SegmentWriter(comparator, hook, metrics, config.segmentRowGroupBytes());
        List<Path> segments = new ArrayList<>();
        List<ListEntry> chunk = new ArrayList<>();
        long chunkBytes = 0;
        int seq = 0;
        for (Path part : inputParts) {
            try (SegmentReader reader = new SegmentReader(part)) {
                while (reader.hasNext()) {
                    ListEntry e = reader.next();
                    checkNotVersioned(e);
                    chunk.add(e);
                    chunkBytes += e.key().length() + PageBlock.ENTRY_OVERHEAD_BYTES;
                    if (chunkBytes >= config.segmentBytes() || chunk.size() >= config.segmentEntries()) {
                        segments.add(flushChunk(chunk, comparator, hook, segmentWriter, stagingDir, seq++));
                        chunk = new ArrayList<>();
                        chunkBytes = 0;
                    }
                }
            }
        }
        if (!chunk.isEmpty()) {
            segments.add(flushChunk(chunk, comparator, hook, segmentWriter, stagingDir, seq++));
        }
        return segments;
    }

    private Path flushChunk(List<ListEntry> chunk, Comparator<ListEntry> comparator, DuplicateHook hook,
                            SegmentWriter segmentWriter, Path stagingDir, int seq) throws IOException {
        chunk.sort(comparator);
        Path path = stagingDir.resolve("fixture-" + seq + ".parquet");
        try (SortedCursor cursor = new InMemoryCursor(chunk, comparator, hook)) {
            segmentWriter.writeIntermediate(cursor, path);
        }
        // writeIntermediate() itself emits no counters (it's also the merge-phase cascade-
        // intermediate path, which has its own accounting), but this call site is sort-fixture's own
        // staging-segment build, the direct analog of the listing lane's SegmentWriter#flush(),
        // which DOES emit SORT.segment_flushed. Emit it here too so a real sort-fixture run's
        // segment count is observable via metrics, not just log lines.
        metrics.recordStealReason("SORT", "segment_flushed");
        return path;
    }

    /** §0.6: any non-null {@code version_id} means this capture isn't sort-fixture eligible in v1. */
    private static void checkNotVersioned(ListEntry e) {
        String versionId = switch (e) {
            case ObjectEntry o -> o.versionId();
            case DeleteMarkerEntry d -> d.versionId();
            case CommonPrefixEntry ignored -> null;
        };
        if (versionId != null) {
            throw new VersionedCaptureException(
                    "sort-fixture is non-versioned-only (v1): key '" + e.key().asString()
                            + "' carries version_id '" + versionId + "'");
        }
    }

    /** §0.5: adjacent-equal keys are a broken fixture, not a policy choice — fail fast, name the key. */
    private static void failOnDuplicate(ListEntry previous, ListEntry current) {
        throw new DuplicateKeyException(
                "sort-fixture found a duplicate key (adjacent-equal under the sort order): '"
                        + current.key().asString() + "'");
    }

    private static List<Path> resolveParts(Path captureDir) throws IOException {
        if (Files.isRegularFile(captureDir)) {
            return List.of(captureDir);
        }
        if (!Files.isDirectory(captureDir)) {
            throw new NoSuchFileException(captureDir.toString());
        }
        List<Path> parts = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(captureDir, "*.parquet")) {
            for (Path p : stream) {
                parts.add(p);
            }
        }
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("no *.parquet files found in " + captureDir);
        }
        parts.sort(Comparator.comparing(p -> p.getFileName().toString()));
        return parts;
    }

    /** Recursively delete {@code dir} if present — this engine's own transient staging area only. */
    private static void wipe(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    /** Thrown when a capture carries a non-null {@code version_id} (§0.6 — v1 is non-versioned-only). */
    public static final class VersionedCaptureException extends RuntimeException {
        public VersionedCaptureException(String message) {
            super(message);
        }
    }

    /** Thrown when the sorted capture has adjacent-equal keys (§0.5 — a broken, non-versioned fixture). */
    public static final class DuplicateKeyException extends RuntimeException {
        public DuplicateKeyException(String message) {
            super(message);
        }
    }

    /**
     * Folded into the single write pass: {@link
     * ListEntryComparator} equality folds in {@code row_type} rank, so the SAME raw key emitted once
     * as {@code OBJECT} and once as {@code COMMON_PREFIX} (or {@code DELETE_MARKER}) sorts adjacent in
     * the final output but never compares EQUAL under that comparator — the merge's own {@link
     * DuplicateHook} (see {@link #failOnDuplicate}) is invoked only on comparator-equal pairs and
     * structurally cannot see this case. This class already asserts non-versioned (§0.6) by
     * construction, so the only way two rows can share a raw key by the time this runs is a genuine
     * key clash.
     *
     * <p>Wraps the delegate {@link SortedFileWriterFactory} (the stamped {@link
     * SortedParquetWriterFactory}) and intercepts every {@link SortedFileWriter#write}: since {@link
     * SortTransform} writes the already-merged, totally-ordered stream, comparing each row's raw key
     * bytes against the immediately preceding row's (tracked in this factory instance, so the
     * comparison carries across a multi-file roll — the {@link RolledPartWriter#drain} loop opens a new
     * delegate writer per file but one {@link SortedFileWriterFactory#forOutputSequence() output
     * sequence} is shared across all rolls) is exactly equivalent to the old post-hoc read-back of the published
     * output, without a second full decode pass. The duplicate check fires BEFORE the row reaches the
     * delegate writer, so it throws while the offending file is still {@code part-NNNNN.parquet.tmp}
     * — before {@link SortTransform}'s rename loop runs — preserving the same tmp-then-rename publish
     * discipline the merge-phase {@link DuplicateHook} already relies on (a thrown exception here
     * never leaves a corrupt PUBLISHED file; only stale {@code .tmp}s, cleaned on the next run).
     */
    static final class DuplicateKeyCheckingWriterFactory implements SortedFileWriterFactory {
        private final SortedFileWriterFactory delegate;
        private final SortMetrics metrics;
        private byte[] previousKey;

        DuplicateKeyCheckingWriterFactory(SortedFileWriterFactory delegate, SortMetrics metrics) {
            this.delegate = delegate;
            this.metrics = metrics;
        }

        @Override
        public SortedFileWriterFactory forOutputSequence() {
            // ParallelRangeMerge shares the configured factory across range threads, but each range
            // is its own ordered sequence. Give it independent previous-key state while retaining
            // that state across every rolled file within the range. The serial path calls this once
            // for the whole publish, so its cross-roll duplicate detection is unchanged.
            return new DuplicateKeyCheckingWriterFactory(delegate.forOutputSequence(), metrics);
        }

        @Override
        public SortedFileWriter create(Path path, int fileIndex) throws IOException {
            // Engagement counter: fires once per
            // final file opened via this factory, so post-analysis can see the inline check engaged
            // on a sort-fixture run — and, by its ABSENCE, that the live --sort path (which drives
            // SortedParquetWriterFactory directly, never through this wrapper) never does.
            metrics.recordStealReason("SORT", "fixture_dup_check_inline");
            SortedFileWriter target = delegate.create(path, fileIndex);
            return new SortedFileWriter() {
                @Override
                public void write(ListEntry e) throws IOException {
                    byte[] key = e.key().rawUnsafe();
                    if (previousKey != null && KeyBytes.compareUnsigned(previousKey, key) == 0) {
                        throw new DuplicateKeyException(
                                "sort-fixture found a duplicate key across row types (adjacent-equal key "
                                        + "bytes regardless of row_type): '" + e.key().asString() + "'");
                    }
                    previousKey = key;
                    target.write(e);
                }

                @Override
                public long rows() {
                    return target.rows();
                }

                @Override
                public long dataSize() {
                    return target.dataSize();
                }

                @Override
                public void markFinal() {
                    target.markFinal();
                }

                @Override
                public void setFileIndex(int fileIndex) {
                    // Pure forward: this decorator only adds the duplicate-key check, never stamp
                    // semantics. Swallowing it would silently downgrade fixture output's
                    // completeness proof to the range-local one.
                    target.setFileIndex(fileIndex);
                }

                @Override
                public void close() throws IOException {
                    target.close();
                }
            };
        }
    }
}
