/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.ParquetParts;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.parquet.sorted.SortedParquetWriterFactory;
import io.varve.swath.output.sorted.SortedDatasetCommitter;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StagingReconciliation;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageRunWriter;
import io.varve.swath.sort.stage.SpillGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The <b>sort-fixture engine</b> (§0.7): turns a legacy/unsorted
 * swath capture (a directory of {@code *.parquet} parts, or a single part file) into a stamped,
 * globally sorted Parquet output — one file, or a range-disjoint {@code part-NNNNN.parquet} roll when
 * {@code final-file-bytes} is set — by driving the same staging-segment → {@link CascadeReducer} →
 * {@link SortedDatasetCoordinator} pipeline the {@code --sort} listing path uses. This is the only class the
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
 *   <li><b>Raw-key uniqueness (§0.5).</b> Once entries reach the shared final drain, two adjacent
 *       rows with the same raw key throw {@link DuplicateKeyException} naming the offending key.
 *       This catches both comparator-equal rows and same-key rows of different types, including a
 *       pair split across staging chunks or lying at a final-file roll threshold. The fixture sets
 *       {@link EqualKeyPolicy#REJECT}; live current-object {@code --sort} now applies the same
 *       raw-key uniqueness check, while the dormant versions mode permits equal-key groups.</li>
 * </ul>
 *
 * <p><b>Crash-safe and idempotent by re-run (like {@code --sort}'s own publish).</b> Each final file
 * is written to {@code part-NNNNN.parquet.tmp} in the staging dir, then atomically renamed into
 * {@code outputDir} in key order by {@link SortedDatasetCoordinator}, which sweeps stale {@code .tmp}s at the
 * start and abandoned {@code part-*} finals only after the complete replacement is closed. A
 * crash mid-publish can leave tmp files, or — for a multi-file roll — a prefix of renamed finals
 * alongside the still-unrenamed tmps; a re-run clears disposable tmp work first, regenerates the
 * replacement, then clears stale finals immediately before publication. This engine passes
 * {@link SortedDatasetCommitter#NO_OP}, so there is no manifest commit point over
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
    public SortedDatasetResult sort(Path captureDir, Path outputDir) throws IOException {
        List<Path> parts = ParquetParts.resolve(captureDir);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("no *.parquet files found in " + captureDir);
        }
        return sort(parts, outputDir);
    }

    /** As {@link #sort(Path, Path)}, with the input parts already resolved by the caller. */
    public SortedDatasetResult sort(List<Path> inputParts, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        Path stagingDir = outputDir.resolve(STAGING_DIR_NAME);
        StagingReconciliation.discardStagingTree(stagingDir);
        Files.createDirectories(stagingDir);

        Comparator<ListEntry> comparator = new ListEntryComparator();
        List<Path> segments = stageSegments(inputParts, stagingDir, comparator);

        SortedDatasetCoordinator transform = new SortedDatasetCoordinator(
                new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.REJECT,
                        metrics, finalWriterDelegate, SortRun.PROCESS_SOFT_FD_LIMIT,
                        StaleFinalSweep.OWN_PARTS_ONLY));
        return transform.transform(segments, outputDir, stagingDir, SortedDatasetCommitter.NO_OP,
                ignored -> metrics.markProgress(), FinalPassListener.NO_OP);
    }

    /**
     * Stream every input part's entries via the low-level, one-row-group-at-a-time {@link
     * ParquetEntryReader}, since a plain unsorted swath part shares the canonical schema, scanning for a
     * version_id as they go
     * (§0.6), and chunk them into locally-sorted staging segments gated by the same
     * {@link SortConfig#segmentBytes()}/{@link SortConfig#segmentEntries()} knobs the listing-time
     * sort lane uses.
     */
    private List<Path> stageSegments(List<Path> inputParts, Path stagingDir,
                                     Comparator<ListEntry> comparator) throws IOException {
        PageRunWriter segmentWriter =
                new PageRunWriter(comparator, DuplicateHook.NO_OP, metrics, config.segmentCodec());
        SpillGate gate = new SpillGate(config);
        List<Path> segments = new ArrayList<>();
        List<ListEntry> chunk = new ArrayList<>();
        long chunkBytes = 0;
        int seq = 0;
        for (Path part : inputParts) {
            try (ParquetEntryReader reader = new ParquetEntryReader(part)) {
                while (reader.hasNext()) {
                    ListEntry e = reader.next();
                    checkNotVersioned(e);
                    chunk.add(e);
                    chunkBytes += PageBlock.estimatedBytes(e);
                    if (gate.full(chunkBytes, chunk.size())) {
                        segments.add(flushChunk(chunk, comparator, segmentWriter, stagingDir, seq++));
                        chunk = new ArrayList<>();
                        chunkBytes = 0;
                    }
                }
            }
        }
        if (!chunk.isEmpty()) {
            segments.add(flushChunk(chunk, comparator, segmentWriter, stagingDir, seq++));
        }
        return segments;
    }

    private Path flushChunk(List<ListEntry> chunk, Comparator<ListEntry> comparator,
                            PageRunWriter segmentWriter, Path stagingDir, int seq) throws IOException {
        chunk.sort(comparator);
        Path path = stagingDir.resolve(StagingNames.fixtureSegment(seq));
        try (SortedEntryCursor cursor = new InMemoryCursor(chunk, comparator, DuplicateHook.NO_OP)) {
            // Fixture chunks can overlap across their whole key ranges; the pipeline routes their
            // page references by the persisted frame headers.
            segmentWriter.writeFixtureChunk(cursor, path);
        }
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

}
