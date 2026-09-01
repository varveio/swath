/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.output.parquet.ParquetParts;
import io.varve.swath.output.parquet.sorted.SortedParquetIndex;
import io.varve.swath.output.parquet.sorted.SortedParquetIndex.RowGroupKey;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.replay.protocol.ByteKey;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Sortedness detection + in-memory index load/derive for the replay server's sorted-serving path:
 * the only place {@code io.varve.swath.replay} touches the
 * root {@code io.varve.swath.sort} API for this concern, so no parquet/hadoop type ever needs to appear
 * on this module's compile classpath — everything here traffics in {@link Path}s, {@link SortedParquetStamp}
 * (a plain record), and this package's own {@link ByteKey}-keyed {@link IndexEntry}.
 *
 * <p>Multi-file aware throughout: a sorted fixture may be one file or a {@code final-file-bytes}
 * roll producing several range-disjoint {@code part-NNNNN.parquet} files named in key order —
 * {@link #resolveFiles} always returns them in lexical (== key) order, and {@link
 * #loadIndex} treats the flattened row-group sequence across every file as one global array.
 */
public final class SortedFixtures {

    /** {@code serving.fallback} reason: no recognized sortedness stamp. */
    public static final String NO_STAMP = "no_stamp";

    /** {@code serving.fallback} reason: a stamped {@code versions} file, unsupported for serving in v1. */
    public static final String UNSUPPORTED_MODE = "unsupported_mode";

    /** {@code serving.fallback} reason: the derived row-group first keys were not strictly ascending. */
    public static final String SANITY_FAILED = "sanity_failed";

    /**
     * {@code serving.fallback} reason: a row group's {@code row_type} footer statistics do not prove
     * every row is {@code 'OBJECT'} (a sorted file may legitimately mix row types, e.g. a legacy
     * delimiter'd capture re-sorted by {@code sort-fixture}, which still stamps {@code mode=objects}
     * unconditionally). Role-2's routing invariant assumes {@code rowCount == OBJECT-row-count} per
     * group, so such a file falls back to role-1 (which filters {@code row_type} at materialization)
     * rather than silently under-counting and truncating a listing early. See
     * {@link #loadIndex(List, FixtureMetrics, boolean)} for where this is checked.
     */
    public static final String MIXED_ROW_TYPES = "mixed_row_types";

    /**
     * {@code serving.fallback} reason: a resolved file's sortedness stamp carries a
     * {@code format_version} this reader doesn't recognize — a forward-
     * compatibility guard, since a future incompatible stamp change must never be silently trusted by
     * an older reader.
     */
    public static final String UNKNOWN_FORMAT_VERSION = "unknown_format_version";

    /**
     * {@code serving.fallback} reason: the resolved file set does not prove multi-file completeness
     * — either the {@code file_index} values are not exactly {@code 1..N}
     * contiguous for the {@code N} files actually present, or the {@code file_final} flag isn't on
     * exactly one file at index {@code N}. This is exactly what a crash between renaming files 1..k
     * and k+1 of a multi-file publish leaves behind: a stamped PREFIX of the true output, silently
     * servable-and-wrong unless this check refuses it. Role-1 remains correct for such a fixture (it
     * materialises and serves whatever files are actually present).
     */
    public static final String INCOMPLETE_MULTIFILE = "incomplete_multifile";

    private SortedFixtures() {
    }

    /**
     * Reads {@code file}'s footer sortedness stamp, if any recognized one is present —
     * {@link SortedParquetStamp#read} already treats a corrupt/foreign/future stamp as "not stamped".
     */
    public static Optional<SortedParquetStamp> detect(Path file) throws IOException {
        return SortedParquetStamp.read(file);
    }

    /**
     * Resolves a fixture path to its {@code *.parquet} file(s) in lexical (== key) order:
     * {@code fixture} itself if it names a file, or every {@code *.parquet} directly inside it if
     * it's a directory — the same file-or-directory convention {@code DuckDbListingStore} already
     * uses. Never recurses.
     */
    public static List<Path> resolveFiles(Path fixture) throws IOException {
        return ParquetParts.resolve(fixture);
    }

    /**
     * One row group's derived routing entry: which file, its actual first key, and its row count.
     *
     * @param rowGroup {@code file}'s <b>physical</b> row-group block index — the ordinal
     *                 {@link io.varve.swath.output.parquet.sorted.SortedParquetIndex.RowGroupSpan#blockIndex} names and
     *                 {@link io.varve.swath.output.parquet.sorted.SortedParquetRowGroupReader#openKeyCursor}/{@link
     *                 io.varve.swath.output.parquet.sorted.SortedParquetRowGroupReader#rows} take directly. Plain range routing
     *                 ({@code SortedRouting}) never reads it — only the {@code delimiter=/} skip-scan
     *                 ({@code SortedParquetStore#delimitedRollup}) does, to random-access the exact row
     *                 group a scan cursor lands in without decoding the groups before it.
     */
    public record IndexEntry(Path file, int rowGroup, ByteKey firstKey, long rowCount) {
    }

    /**
     * The outcome of {@link #loadIndex}: either the full in-memory routing index, or a typed
     * eligibility failure the caller can act on rather than serve a file that isn't safely servable
     * sorted as if it were. The replay server turns that failure into a refusal to start; {@code
     * swath-sim}, which forces a tier rather than choosing one, uses it to decline a backend.
     */
    public sealed interface IndexLoadResult {

        record Loaded(List<IndexEntry> entries) implements IndexLoadResult {
        }

        /**
         * {@code reason} is {@link #SANITY_FAILED} (row-group first keys not strictly ascending) or
         * {@link #MIXED_ROW_TYPES} (a row group is not provably pure {@code OBJECT}) — both
         * {@code serving.fallback} tag values; {@code detail} is for logs.
         */
        record SanityFailed(String reason, String detail) implements IndexLoadResult {
        }
    }

    /**
     * As {@link #loadIndex(List, FixtureMetrics, boolean)} with {@code recordFallbackOnFailure=true}
     * (the default — every existing caller wants the counter recorded on failure).
     */
    public static IndexLoadResult loadIndex(List<Path> files, FixtureMetrics metrics) throws IOException {
        return loadIndex(files, metrics, true);
    }

    /**
     * Derives {@code (file, rowGroup, firstKey, rowCount)} for every row group across every file in
     * {@code files} (already in key order, {@link #resolveFiles}), via the root {@link
     * SortedParquetIndex#firstKeysPerRowGroup} — the {@code firstKey}/{@code rowCount} pair never comes
     * from Parquet footer stats. {@code rowGroup} (the physical block index) comes from a second,
     * equally cheap footer-only pass, {@link SortedParquetIndex#rowGroupSpans}: both derive from the same
     * physical row-group sequence with the same empty-group skip, so the two lists stay positionally
     * aligned entry-for-entry — a per-file invariant {@link SortedParquetIndex} itself is tested against
     * ({@code rowGroupSpansCarryPhysicalBlockIndicesAndTrueFirstKeys}), not re-verified here. Two
     * eligibility checks gate
     * a {@link IndexLoadResult.Loaded} result, evaluated per row group in file/row-group order (the
     * first failure wins): {@link #MIXED_ROW_TYPES} — the one place this method legitimately consults
     * footer stats despite the no-footer-stats rule above, a narrow, deliberate exception (see
     * {@code SortedParquetIndex#isPureObjectRowType} for why it is sound) — and {@link #SANITY_FAILED},
     * whose ascending check spans file boundaries and so also catches cross-file duplicate keys. See
     * {@code swath-replay.md}'s "sort-fixture / sorted-serving meters" table for what each
     * reason means and when it fires.
     * <p>Records {@code swath.replay.index.load.latency\{source=derived\}} + {@code .index.entries} on
     * success either way (the derive work happens regardless of the outcome). Records
     * {@code swath.replay.serving.fallback\{reason\}} on failure only when
     * {@code recordFallbackOnFailure} is {@code true} — {@code --serving-mode sorted} (a hard fail,
     * no fallback ever happens) passes {@code false} so it doesn't misleadingly bump a "fallback"
     * counter for a run that never served anything.
     */
    public static IndexLoadResult loadIndex(List<Path> files, FixtureMetrics metrics,
                                            boolean recordFallbackOnFailure) throws IOException {
        Timer.Sample sample = metrics.startTimer();
        List<IndexEntry> entries = new ArrayList<>();
        ByteKey previous = null;
        for (Path file : files) {
            List<RowGroupKey> rowGroups = SortedParquetIndex.firstKeysPerRowGroup(file);
            List<SortedParquetIndex.RowGroupSpan> spans = SortedParquetIndex.rowGroupSpans(file);
            for (int i = 0; i < rowGroups.size(); i++) {
                RowGroupKey rg = rowGroups.get(i);
                if (!rg.pureObjectRowType()) {
                    metrics.recordIndexLoad(sample, entries.size());
                    if (recordFallbackOnFailure) {
                        metrics.recordFallback(MIXED_ROW_TYPES);
                    }
                    String detail = "row group " + i + " of " + file + " is not provably pure OBJECT "
                            + "row_type (footer row_type stats absent, empty, or not min==max=='OBJECT')";
                    return new IndexLoadResult.SanityFailed(MIXED_ROW_TYPES, detail);
                }
                ByteKey firstKey = ByteKey.copyOf(rg.firstKey());
                if (previous != null && previous.compareTo(firstKey) >= 0) {
                    metrics.recordIndexLoad(sample, entries.size());
                    if (recordFallbackOnFailure) {
                        metrics.recordFallback(SANITY_FAILED);
                    }
                    String detail = "row-group first keys not strictly ascending at " + file + " row group " + i;
                    return new IndexLoadResult.SanityFailed(SANITY_FAILED, detail);
                }
                entries.add(new IndexEntry(file, spans.get(i).blockIndex(), firstKey, rg.rowCount()));
                previous = firstKey;
            }
        }
        metrics.recordIndexLoad(sample, entries.size());
        return new IndexLoadResult.Loaded(List.copyOf(entries));
    }
}
