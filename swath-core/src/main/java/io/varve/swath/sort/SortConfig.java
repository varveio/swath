/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.output.sorted.StagingRetention;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageCompression;
import io.varve.swath.sort.stage.SpillLane;
import java.util.function.UnaryOperator;

/**
 * Immutable snapshot of the {@code swath.sort.*} knobs, read <b>once</b> at construction.
 * {@link #fromSystemProperties()} is the production entry point; {@link #fromProperties} takes an
 * injectable property-lookup function so tests can exercise every knob without mutating the real,
 * process-global {@link System} properties. All sizes are bytes.
 *
 * <ul>
 *   <li>{@code segmentBytes} — the primary segment-flush gate (§6): estimated pre-encode bytes of
 *       a sealed buffer. Heap-adaptive by default: {@code heapFraction × Runtime.maxMemory()},
 *       floored at {@link #SEGMENT_BYTES_FLOOR} (64&nbsp;MB); an explicit
 *       {@code swath.sort.segment-bytes} overrides the adaptive value. Bigger heap ⇒ bigger
 *       segments ⇒ single-pass merge as the design point (I11: memory is a function of {@code -Xmx},
 *       never of object count).</li>
 *   <li>{@code segmentEntries} — secondary backstop entry cap on a sealed buffer.</li>
 *   <li>{@code heapFraction} — the adaptive ratio {@code segmentBytes} derives from {@code -Xmx}.</li>
 *   <li>{@code buffers} — in-flight sealed buffers (fill while the sealed buffer encodes off-thread);
 *       consumed by the pipeline. Must be {@code >= 2}: {@link SpillLane} bounds
 *       live sealed buffers to exactly {@code buffers()} (fill + {@code buffers() - 1} off-thread), so
 *       {@code buffers=1} would either deadlock (0 off-thread slots) or silently violate that corridor
 *       invariant if floored — rejected outright instead.</li>
 *   <li>{@code fanIn} — merge fan-in {@code F} (§6); open segment readers never exceed {@code F}.</li>
 *   <li>{@code finalFileBytes} — roll threshold for multi-file sorted output; {@link Long#MAX_VALUE}
 *       means a single file (contract default).</li>
 *   <li>{@code finalRowGroupBytes} — the served file's seek granularity (row-group size) for the
 *       final writer.</li>
 *   <li>{@code finalPageRows} — the served file's seek granularity <em>within</em> a row group: the
 *       maximum rows a data page of the final file may hold ({@link #DEFAULT_FINAL_PAGE_ROWS}). A page
 *       is Parquet's smallest addressable unit, so this is the floor on what a bounded key-range read
 *       must decode; see the constant for why the default is a listing page rather than parquet's
 *       20,000.</li>
 *   <li>{@code mergePerStreamBytes} — the configured planning price for one open merge stream,
 *       and the denominator of {@link #effectiveFanIn()}. The default uses 64&nbsp;KiB as a
 *       capacity floor. Runtime planning additionally accounts for encoded and decoded pages.</li>
 *   <li>{@code mergeBudgetBytes} — the merge-phase residency budget: caps how many segment streams
 *       a single {@link CascadeReducer} pass may hold open and bounds decoded page state, via
 *       {@link #effectiveFanIn()} = {@code min(fanIn, max(2, mergeBudgetBytes / mergePerStreamBytes))}
 *       — keeping planned open-stream capacity a function of the budget, never of segment count
 *       (I11), even when {@code fanIn} alone would allow more streams. Same heap-adaptive shape as
 *       {@code segmentBytes} by default (floor 64&nbsp;MB). The {@code max(2, …)} floor is a
 *       documented merge-width floor, not a claim that two streams consume exactly twice the
 *       configured price.</li>
 *   <li>{@code segmentCodec} — the codec {@link PageBlock#pack} compresses a page's
 *       front-coded PAYLOAD at pack time ({@code NONE}, {@code LZ4}, or {@code ZSTD1}; default
 *       {@code ZSTD1}) — the record HEADER (min/max, dict
 *       tables, {@code useDict}, count, ordered-bit) is never compressed. Threaded to
 *       {@link PageBlock#pack} at the one seal call site ({@link SortBuffer#admit}).</li>
 *   <li>{@code stagingRetention} — whether a successful live sorted publish keeps its original
 *       checkpoint-tracked page-run inputs for diagnostics. Cascade intermediates and temporary
 *       files remain disposable in either mode.</li>
 * </ul>
 *
 * <p>This is an internal CLI configuration snapshot, not a supported generic Java API before 1.0.
 * Its former record/canonical constructor was likewise unsupported; construct production snapshots
 * through {@link #fromSystemProperties()} and derive test or internal variants with {@code withX}.
 */
public final class SortConfig {

    /** Largest merge range count supported by the core sorter and the public tune surface. */
    public static final int MAX_MERGE_PARALLELISM = 16;

    /** Floor for the heap-adaptive segment gate (§7). */
    public static final long SEGMENT_BYTES_FLOOR = 64L * 1024 * 1024;

    /**
     * Default for {@code mergePerStreamBytes}: a conservative ~64&nbsp;KiB estimate of the heap a
     * single open page-run merge stream holds. Documented as an estimate to be validated at the
     * perf gate.
     */
    public static final long DEFAULT_MERGE_PER_STREAM_BYTES = 64L * 1024;

    /**
     * Default for {@code segmentCodec}: compress-at-pack is ON by default ({@link
     * PageCompression#ZSTD1}) — staging disk exhaustion ({@code sort_disk_exhausted}) is the primary
     * staging-disk risk, and compressing the page-run PAYLOAD (never the plain header) directly
     * cuts both staging disk and in-flight buffered memory.
     */
    static final PageCompression DEFAULT_SEGMENT_CODEC = PageCompression.ZSTD1;

    /**
     * Default for {@code finalPageRows}: <b>1,024</b> rows per data page in the final, served file.
     *
     * <p>A page is the unit of random access — the page index prunes whole pages, never rows, and a
     * page's encodings decode strictly forward — so a bounded key-range read decodes at least one
     * whole page per column however few rows it wanted. Parquet's own default caps a page at 20,000
     * rows, and its byte cap only binds on columns wide enough to reach it, so every narrow column
     * sat at 20,000 and a thousand-row read cost the same as a one-row read. 1,024 is one listing
     * page, the request shape a served file exists for.
     *
     * <p>It is paid for in page headers, index entries and some encoding efficiency, once per
     * fixture, against a saving on every request. Distinct from the data-page BYTE cap, which two
     * gates (2026-07-04 P1/P4) measured dead in both directions: that one also fragments the wide
     * columns that were never the problem.
     */
    public static final int DEFAULT_FINAL_PAGE_ROWS = 1024;

    private static final String PREFIX = "swath.sort.";

    /** The shared CLI tune suffix for retaining original page-run staging after publish. */
    public static final String KEEP_STAGING_TUNE_KEY = "sort.keep-staging";

    /** JVM-property form of {@link #KEEP_STAGING_TUNE_KEY}. */
    public static final String KEEP_STAGING_PROPERTY = "swath." + KEEP_STAGING_TUNE_KEY;

    /**
     * The maximum number of concurrent final-file encoders. Runtime planning may admit fewer to
     * remain within the merge residency budget.
     */
    public static final int DEFAULT_MERGE_PARALLELISM =
            Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors() / 2));

    /** The §7 heap-adaptive ratio: {@code segmentBytes}/{@code mergeBudgetBytes} derive from {@code -Xmx}. */
    private static final double DEFAULT_HEAP_FRACTION = 0.08;

    /**
     * The canonical default configuration — every knob at the value {@link #fromProperties} applies
     * when the corresponding {@code swath.sort.*} property is unset. Callers derive a variant with the
     * {@code withX} methods (a single differing knob is {@code DEFAULT.withFanIn(2)}); {@link
     * #fromProperties} reads its per-knob defaults from here, so the defaults live in exactly one place.
     */
    public static final SortConfig DEFAULT = new SortConfig(
            new StagingBuffering(
                    adaptiveSegmentBytes(DEFAULT_HEAP_FRACTION), Long.MAX_VALUE, DEFAULT_HEAP_FRACTION, 2,
                    DEFAULT_SEGMENT_CODEC),
            new Merge(
                    10000, adaptiveSegmentBytes(DEFAULT_HEAP_FRACTION), DEFAULT_MERGE_PARALLELISM,
                    DEFAULT_MERGE_PER_STREAM_BYTES),
            new FinalOutput(1L << 30, 8L * 1024 * 1024, DEFAULT_FINAL_PAGE_ROWS),
            new Retention(StagingRetention.DELETE_AFTER_PUBLISH));

    private final StagingBuffering stagingBuffering;
    private final Merge merge;
    private final FinalOutput finalOutput;
    private final Retention retention;

    private SortConfig(StagingBuffering stagingBuffering, Merge merge, FinalOutput finalOutput,
                       Retention retention) {
        this.stagingBuffering = stagingBuffering;
        this.merge = merge;
        this.finalOutput = finalOutput;
        this.retention = retention;
        validate();
    }

    private void validate() {
        if (segmentBytes() <= 0) {
            throw new IllegalArgumentException("segment-bytes must be > 0, got " + segmentBytes());
        }
        if (segmentEntries() <= 0) {
            throw new IllegalArgumentException("segment-entries must be > 0, got " + segmentEntries());
        }
        if (!(heapFraction() > 0.0)) {
            throw new IllegalArgumentException("heap-fraction must be > 0, got " + heapFraction());
        }
        if (buffers() < 2) {
            // SpillLane's off-thread capacity is buffers()-1 (the fill buffer is the
            // +1) — buffers=1 would leave 0 off-thread slots (a deadlock, every seal would block
            // forever) unless floored, and any floor would silently violate the documented live<=
            // buffers() corridor invariant (fill + 1 off-thread = 2 live > the requested 1). Rejected
            // here, consistent with every other knob in this configuration.
            throw new IllegalArgumentException("buffers must be >= 2, got " + buffers());
        }
        if (fanIn() < 2) {
            throw new IllegalArgumentException("fan-in must be >= 2, got " + fanIn());
        }
        if (finalFileBytes() <= 0) {
            throw new IllegalArgumentException("final-file-bytes must be > 0, got " + finalFileBytes());
        }
        if (finalRowGroupBytes() <= 0) {
            throw new IllegalArgumentException("final-row-group-bytes must be > 0, got " + finalRowGroupBytes());
        }
        if (finalPageRows() <= 0) {
            throw new IllegalArgumentException("final-page-rows must be > 0, got " + finalPageRows());
        }
        if (mergeBudgetBytes() <= 0) {
            throw new IllegalArgumentException("merge-budget-bytes must be > 0, got " + mergeBudgetBytes());
        }
        if (mergePerStreamBytes() <= 0) {
            throw new IllegalArgumentException("merge-per-stream-bytes must be > 0, got " + mergePerStreamBytes());
        }
        if (mergeParallelism() < 1 || mergeParallelism() > MAX_MERGE_PARALLELISM) {
            // 1 is the explicit serial opt-out. The shipped default is half the processors,
            // capped at eight; the supported override ceiling is sixteen on every entry point.
            throw new IllegalArgumentException("merge-parallelism must be between 1 and "
                    + MAX_MERGE_PARALLELISM + ", got " + mergeParallelism());
        }
        if (segmentCodec() == null) {
            throw new IllegalArgumentException("segment-codec must not be null");
        }
        if (stagingRetention() == null) {
            throw new IllegalArgumentException("staging-retention must not be null");
        }
    }

    public SortConfig withSegmentBytes(long segmentBytes) {
        return copy(new StagingBuffering(segmentBytes, segmentEntries(), heapFraction(), buffers(), segmentCodec()));
    }

    public SortConfig withSegmentEntries(long segmentEntries) {
        return copy(new StagingBuffering(segmentBytes(), segmentEntries, heapFraction(), buffers(), segmentCodec()));
    }

    public SortConfig withHeapFraction(double heapFraction) {
        return copy(new StagingBuffering(segmentBytes(), segmentEntries(), heapFraction, buffers(), segmentCodec()));
    }

    public SortConfig withBuffers(int buffers) {
        return copy(new StagingBuffering(segmentBytes(), segmentEntries(), heapFraction(), buffers, segmentCodec()));
    }

    public SortConfig withFanIn(int fanIn) {
        return copy(new Merge(fanIn, mergeBudgetBytes(), mergeParallelism(), mergePerStreamBytes()));
    }

    public SortConfig withFinalFileBytes(long finalFileBytes) {
        return copy(new FinalOutput(finalFileBytes, finalRowGroupBytes(), finalPageRows()));
    }

    public SortConfig withFinalRowGroupBytes(long finalRowGroupBytes) {
        return copy(new FinalOutput(finalFileBytes(), finalRowGroupBytes, finalPageRows()));
    }

    public SortConfig withFinalPageRows(int finalPageRows) {
        return copy(new FinalOutput(finalFileBytes(), finalRowGroupBytes(), finalPageRows));
    }

    public SortConfig withMergeBudgetBytes(long mergeBudgetBytes) {
        return copy(new Merge(fanIn(), mergeBudgetBytes, mergeParallelism(), mergePerStreamBytes()));
    }

    public SortConfig withMergeParallelism(int mergeParallelism) {
        return copy(new Merge(fanIn(), mergeBudgetBytes(), mergeParallelism, mergePerStreamBytes()));
    }

    public SortConfig withMergePerStreamBytes(long mergePerStreamBytes) {
        return copy(new Merge(fanIn(), mergeBudgetBytes(), mergeParallelism(), mergePerStreamBytes));
    }

    public SortConfig withSegmentCodec(PageCompression segmentCodec) {
        return copy(new StagingBuffering(segmentBytes(), segmentEntries(), heapFraction(), buffers(), segmentCodec));
    }

    /** Read the knobs from system properties once, applying the documented defaults. */
    public static SortConfig fromSystemProperties() {
        return fromProperties(System::getProperty);
    }

    /**
     * Read the knobs from an injected property source — lets tests exercise every knob
     * combination without mutating the real, process-global {@link System} properties. {@code
     * lookup} has the same contract as {@link System#getProperty(String)}: return {@code null} for
     * an unset key. Production goes through {@link #fromSystemProperties()}.
     */
    static SortConfig fromProperties(UnaryOperator<String> lookup) {
        // Each per-knob default is read from DEFAULT so the defaults live in exactly one place; the two
        // heap-adaptive knobs recompute from the (possibly overridden) heap-fraction rather than DEFAULT's.
        double heapFraction = doubleProp(lookup, "heap-fraction", DEFAULT.heapFraction());
        long segmentBytes = longProp(lookup, "segment-bytes", adaptiveSegmentBytes(heapFraction));
        long segmentEntries = longProp(lookup, "segment-entries", DEFAULT.segmentEntries());
        int buffers = intProp(lookup, "buffers", DEFAULT.buffers());
        int fanIn = intProp(lookup, "fan-in", DEFAULT.fanIn());
        long finalFileBytes = longProp(lookup, "final-file-bytes", DEFAULT.finalFileBytes());
        long finalRowGroupBytes = longProp(lookup, "final-row-group-bytes", DEFAULT.finalRowGroupBytes());
        int finalPageRows = intProp(lookup, "final-page-rows", DEFAULT.finalPageRows());
        long mergeBudgetBytes = longProp(lookup, "merge-budget-bytes", adaptiveSegmentBytes(heapFraction));
        int mergeParallelism = DEFAULT.mergeParallelism();
        long mergePerStreamBytes = longProp(lookup, "merge-per-stream-bytes", DEFAULT.mergePerStreamBytes());
        String segmentCodecProp = lookup.apply(PREFIX + "segment-codec");
        PageCompression segmentCodec = segmentCodecProp == null
                ? DEFAULT.segmentCodec()
                : parseSegmentCodec(segmentCodecProp);
        String keepStagingProp = lookup.apply(KEEP_STAGING_PROPERTY);
        StagingRetention stagingRetention = keepStagingProp == null
                ? DEFAULT.stagingRetention()
                : parseStagingRetention(KEEP_STAGING_PROPERTY, keepStagingProp);
        return new SortConfig(
                new StagingBuffering(segmentBytes, segmentEntries, heapFraction, buffers, segmentCodec),
                new Merge(fanIn, mergeBudgetBytes, mergeParallelism, mergePerStreamBytes),
                new FinalOutput(finalFileBytes, finalRowGroupBytes, finalPageRows),
                new Retention(stagingRetention));
    }

    public SortConfig withStagingRetention(StagingRetention stagingRetention) {
        return copy(new Retention(stagingRetention));
    }

    private SortConfig copy(StagingBuffering updated) {
        return new SortConfig(updated, merge, finalOutput, retention);
    }

    private SortConfig copy(Merge updated) {
        return new SortConfig(stagingBuffering, updated, finalOutput, retention);
    }

    private SortConfig copy(FinalOutput updated) {
        return new SortConfig(stagingBuffering, merge, updated, retention);
    }

    private SortConfig copy(Retention updated) {
        return new SortConfig(stagingBuffering, merge, finalOutput, updated);
    }

    public long segmentBytes() {
        return stagingBuffering.segmentBytes();
    }

    public long segmentEntries() {
        return stagingBuffering.segmentEntries();
    }

    public double heapFraction() {
        return stagingBuffering.heapFraction();
    }

    public int buffers() {
        return stagingBuffering.buffers();
    }

    public int fanIn() {
        return merge.fanIn();
    }

    public long finalFileBytes() {
        return finalOutput.fileBytes();
    }

    public long finalRowGroupBytes() {
        return finalOutput.rowGroupBytes();
    }

    public int finalPageRows() {
        return finalOutput.pageRows();
    }

    public long mergeBudgetBytes() {
        return merge.budgetBytes();
    }

    public int mergeParallelism() {
        return merge.parallelism();
    }

    public long mergePerStreamBytes() {
        return merge.perStreamBytes();
    }

    public PageCompression segmentCodec() {
        return stagingBuffering.codec();
    }

    public StagingRetention stagingRetention() {
        return retention.staging();
    }

    private static PageCompression parseSegmentCodec(String raw) {
        try {
            return PageCompression.fromConfigValue(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("swath.sort.segment-codec: " + e.getMessage(), e);
        }
    }

    private static StagingRetention parseStagingRetention(String property, String value) {
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "on" -> StagingRetention.RETAIN_ORIGINALS;
            case "off" -> StagingRetention.DELETE_AFTER_PUBLISH;
            default -> throw new IllegalArgumentException(
                    property + " must be on or off, got " + value);
        };
    }

    /**
     * The <b>static</b> budget-bounded merge fan-in: {@code min(fanIn,
     * max(2, mergeBudgetBytes / mergePerStreamBytes))}. {@link CascadeReducer} opens at most this many streams
     * per pass, so planned open-stream capacity remains a function of the budget knob, never of how
     * many segments a run happens to produce (I11). This result is only the static config-level
     * advisory; runtime planning additionally prices retained encoded/decoded page state and
     * rejects a minimum width that cannot fit. {@code fanIn} alone remains only a correctness/fd
     * bound (I2).
     *
     * <p>The denominator is {@code mergePerStreamBytes} (a page-run packed-page estimate).
     * This is the <em>static</em> config-level bound; the actual merge additionally applies a
     * <em>runtime</em> clamp at merge entry ({@link io.varve.swath.output.sorted.SortedDatasetCoordinator}) against the process fd limit and the
     * largest per-segment encoded {@code maxRecordLen}, the decoded-page maximum, and the runtime
     * aggregate decoded-page guard.
     *
     * <p><b>The {@code max(2, …)} floor is documented, not rejected:</b> a merge pass needs at least
     * 2 streams to merge anything, so this never returns fewer than 2 regardless of how tiny
     * {@code mergeBudgetBytes} is. Below two configured stream prices, the planning arithmetic
     * intentionally exceeds the requested budget at this static configuration layer. Runtime merge
     * admission refuses resumably when that minimum safe width does not fit actual page residency.
     */
    public int effectiveFanIn() {
        long budgetBound = mergeBudgetBytes() / mergePerStreamBytes();
        return (int) Math.min(fanIn(), Math.max(2L, budgetBound));
    }

    /** {@code max(floor, heapFraction × maxMemory)} — the §7 heap-adaptive default. */
    static long adaptiveSegmentBytes(double heapFraction) {
        long adaptive = (long) (heapFraction * Runtime.getRuntime().maxMemory());
        return Math.max(SEGMENT_BYTES_FLOOR, adaptive);
    }

    private static long longProp(UnaryOperator<String> lookup, String name, long dflt) {
        String v = lookup.apply(PREFIX + name);
        return v == null ? dflt : Long.parseLong(v.trim());
    }

    private static int intProp(UnaryOperator<String> lookup, String name, int dflt) {
        String v = lookup.apply(PREFIX + name);
        return v == null ? dflt : Integer.parseInt(v.trim());
    }

    private static double doubleProp(UnaryOperator<String> lookup, String name, double dflt) {
        String v = lookup.apply(PREFIX + name);
        return v == null ? dflt : Double.parseDouble(v.trim());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SortConfig that)) {
            return false;
        }
        return stagingBuffering.equals(that.stagingBuffering)
                && merge.equals(that.merge)
                && finalOutput.equals(that.finalOutput)
                && retention.equals(that.retention);
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash = 31 * hash + Long.hashCode(segmentBytes());
        hash = 31 * hash + Long.hashCode(segmentEntries());
        hash = 31 * hash + Double.hashCode(heapFraction());
        hash = 31 * hash + Integer.hashCode(buffers());
        hash = 31 * hash + Integer.hashCode(fanIn());
        hash = 31 * hash + Long.hashCode(finalFileBytes());
        hash = 31 * hash + Long.hashCode(finalRowGroupBytes());
        hash = 31 * hash + Integer.hashCode(finalPageRows());
        hash = 31 * hash + Long.hashCode(mergeBudgetBytes());
        hash = 31 * hash + Integer.hashCode(mergeParallelism());
        hash = 31 * hash + Long.hashCode(mergePerStreamBytes());
        hash = 31 * hash + segmentCodec().hashCode();
        return 31 * hash + stagingRetention().hashCode();
    }

    @Override
    public String toString() {
        return "SortConfig[segmentBytes=" + segmentBytes()
                + ", segmentEntries=" + segmentEntries()
                + ", heapFraction=" + heapFraction()
                + ", buffers=" + buffers()
                + ", fanIn=" + fanIn()
                + ", finalFileBytes=" + finalFileBytes()
                + ", finalRowGroupBytes=" + finalRowGroupBytes()
                + ", finalPageRows=" + finalPageRows()
                + ", mergeBudgetBytes=" + mergeBudgetBytes()
                + ", mergeParallelism=" + mergeParallelism()
                + ", mergePerStreamBytes=" + mergePerStreamBytes()
                + ", segmentCodec=" + segmentCodec()
                + ", stagingRetention=" + stagingRetention() + ']';
    }

    private record StagingBuffering(long segmentBytes, long segmentEntries, double heapFraction,
                                    int buffers, PageCompression codec) { }

    private record Merge(int fanIn, long budgetBytes, int parallelism, long perStreamBytes) { }

    private record FinalOutput(long fileBytes, long rowGroupBytes, int pageRows) { }

    private record Retention(StagingRetention staging) { }
}
