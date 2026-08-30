/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

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
 *       consumed by the pipeline. Must be {@code >= 2}: {@link SortLane} bounds
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
 *   <li>{@code mergePerStreamBytes} — the configured planning price for one OPEN merge stream,
 *       and the denominator of the serial page-run merge's {@link #effectiveFanIn()}. A
 *       {@link PageFrontierReader} retains packed page bodies per open stream, so the default uses
 *       64&nbsp;KiB as a capacity floor. The runtime merge-entry clamp ({@link SortTransform})
 *       raises that price to two maximum encoded bodies plus the type-3 decoded-page maximum.
 *       Legacy inputs retain the configured floor and are guarded against their actual header
 *       claims immediately before allocation.</li>
 *   <li>{@code mergeBudgetBytes} — the merge-phase residency budget: covers exact parallel
 *       proof-spool backing first, caps how many segment streams a single
 *       {@link KWayMerge} pass may hold open, and bounds page-aware decoded overlap state, via
 *       {@link #effectiveFanIn()} = {@code min(fanIn, max(2, mergeBudgetBytes / mergePerStreamBytes))}
 *       — keeping planned open-stream capacity a function of the budget, never of segment count
 *       (I11), even when {@code fanIn} alone would allow more streams. Same heap-adaptive shape as
 *       {@code segmentBytes} by default (floor 64&nbsp;MB). The {@code max(2, …)} floor is a
 *       documented merge-width floor, not a claim that two streams consume exactly twice the
 *       configured price.</li>
 *   <li>{@code segmentCodec} — the codec {@link PageBlock#pack} compresses a page's
 *       front-coded PAYLOAD at pack time ({@code NONE}, {@code LZ4}, or {@code ZSTD1}; default
 *       {@code LZ4}) — the record HEADER (min/max, dict
 *       tables, {@code useDict}, count, ordered-bit) is never compressed, so
 *       {@link PageFrontierReader} keeps parsing it without decompressing. Threaded to
 *       {@link PageBlock#pack} at the one seal call site ({@link SortBuffer#admit}).</li>
 *   <li>{@code stagingRetention} — whether a successful live sorted publish keeps its original
 *       checkpoint-tracked page-run inputs for diagnostics. Cascade intermediates and temporary
 *       files remain disposable in either mode.</li>
 *   <li>{@code mergeBoundaryPolicy} — how an admitted parallel final merge chooses raw-key range
 *       boundaries. The default {@link MergeBoundaryPolicy#DISTINCT} preserves the shipped output
 *       partitioning; {@link MergeBoundaryPolicy#ROWS} is an explicit measurement arm.</li>
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
     * single open page-run merge stream holds (one packed page body, retained for CRC by
     * {@link PageFrontierReader}). Documented as an estimate to be validated at the perf gate.
     */
    public static final long DEFAULT_MERGE_PER_STREAM_BYTES = 64L * 1024;

    /**
     * Default for {@code segmentCodec}: compress-at-pack is ON by default ({@link
     * PageCodec#LZ4}) — staging disk exhaustion ({@code sort_disk_exhausted}) is the primary
     * staging-disk risk, and compressing the page-run PAYLOAD (never the plain header) directly
     * cuts both staging disk and in-flight buffered memory.
     */
    static final PageCodec DEFAULT_SEGMENT_CODEC = PageCodec.LZ4;

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

    /** Resume-free typed tune key for parallel merge range-boundary selection. */
    public static final String MERGE_BOUNDARY_POLICY_TUNE_KEY = "sort.merge-boundary-policy";

    /** JVM-property form of {@link #MERGE_BOUNDARY_POLICY_TUNE_KEY}. */
    public static final String MERGE_BOUNDARY_POLICY_PROPERTY =
            "swath." + MERGE_BOUNDARY_POLICY_TUNE_KEY;

    /** Resume-free selector for the range and reader-router-encoder finalization paths. */
    public static final String FINALIZATION_TUNE_KEY = "sort.finalization";

    /** JVM-property form of {@link #FINALIZATION_TUNE_KEY}. */
    public static final String FINALIZATION_PROPERTY = "swath." + FINALIZATION_TUNE_KEY;

    /**
     * Default for {@code minParallelStagedBytes}: staged bytes below which the merge stays serial no
     * matter what {@code merge-parallelism} says.
     *
     * <p>Splitting has a fixed price the speedup must earn back — the output gains at least one part
     * per range, which is consumer-visible and permanent, and the path pays a boundary-sampling pass
     * over every segment before any range starts. Measured serial merge throughput is ~20-25 MB of
     * staging per second, so 256 MiB is roughly ten seconds of serial merge: below it a 3-4x speedup
     * saves a few seconds and multiplies the file count, which is a bad trade for whoever reads the
     * output. This is why an ordinary small sorted run still publishes the single file it always did.
     */
    public static final long DEFAULT_MIN_PARALLEL_STAGED_BYTES = 256L * 1024 * 1024;

    /**
     * The number of contiguous key ranges the final merge is split into, each merged on its own
     * thread producing a separate ordered part file whose concatenation (in range order) is the
     * global sort ({@link ParallelRangeMerge}).
     *
     * <p><b>Why this is on by default.</b> The listing phase parallelises across cores and a serial
     * merge does not, so the merge's share of a sorted run's wall clock RISES with core count —
     * measured at 72 % of an 823 M-object run on 32 cores. Amdahl then caps any further core increase
     * at ~1.4×, which makes the serial merge the binding constraint on every large sorted run.
     * Measured end-to-end across five public buckets from 9.9 M to 823 M objects: 3.19×–4.09× on the
     * merge phase, 2.18× on total wall clock at the top end, peak heap within +2.4 %, output
     * byte-identical to the serial merge and carrying the identical completeness stamp.
     *
     * <p><b>Why {@code cores/2}, capped at 8.</b> Parallel efficiency falls off well before core
     * count: measured 68 % at R=4, 51 % at R=8, 34 % at R=16, while peak heap and read amplification
     * keep climbing past the throughput. R=8 buys 74 % of R=16's speedup for 63 % of its heap, so 8
     * is the ceiling and half the cores is the ramp for smaller machines. Defaulting to
     * {@code availableProcessors()} would land on the worst point of the curve on a large box.
     *
     * <p>{@link MergePlanner#effectiveRanges} clamps this further per run — down to 1, meaning
     * the untouched serial path — whenever the merge budget, the staged-segment count or the
     * descriptor budget cannot carry it. So this value is a ceiling, never a promise.
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
                    DEFAULT_MERGE_PER_STREAM_BYTES, DEFAULT_MIN_PARALLEL_STAGED_BYTES,
                    MergeBoundaryPolicy.DISTINCT, SortFinalization.RANGES),
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
            // SortLane's off-thread capacity is buffers()-1 (the fill buffer is the
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
        if (minParallelStagedBytes() < 0) {
            throw new IllegalArgumentException(
                    "min-parallel-staged-bytes must be >= 0, got " + minParallelStagedBytes());
        }
        if (stagingRetention() == null) {
            throw new IllegalArgumentException("staging-retention must not be null");
        }
        if (mergeBoundaryPolicy() == null) {
            throw new IllegalArgumentException("merge-boundary-policy must not be null");
        }
        if (finalization() == null) {
            throw new IllegalArgumentException("finalization must not be null");
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
        return copy(new Merge(fanIn, mergeBudgetBytes(), mergeParallelism(), mergePerStreamBytes(),
                minParallelStagedBytes(), mergeBoundaryPolicy(), finalization()));
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
        return copy(new Merge(fanIn(), mergeBudgetBytes, mergeParallelism(), mergePerStreamBytes(),
                minParallelStagedBytes(), mergeBoundaryPolicy(), finalization()));
    }

    public SortConfig withMergeParallelism(int mergeParallelism) {
        return copy(new Merge(fanIn(), mergeBudgetBytes(), mergeParallelism, mergePerStreamBytes(),
                minParallelStagedBytes(), mergeBoundaryPolicy(), finalization()));
    }

    public SortConfig withMergePerStreamBytes(long mergePerStreamBytes) {
        return copy(new Merge(fanIn(), mergeBudgetBytes(), mergeParallelism(), mergePerStreamBytes,
                minParallelStagedBytes(), mergeBoundaryPolicy(), finalization()));
    }

    public SortConfig withSegmentCodec(PageCodec segmentCodec) {
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
        int mergeParallelism = intProp(lookup, "merge-parallelism", DEFAULT.mergeParallelism());
        long mergePerStreamBytes = longProp(lookup, "merge-per-stream-bytes", DEFAULT.mergePerStreamBytes());
        long minParallelStagedBytes =
                longProp(lookup, "min-parallel-staged-bytes", DEFAULT.minParallelStagedBytes());
        String segmentCodecProp = lookup.apply(PREFIX + "segment-codec");
        PageCodec segmentCodec = segmentCodecProp == null
                ? DEFAULT.segmentCodec()
                : parseSegmentCodec(segmentCodecProp);
        String keepStagingProp = lookup.apply(KEEP_STAGING_PROPERTY);
        StagingRetention stagingRetention = keepStagingProp == null
                ? DEFAULT.stagingRetention()
                : StagingRetention.fromProperty(KEEP_STAGING_PROPERTY, keepStagingProp);
        String boundaryPolicyProp = lookup.apply(MERGE_BOUNDARY_POLICY_PROPERTY);
        MergeBoundaryPolicy mergeBoundaryPolicy = boundaryPolicyProp == null
                ? DEFAULT.mergeBoundaryPolicy()
                : MergeBoundaryPolicy.fromConfigValue(
                        MERGE_BOUNDARY_POLICY_PROPERTY, boundaryPolicyProp);
        String finalizationProp = lookup.apply(FINALIZATION_PROPERTY);
        SortFinalization finalization = finalizationProp == null
                ? DEFAULT.finalization()
                : SortFinalization.fromConfigValue(FINALIZATION_PROPERTY, finalizationProp);
        return new SortConfig(
                new StagingBuffering(segmentBytes, segmentEntries, heapFraction, buffers, segmentCodec),
                new Merge(fanIn, mergeBudgetBytes, mergeParallelism, mergePerStreamBytes,
                        minParallelStagedBytes, mergeBoundaryPolicy, finalization),
                new FinalOutput(finalFileBytes, finalRowGroupBytes, finalPageRows),
                new Retention(stagingRetention));
    }

    public SortConfig withMinParallelStagedBytes(long minParallelStagedBytes) {
        return copy(new Merge(fanIn(), mergeBudgetBytes(), mergeParallelism(), mergePerStreamBytes(),
                minParallelStagedBytes, mergeBoundaryPolicy(), finalization()));
    }

    public SortConfig withStagingRetention(StagingRetention stagingRetention) {
        return copy(new Retention(stagingRetention));
    }

    public SortConfig withMergeBoundaryPolicy(MergeBoundaryPolicy mergeBoundaryPolicy) {
        return copy(new Merge(fanIn(), mergeBudgetBytes(), mergeParallelism(), mergePerStreamBytes(),
                minParallelStagedBytes(), mergeBoundaryPolicy, finalization()));
    }

    public SortConfig withFinalization(SortFinalization finalization) {
        return copy(new Merge(fanIn(), mergeBudgetBytes(), mergeParallelism(), mergePerStreamBytes(),
                minParallelStagedBytes(), mergeBoundaryPolicy(), finalization));
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

    public PageCodec segmentCodec() {
        return stagingBuffering.codec();
    }

    public long minParallelStagedBytes() {
        return merge.minParallelStagedBytes();
    }

    public StagingRetention stagingRetention() {
        return retention.staging();
    }

    public MergeBoundaryPolicy mergeBoundaryPolicy() {
        return merge.boundaryPolicy();
    }

    public SortFinalization finalization() {
        return merge.finalization();
    }

    private static PageCodec parseSegmentCodec(String raw) {
        try {
            return PageCodec.fromConfigValue(raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("swath.sort.segment-codec: " + e.getMessage(), e);
        }
    }

    /**
     * The <b>static</b> budget-bounded merge fan-in: {@code min(fanIn,
     * max(2, mergeBudgetBytes / mergePerStreamBytes))}. {@link KWayMerge} opens at most this many streams
     * per pass, so planned open-stream capacity remains a function of the budget knob, never of how
     * many segments a run happens to produce (I11). This result is only the static config-level
     * advisory; runtime planning additionally prices retained encoded/decoded page state and
     * rejects a minimum width that cannot fit. {@code fanIn} alone remains only a correctness/fd
     * bound (I2).
     *
     * <p>The denominator is {@code mergePerStreamBytes} (a page-run packed-page estimate).
     * This is the <em>static</em> config-level bound; the actual merge additionally applies a
     * <em>runtime</em> clamp at merge entry ({@link SortTransform}) against the process fd limit and the
     * largest per-segment encoded {@code maxRecordLen}, the type-3 decoded-page maximum, exact proof
     * backing, and the runtime aggregate decoded-page guard.
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
        hash = 31 * hash + Long.hashCode(minParallelStagedBytes());
        hash = 31 * hash + stagingRetention().hashCode();
        hash = 31 * hash + mergeBoundaryPolicy().hashCode();
        return 31 * hash + finalization().hashCode();
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
                + ", minParallelStagedBytes=" + minParallelStagedBytes()
                + ", stagingRetention=" + stagingRetention()
                + ", mergeBoundaryPolicy=" + mergeBoundaryPolicy()
                + ", finalization=" + finalization() + ']';
    }

    private record StagingBuffering(long segmentBytes, long segmentEntries, double heapFraction,
                                    int buffers, PageCodec codec) { }

    private record Merge(int fanIn, long budgetBytes, int parallelism, long perStreamBytes,
                         long minParallelStagedBytes, MergeBoundaryPolicy boundaryPolicy,
                         SortFinalization finalization) { }

    private record FinalOutput(long fileBytes, long rowGroupBytes, int pageRows) { }

    private record Retention(StagingRetention staging) { }
}
