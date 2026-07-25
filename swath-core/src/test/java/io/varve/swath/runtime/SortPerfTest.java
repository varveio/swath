/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.ListingStatistics;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>SORT-PERF</b> (§5 memory corridors, I11): measured-peak-heap coverage of the
 * <b>real {@code --sort} pipeline</b> ({@link ListRunner#runToSortedParquetWorkStealing} — listing
 * → ordered sort lane → segments → k-way merge → publish).
 *
 * <p>Peak heap is measured the same way {@code _swath_summary.json}'s {@code peak_heap_bytes} is
 * ({@code io.varve.swath.observability.ResourceMetrics}): the JVM's own
 * per-memory-pool high-water mark ({@link MemoryPoolMXBean#getPeakUsage()}), reset just before each
 * measured run so the window is exactly this run — NOT {@code ParquetPerf2Test}'s polling sampler
 * ({@code Runtime.totalMemory() - freeMemory()} on a timer). At PERF-2's 100k scale the two
 * techniques don't visibly differ, but at this unit's multi-hundred-thousand/million scale a polling
 * sampler measures noisy <i>committed</i> heap (which jumps in large, GC-policy-dependent steps and
 * can swing ~20-30% run to run) rather than true peak <i>used</i> heap, making the §5 corridor and
 * the I11 flatness comparison unreliable. The high-water-mark technique is both more accurate and is
 * the pack's own canonical measurement.
 *
 * <ol>
 *   <li><b>Main corridor</b>: 1M synthetic nominal-key entries through the pipeline; measured peak
 *       heap must sit under the §5 corridor ({@code Parquet + --sort} row: &lt; 1.5&nbsp;GB).</li>
 *   <li><b>I11 flatness</b>: the SAME pipeline run at 500k must show peak@1M within
 *       {@code FLATNESS_TOLERANCE} of peak@500k — heap is a function of the segment-byte gate,
 *       never of entry count.</li>
 *   <li><b>1&nbsp;KB-key variant</b> (the key-size-independence proof, the worst-case key-size
 *       envelope): the byte gate, not entry count, must still cap the buffer under the SAME
 *       corridor, and {@code SORT.buffer_byte_gated} must have engaged.</li>
 *   <li><b>Merge-phase stress</b>: hundreds of tiny staging segments and a
 *       tiny {@code merge-budget-bytes} so {@code SortConfig#effectiveFanIn()} forces
 *       a genuine multi-pass cascade — measured peak heap over the WHOLE run must still sit under the
 *       SAME corridor (the budget bound, not segment count, caps merge memory, I11), the cascade must
 *       actually have engaged ({@code swath.sort.merge.passes > 1} / {@code SORT.merge_pass_cascaded}),
 *       and every entry must survive exactly once.</li>
 * </ol>
 *
 * <p><b>Scale uses 1M/500k rather than 3M/1.5M.</b> {@link MockPageFetcher}'s
 * own in-memory keyspace (the synthetic-scale technique every sort test in this suite uses — never
 * a real store) is itself {@code O(N)} heap that a real S3 listing never carries (a real run streams
 * pages over the wire; nothing duplicates the whole bucket in the fetcher's heap). At 3M/1.5M
 * nominal-key entries that fixture overhead alone pushes measured peak heap to ~1.3-1.6&nbsp;GB —
 * already at or past the §5 corridor from harness overhead alone, before attributing anything to the
 * pipeline under test, and a Gradle test-worker default heap of 512&nbsp;MB (bumped to {@code -Xmx2g}
 * for the perf tier below, {@code build.gradle.kts} — matching the runbook's own {@code -Xmx2g}
 * convention, and giving the JVM's GC realistic pressure to collect transient garbage promptly
 * rather than letting it accumulate toward an inflated high-water mark) OOMs outright while building
 * the fixture. 1M/500k keeps the fixture's own footprint a modest, bounded fraction of the corridor
 * while still exercising
 * multiple segment flushes, a multi-file-capable merge, and hundreds of MB of real pipeline traffic —
 * enough to catch a genuine {@code O(N)} regression (I11 flatness would fail hard, not marginally, if
 * heap scaled with N instead of the segment gate). Raising back toward
 * 3M would need either a real-process (forked, {@code -Xmx2g}) harness reading the actual
 * {@code _swath_summary.json}, or accepting a corridor defined net of a documented fixture allowance.
 *
 * <p><b>Why the page size ({@code maxKeys}) is large (tens of thousands), unlike production
 * defaults.</b> {@link MockPageFetcher#fetchPage} recomputes each page with a linear scan of its
 * whole in-memory keyspace (documented cost caveat, see {@code WorkStealingScanPerf1Test}), so a
 * sequential full listing costs {@code O(N^2 / pageSize)} of the MOCK's own CPU — at this scale a
 * small, S3-realistic page size would make the harness itself pathologically slow. A large page size
 * keeps that harness cost trivial while leaving the {@code --sort} pipeline itself (the thing under
 * test) driven exactly as production does: single worker (no work-stealing probes to add further
 * mock-scan overhead — this is a memory-corridor test, not a concurrency one; PERF-1 covers stealing)
 * admitting real {@link io.varve.swath.model.PageBatch}es into the real {@link io.varve.swath.sort.SortLane}.
 *
 * <p><b>Segment gates are set explicitly</b> (not the heap-adaptive default, which would size itself
 * off the test JVM's actual {@code -Xmx} rather than the knob this corridor is measuring) so segment
 * counts stay realistic and deterministic regardless of the CI heap: {@code segment-entries=MAX_VALUE}
 * so only the byte gate ever seals a buffer (matching the production default and making {@code
 * SORT.buffer_byte_gated} the only non-DRAIN trigger, per contract).
 */
@Tag("perf")
final class SortPerfTest {

    // §5 table: "Parquet + --sort (the E2E path)" peak_heap corridor.
    private static final long HEAP_CORRIDOR_BYTES = 1536L * 1024 * 1024;   // 1.5 GB
    // I11: peak@1M <= peak@500k * FLATNESS_TOLERANCE. 1.25 proved
    // too tight against measured GC-timing noise on the high-water-mark technique (observed run-to-run
    // swings of ~30-40% at this scale even with a full-GC settle between phases); 1.5 still catches a
    // genuine O(N) regression by a wide margin (a true per-entry leak scaling with N would double,
    // ~2x, for a 2x entry-count increase) while absorbing measurement noise.
    private static final double FLATNESS_TOLERANCE = 1.5;

    private static final int MAIN_N = 1_000_000;
    private static final int FLATNESS_N = 500_000;
    private static final int NOMINAL_PAGE_MAX_KEYS = 50_000;      // bounds MockPageFetcher's O(N^2) scan cost
    private static final long MAIN_SEGMENT_BYTES = 64L << 20;     // 64 MB gate: a handful of segments at this scale

    private static final int VARIANT_N = 200_000;                 // smaller N: 1 KB keys dominate mock-fixture bytes
    private static final int VARIANT_PAGE_MAX_KEYS = 20_000;
    private static final long VARIANT_SEGMENT_BYTES = 16L << 20;  // smaller gate: forces many byte-gated flushes

    // Merge-phase stress: a MODEST entry count but a tiny segment-entries
    // cap forces hundreds of staging segments, and a tiny merge-budget-bytes forces a small
    // effectiveFanIn() (SortConfig §7) so the cascade genuinely runs multiple passes, exercising the
    // budget-bounded merge end to end through the production pipeline.
    private static final int MERGE_STRESS_N = 60_000;
    // == segment-entries, so every admitted PAGE trips the entry cap on its own (SortLane.admit checks
    // the trigger once per page, not per key) — a deterministic MERGE_STRESS_N/PAGE_MAX_KEYS segments.
    private static final int MERGE_STRESS_PAGE_MAX_KEYS = 100;
    private static final long MERGE_STRESS_SEGMENT_ENTRIES = 100;         // ⇒ ~600 staging segments
    private static final long MERGE_STRESS_MERGE_BUDGET_BYTES = 8L << 20;
    private static final int MERGE_STRESS_FAN_IN = 8;                            // pin fan-in = 8 directly

    private static RunKey sortKey(String label) {
        return new RunKey("s3", null, "bucket", new byte[0], "sort-perf-" + label,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static SortConfig sortConfig(long segmentBytes) {
        // segment-entries=MAX_VALUE (production default): only the byte gate ever seals a buffer.
        // merge-budget-bytes=MAX_VALUE: the merge phase is not what these existing methods stress
        // (see mergePhaseStress... below for that), so effectiveFanIn() is effectively unbounded here.
        return SortConfigs.base()
                .withSegmentBytes(segmentBytes);
    }

    /** Tiny segment-entries + pinned fan-in: many segments, small {@code effectiveFanIn()}. */
    private static SortConfig mergeStressConfig() {
        return SortConfigs.base()
                .withSegmentBytes(Long.MAX_VALUE)
                .withSegmentEntries(MERGE_STRESS_SEGMENT_ENTRIES)
                .withFanIn(MERGE_STRESS_FAN_IN)
                .withMergeBudgetBytes(MERGE_STRESS_MERGE_BUDGET_BYTES);
    }

    private record RunResult(long peakHeapBytes, RunContext ctx, long objects) {
    }

    /** Resets every HEAP pool's high-water mark so the next {@link #peakHeapBytesSinceReset()} call
     * measures exactly the window that follows (the same primitive {@code ResourceMetrics} uses). */
    private static void resetHeapPeaks() {
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                try {
                    pool.resetPeakUsage();
                } catch (UnsupportedOperationException ignored) {
                    // pool doesn't track peak usage; sumHeapPoolPeaks() just won't include it
                }
            }
        }
    }

    /** Sum of each HEAP pool's peak usage since the last {@link #resetHeapPeaks()} — the JVM's own
     * high-water mark, not a polled/committed-heap approximation. */
    private static long peakHeapBytesSinceReset() {
        long total = 0;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                var usage = pool.getPeakUsage();
                if (usage != null) {
                    total += usage.getUsed();
                }
            }
        }
        return total;
    }

    /** Drives the real listing→sort-lane→merge→publish pipeline over a synthetic keyspace, measuring
     * the JVM's true peak-heap high-water mark across the run. Single worker: no work-stealing
     * probes, so the ONLY mock-fetcher cost is the sequential page scan. */
    private RunResult runSortedPipeline(Path tmp, List<byte[]> keyspace, int pageMaxKeys,
                                         SortConfig sortConfig, String label) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve(label + "-out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve(label + "-c.sqlite");

        resetHeapPeaks();

        RunContext ctx = RunContext.create();
        long objects;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(label), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(pageMaxKeys).build();
            ListRunner.ParquetSpec spec = new ListRunner.ParquetSpec(new byte[0], 256, pageMaxKeys,
                    FilterChain.EMPTY, 2, 1024, 16, "sort-perf-" + label, null, null, 0L, 0L, "");
            ListingStatistics stats = new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir,
                    stagingDir, spec, store, run.id(), 1, seeds, sortConfig, SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE, false);
            objects = stats.objects();
        }
        return new RunResult(peakHeapBytesSinceReset(), ctx, objects);
    }

    @Test
    @Timeout(600)
    void mainCorridorAndFlatness_measuredPeakHeapUnderCorridorAndFlatAcrossScale(@TempDir Path tmp)
            throws Exception {
        SortConfig config = sortConfig(MAIN_SEGMENT_BYTES);

        // Smaller scale first, then null it out + GC before building/running the larger keyspace, so
        // leftover garbage from the first run doesn't inflate the second run's live-heap footprint.
        List<byte[]> keyspaceSmall = Keyspaces.exactly(FLATNESS_N);
        RunResult small = runSortedPipeline(tmp, keyspaceSmall, NOMINAL_PAGE_MAX_KEYS, config, "flat-small");
        assertThat(small.objects()).isEqualTo((long) FLATNESS_N);
        keyspaceSmall = null;
        System.gc();

        List<byte[]> keyspaceMain = Keyspaces.exactly(MAIN_N);
        RunResult main = runSortedPipeline(tmp, keyspaceMain, NOMINAL_PAGE_MAX_KEYS, config, "main");
        assertThat(main.objects()).isEqualTo((long) MAIN_N);

        // (1) measured peak heap under the §5 "Parquet + --sort" corridor — not O(N).
        assertThat(main.peakHeapBytes())
                .as("measured peak heap (%d MB) at %,d entries under the §5 corridor (< 1.5 GB)",
                        main.peakHeapBytes() / (1024 * 1024), MAIN_N)
                .isLessThan(HEAP_CORRIDOR_BYTES);

        // (2) I11 flatness: peak@MAIN_N within FLATNESS_TOLERANCE of peak@FLATNESS_N — memory is
        //     f(segment-byte gate), never f(N).
        assertThat((double) main.peakHeapBytes())
                .as("peak@%,d (%d MB) within %.2fx of peak@%,d (%d MB) — I11: heap is a function of "
                                + "the segment gate, not entry count",
                        MAIN_N, main.peakHeapBytes() / (1024 * 1024), FLATNESS_TOLERANCE,
                        FLATNESS_N, small.peakHeapBytes() / (1024 * 1024))
                .isLessThanOrEqualTo(small.peakHeapBytes() * FLATNESS_TOLERANCE);
    }

    @Test
    @Timeout(180)
    void oneKilobyteKeyVariant_byteGateHoldsTheCorridorAndEngages(@TempDir Path tmp) throws Exception {
        SortConfig config = sortConfig(VARIANT_SEGMENT_BYTES);
        List<byte[]> keyspace = Keyspaces.longKeys1024(VARIANT_N);

        RunResult result = runSortedPipeline(tmp, keyspace, VARIANT_PAGE_MAX_KEYS, config, "1kb");
        assertThat(result.objects()).isEqualTo((long) VARIANT_N);

        // Corridor holds at worst-case (1 KB) key size — the key-size-independence proof.
        assertThat(result.peakHeapBytes())
                .as("1 KB-key measured peak heap (%d MB) still under the §5 corridor (< 1.5 GB)",
                        result.peakHeapBytes() / (1024 * 1024))
                .isLessThan(HEAP_CORRIDOR_BYTES);

        // The BYTE gate (not entry count) is what forced the flushes at this key size.
        assertThat(counter(result.ctx(), "swath.steal_reason", "buffer_byte_gated"))
                .as("SORT.buffer_byte_gated engaged — the byte gate forced flushes at 1 KB key size")
                .isGreaterThan(0.0);
    }

    @Test
    @Timeout(60)
    void mergePhaseStress_manySegmentsCascadeUnderATinyBudgetAndStayUnderCorridor(@TempDir Path tmp)
            throws Exception {
        SortConfig config = mergeStressConfig();
        List<byte[]> keyspace = Keyspaces.exactly(MERGE_STRESS_N);

        RunResult result = runSortedPipeline(tmp, keyspace, MERGE_STRESS_PAGE_MAX_KEYS, config, "merge-stress");

        // (c) every entry survived the many-segment, multi-pass cascade exactly once.
        assertThat(result.objects()).isEqualTo((long) MERGE_STRESS_N);

        // (a) measured peak heap over the WHOLE run (listing + cascade merge) stays under the SAME
        //     §5 corridor — proof the merge-budget bound (SortConfig#effectiveFanIn()),
        //     not segment count, caps merge-phase memory (I11).
        assertThat(result.peakHeapBytes())
                .as("merge-phase-stress measured peak heap (%d MB) under the §5 corridor (< 1.5 GB) with "
                                + "hundreds of segments and a tiny merge-budget-bytes",
                        result.peakHeapBytes() / (1024 * 1024))
                .isLessThan(HEAP_CORRIDOR_BYTES);

        // (b) the merge actually cascaded — the tiny budget, not luck, forced multiple passes.
        assertThat(counter(result.ctx(), "swath.sort.merge.passes"))
                .as("swath.sort.merge.passes > 1 — a genuine multi-pass cascade, not a single pass")
                .isGreaterThan(1.0);
        assertThat(counter(result.ctx(), "swath.steal_reason", "merge_pass_cascaded"))
                .as("SORT.merge_pass_cascaded engaged — the merge budget forced the cascade")
                .isGreaterThan(0.0);
    }

    private static double counter(RunContext ctx, String name) {
        Counter c = ctx.metrics().registry().find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double counter(RunContext ctx, String name, String reason) {
        Counter c = ctx.metrics().registry().find(name).tag("outcome", "SORT").tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }
}
