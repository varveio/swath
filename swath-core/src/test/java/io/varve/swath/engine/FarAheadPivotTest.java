/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.SwathException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.ApiCallBudget;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.SeedSteps;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The far-ahead interpolated pivot — the independent, adversarial guard of the ENGINE-side
 * contract for the dense-tail residual shape. Complements the pure-math sweep in
 * {@link InterpolatePropertyTest} with the three observable engine behaviors:
 *
 * <ol start="3">
 *   <li><b>Density → fraction:</b> a uniformly-dense drainer maps to {@code densityFraction() == 0.75}
 *       (far ahead); a region thinning ahead eases toward 0.5; no page recorded pins exactly 0.5 — and
 *       the fraction is always clamped into {@code [0.5, 0.75]}.</li>
 *   <li><b>Far-ahead actually places the split further ahead — deterministic, no latency:</b> a single
 *       {@link Thief#steal} against a dense victim with high trailing density lands the child boundary
 *       at ~0.75 of {@code (cursor, H]}, materially past the ~0.5 a plain {@code byteMidpoint} would —
 *       the observable proxy for "pivot placed far ahead of a fast drainer."</li>
 *   <li><b>No regression:</b> an end-to-end scan of the dense mega-day shape stays a valid partition
 *       (byte-exact, no gap/no overlap) and does not blow the INT-8 API budget or storm structure
 *       probes.</li>
 * </ol>
 *
 * <p>Deterministic: {@link MockPageFetcher}, no real S3, no wall-clock dependence.
 */
final class FarAheadPivotTest {

    private static final long RUN_ID = 7L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // =========================================================================
    // (3) Density digest -> far-ahead fraction.
    // =========================================================================

    /**
     * A uniformly-dense drainer — the trailing page's local density equals the range's overall
     * density (ratio == 1) — must return the maximum far-ahead fraction 0.75 EXACTLY, so the split
     * lands 3/4 of the way toward {@code hi}, far ahead of the cursor.
     */
    @Test
    void uniformlyDenseDrainerYieldsMaxFarAheadFraction() {
        byte[] lo = b("2022/03/05/10000000");
        byte[] cursor = b("2022/03/05/50000000");
        byte[] hi = b("2022/03/05/99999999");
        WorkerState w = WorkerStates.of(1, lo, cursor, hi);
        w.setCursor(cursor);
        w.addKeysEmitted(1000);
        // trailing page spans exactly the consumed (lo, cursor] with exactly the emitted count, so
        // trailing density == overall density (ratio == 1) — the uniformly-dense residual shape.
        w.recordPage(lo, cursor, 1000);

        assertThat(w.densityFraction())
                .as("uniformly-dense drainer -> far-ahead fraction 0.75 (place split 3/4 toward hi)")
                .isEqualTo(0.75);
    }

    /**
     * A region <b>thinning ahead</b> — the trailing digest is sparser than the range average
     * (ratio &lt; 1) — must ease the fraction back toward the plain midpoint (strictly below 0.75,
     * still above 0.5), so a thinning tail is not over-split far ahead into empty space.
     */
    @Test
    void thinningAheadEasesFractionTowardMidpoint() {
        byte[] lo = b("2022/03/05/10000000");
        byte[] cursor = b("2022/03/05/50000000");
        byte[] hi = b("2022/03/05/99999999");
        WorkerState w = WorkerStates.of(1, lo, cursor, hi);
        w.setCursor(cursor);
        w.addKeysEmitted(1000);
        // trailing density well below the overall average (50 keys where the average implies ~1000):
        // the region just listed is thinning, so the far-ahead push relaxes toward 0.5.
        w.recordPage(lo, cursor, 50);

        double f = w.densityFraction();
        assertThat(f).as("thinning-ahead fraction eases toward 0.5").isGreaterThan(0.5).isLessThan(0.75);
    }

    /**
     * The MAX clamp: a trailing region DENSER than the range average (ratio &gt; 1) must not push the
     * fraction past 0.75 — {@code densityFraction} clamps to {@code [0.5, 0.75]}.
     */
    @Test
    void denserThanAverageClampsAtMaxFraction() {
        byte[] lo = b("2022/03/05/10000000");
        byte[] cursor = b("2022/03/05/50000000");
        byte[] hi = b("2022/03/05/99999999");
        WorkerState w = WorkerStates.of(1, lo, cursor, hi);
        w.setCursor(cursor);
        w.addKeysEmitted(1000);
        w.recordPage(lo, cursor, 5000);   // 5x the average density -> ratio 5, must clamp at 0.75

        assertThat(w.densityFraction()).as("far-ahead fraction is clamped at 0.75").isEqualTo(0.75);
    }

    /** No page recorded yet -> exactly 0.5 (no density basis; interpolate degrades to byteMidpoint). */
    @Test
    void noPageRecordedIsExactlyHalf() {
        WorkerState w = WorkerStates.of(1, b("a"), b("m"), b("z"));
        w.setCursor(b("m"));
        w.addKeysEmitted(1000);
        assertThat(w.densityFraction()).as("no density signal -> plain midpoint 0.5").isEqualTo(0.5);
    }

    // =========================================================================
    // (4) The steal path places the child boundary far ahead of the cursor.
    // =========================================================================

    /**
     * With a bounded victim carrying high trailing density (fraction 0.75), one {@link Thief#steal}
     * over a dense uniform keyspace lands the child boundary at the 0.75 interpolation of
     * {@code (cursor, H]} — <b>materially past</b> the 0.5 pivot a plain {@code byteMidpoint} gives.
     * This is the observable proxy that a fast drainer cannot advance past the pivot before the CAS.
     */
    @Test
    void denseVictimSplitsFarAheadOfPlainMidpoint() throws SwathException, InterruptedException {
        byte[] lo = b("2022/03/05/10000000");
        byte[] cursor = b("2022/03/05/50000000");
        byte[] H = b("2022/03/05/99999999");

        byte[] farAhead = StealMath.interpolate(cursor, H, 0.75);   // expected child boundary
        byte[] plainMid = ByteMidpoint.between(cursor, H);          // what a 0.5 split would give
        // Sanity on the fixture: the far-ahead pivot IS strictly past the plain midpoint.
        assertThat(KeyBytes.compareUnsigned(plainMid, farAhead))
                .as("fixture: 0.75 pivot is strictly past the 0.5 midpoint").isLessThan(0);

        WorkerState victim = WorkerStates.of(1, lo, cursor, H);
        victim.setCursor(cursor);
        victim.addKeysEmitted(1000);
        victim.recordPage(lo, cursor, 1000);                       // uniformly dense -> fraction 0.75
        assertThat(victim.densityFraction()).isEqualTo(0.75);

        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        Thief thief = Thiefs.of(store, denseMegaDayFetcher(), RUN_ID, new byte[0], ListingMode.OBJECTS,
                (id, childLo, childHi) -> { });

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        byte[] pivot = store.lastSplit.pivot();
        assertThat(pivot).as("child boundary == the far-ahead 0.75 interpolation").isEqualTo(farAhead);
        assertThat(KeyBytes.compareUnsigned(plainMid, pivot))
                .as("far-ahead split lands materially further into (cursor, H] than byteMidpoint")
                .isLessThan(0);
        assertThat(victim.hi()).as("victim narrowed to the far-ahead pivot").isEqualTo(pivot);
    }

    /**
     * The contrast that isolates the CAUSE: the SAME victim/keyspace with NO density signal
     * (fraction 0.5) splits at the plain {@code byteMidpoint} — proving it is the density
     * fraction, not the fixture, that pushes the pivot forward in the test above.
     */
    @Test
    void victimWithoutDensitySignalSplitsAtPlainMidpoint() throws SwathException, InterruptedException {
        byte[] lo = b("2022/03/05/10000000");
        byte[] cursor = b("2022/03/05/50000000");
        byte[] H = b("2022/03/05/99999999");

        WorkerState victim = WorkerStates.of(1, lo, cursor, H);
        victim.setCursor(cursor);
        victim.addKeysEmitted(1000);                               // so it scores as a victim...
        // ...but NO recordPage -> no trailing-density signal -> fraction stays the plain 0.5.
        assertThat(victim.densityFraction()).isEqualTo(0.5);

        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        Thief thief = Thiefs.of(store, denseMegaDayFetcher(), RUN_ID, new byte[0], ListingMode.OBJECTS,
                (id, childLo, childHi) -> { });

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(store.lastSplit.pivot())
                .as("no density signal -> split at the plain code-point midpoint")
                .isEqualTo(ByteMidpoint.between(cursor, H));
    }

    /** A dense uniform mega-day directory: keys straddle the whole (cursor, H] so the far probe is non-empty. */
    private static PageFetcher denseMegaDayFetcher() {
        return MockPageFetcher.builder().keys(megaDayKeys()).build();
    }

    private static List<byte[]> megaDayKeys() {
        List<byte[]> keys = new ArrayList<>();
        for (int v = 10_000_000; v <= 99_000_000; v += 1_000_000) {
            keys.add(b("2022/03/05/%08d".formatted(v)));
        }
        keys.add(b("2022/03/05/99999999"));   // a key exactly at H so the far-ahead probe is non-empty
        return keys;
    }

    /**
     * A mega-day fetcher that, on the thief's (single {@code maxKeys==1}) probe, advances the victim's
     * in-memory cursor to {@code c2} — the exact seam the engine re-reads at lock-guarded revalidation
     * ({@code victim.cursor()}). This forces the {@code cursor_passed_pivot} race that far-ahead
     * placement fixes: the owner's cursor moves between the thief's snapshot and its revalidation.
     */
    private static MockPageFetcher advancingCursorFetcher(WorkerState victim, byte[] c2, AtomicBoolean once) {
        return MockPageFetcher.builder()
                .keys(megaDayKeys())
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    if (req.maxKeys() == 1 && once.compareAndSet(false, true)) {
                        victim.setCursor(c2);   // the owner committed a new cursor c2 mid-steal
                    }
                    return page;
                })
                .build();
    }

    // =========================================================================
    // (T3) The victim cursor ADVANCES between the thief's snapshot and its lock-revalidation — the
    //      real cursor_passed_pivot race that far-ahead placement fixes.
    // =========================================================================

    /**
     * While the thief probes, the owner advances the cursor to {@code c2} with
     * {@code plainMidpoint <= c2 < farAheadPivot}. A plain {@code 0.5} split would now revalidate to
     * {@code RETRY.cursor_passed_pivot} (the cursor passed the midpoint), but the far-ahead pivot is
     * still ahead of {@code c2}, so the split STILL commits at the far-ahead boundary
     * ({@code CHILD_CREATED} with {@code pivot > c2}).
     */
    @Test
    void farAheadPivotSurvivesCursorAdvancePastThePlainMidpoint() throws SwathException, InterruptedException {
        byte[] lo = b("2022/03/05/00000000");
        byte[] cursor = b("2022/03/05/10000000");
        byte[] H = b("2022/03/05/99999999");

        byte[] farAhead = StealMath.interpolate(cursor, H, 0.75);   // the committed far-ahead boundary
        byte[] plainMid = ByteMidpoint.between(cursor, H);
        byte[] c2 = ByteMidpoint.between(plainMid, farAhead);       // strictly plainMid < c2 < farAhead
        // Fixture sanity: the advanced cursor is strictly past the plain midpoint but before the far pivot.
        assertThat(KeyBytes.compareUnsigned(plainMid, c2)).as("c2 is strictly past the plain 0.5 midpoint")
                .isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(c2, farAhead)).as("c2 is strictly before the far-ahead pivot")
                .isLessThan(0);

        WorkerState victim = WorkerStates.of(1, lo, cursor, H);
        victim.setCursor(cursor);
        victim.addKeysEmitted(1000);
        victim.recordPage(lo, cursor, 1000);   // uniformly dense ⇒ far-ahead fraction 0.75
        assertThat(victim.densityFraction()).isEqualTo(0.75);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        Thief thief = Thiefs.of(store, advancingCursorFetcher(victim, c2, new AtomicBoolean()),
                RUN_ID, new byte[0], ListingMode.OBJECTS, (id, l, h) -> { }, metrics);

        assertThat(thief.steal(List.of(victim)))
                .as("far-ahead pivot commits despite the cursor passing the plain midpoint")
                .isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(store.lastSplit.pivot()).as("committed at the far-ahead boundary").isEqualTo(farAhead);
        assertThat(KeyBytes.compareUnsigned(c2, store.lastSplit.pivot()))
                .as("the committed pivot is strictly beyond the advanced cursor c2").isLessThan(0);
    }

    /**
     * The isolating contrast: the SAME cursor advance to {@code c2}, but with NO density signal so the
     * pivot is the plain {@code 0.5} midpoint. The advance (c2 >= plainMidpoint) now trips
     * {@code RETRY.cursor_passed_pivot} and no child is created — proving it is the far-ahead placement,
     * not the fixture, that saves the split in the test above.
     */
    @Test
    void plainMidpointSplitLosesTheCursorRaceWithoutFarAhead() throws SwathException, InterruptedException {
        byte[] lo = b("2022/03/05/00000000");
        byte[] cursor = b("2022/03/05/10000000");
        byte[] H = b("2022/03/05/99999999");

        byte[] farAhead = StealMath.interpolate(cursor, H, 0.75);
        byte[] plainMid = ByteMidpoint.between(cursor, H);
        byte[] c2 = ByteMidpoint.between(plainMid, farAhead);   // > plainMid ⇒ passes the 0.5 midpoint
        assertThat(KeyBytes.compareUnsigned(plainMid, c2)).as("c2 passes the plain midpoint").isLessThan(0);

        WorkerState victim = WorkerStates.of(1, lo, cursor, H);
        victim.setCursor(cursor);
        victim.addKeysEmitted(1000);          // scores as a victim, but NO recordPage ⇒ densityFraction 0.5
        assertThat(victim.densityFraction()).isEqualTo(0.5);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        Thief thief = Thiefs.of(store, advancingCursorFetcher(victim, c2, new AtomicBoolean()),
                RUN_ID, new byte[0], ListingMode.OBJECTS, (id, l, h) -> { }, metrics);

        assertThat(thief.steal(List.of(victim)))
                .as("a plain-midpoint split loses the cursor race").isEqualTo(Thief.Outcome.RETRY);
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("RETRY.cursor_passed_pivot", 0L))
                .as("the cursor passed the plain midpoint before the CAS").isEqualTo(1L);
        assertThat(store.lastSplit).as("no child committed").isNull();
    }

    // =========================================================================
    // (5) End-to-end: dense mega-day stays a valid partition within the INT-8 budget.
    // =========================================================================

    @Test
    @Timeout(120)
    void denseMegaDayTilesExactlyOnceWithinApiBudget(@TempDir Path dir) throws Exception {
        // A pages-dominated fixture: a big keyspace listed at the S3 max page size (maxKeys=1000)
        // with few workers makes the O(pages) LIST term dominate the O(workers) + slack terms, so the
        // INT-8 budget has comfortable headroom over steal/probe scheduling variance. Do not shrink
        // this back to a small-keyspace / many-worker fixture (e.g. 6000 keys, maxKeys=16, 16
        // workers): that shape leaves only a thin headroom margin and is warmth-fragile. The
        // INT-8 invariant itself (api ≤ 4·pages + 4·workers + 64) is UNCHANGED — only the fixture.
        int n = 100_000;
        List<byte[]> keyspace = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keyspace.add(b("2022/03/05/%08d".formatted(i)));   // one dense flat directory
        }
        int workers = 4;
        int maxKeys = 1000;

        ScanResult run = scan(dir, keyspace, workers, maxKeys);

        EngineHarness.assertExactlyOnce(run.emitted, keyspace);

        long pages = (long) Math.ceil((double) keyspace.size() / maxKeys);
        long int8Budget = ApiCallBudget.int8Budget(pages, workers);

        // INT-8: total LIST stays O(pages)+slack, never O(N) — far-ahead placement must not inflate it.
        ApiCallBudget.assertWithinInt8Budget(run.apiCalls, pages, workers,
                "total LIST within the INT-8 budget (4*pages + 4*workers + 64)");
        // A flat dense directory has no sub-directory structure; far-ahead splits land in populated
        // space, so structure probes must stay bounded (no per-key/per-split storm).
        assertThat(run.structureProbes)
                .as("far-ahead placement must not storm structure probes on a flat dense directory")
                .isLessThan(keyspace.size())
                .isLessThanOrEqualTo((int) int8Budget);
    }

    // -------------------------------------------------------------------------

    private record ScanResult(List<byte[]> emitted, int structureProbes, long apiCalls) {
    }

    private static ScanResult scan(Path baseDir, List<byte[]> keyspace, int workers, int maxKeys)
            throws Exception {
        Path ckptDir = baseDir.resolve("r2");
        Files.createDirectories(ckptDir);

        AtomicInteger structureProbes = new AtomicInteger();
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                .interceptor((PageRequest req, int idx, ListPage page) -> {
                    // A steal-time structure probe = a delimiter listing WITH a start_after (the cursor).
                    if (req.delimiter() != null && req.startAfter() != null) {
                        structureProbes.incrementAndGet();
                    }
                    return page;
                })
                .build();

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RunKey rk = new RunKey("s3", null, "bucket", new byte[0], "r2-hash",
                    "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
            RunMeta run = store.openRun(rk, false, false);
            List<NodeSpec> specs = SeedSteps.of(mock, new byte[0], workers).seedSpecs(run.id(), SeedMode.SHALLOW);
            store.insertNodes(specs);
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics),
                    mock, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(5000, engine, emitted);
        }
        return new ScanResult(emitted, structureProbes.get(), mock.apiCalls());
    }

}
