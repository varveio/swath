/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PageGate;
import io.varve.swath.testkit.PipelineDrain;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.RecordingSplitStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Adversarial guard for the density-reflected pivots (Thief empty-upper
 * reflect + owner-split reflection clamp). These new pivot placements change WHERE a split lands,
 * so the completeness risk is a mis-tiled reflected pivot that drops or double-emits keys, and the
 * observability risk is a toggle that silently keeps engaging when ablated. These tests prove neither
 * happens on the shapes the reflect path was built for (heavy low-mass skew, a thinning tail) plus a
 * dense-flat control, and that a mid-flight cursor race against a committed reflect pivot is rejected
 * exactly-once (no reopened overlap), and that a crash immediately after a reflect-committed durable
 * split resumes byte-exact.
 *
 * <p>Disjoint from the focused single-attempt unit guards ({@code ThiefReflectPivotTest},
 * {@code OwnerSplitReflectClampTest}). Everything is deterministic: an in-memory
 * {@link MockPageFetcher}, byte-exact set equality (schedule-invariant), a {@link PageGate} for the
 * one interleaving, and a preemptive timeout that fails rather than hangs on a termination regression.
 */
final class ReflectPivotContractTest {

    private static final int MAX_KEYS = 100;
    private static final long RUN_ID = 4711L;

    /** The {@code reflect}-toggle-gated engagement counters. {@code OWNER_SPLIT.floor_reflected_blocked}
     * is deliberately EXCLUDED — the observed-mass floor is gated by {@code density_ewma} (see
     * {@code EngineTogglesEngagementSmokeTest}), not {@code reflect}, so it fires regardless here. */
    private static final List<String> REFLECT_GATED = List.of(
            "PIVOT.reflect_hit", "PIVOT.reflect_empty", "PIVOT.reflect", "OWNER_SPLIT.pivot_reflect_clamped");

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static EngineToggles reflectOff() {
        return EngineToggles.DEFAULT.withReflect(false);
    }

    // ============================================================================================
    // Byte-exactness under the new pivots on adversarial shapes, reflect ON and OFF.
    // ============================================================================================

    private record Run(List<byte[]> emitted, Map<String, Long> stealReasons,
                       long apiCalls, List<RangePartition.Split> splits, long rootId) {
        long counter(String k) {
            return stealReasons.getOrDefault(k, 0L);
        }
    }

    private Run run(Path dir, String label, List<byte[]> keyspace, byte[] lo, byte[] hi,
                    int workers, EngineToggles toggles) throws Exception {
        Path ckptDir = dir.resolve(label);
        Files.createDirectories(ckptDir);
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], label,
                    "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl"), false, false);
            long rootId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
            List<Node> seeds = store.loadResumable(run.id(), false);
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, workers, MAX_KEYS, seeds, FilterChain.EMPTY);
            drain(engine, emitted);
            RunMetrics.RunDiagnostics diag = metrics.diagnostics(Duration.ZERO);
            return new Run(emitted, diag.stealReasons(), mock.apiCalls(), store.splits(), rootId);
        }
    }

    private static void drain(WorkStealingScan engine, List<byte[]> emitted) throws Exception {
        PipelineDrain.collectKeys(5000, engine, emitted);
    }

    @Test
    @Timeout(120)
    void adversarialSkewIsByteExactWithReflectOnAndOff(@TempDir Path dir) throws Exception {
        // Two heavy-skew shapes the reflect pivots are built for: mass concentrated LOW with the far
        // end of the code-point span nearly empty (time-decayed heavy-low = the empty-upper fingerprint;
        // badly-skewed hot/cold). Open-frontier seed (the realistic root); every child a bounded range
        // where the reflect path is live. Byte-exact set equality holds for EVERY schedule and under
        // BOTH toggle settings — the load-bearing correctness guard for the new pivot placement.
        record Shape(String name, List<byte[]> keys) { }
        List<Shape> shapes = List.of(
                new Shape("time-heavy-low", Keyspaces.timeDecayed(11, 20_000, false)),
                new Shape("badly-skewed", Keyspaces.badlySkewed(22, 20_000, 0.9)));

        for (Shape shape : shapes) {
            List<byte[]> keyspace = distinct(shape.keys());
            long pages = (long) Math.ceil((double) keyspace.size() / MAX_KEYS);
            long apiBudget = 6L * pages + 8L * 8 + 128L;   // O(pages+workers); catastrophically < N (INT-8)

            Run on = run(dir, shape.name() + "-on", keyspace, null, null, 8, EngineToggles.DEFAULT);
            EngineHarness.assertExactlyOnce(on.emitted(), keyspace);
            Run off = run(dir, shape.name() + "-off", keyspace, null, null, 8, reflectOff());
            EngineHarness.assertExactlyOnce(off.emitted(), keyspace);

            // INT-8: the engine flat-scans — LIST calls stay O(pages+workers), NEVER ~N-per-key.
            assertThat(on.apiCalls()).as("%s reflect-on INT-8 budget", shape.name())
                    .isLessThanOrEqualTo(apiBudget).isLessThan(keyspace.size());
            assertThat(off.apiCalls()).as("%s reflect-off INT-8 budget", shape.name())
                    .isLessThanOrEqualTo(apiBudget).isLessThan(keyspace.size());

            // Toggle ablation is honored on the Thief side: reflect OFF ⇒ NO thief-reflect engagement.
            assertThat(off.counter("PIVOT.reflect_hit")).as("%s off: no reflect_hit", shape.name()).isZero();
            assertThat(off.counter("PIVOT.reflect_empty")).as("%s off: no reflect_empty", shape.name()).isZero();
            assertThat(off.counter("PIVOT.reflect")).as("%s off: no reflect commit", shape.name()).isZero();
            assertThat(off.counter("TOGGLE.reflect_off")).as("%s off: ablation mark once", shape.name()).isEqualTo(1L);
        }
    }

    @Test
    @Timeout(60)
    void denseFlatControlEngagesTheClampOnlyWhenReflectIsOn(@TempDir Path dir) throws Exception {
        // The dense-flat control that reliably overshoots the owner-split pivot (clustered d/000000..
        // inside a snug (d/00, d/05] window) so the reflection clamp engages — proving the toggle actually
        // changes engagement, not just that OFF is silent. Byte-exact either way.
        List<byte[]> keyspace = distinct(denseFlat(20_000));
        byte[] lo = b("d/00");
        byte[] hi = b("d/05");

        Run on = run(dir, "control-on", keyspace, lo, hi, 4, EngineToggles.DEFAULT);
        EngineHarness.assertExactlyOnce(on.emitted(), keyspace);
        assertThat(on.counter("OWNER_SPLIT.self_published")).as("owner-splits fired").isGreaterThanOrEqualTo(1L);
        assertThat(on.counter("OWNER_SPLIT.pivot_reflect_clamped"))
                .as("reflect on: the overshooting owner pivot was clamped at least once")
                .isGreaterThanOrEqualTo(1L);

        Run off = run(dir, "control-off", keyspace, lo, hi, 4, reflectOff());
        EngineHarness.assertExactlyOnce(off.emitted(), keyspace);
        for (String c : REFLECT_GATED) {
            assertThat(off.counter(c)).as("reflect off: %s never fires", c).isZero();
        }
        assertThat(off.counter("TOGGLE.reflect_off")).as("ablation mark once").isEqualTo(1L);

        // Every clamped/committed split still tiles the seed range: pivot strictly inside (lo, oldHi],
        // replayed from the actual bounded seed frame (NOT the (⊥,⊤] root).
        assertTilesFromSeed(on.rootId(), lo, hi, on.splits());
    }

    /** Replay {@code splits} from a single bounded seed {@code (lo, hi]} and assert a gap-free,
     * overlap-free tiling — the seed-frame analog of {@link RangePartition#assertTilesFromSplits}. */
    private static void assertTilesFromSeed(long rootId, byte[] lo, byte[] hi,
                                            List<RangePartition.Split> splits) {
        Map<Long, RangePartition.Interval> live = new HashMap<>();
        live.put(rootId, new RangePartition.Interval(lo, hi));
        for (RangePartition.Split s : splits) {
            RangePartition.Interval v = live.get(s.victimId());
            assertThat(v).as("split victim %d is a live range", s.victimId()).isNotNull();
            assertThat(Arrays.equals(v.hi(), s.oldHi())).as("split oldHi == victim hi").isTrue();
            if (v.lo() != null) {
                assertThat(KeyBytes.compareUnsigned(v.lo(), s.pivot())).as("lo < pivot").isLessThan(0);
            }
            if (s.oldHi() != null) {
                assertThat(KeyBytes.compareUnsigned(s.pivot(), s.oldHi())).as("pivot < oldHi").isLessThan(0);
            }
            live.put(s.victimId(), new RangePartition.Interval(v.lo(), s.pivot()));
            live.put(s.childId(), new RangePartition.Interval(s.pivot(), s.oldHi()));
        }
        // Contiguity over the bounded seed (lo, hi]: sorted by lo, first starts at lo, each hi == next
        // lo (no gap, no overlap — boundary belongs LEFT), last ends at hi.
        List<RangePartition.Interval> sorted = new ArrayList<>(live.values());
        sorted.sort((x, y) -> KeyBytes.compareUnsigned(x.lo(), y.lo()));
        assertThat(Arrays.equals(sorted.getFirst().lo(), lo)).as("partition starts at the seed lo").isTrue();
        for (int i = 0; i < sorted.size() - 1; i++) {
            assertThat(Arrays.equals(sorted.get(i).hi(), sorted.get(i + 1).lo()))
                    .as("interval %d hi == interval %d lo (no gap/overlap)", i, i + 1).isTrue();
        }
        assertThat(Arrays.equals(sorted.getLast().hi(), hi)).as("partition ends at the seed hi").isTrue();
    }

    // ============================================================================================
    // The drainer race — a reflect pivot committed while the victim's cursor races past m_r.
    // ============================================================================================

    /** A low sliver {@code k/0000..k/00ff}; the real mass sits in a thin low window of {@code (cursor, HI]}. */
    private static final byte[] SLIVER_HI = b("k/z");

    private static List<byte[]> sliverKeys() {
        List<byte[]> ks = new ArrayList<>(256);
        for (int i = 0; i < 256; i++) {
            ks.add(b("k/%04x".formatted(i)));
        }
        return ks;
    }

    @Test
    @Timeout(60)
    void reflectCommitLosesToACursorThatRacedPastMr(@TempDir Path dir) throws Exception {
        // The reflect-hit scenario (cursor mid-sliver so the uniform midpoint overshoots and the
        // reflected pivot m_r lands inside the mass), but a PageGate parks the thief's reflect probe
        // (start_after == m_r) so that WHILE it is parked the drainer advances the victim's cursor
        // PAST m_r. On release the thief takes the lock, sees cursor >= m_r, and the SAME
        // cursor_passed_pivot guard that protects the uniform pivot must reject the reflected pivot:
        //   * RETRY (never CHILD_CREATED), no durable split, victim.hi untouched (never narrowed to m_r
        //     — the narrow happens only AFTER the cursor check), so the child range is never reopened;
        //   * counters sane: reflect_hit fired (the probe DID hit) but PIVOT.reflect did NOT (the commit
        //     lost the race), and RETRY.cursor_passed_pivot recorded exactly once.
        List<byte[]> keys = sliverKeys();
        byte[] lo = b("k/0000");
        byte[] cursor = b("k/0040");
        byte[] mReflect = StealMath.extrapolate(lo, cursor, SLIVER_HI);
        assertThat(mReflect).as("scenario precondition: extrapolate yields a reflected pivot").isNotNull();
        assertThat(KeyBytes.compareUnsigned(cursor, mReflect)).as("cursor < m_r").isLessThan(0);

        // Gate ONLY the reflect probe: the 1-key probe whose start_after is exactly m_r.
        PageGate gate = new PageGate(req -> req.maxKeys() == 1
                && req.startAfter() != null && Arrays.equals(req.startAfter(), mReflect));
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).interceptor(gate.interceptor()).build();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("race.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], "reflect-race",
                    "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl"), false, false);
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, SLIVER_HI, cursor, null));
            WorkerState victim = WorkerStates.of(victimId, lo, cursor, SLIVER_HI);
            victim.addKeysEmitted(64);
            RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS,
                    (id, cl, ch) -> { }, metrics);

            AtomicReference<Thief.Outcome> outcome = new AtomicReference<>();
            AtomicReference<Throwable> err = new AtomicReference<>();
            Thread b = new Thread(() -> {
                try {
                    outcome.set(thief.steal(List.of(victim)));
                } catch (Throwable t) {
                    err.set(t);
                }
            }, "reflect-thief");
            b.start();

            // The thief is parked mid reflect probe, m_r captured. The drainer advances the cursor PAST m_r.
            gate.awaitFetched();
            victim.setCursor(mReflect);
            gate.release();
            b.join(30_000);
            assertThat(err.get()).as("thief threw").isNull();

            assertThat(outcome.get()).as("reflect commit loses the cursor race → RETRY")
                    .isEqualTo(Thief.Outcome.RETRY);
            assertThat(store.splits()).as("no durable split committed").isEmpty();
            assertThat(victim.hi()).as("hi never narrowed to m_r — the child range is not reopened")
                    .isEqualTo(SLIVER_HI);

            Map<String, Long> r = metrics.diagnostics(Duration.ZERO).stealReasons();
            assertThat(r.getOrDefault("PIVOT.reflect_hit", 0L)).as("the reflect probe DID hit").isEqualTo(1L);
            assertThat(r.getOrDefault("PIVOT.reflect", 0L)).as("but the reflect commit did NOT win").isZero();
            assertThat(r.getOrDefault("RETRY.cursor_passed_pivot", 0L))
                    .as("the racing cursor was detected exactly once").isEqualTo(1L);
        }
    }

    // ============================================================================================
    // Crash immediately after a reflect-committed durable split resumes exactly-once.
    // ============================================================================================

    @Test
    @Timeout(60)
    void crashAfterReflectCommittedSplitResumesByteExact(@TempDir Path dir) throws Exception {
        // Drive a reflect-hit steal against a REAL durable store: the reflected pivot m_r commits the
        // split durably (victim narrows to (lo, m_r], child owns (m_r, HI]). Then the process "crashes"
        // BEFORE the child is ever listed (no drain) — the RES-3 after-commit-before-child-listing crash
        // point, but forced onto the reflect pivot. Resume via loadResumable and drain both nodes: the
        // tail (cursor, HI] must be emitted exactly once (no key lost at the m_r boundary, none doubled).
        List<byte[]> keys = sliverKeys();
        byte[] lo = b("k/0000");
        byte[] cursor = b("k/0040");
        byte[] mReflect = StealMath.extrapolate(lo, cursor, SLIVER_HI);
        assertThat(mReflect).isNotNull();

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("resume.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], "reflect-resume",
                    "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl"), false, false);
            long victimId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, SLIVER_HI, cursor, null));
            WorkerState victim = WorkerStates.of(victimId, lo, cursor, SLIVER_HI);
            victim.addKeysEmitted(64);
            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
            RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS,
                    (id, cl, ch) -> { }, metrics);

            assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
            // The split went through the REFLECT path (m_r), not the blind bisection.
            assertThat(store.splits()).singleElement()
                    .satisfies(s -> assertThat(s.pivot()).as("committed pivot is the reflected m_r").isEqualTo(mReflect));
            assertThat(metrics.diagnostics(Duration.ZERO).stealReasons().getOrDefault("PIVOT.reflect", 0L))
                    .as("the reflect pivot carried the commit").isEqualTo(1L);

            // CRASH here — the child was committed durable but never listed. Resume from the durable state.
            List<Node> resumable = store.loadResumable(run.id(), false);
            assertThat(resumable).as("victim + reflect child both resume").hasSize(2);

            MockPageFetcher fresh = MockPageFetcher.builder().keys(keys).build();
            WorkStealingScan resumeEngine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, new RunMetrics(new SimpleMeterRegistry())),
                    fresh, store, 4, MAX_KEYS, resumable, FilterChain.EMPTY);
            List<byte[]> emitted = new ArrayList<>();
            drain(resumeEngine, emitted);

            List<byte[]> tail = keys.stream()
                    .filter(k -> KeyBytes.compareUnsigned(k, cursor) > 0)
                    .toList();
            EngineHarness.assertExactlyOnce(emitted, tail);
        }
    }

    // ============================================================================================
    // Helpers.
    // ============================================================================================

    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(b("d/%06d".formatted(i)));
        }
        return keys;
    }

    private static List<byte[]> distinct(List<byte[]> keys) {
        TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
        set.addAll(keys);
        return new ArrayList<>(set);
    }

}
