/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.SeedSteps;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

/**
 * Decision-trace goldens — the policy-seam safety net every extraction slice (the thief brain, the
 * owner-split governor, pacing, the seed planner) is checked against: recorded
 * {@code (view, decision)} sequences from the CURRENT engine, deterministic, committed as JSONL
 * golden files under {@code src/test/resources/goldens/decision-trace/}, replayed and diffed here
 * on every {@code :swath-core:test} run.
 *
 * <p><b>Zero production behavior change.</b> Every seam this recorder uses already exists for
 * tests: {@link MockPageFetcher}'s {@code PageInterceptor} (probe request/response log — the
 * pivot cascade's probe verdicts), {@link RunMetrics#diagnostics}/{@link RunMetrics#summary} (the
 * already-instrumented {@code recordStealReason}/{@code recordSeedSummary} engagement counters and
 * per-level seed classification — AGENTS.md's "instrument every new algo path" law, read back
 * instead of newly added to), and the production {@link io.varve.swath.observability.TraceSink}
 * interface (implemented here by {@link RecordingTraceSink}, a test double — not a new hook).
 * {@link Thief}, {@link OwnerSelfSplit}, {@link WorkerState}, and {@link SeedStep} are driven
 * exactly as {@code ThiefTest}/{@code OwnerSelfSplitTriggerTest}/{@code SeedStepTest} already do:
 * single-thief/single-victim deterministic drivers, never a storm.
 *
 * <p><b>Regenerating goldens.</b> After a deliberate, reviewed engine change,
 * {@code ./gradlew :swath-core:test --tests '*.DecisionTraceGoldenTest' -Dswath.goldens.update=true}
 * rewrites every fixture under {@code src/test/resources/goldens/decision-trace/}; review the diff
 * before committing. See {@code docs/ops/dev/decision-trace-goldens.md} (including its "known
 * gaps" section — real-bucket replay fixtures and the flat-wide fixture's coverage gap).
 *
 * <p><b>Coverage matrix</b> (decision site × fixture — every site appears in ≥3 fixtures):
 * <ul>
 *   <li>{@code thief.steal}: deep-narrow, flat-wide, explosion-1to1, partition-key-value,
 *       thief-edge-cases, thief-cascade-mechanisms (6)</li>
 *   <li>{@code owner_self_split}: deep-narrow, flat-wide, explosion-1to1, partition-key-value,
 *       owner-split-gates (5)</li>
 *   <li>{@code seed.seed_specs}: deep-narrow, flat-wide, explosion-1to1, partition-key-value (4)</li>
 *   <li>{@code pacing.steal_paced}: pacing-trip-and-recover, pacing-below-threshold,
 *       pacing-multi-victim-isolation (3)</li>
 * </ul>
 *
 * <p>A per-file count overstates {@code thief.steal} coverage on its own — the same file can pin
 * many pivot-cascade branches once each, or pin the same branch repeatedly and miss others
 * entirely. The mechanism-level count (how many events across how many distinct fixtures commit
 * each named {@code PIVOT.*} branch — verified by grepping the committed goldens, not hand-counted)
 * is the real measure:
 * <ul>
 *   <li>{@code midpoint}: 2 events, 1 fixture</li>
 *   <li>{@code far_ahead}: 1 event, 1 fixture</li>
 *   <li>{@code step_back}: 12 events, 3 fixtures</li>
 *   <li>{@code bisect}: 2 events, 2 fixtures</li>
 *   <li>{@code reflect} (committed hit): 4 events, 2 fixtures</li>
 *   <li>{@code reflect_hit} / {@code reflect_empty} (probe verdicts, whether or not reflect won the
 *       commit): 4 / 3 events, 2 / 3 fixtures</li>
 *   <li>{@code structure_probe} / {@code structure_capped}: 8 / 6 events, 3 / 2 fixtures</li>
 *   <li>{@code adaptive_structure} / {@code adaptive_structure_capped}: 2 / 1 events, 2 / 1
 *       fixtures</li>
 *   <li>{@code flat_leaf}: 10 events, 2 fixtures</li>
 * </ul>
 * Every named pivot-cascade branch the bounded-range cascade can commit appears at least once
 * (the {@code extrapolate} mechanism belongs to the separate open-frontier path, not this
 * cascade, and is exercised — without committing, since its own scenarios are transient/terminal
 * — by {@code thiefEdgeCases}' unstarted/exhausted-frontier events instead). {@code
 * adaptive_structure_capped} is the one mechanism pinned only once, in {@code
 * thiefCascadeMechanisms}: reaching it requires the parent-empty sliver's byte-ADJACENT
 * cursor/bound divergence (see {@code Thief#isCursorAdjacentSliver}) on top of a truncated
 * probe, a narrow enough combination that a second independent recipe was not attempted here.
 */
final class DecisionTraceGoldenTest {

    private static final long RUN_ID = 7L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ============================================================================================
    // Bucket-shape fixtures: seed.seed_specs, thief.steal cascade, and owner_self_split, all
    // driven against the same deterministic keyspace and recorded into one JSONL fixture file.
    // ============================================================================================

    @Test
    void deepNarrow() throws Exception {
        // wis2-like: YYYY/MM/DD/HH/<uuid>, mass concentrated in the later (ascending) years.
        List<byte[]> keyspace = Keyspaces.timeDecayed(1L, 300, true);
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("deep-narrow");
        seedSpecsAttempt(fx, keyspace, new byte[0], 4);
        thiefCascade(fx, keyspace, new byte[0], b("9999"), 8);
        // Single worker (no demand gate) + production-scale mass, probed EARLY (0.15 of the way
        // through the sorted keyspace, not the midpoint): timeDecayed's ascending mass means the
        // index-midpoint sits deep in code-point space too (most of the remaining SPAN, not just
        // count, is already behind it), so a later probe point starves the remaining-work estimate
        // regardless of scale. Reaches a genuine self-published carve, distinct from the
        // tiny/rate-limited/demand-gated negatives owner-split-gates.jsonl already covers explicitly.
        ownerSplitScenarios(fx, keyspace, 0.15, 1, 20, 100L, 0L);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void flatWide() throws Exception {
        // A proportionate 3-digit counter (000-399, no zero-padding waste beyond the digit count
        // itself needed to hold 400 values) rather than testkit's Keyspaces.singlePrefixFlat's
        // deliberately-adversarial 8-digit padding: at this fixture's small (400-key) scale, an
        // 8-digit counter leaves the density-reflected flat-leaf pivot (StealMath.extrapolate over
        // the (leaf lo, leaf ceiling] window) reflecting a consumed span that is a vanishingly small
        // fraction of the nominal 10^8 window every round, so it never lands meaningfully past the
        // cursor — a real reflection-accuracy limit (Keyspaces.zeroPaddedCounter's own javadoc calls
        // out exactly this "radix-1 positions" cost), but a keyspace/fixture-scale mismatch, not a
        // property of the thief cascade this fixture exists to exercise.
        List<byte[]> keyspace = flatWideKeys(400);
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("flat-wide");
        seedSpecsAttempt(fx, keyspace, new byte[0], 4);
        thiefCascade(fx, keyspace, new byte[0], b("flat0"), 8);
        // 4 workers, outstanding already at capacity + production-scale mass: reaches the
        // demand-gated negative (extra parallelism would buy nothing) rather than the tiny-mass gate.
        ownerSplitScenarios(fx, keyspace, 0.5, 4, 20, 100L, 4L);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void explosion1to1() throws Exception {
        // Every prefix carries exactly one object — SeedStep's tiny-leaf-explosion shape.
        List<byte[]> keyspace = Keyspaces.tinyManyPrefix(200, 1);
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("explosion-1to1");
        seedSpecsAttempt(fx, keyspace, new byte[0], 4);
        thiefCascade(fx, keyspace, new byte[0], b("p99999"), 6);
        // No mass scaling: every leaf genuinely is tiny (1 object each) — this shape legitimately
        // never clears the owner-split remaining-work floor, unlike the other three shapes here.
        ownerSplitScenarios(fx, keyspace, 0.5, 1, 20, 1L, 0L);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void partitionKeyValue() throws Exception {
        List<byte[]> keyspace = hivePartitioned(20, 10);
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("partition-key-value");
        seedSpecsAttempt(fx, keyspace, new byte[0], 4);
        thiefCascade(fx, keyspace, new byte[0], b("dt=2024-01-99"), 6);
        // Single worker + production-scale mass: a second genuine self-published carve, on a
        // structurally different (Hive-partitioned) shape than deepNarrow's.
        ownerSplitScenarios(fx, keyspace, 0.5, 1, 20, 100L, 0L);
        GoldenTrace.writeOrVerify(fx);
    }

    /** Hive/Spark-style {@code key=value/} partition layout: {@code dt=2024-01-<day>/part-<n>.parquet}. */
    private static List<byte[]> hivePartitioned(int days, int partsPerDay) {
        List<byte[]> keys = new ArrayList<>(days * partsPerDay);
        for (int d = 1; d <= days; d++) {
            for (int p = 0; p < partsPerDay; p++) {
                keys.add(b(String.format("dt=2024-01-%02d/part-%05d.parquet", d, p)));
            }
        }
        return keys;
    }

    /** A single flat directory {@code flat/<NNN>} zero-padded to exactly {@code n}'s own digit width
     *  (no extra radix-1 padding) — see {@link #flatWide} for why that matters at this fixture's scale. */
    private static List<byte[]> flatWideKeys(int n) {
        int digits = Integer.toString(n - 1).length();
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(b("flat/" + String.format("%0" + digits + "d", i)));
        }
        return keys;
    }

    // ============================================================================================
    // thief-edge-cases: the algorithms.md §11 checklist's precision recipes, byte-exact and
    // hand-placed (adapted from ThiefTest's own choreography) — one steal() attempt per event.
    // ============================================================================================

    @Test
    void thiefEdgeCases() throws Exception {
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("thief-edge-cases");

        // 1. Empty pool -> NO_VICTIM.pool_empty.
        oneShotSteal(fx, List.of(), fetcher("m1"), StubCheckpointStore.returning(1L));

        // 2. Un-started open frontier (H == null, cursor == lo) -> transient RETRY, never cached.
        oneShotSteal(fx, List.of(WorkerStates.of(1, null, null, null)),
                fetcher("m1"), StubCheckpointStore.returning(1L));

        // 3. Open frontier whose cursor sits at the prefix ceiling -> genuinely UNSPLITTABLE.
        oneShotSteal(fx, List.of(WorkerStates.of(1, b("hot/00"), b("hot0"), null)),
                fetcher("m1"), StubCheckpointStore.returning(1L), b("hot/"));

        // 4. Victim selection: the wider remaining span wins.
        oneShotSteal(fx, List.of(WorkerStates.of(2, b("a"), b("a"), b("c")),
                WorkerStates.of(3, b("a"), b("a"), b("z"))),
                fetcher("m1"), StubCheckpointStore.returning(99L));

        // 5. Far-ahead pivot commit: a dense trailing page pushes the fraction to 0.75.
        WorkerState farAhead = WorkerStates.of(1, b("a"), b("m"), b("z"));
        farAhead.addKeysEmitted(500);
        farAhead.recordPage(b("p"), b("q"), 500);
        oneShotSteal(fx, List.of(farAhead), fetcher("w"), StubCheckpointStore.returning(42L));

        // 6. Far-ahead step-back: the far-ahead upper is empty, steps back to the plain midpoint.
        WorkerState stepBack = WorkerStates.of(1, b("a"), b("m"), b("z"));
        stepBack.addKeysEmitted(500);
        stepBack.recordPage(b("l"), b("m"), 500);
        oneShotSteal(fx, List.of(stepBack), fetcher("t"), StubCheckpointStore.returning(7L));

        // 7. Empty-upper bisection: the only key sits just past the cursor (dense left head).
        oneShotSteal(fx, List.of(WorkerStates.of(1, b("a"), b("a"), b("z"))),
                fetcher("ab"), StubCheckpointStore.returning(5L));

        // 8. Empty-upper bisection exhausts to a control sliver -> transient RETRY, not cached.
        oneShotSteal(fx, List.of(WorkerStates.of(1, b("a"), b("a"), b("a!"))),
                fetcher("z"), StubCheckpointStore.returning(5L));

        // 9. Late loser: the durable CAS rejects the split -> hi restored, RETRY.
        oneShotSteal(fx, List.of(WorkerStates.of(1, b("a"), b("a"), b("z"))),
                fetcher("m1"), StubCheckpointStore.alwaysAborts());

        // 10. Early loser: a rival thief narrows hi mid-probe -> RETRY, hi left untouched.
        earlyLoserFixture(fx);

        GoldenTrace.writeOrVerify(fx);
    }

    // ============================================================================================
    // thief-cascade-mechanisms: the pivot-cascade branches the edge-case fixture above never
    // reaches (reflect commit, flat-leaf, plain and adaptive structure-probe, both uncapped and
    // truncated) — ported from ThiefReflectPivotTest/ThiefFlatLeafPivotTest/StealStructureProbeTest's
    // own recipes through the same single-attempt oneShotSteal driver.
    // ============================================================================================

    @Test
    void thiefCascadeMechanisms() throws Exception {
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("thief-cascade-mechanisms");

        // 1. Reflect commits directly: the far-ahead AND step-back midpoint both probe empty, but
        //    the density-reflected pivot lands inside the low sliver and probes non-empty.
        //    (ThiefReflectPivotTest.reflectHitCommitsAtTheReflectedPivot)
        List<byte[]> sliver = sliverKeys();
        WorkerState reflectVictim = WorkerStates.of(1, b("k/0000"), b("k/0040"), b("k/z"));
        reflectVictim.addKeysEmitted(64);
        oneShotSteal(fx, List.of(reflectVictim), MockPageFetcher.builder().keys(sliver).build(),
                StubCheckpointStore.returning(88L));

        // 2. Flat-leaf bootstrap: a parent-empty sliver over one flat high-entropy directory with NO
        //    sub-directories — the structure probe finds nothing, so density extrapolation carves a
        //    balanced interior pivot instead of the degenerate cursor-adjacent sliver.
        //    (ThiefFlatLeafPivotTest.bootstrapFlatLeafDensityReflectsBalancedInteriorPivot)
        List<byte[]> flatLeaf = flatLeafKeys();
        WorkerState flatLeafBootstrap = WorkerStates.of(2, b("led/0100/"), b("led/0100/4000"), b("led/02"));
        flatLeafBootstrap.addKeysEmitted(100);
        oneShotSteal(fx, List.of(flatLeafBootstrap), MockPageFetcher.builder().keys(flatLeaf).build(),
                StubCheckpointStore.returning(89L));

        // 3. Flat-leaf, child worker: lo is a real deep key (a prior split's pivot, not the leaf
        //    directory), so the reflection reference is the child's own lo — zero extra probes.
        //    (ThiefFlatLeafPivotTest.childFlatLeafReflectsOwnLoWithZeroExtraProbes)
        WorkerState flatLeafChild = WorkerStates.of(2, b("led/0100/2000"), b("led/0100/4000"), b("led/02"));
        flatLeafChild.addKeysEmitted(100);
        oneShotSteal(fx, List.of(flatLeafChild), MockPageFetcher.builder().keys(flatLeaf).build(),
                StubCheckpointStore.returning(90L));

        // 4. Adaptive structure probe (parent-empty sliver, uncapped): a date/uuid shape whose
        //    coarse->fine back-out lands on the DAY level and splits at a real sub-directory boundary.
        //    (ThiefFlatLeafPivotTest.leafWithSubdirsStillTakesStructureProbePath)
        List<byte[]> dateUuid = dateUuidKeys(28);
        WorkerState adaptiveVictim = WorkerStates.of(3, b("2022/03/05/u05/"), b("2022/03/05/u05/f"), b("2022/04"));
        adaptiveVictim.addKeysEmitted(100);
        oneShotSteal(fx, List.of(adaptiveVictim), MockPageFetcher.builder().keys(dateUuid).build(),
                StubCheckpointStore.returning(91L));

        // 5. Plain structure probe (empty-upper, uncapped): lo/cursor share a directory with < 32
        //    qualifying sibling boundaries past the cursor, so one bounded probe returns the true
        //    median — never truncated.
        WorkerState structureVictim = WorkerStates.of(4, b("root/"), b("root/005/obj00"), b("root/zzz"));
        structureVictim.addKeysEmitted(100);
        oneShotSteal(fx, List.of(structureVictim), MockPageFetcher.builder().keys(manyDirs("root", 20)).build(),
                StubCheckpointStore.returning(92L));

        // 6. Plain structure probe, capped: the same shape with far more than
        //    STRUCTURE_PROBE_MAX_KEYS (32) sibling directories past the cursor, so the probe page
        //    truncates and the pivot is the furthest boundary it could prove, not the true median.
        WorkerState structureCappedVictim = WorkerStates.of(5, b("root2/"), b("root2/005/obj00"), b("root2/zzz"));
        structureCappedVictim.addKeysEmitted(100);
        oneShotSteal(fx, List.of(structureCappedVictim),
                MockPageFetcher.builder().keys(manyDirs("root2", 150)).build(), StubCheckpointStore.returning(93L));

        // 7. Adaptive structure probe, capped: the same parent-empty-sliver date/uuid shape as
        //    scenario 4, but with 100 synthetic "day" sub-directories instead of a calendar month, so
        //    the DAY-level probe truncates too.
        List<byte[]> dateUuidWide = dateUuidKeys(100);
        WorkerState adaptiveCappedVictim = WorkerStates.of(6, b("2022/03/05/u05/"), b("2022/03/05/u05/f"),
                b("2022/04"));
        adaptiveCappedVictim.addKeysEmitted(100);
        oneShotSteal(fx, List.of(adaptiveCappedVictim), MockPageFetcher.builder().keys(dateUuidWide).build(),
                StubCheckpointStore.returning(94L));

        GoldenTrace.writeOrVerify(fx);
    }

    /** {@code k/0000..k/00ff}: a low sliver — the mass occupies a thin low window of a far-bounded range. */
    private static List<byte[]> sliverKeys() {
        List<byte[]> keys = new ArrayList<>(256);
        for (int i = 0; i < 256; i++) {
            keys.add(b("k/%04x".formatted(i)));
        }
        return keys;
    }

    /** {@code led/0100/<hhhh>}: one flat high-entropy directory, no sub-directories. */
    private static List<byte[]> flatLeafKeys() {
        List<byte[]> keys = new ArrayList<>(4096);
        for (int i = 0; i < 4096; i++) {
            keys.add(b("led/0100/%04x".formatted(i * 16)));
        }
        return keys;
    }

    /** {@code 2022/03/<day>/u<NN>/f} for {@code days} zero-padded day-equivalents (1-based, 2 digits). */
    private static List<byte[]> dateUuidKeys(int days) {
        List<byte[]> keys = new ArrayList<>(days * 10);
        for (int day = 1; day <= days; day++) {
            for (int u = 0; u < 10; u++) {
                keys.add(b("2022/03/%02d/u%02d/f".formatted(day, u)));
            }
        }
        return keys;
    }

    /** {@code <root>/<ddd>/obj00} for {@code dirs} zero-padded (3-digit) sibling directories. */
    private static List<byte[]> manyDirs(String root, int dirs) {
        List<byte[]> keys = new ArrayList<>(dirs);
        for (int d = 0; d < dirs; d++) {
            keys.add(b(root + "/%03d/obj00".formatted(d)));
        }
        return keys;
    }

    private static MockPageFetcher fetcher(String... keys) {
        List<byte[]> ks = new ArrayList<>();
        for (String k : keys) {
            ks.add(b(k));
        }
        return MockPageFetcher.builder().keys(ks).build();
    }

    private void oneShotSteal(GoldenTrace.Fixture fx, List<WorkerState> pool, MockPageFetcher rawFetcher,
                               StubCheckpointStore store) throws SwathException, InterruptedException {
        oneShotSteal(fx, pool, rawFetcher, store, new byte[0]);
    }

    private void oneShotSteal(GoldenTrace.Fixture fx, List<WorkerState> pool, MockPageFetcher rawFetcher,
                               StubCheckpointStore store, byte[] scanPrefix)
            throws SwathException, InterruptedException {
        GoldenTrace.ProbeLog probes = new GoldenTrace.ProbeLog();
        RecordingTraceSink trace = new RecordingTraceSink();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // rawFetcher's backing keyspace isn't reachable from here to rebuild with an interceptor
        // baked in, so wrap it in a probe-logging delegate instead.
        Thief thief = new Thief(store, new LoggingFetcher(rawFetcher, probes), RUN_ID, scanPrefix,
                ListingMode.OBJECTS, (childId, lo, hi) -> { }, metrics, EngineToggles.DEFAULT, trace);
        recordStealAttempt(fx, thief, pool, store, metrics, probes, trace);
    }

    private void earlyLoserFixture(GoldenTrace.Fixture fx) throws SwathException, InterruptedException {
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("z"));
        GoldenTrace.ProbeLog probes = new GoldenTrace.ProbeLog();
        RecordingTraceSink trace = new RecordingTraceSink();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher.Builder builder = MockPageFetcher.builder().keys(List.of(b("m1")))
                .interceptor((req, idx, page) -> {
                    probes.log(req, page);
                    victim.lock().lock();
                    try {
                        victim.narrowHi(b("p"));   // a rival winner narrowed (lo, p] before we re-validate
                    } finally {
                        victim.lock().unlock();
                    }
                    return page;
                });
        StubCheckpointStore store = StubCheckpointStore.returning(1L);
        Thief thief = new Thief(store, builder.build(), RUN_ID, new byte[0], ListingMode.OBJECTS,
                (childId, lo, hi) -> { }, metrics, EngineToggles.DEFAULT, trace);
        recordStealAttempt(fx, thief, List.of(victim), store, metrics, probes, trace);
    }

    /** Routes every {@link PageFetcher} call through {@code probes}, delegating to {@code delegate}. */
    private record LoggingFetcher(MockPageFetcher delegate, GoldenTrace.ProbeLog probes) implements PageFetcher {
        @Override
        public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
            ListPage page = delegate.fetchPage(req);
            probes.log(req, page);
            return page;
        }

        @Override
        public StoreCapabilities capabilities() {
            return delegate.capabilities();
        }
    }

    // ============================================================================================
    // owner-split-gates: the OwnerSelfSplit trigger/gate cascade, driven directly (no Thief, no
    // full engine) against hand-built WorkerState density.
    // ============================================================================================

    @Test
    void ownerSplitGates() throws Exception {
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("owner-split-gates");

        // 1. Open frontier: never self-splits.
        RunMetrics m1 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t1 = new RecordingTraceSink();
        WorkerState frontier = WorkerStates.of(1, b("a"), b("a"), null);
        long[] selfSplit1 = {0, 0};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m1, t1, 4, 100, () -> 0L, () -> 4),
                frontier, b("a"), selfSplit1, m1, t1);

        // 2. Too small a remaining span: the issue #16 UNINSTRUMENTED silent gate.
        RunMetrics m2 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t2 = new RecordingTraceSink();
        WorkerState tiny = WorkerStates.of(2, b("d/00"), b("d/00"), b("d/05"));
        long[] selfSplit2 = {0, 0};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m2, t2, 4, 100, () -> 0L, () -> 4),
                tiny, b("d/001"), selfSplit2, m2, t2);

        // 3. Rate limit: a large dense drain that already committed 5 pages ago (< 32 apart).
        RunMetrics m3 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t3 = new RecordingTraceSink();
        WorkerState rateLimited = denseVictim(3, "d/00", "d/05");
        long[] selfSplit3 = {5, 0};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m3, t3, 4, 100, () -> 0L, () -> 4),
                rateLimited, b("d/002500"), selfSplit3, m3, t3);

        // 4. Demand-gated: outstanding already >= workerCount, extra parallelism would buy nothing.
        //    selfSplit[1] starts well clear of the rate-limit window (this is the FIRST carve on this
        //    victim) so the attempt reaches the demand gate instead of tripping gate 3 first.
        RunMetrics m4 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t4 = new RecordingTraceSink();
        WorkerState gated = denseVictim(4, "d/00", "d/05");
        long[] selfSplit4 = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m4, t4, 4, 100, () -> 4L, () -> 4),
                gated, b("d/002500"), selfSplit4, m4, t4);

        // 5. Successful carve: a large bounded dense drain, single worker (no demand gate), clear
        //    of the rate limit -> OWNER_SPLIT.self_published.
        RunMetrics m5 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t5 = new RecordingTraceSink();
        WorkerState published = denseVictim(5, "d/00", "d/05");
        long[] selfSplit5 = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m5, t5, 1, 100, () -> 0L, () -> 1),
                published, b("d/002500"), selfSplit5, m5, t5);

        GoldenTrace.writeOrVerify(fx);
    }

    /** A dense, bounded, far-consumed victim large enough to clear the owner-split remaining-work floor. */
    private static WorkerState denseVictim(long nodeId, String lo, String hi) {
        WorkerState ws = WorkerStates.of(nodeId, b(lo), b(lo), b(hi));
        ws.addKeysEmitted(50_000);
        ws.recordPage(b(lo), b(hi), 50_000);
        return ws;
    }

    private static OwnerSelfSplit ownerSelfSplit(RunMetrics metrics, RecordingTraceSink trace, int workerCount,
            int maxKeys, LongSupplier outstanding, IntSupplier effectiveT) {
        StubCheckpointStore store = StubCheckpointStore.returning(999L);
        return new OwnerSelfSplit(RUN_ID, workerCount, maxKeys, store, EngineToggles.DEFAULT, metrics, trace,
                outstanding, effectiveT, (childId, lo, hi) -> { });
    }

    private void recordOwnerSplitAttempt(GoldenTrace.Fixture fx, OwnerSelfSplit gov, WorkerState ws,
            byte[] cursorTo, long[] selfSplit, RunMetrics metrics, RecordingTraceSink trace)
            throws SwathException, InterruptedException {
        ObjectNode view = GoldenTrace.newNode();
        view.put("node_id", ws.nodeId());
        GoldenTrace.putHex(view, "lo", ws.lo());
        GoldenTrace.putHex(view, "cursor_to", cursorTo);
        GoldenTrace.putHex(view, "hi", ws.hi());
        view.put("keys_emitted", ws.keysEmitted());
        view.put("self_split_committed_before", selfSplit[0]);
        view.put("self_split_last_committed_at_before", selfSplit[1]);

        Map<String, Long> before = GoldenTrace.snapshotReasons(metrics);
        trace.clear();
        OwnerSelfSplit.OwnerSplitTrace result = gov.maybeOwnerSelfSplit(ws.nodeId(), ws, cursorTo, selfSplit);
        Map<String, Long> after = GoldenTrace.snapshotReasons(metrics);

        ObjectNode event = GoldenTrace.newEvent("owner_self_split", fx.name(), fx.nextSeq());
        event.set("view", view);
        event.set("reason_deltas", GoldenTrace.reasonDeltasNode(before, after));
        ArrayNode traceEvents = event.putArray("trace_events");
        for (ObjectNode e : trace.events()) {
            traceEvents.add(e);
        }
        ObjectNode decision = GoldenTrace.newNode();
        if (result != null) {
            decision.put("split", true);
            decision.put("child_node_id", result.childId());
            GoldenTrace.putHex(decision, "pivot", result.pivot());
            GoldenTrace.putHex(decision, "hi", result.hi());
        } else {
            decision.put("split", false);
        }
        event.set("decision", decision);
        fx.record(event);
    }

    // ============================================================================================
    // pacing: WorkerState.stealPaced()'s per-victim cooldown state machine, driven directly.
    // ============================================================================================

    @Test
    void pacingTripAndRecover() {
        // FUTILITY_PACE_THRESHOLD (4) consecutive futile outcomes trip a bounded-exponential
        // cooldown (2^trips, capped at 64) — here trips=1 -> a 2-skip cooldown.
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("pacing-trip-and-recover");
        WorkerState ws = WorkerStates.of(1, b("a"), b("a"), b("z"));
        for (int i = 0; i < 4; i++) {
            ws.recordFutileSteal();
        }
        recordPacingConsult(fx, "trip-and-recover", 0, ws);
        recordPacingConsult(fx, "trip-and-recover", 1, ws);
        recordPacingConsult(fx, "trip-and-recover", 2, ws);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void pacingBelowThreshold() {
        // 3 consecutive futile outcomes stay below FUTILITY_PACE_THRESHOLD (4): never pace.
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("pacing-below-threshold");
        WorkerState ws = WorkerStates.of(1, b("a"), b("a"), b("z"));
        for (int i = 0; i < 3; i++) {
            ws.recordFutileSteal();
        }
        recordPacingConsult(fx, "below-threshold", 0, ws);
        recordPacingConsult(fx, "below-threshold", 1, ws);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void pacingMultiVictimIsolation() {
        // Victim A trips its cooldown; sibling victim B stays fresh — pacing is per-victim, never
        // a global cooldown (algorithms.md §3.2 / WorkerState.stealPaced's javadoc).
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("pacing-multi-victim-isolation");
        WorkerState a = WorkerStates.of(1, b("a"), b("a"), b("z"));
        WorkerState b = WorkerStates.of(2, b("a"), b("a"), b("z"));
        for (int i = 0; i < 4; i++) {
            a.recordFutileSteal();
        }
        recordPacingConsult(fx, "victim-a-tripped", 0, a);
        recordPacingConsult(fx, "victim-b-fresh", 0, b);
        recordPacingConsult(fx, "victim-a-tripped", 1, a);
        recordPacingConsult(fx, "victim-b-fresh", 1, b);
        GoldenTrace.writeOrVerify(fx);
    }

    private void recordPacingConsult(GoldenTrace.Fixture fx, String scriptVictim, int stepInScript, WorkerState ws) {
        boolean paced = ws.stealPaced();
        ObjectNode event = GoldenTrace.newEvent("pacing.steal_paced", fx.name(), fx.nextSeq());
        ObjectNode view = GoldenTrace.newNode();
        view.put("node_id", ws.nodeId());
        view.put("script_victim", scriptVictim);
        view.put("step_in_script", stepInScript);
        event.set("view", view);
        ObjectNode decision = GoldenTrace.newNode();
        decision.put("paced", paced);
        event.set("decision", decision);
        fx.record(event);
    }

    // ============================================================================================
    // Shared drivers: thief.steal cascade (iterative, single-thief), and seed.seed_specs.
    // ============================================================================================

    /** Keys drained per worker per round in {@link #drainOneRound} — a small, realistic page size. */
    private static final int DRAIN_PAGE_KEYS = 8;

    /**
     * Iteratively drains one root victim over {@code attempts} single {@link Thief#steal} calls: each
     * successful split's child is enqueued back into the live pool (via {@link Thief.ChildSink}) with
     * its resume cursor at the pivot — matching production's {@code WorkStealingScan.enqueueChild}
     * (cursor == lo, never {@code null}: see {@code Thief.ChildSink}'s own javadoc, "the child's resume
     * cursor is m"). {@link #drainOneRound} advances every still-splittable candidate a little BEFORE
     * every attempt, including the first (a real bounded page fetch against the same keyspace, folded
     * into both the density EWMA and the observed-alphabet digest via {@link WorkerState#recordPage} —
     * see that method), so later attempts steal from a genuinely progressively-consumed pool — not a
     * repeatedly-reset, always-fresh one — the same single-thief-at-a-time choreography a real fleet's
     * idle loop performs one attempt at a time, without the scheduler nondeterminism a multi-thief
     * storm would introduce. This mirrors production directly: a worker is only ever thief-visible once
     * it is progress-gated eligible ({@link WorkerState#stealEligible()}, set by {@link
     * WorkerState#setCursor} and consulted by {@code WorkStealingScan.eligibleVictims()}), i.e. only
     * after it has committed at least one page — draining before round 0 too, not just between rounds,
     * keeps every steal attempt here facing a worker production would actually offer one.
     *
     * <p>The root worker's {@code lo} is anchored at the keyspace's OWN minimum key (never the global
     * {@code ⊥}). This is NOT about the pivot itself — {@code Thief}'s bounded-range pivot placement
     * ({@code cForPivot = (cursor == null) ? BOTTOM : cursor}, then {@code interpolate(cForPivot, H,
     * ...)}) reads the CURSOR, never {@code lo}, so a {@code ⊥} lo has no direct effect there. What it
     * drives is {@link StealMath#spanIn}, which {@link WorkerState#recordPage} calls to normalize each
     * drained page against the FULL {@code (lo, hi]} window: with {@code lo == ⊥} that window spans
     * from the keyspace's absolute floor, so a page drained from deep inside the real data measures as
     * a vanishingly small fraction of it — the density EWMA collapses toward zero, {@link
     * WorkerState#densityFraction()} never clears its {@code 0.5} floor, and every density-gated
     * mechanism (far-ahead, step-back, reflect, flat-leaf) never engages. Anchoring {@code lo} at the
     * keyspace's real minimum keeps that denominator meaningful. This matches what an INTERIOR
     * post-split range looks like in production (a split child's {@code lo} is always a real prior
     * pivot) — not the very first seed range, which genuinely does carry {@code lo == ⊥}
     * (see {@code deep-narrow.jsonl}'s {@code seed.seed_specs} event: {@code ranges[0].lo == null}).
     */
    private void thiefCascade(GoldenTrace.Fixture fx, List<byte[]> keyspace, byte[] scanPrefix, byte[] hi,
                               int attempts) throws SwathException, InterruptedException {
        GoldenTrace.ProbeLog probes = new GoldenTrace.ProbeLog();
        RecordingTraceSink trace = new RecordingTraceSink();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace)
                .interceptor(probes.interceptor())
                .build();
        // A second, undecorated fetcher over the SAME keyspace for the between-round draining below —
        // draining is not itself a decision this fixture records, so it must not appear in the probe
        // log Thief.steal's own attempts are judged against.
        MockPageFetcher drainFetcher = MockPageFetcher.builder().keys(keyspace).build();
        byte[] rootLo = keyspace.stream().min(Arrays::compareUnsigned).orElse(null);
        List<WorkerState> pool = new ArrayList<>();
        pool.add(WorkerStates.of(1, rootLo, rootLo, hi));
        AtomicLong nextChildId = new AtomicLong(1000L);
        StubCheckpointStore store = new StubCheckpointStore(s -> nextChildId.getAndIncrement());
        Thief.ChildSink sink = (childId, lo, childHi) -> pool.add(WorkerStates.of(childId, lo, lo, childHi));
        Thief thief = new Thief(store, fetcher, RUN_ID, scanPrefix, ListingMode.OBJECTS, sink, metrics,
                EngineToggles.DEFAULT, trace);

        for (int i = 0; i < attempts; i++) {
            // Drain BEFORE every attempt, including the first: production only ever presents a
            // progress-gated (stealEligible) worker to the thief (WorkerState.stealEligible's
            // javadoc, algorithms.md §3.2) — a worker that has emitted nothing yet (cursor == lo,
            // zero consumed span) is never a real candidate. Skipping this on round 0 left a
            // zero-progress root worker in the pool, which the reflect/flat-leaf pivot mechanisms
            // (both density-based) treat as "no span to reflect" and can degenerate to a
            // near-cursor pivot — a real edge case, but not the one this fixture is after.
            drainOneRound(pool, scanPrefix, drainFetcher);
            recordStealAttempt(fx, thief, pool, store, metrics, probes, trace);
        }
    }

    /**
     * Advances every still-splittable pool member by one small bounded page (up to {@link
     * #DRAIN_PAGE_KEYS} in-range keys past its own cursor), exactly as a real worker's page commit
     * would: {@link WorkerState#setCursor}, {@link WorkerState#recordPage} (the density digest), and
     * {@link WorkerState#addKeysEmitted}. A worker whose own {@code (cursor, hi]} has no keys left
     * (fully drained, or a fresh child whose narrow slice is empty) is simply left unchanged — {@link
     * Thief#steal} already skips zero-remaining-span candidates during victim selection.
     */
    private void drainOneRound(List<WorkerState> pool, byte[] scanPrefix, MockPageFetcher drainFetcher)
            throws SwathException, InterruptedException {
        for (WorkerState w : pool) {
            byte[] hi = w.hi();
            if (hi == null || w.unsplittable()) {
                continue;   // the open frontier and cached-dead ranges are never drained here
            }
            ListPage page = drainFetcher.fetchPage(new PageRequest(ListingMode.OBJECTS, DRAIN_PAGE_KEYS,
                    scanPrefix, null, w.cursor(), null, null, null, null, 0));
            byte[] first = null;
            byte[] last = null;
            int count = 0;
            for (ListEntry e : page.entries()) {
                byte[] k = e.key().rawUnsafe();
                if (KeyBytes.compareUnsigned(k, hi) > 0) {
                    break;   // past this worker's own (possibly since-narrowed) bound
                }
                if (first == null) {
                    first = k;
                }
                last = k;
                count++;
            }
            if (count > 0) {
                w.setCursor(last);
                w.recordPage(first, last, count);
                w.addKeysEmitted(count);
            }
        }
    }

    private void recordStealAttempt(GoldenTrace.Fixture fx, Thief thief, List<WorkerState> pool,
            StubCheckpointStore store, RunMetrics metrics, GoldenTrace.ProbeLog probes, RecordingTraceSink trace)
            throws SwathException, InterruptedException {
        ObjectNode view = poolView(pool);
        Map<String, Long> before = GoldenTrace.snapshotReasons(metrics);
        probes.clear();
        trace.clear();
        int splitCallsBefore = store.splitCalls;

        Thief.Outcome outcome = thief.steal(pool);

        Map<String, Long> after = GoldenTrace.snapshotReasons(metrics);
        ObjectNode event = GoldenTrace.newEvent("thief.steal", fx.name(), fx.nextSeq());
        event.set("view", view);
        ArrayNode probesArr = event.putArray("probes");
        for (ObjectNode p : probes.calls()) {
            probesArr.add(p);
        }
        event.set("reason_deltas", GoldenTrace.reasonDeltasNode(before, after));
        ArrayNode traceEvents = event.putArray("trace_events");
        for (ObjectNode e : trace.events()) {
            traceEvents.add(e);
        }
        ObjectNode decision = GoldenTrace.newNode();
        decision.put("outcome", outcome.name());
        if (store.splitCalls > splitCallsBefore) {
            ObjectNode split = decision.putObject("split");
            split.put("victim_node_id", store.lastSplit.victimId());
            GoldenTrace.putHex(split, "pivot", store.lastSplit.pivot());
            GoldenTrace.putHex(split, "old_hi", store.lastSplit.oldHi());
        } else {
            decision.putNull("split");
        }
        event.set("decision", decision);
        fx.record(event);
    }

    private static ObjectNode poolView(List<WorkerState> pool) {
        ObjectNode view = GoldenTrace.newNode();
        ArrayNode candidates = view.putArray("pool");
        for (WorkerState w : pool) {
            ObjectNode c = GoldenTrace.newNode();
            c.put("node_id", w.nodeId());
            GoldenTrace.putHex(c, "lo", w.lo());
            GoldenTrace.putHex(c, "cursor", w.cursor());
            GoldenTrace.putHex(c, "hi", w.hi());
            c.put("unsplittable", w.unsplittable());
            candidates.add(c);
        }
        return view;
    }

    /**
     * One owner-self-split scenario per fixture: a fresh victim bounded {@code (⊥, hi]}, {@code hi}
     * one byte past the keyspace's own max key (always strictly greater, by the byte-prefix rule),
     * probed at {@code cursorTo} — the sorted keyspace's key at {@code cursorFraction} of its length
     * — so the density digest reflects the keyspace's own real shape instead of a hand-guessed bound.
     */
    private void ownerSplitScenarios(GoldenTrace.Fixture fx, List<byte[]> keyspace, double cursorFraction,
            int workerCount, int maxKeys, long massScale, long outstanding) throws SwathException, InterruptedException {
        if (keyspace.isEmpty()) {
            return;
        }
        List<byte[]> sorted = new ArrayList<>(keyspace);
        sorted.sort(Arrays::compareUnsigned);
        byte[] lo = sorted.get(0);
        byte[] max = sorted.get(sorted.size() - 1);
        byte[] hi = Arrays.copyOf(max, max.length + 1);   // strictly > every key (prefix rule)
        int idx = Math.max(0, Math.min(sorted.size() - 1, (int) (sorted.size() * cursorFraction)));
        byte[] cursorTo = sorted.get(idx);
        // massScale simulates this SHAPE at production scale (a real bucket of this layout typically
        // carries orders of magnitude more objects per unit of key-space than a several-hundred-key
        // test fixture) — the byte-space view (lo/cursor/hi) stays the fixture's own real keys; only
        // the density signal fed to the gate is scaled, so each shape can reach a DIFFERENT gate
        // instead of every shape fixture landing on the same too-small-to-carve floor (the one
        // owner-split-gates.jsonl already covers explicitly as scenario 2).
        long keysEmitted = (idx + 1L) * massScale;

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink trace = new RecordingTraceSink();
        // lo anchored at the keyspace's own minimum (never the global ⊥) — the same StealMath.spanIn
        // reasoning as thiefCascade's rootLo (see that method's javadoc): est/density math normalizes
        // against the full (lo, hi] window, so a ⊥ lo spreads it over the empty byte-space below this
        // shape's real data and collapses the signal regardless of massScale.
        WorkerState ws = WorkerStates.of(500, lo, lo, hi);
        ws.addKeysEmitted(keysEmitted);
        ws.recordPage(lo, cursorTo, keysEmitted);
        // This is the FIRST carve attempt on this victim: start selfSplit[1] clear of the
        // rate-limit window so the attempt can reach the remaining-work / demand / pivot gates
        // instead of tripping the page-spacing rate limit unconditionally on every fixture.
        long[] selfSplit = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(metrics, trace, workerCount, maxKeys, () -> outstanding,
                () -> workerCount), ws, cursorTo, selfSplit, metrics, trace);
    }

    // ============================================================================================
    // seed.seed_specs
    // ============================================================================================

    private void seedSpecsAttempt(GoldenTrace.Fixture fx, List<byte[]> keyspace, byte[] prefix, int workerCount)
            throws SwathException, InterruptedException {
        GoldenTrace.ProbeLog probes = new GoldenTrace.ProbeLog();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace)
                .interceptor(probes.interceptor())
                .build();
        SeedStep step = SeedSteps.of(fetcher, prefix, workerCount, metrics, EngineToggles.DEFAULT);

        Map<String, Long> before = GoldenTrace.snapshotReasons(metrics);
        List<NodeSpec> specs = step.seedSpecs(RUN_ID, SeedMode.SHALLOW);
        Map<String, Long> after = GoldenTrace.snapshotReasons(metrics);

        ObjectNode event = GoldenTrace.newEvent("seed.seed_specs", fx.name(), fx.nextSeq());
        ObjectNode view = GoldenTrace.newNode();
        GoldenTrace.putHex(view, "prefix", prefix);
        view.put("worker_count", workerCount);
        event.set("view", view);
        ArrayNode probesArr = event.putArray("probes");
        for (ObjectNode p : probes.calls()) {
            probesArr.add(p);
        }
        event.set("reason_deltas", GoldenTrace.reasonDeltasNode(before, after));

        RunSummary.SeedSummary seed = metrics.summary(Duration.ofMillis(1), "WORK_STEALING", 0L, 0L).seed();
        ArrayNode levels = event.putArray("levels");
        for (RunSummary.SeedSummary.SeedDecision d : seed.decisions()) {
            ObjectNode ln = GoldenTrace.newNode();
            ln.put("prefix_display", d.prefix());
            ln.put("fanout", d.fanout());
            ln.put("truncated", d.truncated());
            ln.put("classification", d.classification());
            ln.put("cuts_kept", d.cutsKept());
            ln.put("cuts_discarded", d.cutsDiscarded());
            levels.add(ln);
        }

        ObjectNode decision = GoldenTrace.newNode();
        decision.put("mode", seed.mode());
        decision.put("probes", seed.probes());
        decision.put("cut_points", seed.cutPoints());
        decision.put("synthesized_cuts", seed.synthesizedCuts());
        ArrayNode ranges = decision.putArray("ranges");
        for (NodeSpec spec : specs) {
            ObjectNode r = GoldenTrace.newNode();
            GoldenTrace.putHex(r, "lo", spec.rangeStart());
            GoldenTrace.putHex(r, "hi", spec.rangeEnd());
            GoldenTrace.putHex(r, "cursor", spec.cursor());
            ranges.add(r);
        }
        event.set("decision", decision);
        fx.record(event);
    }
}
