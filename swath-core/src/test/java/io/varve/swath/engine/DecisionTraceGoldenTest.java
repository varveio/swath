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
import io.varve.swath.error.ListingException;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
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
 * B1 decision-trace goldens — the safety net every seam-extraction slice (B2 thief brain, B3
 * owner-split governor, B4 pacing, B5 seed planner) is checked against: recorded
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
 * before committing. See {@code docs/ops/dev/decision-trace-goldens.md}.
 *
 * <p><b>Coverage matrix</b> (decision site × fixture — every site appears in ≥3 fixtures):
 * <ul>
 *   <li>{@code thief.steal}: deep-narrow, flat-wide, explosion-1to1, partition-key-value,
 *       thief-edge-cases (5)</li>
 *   <li>{@code owner_self_split}: deep-narrow, flat-wide, explosion-1to1, partition-key-value,
 *       owner-split-gates (5)</li>
 *   <li>{@code seed.seed_specs}: deep-narrow, flat-wide, explosion-1to1, partition-key-value (4)</li>
 *   <li>{@code pacing.steal_paced}: pacing-trip-and-recover, pacing-below-threshold,
 *       pacing-multi-victim-isolation (3)</li>
 * </ul>
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
        // cursorTo well inside (lo, hi]: a large realized remaining span for the owner-split gate.
        ownerSplitScenarios(fx, keyspace, 0.5, 4, 20);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void flatWide() throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(400);
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("flat-wide");
        seedSpecsAttempt(fx, keyspace, new byte[0], 4);
        thiefCascade(fx, keyspace, new byte[0], b("flat0"), 8);
        ownerSplitScenarios(fx, keyspace, 0.5, 4, 20);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void explosion1to1() throws Exception {
        // Every prefix carries exactly one object — SeedStep's tiny-leaf-explosion shape.
        List<byte[]> keyspace = Keyspaces.tinyManyPrefix(200, 1);
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("explosion-1to1");
        seedSpecsAttempt(fx, keyspace, new byte[0], 4);
        thiefCascade(fx, keyspace, new byte[0], b("p99999"), 6);
        ownerSplitScenarios(fx, keyspace, 0.5, 1, 20);
        GoldenTrace.writeOrVerify(fx);
    }

    @Test
    void partitionKeyValue() throws Exception {
        List<byte[]> keyspace = hivePartitioned(20, 10);
        GoldenTrace.Fixture fx = new GoldenTrace.Fixture("partition-key-value");
        seedSpecsAttempt(fx, keyspace, new byte[0], 4);
        thiefCascade(fx, keyspace, new byte[0], b("dt=2024-01-99"), 6);
        ownerSplitScenarios(fx, keyspace, 0.5, 1, 20);
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
        long[] selfSplit4 = {0, -OwnerSelfSplit.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m4, t4, 4, 100, () -> 4L, () -> 4),
                gated, b("d/002500"), selfSplit4, m4, t4);

        // 5. Successful carve: a large bounded dense drain, single worker (no demand gate), clear
        //    of the rate limit -> OWNER_SPLIT.self_published.
        RunMetrics m5 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t5 = new RecordingTraceSink();
        WorkerState published = denseVictim(5, "d/00", "d/05");
        long[] selfSplit5 = {0, -OwnerSelfSplit.SELF_SPLIT_MIN_PAGES_BETWEEN};
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

    /**
     * Iteratively drains one root victim over {@code attempts} single {@link Thief#steal} calls: each
     * successful split's child is enqueued back into the live pool (via {@link Thief.ChildSink}), so
     * later attempts steal from whichever candidate has the widest remaining span — the same
     * single-thief-at-a-time choreography a real fleet's idle loop performs one attempt at a time,
     * without the scheduler nondeterminism a multi-thief storm would introduce.
     */
    private void thiefCascade(GoldenTrace.Fixture fx, List<byte[]> keyspace, byte[] scanPrefix, byte[] hi,
                               int attempts) throws SwathException, InterruptedException {
        GoldenTrace.ProbeLog probes = new GoldenTrace.ProbeLog();
        RecordingTraceSink trace = new RecordingTraceSink();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace)
                .interceptor(probes.interceptor())
                .build();
        List<WorkerState> pool = new ArrayList<>();
        pool.add(WorkerStates.of(1, null, null, hi));
        AtomicLong nextChildId = new AtomicLong(1000L);
        StubCheckpointStore store = new StubCheckpointStore(s -> nextChildId.getAndIncrement());
        Thief.ChildSink sink = (childId, lo, childHi) -> pool.add(WorkerStates.of(childId, lo, null, childHi));
        Thief thief = new Thief(store, fetcher, RUN_ID, scanPrefix, ListingMode.OBJECTS, sink, metrics,
                EngineToggles.DEFAULT, trace);

        for (int i = 0; i < attempts; i++) {
            recordStealAttempt(fx, thief, pool, store, metrics, probes, trace);
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
            int workerCount, int maxKeys) throws SwathException, InterruptedException {
        if (keyspace.isEmpty()) {
            return;
        }
        List<byte[]> sorted = new ArrayList<>(keyspace);
        sorted.sort(Arrays::compareUnsigned);
        byte[] max = sorted.get(sorted.size() - 1);
        byte[] hi = Arrays.copyOf(max, max.length + 1);   // strictly > every key (prefix rule)
        int idx = Math.max(0, Math.min(sorted.size() - 1, (int) (sorted.size() * cursorFraction)));
        byte[] cursorTo = sorted.get(idx);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink trace = new RecordingTraceSink();
        WorkerState ws = WorkerStates.of(500, null, null, hi);
        ws.addKeysEmitted(idx + 1L);
        ws.recordPage(sorted.get(0), cursorTo, idx + 1L);
        // This is the FIRST carve attempt on this victim: start selfSplit[1] clear of the
        // rate-limit window so the attempt can reach the remaining-work / demand / pivot gates
        // instead of tripping the page-spacing rate limit unconditionally on every fixture.
        long[] selfSplit = {0, -OwnerSelfSplit.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(metrics, trace, workerCount, maxKeys, () -> 0L,
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
        SeedStep step = new SeedStep(fetcher, prefix, workerCount, metrics, EngineToggles.DEFAULT);

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
