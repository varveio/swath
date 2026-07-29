/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.engine.policy.Carve;
import io.varve.swath.engine.policy.DecisionRng;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.engine.policy.OwnerSplitView;
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
 *   <li>{@code seed.seed_specs}: deep-narrow, flat-wide, explosion-1to1, partition-key-value (4 FILES
 *       — a distinct count from the per-branch mark coverage immediately below, which this same
 *       campaign found conflated three times before converging: {@code HybridSeedPlanner} fires 22
 *       distinct {@code SEED.*} marks (21 via {@code mark()}, recounted directly from source, plus the
 *       {@code radix_bands} magnitude counter), of which these 4 fixtures' goldens pin only 6 —
 *       {@code delimiter_seeded}/{@code descent_cuts_subsampled}/{@code frontier_level_ordered}/
 *       {@code frontier_reordered}/{@code mass_weighted_subsample}/{@code top_complete}, recounted from
 *       the committed JSONL, not hand-counted. The other ~16 are exercised instead by
 *       {@code SeedStepTest}/{@code SeedMassAwareDescentTest} and this package's other dedicated seed
 *       tests, never by a decision-trace golden — see decision-trace-goldens.md's known gaps)</li>
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
 *   <li>{@code bisect}: 3 events, 3 fixtures</li>
 *   <li>{@code reflect} (committed hit): 4 events, 2 fixtures</li>
 *   <li>{@code reflect_hit} / {@code reflect_empty} (probe verdicts, whether or not reflect won the
 *       commit): 4 / 3 events, 2 / 3 fixtures</li>
 *   <li>{@code structure_probe} / {@code structure_capped}: 9 / 6 events, 3 / 2 fixtures</li>
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
 *
 * <p>{@code ThiefPolicy#structureProbingEnabled}'s over-threshold escape hatch — the 1-in-{@code
 * STRUCTURE_SUPPRESS_RETRY_DIVISOR} draw once a victim's consecutive zero-fan-out streak reaches
 * {@code STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD} — is now pinned on BOTH sides in {@code
 * thiefCascadeMechanisms} (scenarios 8 and 9), only possible since issue #20 injected the draw as
 * a {@link DecisionRng} rather than reaching for ambient {@code ThreadLocalRandom}: a stub
 * returning 0 makes the draw hit (probing proceeds, {@code PIVOT.structure_probe} — the same
 * terminal mechanism as an under-threshold victim); a stub returning any other value in range
 * makes it miss ({@code STRUCTURE.suppressed_zero_fanout}, falling through to the
 * structure-exhausted fallback). Before this fix a golden reaching the miss side would have been
 * encoding a 1-in-64 flake that had simply never fired, not a genuine (view → decision) property.
 *
 * <p>{@code owner_self_split}'s per-fixture count above is the same trap the thief mechanism
 * count exists to avoid: it says nothing about which of {@link OwnerSplitGovernor}'s gate chain
 * (algorithms.md §3.3) each event actually exercises. {@link OwnerSplitSkipReason} has SEVEN
 * constants: six genuine suppression gates (REMAINING_EST_FLOOR/RATE_LIMITED/DEMAND_GATED/
 * FLOOR_REFLECTED_BLOCKED/CONFETTI_SUPPRESSED/UNSPLITTABLE_PIVOT) plus OPEN_FRONTIER, which is not
 * a gate declining a carve — it is a path that structurally does not apply, since a {@code null}
 * upper bound has no midpoint to interpolate — but is COUNTED all the same (issue #76: it is the
 * diagnostic that tells a serial tail apart from a gate-blocked one, not a rate to compare against
 * a sibling gate's) — an earlier version of this section said "SIX gates" and omitted
 * OPEN_FRONTIER/UNSPLITTABLE_PIVOT from that count, since corrected. The bullet list below covers
 * all seven {@code OwnerSplitSkipReason} values plus the two Carve-side outcomes
 * ({@code confetti_probe}, the successful-carve {@code self_published}) — nine bullets, not "gates"
 * uniformly, since two of them are outcomes of a carve succeeding, not a gate declining one. The
 * per-outcome count (grepping the committed goldens' {@code reason_deltas}, not hand-counted):
 * <ul>
 *   <li>{@code OPEN_FRONTIER} (structural, not a suppression, but counted like its siblings since
 *       issue #76 — see {@code OwnerSplitSkipReason}'s javadoc): 1 event, 1 fixture
 *       (owner-split-gates)</li>
 *   <li>{@code REMAINING_EST_FLOOR}: 2 events, 2 fixtures (owner-split-gates, explosion-1to1)</li>
 *   <li>{@code RATE_LIMITED}: 1 event, 1 fixture (owner-split-gates)</li>
 *   <li>{@code DEMAND_GATED}: 2 events, 2 fixtures (owner-split-gates, flat-wide)</li>
 *   <li>{@code FLOOR_REFLECTED_BLOCKED}: 1 event, 1 fixture (owner-split-gates) — a dense-head/
 *       thinning-tail EWMA transition ({@code ObservedMassFloorContractTest}'s own recipe, driven
 *       through this recorder instead of {@code StealMath.childTailBelowObservedMassFloor}
 *       directly), decoupling the victim's own cursor (feeds {@code observedDensityRatio}) from
 *       the separate post-commit {@code cursorTo} (feeds {@code est}), as scenarios 3-5 already do</li>
 *   <li>{@code CONFETTI_SUPPRESSED}: 1 event, 1 fixture (owner-split-gates) — {@code
 *       MIN_SAMPLE}=8 tagged children warmed to 100% confetti via the real {@code
 *       maybeOwnerSelfSplit}/{@code onNodeCompleted} tag/classify cycle on a shared {@link
 *       OwnerSelfSplit}, then one more call past warmup; deterministic because it is the FIRST
 *       call to reach the gate's over-threshold branch on that instance (every warm-up call
 *       itself returned {@code CARVE} without touching the probe sequence)</li>
 *   <li>{@code confetti_probe} (the every-{@code PROBE_K}-th over-threshold call let through,
 *       engaged on the {@link Carve} it produces): 1 event, 1 fixture (owner-split-gates) — issue
 *       #22's fix made {@code probeSeq} a genuine {@link OwnerSplitView} field, so THIS is a real
 *       (view → decision) pin: the same warm-up as {@code CONFETTI_SUPPRESSED}'s scenario, then 15
 *       more plain (unrecorded) attempts each landing on {@code SUPPRESSED} and consuming one
 *       probe slot, so the RECORDED call's view shows {@code probe_seq: 15} and lands on the
 *       {@code PROBE_K}-th (16th) over-threshold consult — the recorded view is what makes this a
 *       property of the scenario rather than an accident of how many {@code decide()} calls
 *       happened to precede it (the failure mode issue #22 described, before its fix). The RECORDED
 *       event alone cannot detect a failure to actually consume the probe slot on this carve (an
 *       independent review's finding): {@code view}/{@code decision}/{@code reason_deltas} are all
 *       identical whether or not {@code CONSUME_CONFETTI_PROBE_SLOT} was applied, since the engagement
 *       and the decision are recorded from the GOVERNOR's return value, not from the gate's post-call
 *       state. So this scenario also asserts, OUTSIDE the golden, that {@code probeSeq} actually
 *       advanced to 16 after the call and that the very next over-threshold consult is genuinely
 *       {@code SUPPRESSED} rather than another probe carve — see the assertions immediately following
 *       this scenario's {@code recordOwnerSplitAttempt} call</li>
 *   <li>{@code UNSPLITTABLE_PIVOT}: 1 event, 1 fixture (owner-split-gates) — the byte-adjacent
 *       {@code (cursorTo, hi]} recipe below, mirroring {@code
 *       ThiefPolicyCascadeTest#unsplittable_terminalNullPivotHasNoSafeKeyStrictlyBetweenTheBounds}'s
 *       technique (a proper-prefix extension by one C0-control byte, per {@code ByteMidpoint}'s
 *       I12 null contract)</li>
 *   <li>A successful carve ({@code OWNER_SPLIT.self_published}): 4 events, 3 fixtures
 *       (deep-narrow, owner-split-gates — twice, scenarios 5 and 9 — partition-key-value) — of
 *       which two also engage {@code pivot_reflect_clamped} (owner-split-gates' scenarios 5 and 9)
 *       and one {@code pivot_reflect_lifted} (partition-key-value)</li>
 * </ul>
 * Every field {@link OwnerSplitView} carries is now visible in the recorded view, including the
 * confetti gate's {@code tagged_total}/{@code tagged_confetti}/{@code probe_seq} ({@link
 * io.varve.swath.engine.policy.ConfettiObservation}, issue #22) — so a future change to any of
 * this cascade's inputs shows up as a diffable view change here, not a silent decision flip.
 * {@code OWNER_SPLIT.self_aborted} (the durable-split CAS rejection) remains the one branch
 * genuinely out of this recorder's reach: every scenario's {@code StubCheckpointStore} always
 * accepts the split, so the abort path never triggers here — it is covered instead by {@code
 * OwnerSelfSplitContractTest}'s dedicated abort-path test (T2), which forces every split to reject.
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

        // 4. Victim selection: the wider remaining span wins. Also carries a THIRD, futility-paced
        //    candidate (node 4) purely so the recorded pool view actually shows a true
        //    pacing_skip_available at least once in this fixture matrix -- an earlier version of this
        //    recorder's poolView omitted the field entirely, so a mutant forcing
        //    pacingSkipAvailable=false in Thief's view construction evaded every fixture here
        //    byte-identically (an independent review's finding); selection skips a paced candidate
        //    outright, so node 4 changes neither which victim wins (node 3, unchanged) nor the
        //    outcome, only adding one STEAL.futility_paced engagement/CONSUME_PACING_SKIP mutation to
        //    this event's own reason_deltas -- the deliberate, reviewed regeneration this scenario's
        //    addition causes.
        WorkerState pacedJunk = WorkerStates.of(4, b("j"), b("j"), b("k"));
        for (int i = 0; i < WorkerState.FUTILITY_PACE_THRESHOLD; i++) {
            pacedJunk.recordFutileSteal();
        }
        oneShotSteal(fx, List.of(WorkerStates.of(2, b("a"), b("a"), b("c")),
                WorkerStates.of(3, b("a"), b("a"), b("z")), pacedJunk),
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

        // 8. Structure-probe suppression's escape hatch, retry-allowed side (issue #20): the SAME
        //    uncapped shape as scenario 5, but the victim already carries a consecutive zero-fan-out
        //    streak at STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD (8) -- structureProbingEnabled's first
        //    branch no longer short-circuits true, so it falls to the 1-in-64 draw. A stub
        //    DecisionRng returning 0 makes the draw hit, so probing proceeds exactly as if the victim
        //    were under threshold -> PIVOT.structure_probe, same terminal mechanism as scenario 5 --
        //    only reachable now that the draw is injected rather than ambient.
        List<byte[]> retryDirs = manyDirs("root3", 20);
        WorkerState retryAllowedVictim = WorkerStates.of(7, b("root3/"), b("root3/005/obj00"), b("root3/zzz"));
        retryAllowedVictim.addKeysEmitted(100);
        for (int i = 0; i < 8; i++) {
            retryAllowedVictim.recordZeroFanoutStructureProbe();
        }
        oneShotSteal(fx, List.of(retryAllowedVictim), MockPageFetcher.builder().keys(retryDirs).build(),
                StubCheckpointStore.returning(95L), bound -> 0);

        // 9. The same over-threshold streak, but the draw misses (a stub returning 1, any nonzero
        //    value < 64) -> structureProbingEnabled returns false this attempt ->
        //    STRUCTURE.suppressed_zero_fanout, falling through to the structure-exhausted fallback
        //    (bisect/reflect/flat-leaf) instead of issuing a structure probe.
        List<byte[]> suppressedDirs = manyDirs("root4", 20);
        WorkerState suppressedVictim = WorkerStates.of(8, b("root4/"), b("root4/005/obj00"), b("root4/zzz"));
        suppressedVictim.addKeysEmitted(100);
        for (int i = 0; i < 8; i++) {
            suppressedVictim.recordZeroFanoutStructureProbe();
        }
        oneShotSteal(fx, List.of(suppressedVictim), MockPageFetcher.builder().keys(suppressedDirs).build(),
                StubCheckpointStore.returning(96L), bound -> 1);

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
                ListingMode.OBJECTS, (childId, lo, hi) -> { }, metrics, EngineToggles.DEFAULT, trace, null);
        recordStealAttempt(fx, thief, pool, store, metrics, probes, trace);
    }

    /**
     * As the 4-arg {@link #oneShotSteal}, but with a caller-supplied {@link DecisionRng} instead of
     * {@code Thief}'s live ambient default — for pinning {@code ThiefPolicy#structureProbingEnabled}'s
     * 1-in-{@code STRUCTURE_SUPPRESS_RETRY_DIVISOR} escape hatch (issue #20), only possible now that
     * the draw is injected rather than reaching for {@code ThreadLocalRandom.current()}.
     */
    private void oneShotSteal(GoldenTrace.Fixture fx, List<WorkerState> pool, MockPageFetcher rawFetcher,
                               StubCheckpointStore store, DecisionRng rng)
            throws SwathException, InterruptedException {
        GoldenTrace.ProbeLog probes = new GoldenTrace.ProbeLog();
        RecordingTraceSink trace = new RecordingTraceSink();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = new Thief(store, new LoggingFetcher(rawFetcher, probes), RUN_ID, new byte[0],
                ListingMode.OBJECTS, (childId, lo, hi) -> { }, metrics, EngineToggles.DEFAULT, trace, null, rng);
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
                (childId, lo, hi) -> { }, metrics, EngineToggles.DEFAULT, trace, null);
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

        // 2. Too small a remaining span -> OWNER_SPLIT.remaining_est_floor (issue #16).
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

        // 6. Genuinely unsplittable THIS page: cursorTo is a proper prefix of hi, and hi's one
        //    extra byte (U+001F, below the safe-scalar floor MIN_SAFE=U+0020) leaves no safe key
        //    strictly between (ByteMidpoint's I12 null contract -- mirrors ThiefPolicyCascadeTest's
        //    terminal-null-pivot recipe of extending by one C0-control byte). Single worker (no
        //    demand gate), clear of the rate limit; keysEmitted is picked so est still clears the
        //    remaining-est floor even though the byte-adjacent tail is almost the whole weight of
        //    the consumed span (est = keysEmitted * (0x1F/65536)/(0x01/256) = keysEmitted *
        //    31/256 -- exact in double precision, both terms being small integers over powers of
        //    two) -> OWNER_SPLIT.unsplittable_pivot, decision.split=false. Transient, not cached:
        //    unlike the thief's UNSPLITTABLE.no_pivot, the SAME range is reconsidered at its next
        //    qualifying page-commit.
        RunMetrics m6 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t6 = new RecordingTraceSink();
        byte[] unsplittableCursorTo = extendByte(b("d/00"), 0x01);   // one byte past lo, minimal value
        byte[] unsplittableHi = extendByte(unsplittableCursorTo, 0x1F);   // cursorTo + one C0-control byte
        WorkerState unsplittable = WorkerStates.of(6, b("d/00"), b("d/00"), unsplittableHi);
        unsplittable.addKeysEmitted(20_000);
        long[] selfSplit6 = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m6, t6, 1, 100, () -> 0L, () -> 1),
                unsplittable, unsplittableCursorTo, selfSplit6, m6, t6);

        // 7. Observed-mass child-tail floor: a thinning tail blocks the carve even though est
        //    clears the remaining-work floor. The victim's OWN cursor sits near hi ("y" of "a".."z")
        //    so observedDensityRatio() has a real consumed span to compute from -- decoupled here
        //    from the SEPARATE post-commit cursorTo ("e") the est/pivot math uses, exactly as
        //    scenarios 3-5 decouple them. One dense recordPage seeds the trailing EWMA high, then a
        //    long run of sparse pages decays it far below the range's overall average
        //    (ObservedMassFloorContractTest's own recipe, reused here) -- observedDensityRatio()
        //    lands near the sparse page's own local/overall ratio (~0.03), and farAheadFraction
        //    bottoms out at its own floor (0.5), so childTailBelowObservedMassFloor's
        //    reach-f collapses to <= 0 regardless of est's size -> OWNER_SPLIT.floor_reflected_blocked.
        RunMetrics m7 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t7 = new RecordingTraceSink();
        WorkerState thinningTail = WorkerStates.of(7, b("a"), b("y"), b("z"));
        thinningTail.addKeysEmitted(200);
        thinningTail.recordPage(b("m"), b("n"), 500);   // dense head: seeds the EWMA high
        for (int i = 0; i < 60; i++) {
            thinningTail.recordPage(b("m"), b("x"), 3);   // sparse tail: decays the EWMA far below average
        }
        long[] selfSplit7 = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, ownerSelfSplit(m7, t7, 1, 100, () -> 0L, () -> 1),
                thinningTail, b("e"), selfSplit7, m7, t7);

        // 8. Confetti feedback gate suppresses: after MIN_SAMPLE (8) tagged owner-split children all
        //    complete confetti-sized (never split, tiny own tally), the observed confetti rate
        //    (100% > SUPPRESS_THRESHOLD) trips the gate. Warms a SHARED OwnerSelfSplit's
        //    ConfettiFeedbackGate directly (maybeOwnerSelfSplit + onNodeCompleted, the exact
        //    tag/classify cycle a real run drives) rather than through recordOwnerSplitAttempt, so
        //    only the FINAL, gate-tripping call becomes a golden event -- the eight warm-up carves
        //    themselves are ordinary denseVictim/self_published attempts, already pinned by
        //    scenario 5. This is the FIRST call to reach the gate's over-threshold branch on this
        //    fresh instance (every warm-up call itself returned CARVE via the warmup branch, which
        //    never touches the probe sequence), so the outcome is deterministic and not an accident
        //    of call-count parity -> OWNER_SPLIT.confetti_suppressed.
        RunMetrics m8 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t8 = new RecordingTraceSink();
        OwnerSelfSplit confettiGov = ownerSelfSplit(m8, t8, 1, 100, () -> 0L, () -> 1);
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            WorkerState warmVictim = denseVictim(100 + i, "d/00", "d/05");
            long[] warmSelfSplit = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
            OwnerSelfSplit.OwnerSplitTrace warmResult =
                    confettiGov.maybeOwnerSelfSplit(warmVictim.nodeId(), warmVictim, b("d/002500"), warmSelfSplit);
            // A fresh, never-split, small-tally child classifies as confetti (isConfettiChild).
            WorkerState completedChild =
                    WorkerStates.of(warmResult.childId(), warmResult.pivot(), warmResult.pivot(), warmResult.hi());
            confettiGov.onNodeCompleted(warmResult.childId(), completedChild);
        }
        WorkerState suppressed = denseVictim(9, "d/00", "d/05");
        long[] selfSplit8 = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, confettiGov, suppressed, b("d/002500"), selfSplit8, m8, t8);

        // 9. Confetti feedback gate's periodic probe lets a carve through: issue #22's fix made
        //    probeSeq a genuine OwnerSplitView field, so this is now a real (view -> decision) pin,
        //    not encoded call-count parity -- the recorded view's probe_seq is the exact input the
        //    decision reads, visible and diffable like every other field. Warms the SAME shared-gate
        //    tag/classify cycle as scenario 8 to cross the over-threshold rate, then drives 15 MORE
        //    plain (unrecorded) attempts that each land on SUPPRESSED, consuming one probe slot each
        //    -- after which probeSeq==15 and (15+1)%PROBE_K(16)==0, so THIS (the 16th over-threshold)
        //    call lands on PROBE and the carve proceeds -> OWNER_SPLIT.confetti_probe,
        //    decision.split=true.
        RunMetrics m9 = new RunMetrics(new SimpleMeterRegistry());
        RecordingTraceSink t9 = new RecordingTraceSink();
        OwnerSelfSplit probeGov = ownerSelfSplit(m9, t9, 1, 100, () -> 0L, () -> 1);
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            WorkerState warmVictim = denseVictim(200 + i, "d/00", "d/05");
            long[] warmSelfSplit = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
            OwnerSelfSplit.OwnerSplitTrace warmResult =
                    probeGov.maybeOwnerSelfSplit(warmVictim.nodeId(), warmVictim, b("d/002500"), warmSelfSplit);
            WorkerState completedChild =
                    WorkerStates.of(warmResult.childId(), warmResult.pivot(), warmResult.pivot(), warmResult.hi());
            probeGov.onNodeCompleted(warmResult.childId(), completedChild);
        }
        for (int i = 0; i < 15; i++) {   // PROBE_K - 1: drives probeSeq from 0 to 15, each call SUPPRESSED
            WorkerState suppressedVictim = denseVictim(300 + i, "d/00", "d/05");
            long[] suppressedSelfSplit = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
            probeGov.maybeOwnerSelfSplit(suppressedVictim.nodeId(), suppressedVictim, b("d/002500"),
                    suppressedSelfSplit);
        }
        WorkerState probed = denseVictim(10, "d/00", "d/05");
        long[] selfSplit9 = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        recordOwnerSplitAttempt(fx, probeGov, probed, b("d/002500"), selfSplit9, m9, t9);

        // Post-call assertions, deliberately OUTSIDE the golden: the recorded event above is silent on
        // whether CONSUME_CONFETTI_PROBE_SLOT was actually applied after the carve -- the view is a
        // BEFORE-call snapshot (probe_seq: 15) and the decision/reason_deltas come from the governor's
        // return value, which is identical whether or not the executor went on to apply the returned
        // mutation. A mutant that applies OwnerSplitMutation.CONSUME_CONFETTI_PROBE_SLOT only when the
        // decision is a Skip (never a Carve) leaves this scenario's recorded event byte-identical but
        // sticks probeSeq at 15 forever, turning every later over-threshold call into another probe
        // instead of the intended one-in-PROBE_K cadence -- caught here, not by the golden.
        assertThat(probeGov.confettiSnapshot().probeSeq())
                .as("the confetti probe slot was consumed by this carve, advancing probeSeq from 15 to 16")
                .isEqualTo(16L);
        WorkerState nextOverThreshold = denseVictim(11, "d/00", "d/05");
        long[] selfSplitNext = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        OwnerSelfSplit.OwnerSplitTrace nextResult =
                probeGov.maybeOwnerSelfSplit(nextOverThreshold.nodeId(), nextOverThreshold, b("d/002500"), selfSplitNext);
        assertThat(nextResult.split())
                .as("with the slot correctly consumed, the very next over-threshold call must land back "
                        + "on CONFETTI_SUPPRESSED, not another probe carve")
                .isFalse();
        assertThat(nextResult.gateInputs().reason())
                .as("...and its gate-decision trace names that gate")
                .isEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code());

        GoldenTrace.writeOrVerify(fx);
    }

    /**
     * {@code prefix} with one extra byte appended (a raw value, not through a {@code String}
     * literal — a C0-control byte is not safely visible/reviewable embedded in source text).
     */
    private static byte[] extendByte(byte[] prefix, int extra) {
        byte[] out = Arrays.copyOf(prefix, prefix.length + 1);
        out[prefix.length] = (byte) extra;
        return out;
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
                null, outstanding, effectiveT, (childId, lo, hi) -> { });
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
        // Issue #22: the confetti gate's tallies/probe sequence are now a genuine OwnerSplitView
        // field (ConfettiObservation), read BEFORE this call the same way maybeOwnerSelfSplit itself
        // reads them -- recording them makes the confetti_probe/confetti_suppressed pins real
        // (view -> decision), not encoded call-count parity.
        ConfettiFeedbackGate.Snapshot confettiBefore = gov.confettiSnapshot();
        view.put("tagged_total", confettiBefore.taggedTotal());
        view.put("tagged_confetti", confettiBefore.taggedConfetti());
        view.put("probe_seq", confettiBefore.probeSeq());

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
        // A non-null result no longer means "a child was published": every gate-chain evaluation past
        // the open-frontier early-out now returns its owner_split_decision payload too (§7), so the
        // published-carve question is result.split(), not result != null.
        if (result != null && result.split()) {
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
                EngineToggles.DEFAULT, trace, null);

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

    /**
     * The recorded pool view: one entry per {@link WorkerState}, carrying every field {@link
     * io.varve.swath.engine.policy.VictimView} itself carries ({@code node_id}/{@code lo}/{@code
     * cursor}/{@code hi}/{@code keys_emitted}/{@code unsplittable}/{@code pacing_skip_available})
     * <b>plus</b>, since issue #25's resolution, every field that exists only on {@link
     * io.varve.swath.engine.policy.StealAttemptView} — {@code density_fraction}, {@code
     * alphabet_digest}, {@code unchanged_since_non_productive_steal}, and both structure-probe
     * streaks — computed here for EVERY candidate the same way {@link Thief#steal} computes them for
     * the one it goes on to choose, not just the eventual choice: which candidate {@code
     * selectVictim} picks is itself part of the decision under test, so recording only the winner's
     * per-attempt state would mean this view's shape depended on the decision it exists to verify.
     * Issue #25 chose this (option 1 on the issue: record the full per-victim attempt state) over
     * leaving the subset undocumented; recording every candidate is a superset of what any single
     * attempt reads, so the golden's decision is now verifiable from its recorded view ALONE — no
     * scenario-setup side channel needed to know what the chosen victim's per-attempt state was.
     *
     * <p>{@link io.varve.swath.engine.AlphabetDigest.Snapshot} is serialized via its package-private,
     * test-only {@code base()}/{@code cleanBits()}/{@code wordsHex()} accessors (added for this),
     * hex-encoding the presence-mask words rather than exposing them as an array — the same
     * byte-exactness discipline this golden format already uses for key-valued fields, and the shape
     * {@code DecisionPathPurityTest#viewsCarryNoLiveExecutorState} requires of anything reachable from
     * a policy view.
     */
    private static ObjectNode poolView(List<WorkerState> pool) {
        ObjectNode view = GoldenTrace.newNode();
        ArrayNode candidates = view.putArray("pool");
        for (WorkerState w : pool) {
            ObjectNode c = GoldenTrace.newNode();
            c.put("node_id", w.nodeId());
            GoldenTrace.putHex(c, "lo", w.lo());
            GoldenTrace.putHex(c, "cursor", w.cursor());
            GoldenTrace.putHex(c, "hi", w.hi());
            c.put("keys_emitted", w.keysEmitted());
            c.put("unsplittable", w.unsplittable());
            c.put("pacing_skip_available", w.pacingSkipAvailable());
            c.put("density_fraction", w.densityFraction());
            c.set("alphabet_digest", alphabetDigestNode(w.alphabetDigest().snapshot()));
            c.put("unchanged_since_non_productive_steal", w.unchangedSinceNonProductiveSteal(w.snapshot()));
            c.put("consecutive_zero_fanout_structure_probes", w.consecutiveZeroFanoutProbes());
            c.put("consecutive_timed_out_structure_probes", w.consecutiveTimedOutStructureProbes());
            candidates.add(c);
        }
        return view;
    }

    /** Serializes an {@link AlphabetDigest.Snapshot} into the golden's {@code alphabet_digest} shape. */
    private static ObjectNode alphabetDigestNode(AlphabetDigest.Snapshot snapshot) {
        ObjectNode node = GoldenTrace.newNode();
        node.put("base", snapshot.base());
        node.put("words_hex", snapshot.wordsHex());
        node.put("clean_bits", snapshot.cleanBits());
        return node;
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
            ln.put("depth", d.depth());
            ln.put("quota_cut_off", d.quotaCutOff());
            levels.add(ln);
        }

        ObjectNode decision = GoldenTrace.newNode();
        decision.put("mode", seed.mode());
        decision.put("probes", seed.probes());
        decision.put("cut_points", seed.cutPoints());
        decision.put("synthesized_cuts", seed.synthesizedCuts());
        decision.put("cuts_discovered", seed.cutsDiscovered());
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
