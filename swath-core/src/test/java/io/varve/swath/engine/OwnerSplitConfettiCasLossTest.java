/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.engine.policy.OwnerSplitGovernor;
import io.varve.swath.engine.policy.OwnerSplitSkipReason;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

/**
 * <b>The executor-level half of the confetti probe claim, on the path where the claim LOSES.</b>
 *
 * <p>{@link OwnerSplitGovernor} decides {@code PROBE} purely from the {@code probeSeq} its view was
 * built from, so every owner that snapshotted the same value decides {@code PROBE} and returns a
 * {@code Carve} whose terminal reason is {@code confetti_probe}. Exactly one of them may actually
 * carve, so {@link OwnerSelfSplit} resolves the claim against the run-scoped gate
 * ({@code ConfettiFeedbackGate#claimProbeSlot}, a single {@code compareAndSet}) and suppresses the
 * losers — issue #31.
 *
 * <p>That leaves a deliberate divergence on the losing path, which this pins: the metric says
 * {@code OWNER_SPLIT.confetti_suppressed} (what the executor did), while the decision trace carries
 * the <em>carve</em> the governor decided (what the policy layer saw). Both are correct about their
 * own layer and the two are read together, so neither may drift into agreeing with the other by
 * accident. Only the governor-level half of this was pinned before — see {@code
 * DecisionTraceGoldenTest} scenario 9, which pins the WINNING probe and the suppressed call after
 * it, both of which take the claim uncontended.
 *
 * <p>The traced reason is asserted as "the same reason the winner traced, and not the suppression
 * code" rather than as a literal string. A carve's terminal reason is the LAST gate its path
 * engaged, so on this victim shape it is {@code pivot_reflect_clamped}, with {@code confetti_probe}
 * recorded alongside it as an engagement (visible in the {@code owner-split-gates} golden's
 * scenario-9 deltas). Which pivot gate fires last is a property of the fixture; that the loser
 * traces a carve while its metric says suppressed is the property of the mechanism.
 *
 * <p><b>Why this is deterministic and not a hopeful race.</b> {@link OwnerSelfSplit} snapshots the
 * gate BEFORE it reads {@code outstanding} to build the view, so an {@code outstanding} supplier
 * that blocks both callers on a barrier guarantees both have snapshotted the same {@code probeSeq}
 * before either reaches its claim. The {@code compareAndSet} then decides the winner — the real
 * mechanism, exercised at the real seam, with the interleaving that makes it interesting forced
 * rather than waited for.
 */
class OwnerSplitConfettiCasLossTest {

    private static final long RUN_ID = 7L;
    private static final int MAX_KEYS = 100;

    /** {@code PROBE_K - 1}: the over-threshold calls that carry probeSeq to the probe boundary. */
    private static final int CALLS_TO_PROBE_BOUNDARY = 15;

    @Test
    void theLoserOfTheProbeClaimIsSuppressedWhileItsTraceStillNamesTheCarveItDecided() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AtomicBoolean racing = new AtomicBoolean(false);
        CyclicBarrier bothSnapshotted = new CyclicBarrier(2);
        // Read AFTER the gate snapshot and BEFORE the claim — the one seam that can hold both callers
        // between the two, without touching the code under test.
        LongSupplier outstanding = () -> {
            if (racing.get()) {
                try {
                    bothSnapshotted.await(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError("the two racers never met at the claim", e);
                }
            }
            return 0L;
        };
        // workerCount/maxKeys as DecisionTraceGoldenTest scenario 9, the pinned scenario this extends:
        // the pivot gates ahead of the confetti branch read them, and a different fleet size lands the
        // carve on a different terminal reason before the probe claim is ever reached.
        OwnerSelfSplit gov = new OwnerSelfSplit(RUN_ID, 1, MAX_KEYS, StubCheckpointStore.returning(999L),
                EngineToggles.DEFAULT, metrics, new RecordingTraceSink(), null, outstanding, () -> 1,
                (childId, lo, hi) -> { });

        driveGateOverThreshold(gov);
        for (int i = 0; i < CALLS_TO_PROBE_BOUNDARY; i++) {
            attempt(gov, 300 + i);
        }
        assertThat(gov.confettiSnapshot().probeSeq())
                .as("the next over-threshold call is the one that lands on PROBE")
                .isEqualTo(CALLS_TO_PROBE_BOUNDARY);

        // Deltas across the race only: the warm-up above already drove CALLS_TO_PROBE_BOUNDARY
        // suppressions through this same counter.
        Map<String, Long> before = reasons(metrics);
        racing.set(true);
        List<OwnerSelfSplit.OwnerSplitTrace> results = raceTwoOwners(gov);
        Map<String, Long> after = reasons(metrics);

        List<OwnerSelfSplit.OwnerSplitTrace> published = results.stream().filter(r -> r.split()).toList();
        List<OwnerSelfSplit.OwnerSplitTrace> suppressed = results.stream().filter(r -> !r.split()).toList();
        assertThat(published).as("exactly one owner may carve on a claimed probe slot").hasSize(1);
        assertThat(suppressed).as("and the other is suppressed, not carved").hasSize(1);

        // The divergence itself, on the losing side: the trace carries the CARVE the governor decided
        // from its view, while the metric carries the suppression the executor applied to it.
        assertThat(suppressed.getFirst().gateInputs().reason())
                .as("the loser's trace names the same carve the winner's does — both decided from the "
                        + "same snapshot, and the claim is resolved after the decision, not inside it")
                .isEqualTo(published.getFirst().gateInputs().reason());
        assertThat(suppressed.getFirst().gateInputs().reason())
                .as("so the loser's trace does NOT name the gate that actually stopped it")
                .isNotEqualTo(OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code());
        assertThat(delta(before, after, OwnerSplitSkipReason.CONFETTI_SUPPRESSED.code()))
                .as("while the metric names exactly that — one suppression, for the one loser")
                .isEqualTo(1L);
        assertThat(delta(before, after, "self_published"))
                .as("exactly one carve was published").isEqualTo(1L);
        assertThat(delta(before, after, "confetti_probe"))
                .as("the probe engagement is credited once, to the winner — the loser returns before "
                        + "applyEngagements, so a carve that never happened is never credited")
                .isEqualTo(1L);

        // The sequence advanced twice: the winner's CAS and the loser's consumed slot (issue #31 —
        // N racers at s leave the counter at s + N, exactly as the pre-#22 fused increment did).
        assertThat(gov.confettiSnapshot().probeSeq())
                .as("winner and loser each consumed a slot")
                .isEqualTo(CALLS_TO_PROBE_BOUNDARY + 2);
    }

    /** Both owners enter {@code maybeOwnerSelfSplit} and are held until each has snapshotted the gate. */
    private static List<OwnerSelfSplit.OwnerSplitTrace> raceTwoOwners(OwnerSelfSplit gov) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<OwnerSelfSplit.OwnerSplitTrace>> futures = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                long nodeId = 400 + i;
                futures.add(pool.submit(() -> attempt(gov, nodeId)));
            }
            List<OwnerSelfSplit.OwnerSplitTrace> results = new ArrayList<>();
            for (Future<OwnerSelfSplit.OwnerSplitTrace> f : futures) {
                results.add(f.get(60, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The tag/classify cycle a real run drives, until the observed confetti rate trips the gate: each
     * warm-up carve's child completes fresh, never-split and small-tallied, which classifies as
     * confetti.
     */
    private static void driveGateOverThreshold(OwnerSelfSplit gov) throws Exception {
        for (int i = 0; i < ConfettiFeedbackGate.MIN_SAMPLE; i++) {
            OwnerSelfSplit.OwnerSplitTrace warm = attempt(gov, 100 + i);
            gov.onNodeCompleted(warm.childId(),
                    WorkerStates.of(warm.childId(), warm.pivot(), warm.pivot(), warm.hi()));
        }
    }

    /** One owner-split attempt on a dense victim — the shape every scenario here carves from. */
    private static OwnerSelfSplit.OwnerSplitTrace attempt(OwnerSelfSplit gov, long nodeId) throws Exception {
        WorkerState ws = WorkerStates.of(nodeId, b("d/00"), b("d/00"), b("d/05"));
        ws.addKeysEmitted(50_000);
        ws.recordPage(b("d/00"), b("d/05"), 50_000);
        long[] selfSplit = {0, -OwnerSplitGovernor.SELF_SPLIT_MIN_PAGES_BETWEEN};
        return gov.maybeOwnerSelfSplit(nodeId, ws, b("d/002500"), selfSplit);
    }

    private static Map<String, Long> reasons(RunMetrics metrics) {
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    /** What {@code OWNER_SPLIT.<code>} moved by across the race — the warm-up drove this counter too. */
    private static long delta(Map<String, Long> before, Map<String, Long> after, String code) {
        String key = "OWNER_SPLIT." + code;
        return after.getOrDefault(key, 0L) - before.getOrDefault(key, 0L);
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
