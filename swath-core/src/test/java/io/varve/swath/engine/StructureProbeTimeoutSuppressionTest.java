/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A structure probe that TIMES OUT feeds the per-victim suppression counters, so a region that
 * cannot answer stops being re-probed.
 *
 * <p>REGRESSION: {@link Thief#structureProbesEnabled} is fed by what a probe REPORTS — a zero-fan-out
 * answer increments the flat-region streak, a non-zero answer resets it. A probe that times out
 * reports nothing: it throws past the fan-out accounting entirely. So a timing-out probe left every
 * suppression counter untouched, and <em>the timeout destroyed the very evidence that would have
 * stopped the next probe</em>. The thief kept re-probing regions that had already proved they could
 * not answer, paying the full escalated budget (base, then 2x, then 4x) each time.
 *
 * <p>This is the flat-directory case no per-attempt budget can fix — a {@code delimiter=/} scan
 * crossing a very large flat region can exceed any fixed deadline — which is why the fix stops
 * issuing the wrong probe SHAPE rather than re-sizing its deadline. See
 * {@code docs/internals/probe-budgets.md} §5 and {@code docs/ops/dev/field-investigations.md}.
 */
final class StructureProbeTimeoutSuppressionTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static WorkerState victim() {
        return WorkerStates.of(1L, b("data/"), b("data/"), b("data/z"));
    }

    /**
     * The core property: consecutive timed-out probes accumulate on the victim and cross the
     * suppression threshold. Against the old behaviour the counter never moves and this fails at
     * the first assertion.
     */
    @Test
    void consecutiveTimedOutProbesAccumulateOnTheVictim() {
        WorkerState victim = victim();

        assertThat(victim.consecutiveTimedOutStructureProbes())
                .as("a fresh victim has no timeout streak").isZero();

        victim.recordTimedOutStructureProbe();
        assertThat(victim.consecutiveTimedOutStructureProbes()).isEqualTo(1);

        victim.recordTimedOutStructureProbe();
        assertThat(victim.consecutiveTimedOutStructureProbes())
                .as("two consecutive timeouts is evidence about the keyspace, not the network")
                .isEqualTo(2);
    }

    /** A probe that ANSWERS breaks the streak — the counter is consecutive, not cumulative. */
    @Test
    void anAnsweringProbeResetsTheTimeoutStreak() {
        WorkerState victim = victim();
        victim.recordTimedOutStructureProbe();
        victim.recordTimedOutStructureProbe();

        victim.resetTimedOutStructureProbes();

        assertThat(victim.consecutiveTimedOutStructureProbes())
                .as("an isolated blip must never accumulate into permanent suppression")
                .isZero();
    }

    /**
     * The two streaks are independent: a timeout must not be laundered into "this region is flat",
     * because they are different evidence with different diagnoses (and very different cost).
     */
    @Test
    void timeoutAndZeroFanoutStreaksAreIndependent() {
        WorkerState victim = victim();

        victim.recordTimedOutStructureProbe();
        victim.recordTimedOutStructureProbe();

        assertThat(victim.consecutiveZeroFanoutProbes())
                .as("timeouts prove nothing about fan-out -- the probe never answered")
                .isZero();

        victim.resetTimedOutStructureProbes();
        victim.recordZeroFanoutStructureProbe();

        assertThat(victim.consecutiveZeroFanoutProbes()).isEqualTo(1);
        assertThat(victim.consecutiveTimedOutStructureProbes()).isZero();
    }

    /**
     * INTEGRATION: driving a real {@link Thief} whose structure probe times out must move the
     * victim's counter and fire the engagement counter — proving the wiring, not just the field.
     * Against the old behaviour the fetch threw past the accounting and the counter stayed at 0.
     *
     * <p>Uses the baton-passing shape from {@code ThiefParentEmptyPivotTest}: {@code hi} is the
     * adjacent-code-point bound that forces byte-midpoint to sliver, so a {@code delimiter=/}
     * structure probe is what the thief must reach for.
     */
    @Test
    void aTimingOutStructureProbeRecordsAgainstTheVictimThroughTheThief() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AtomicInteger delimiterProbes = new AtomicInteger();
        List<byte[]> siblingDirs = new ArrayList<>();
        for (int i = 0; i <= 199; i++) {
            siblingDirs.add(b("led/%04d/obj".formatted(i)));
        }
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(siblingDirs)
                .interceptor((req, idx, page) -> {
                    // Only the delimiter=/ structure probe wedges; point probes answer normally, so
                    // the streak observed below can only have come from the structure path.
                    if (req.delimiter() != null && req.delimiter().length > 0) {
                        delimiterProbes.incrementAndGet();
                        throw ThrottleException.attemptTimeout("wedged structure probe");
                    }
                    return page;
                })
                .build();
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);
        GaugedFetcher probeFetcher = new GaugedFetcher(delegate, gauge, false, false, metrics,
                CancellationToken::new, ms -> { }, RetryPolicy.RIDE_OUT);
        WorkerState victim = WorkerStates.of(1, b("led/0100/"), b("led/0100/obj"), b("led/02"));
        victim.addKeysEmitted(100);
        Thief thief = Thiefs.of(StubCheckpointStore.returning(42L), probeFetcher, 1L, new byte[0],
                ListingMode.OBJECTS, (childNodeId, childLo, childHi) -> { }, metrics);

        thief.steal(List.of(victim));

        assertThat(delimiterProbes.get())
                .as("the structure probe path was actually exercised").isPositive();
        assertThat(victim.consecutiveTimedOutStructureProbes())
                .as("the timeout is attributed to the victim instead of vanishing with the exception")
                .isPositive();
        assertThat(counter(metrics, "STRUCTURE", "probe_timed_out"))
                .as("§5: the path must be visible in the metrics alone").isPositive();
        assertThat(victim.consecutiveZeroFanoutProbes())
                .as("a timeout must not be laundered into flat-region evidence").isZero();
    }

    private static double counter(RunMetrics m, String outcome, String reason) {
        Counter c = m.registry().find("swath.steal_reason")
                .tag("outcome", outcome).tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    /**
     * Only an ATTEMPT_TIMEOUT counts. A 503/5xx is store backpressure — a statement about the
     * store's load, not this keyspace's shape — so suppressing structure discovery on it would
     * misattribute a transient service condition to the bucket.
     */
    @Test
    void onlyAttemptTimeoutKindIsTreatedAsProbeTimeoutEvidence() {
        assertThat(ThrottleException.Kind.ATTEMPT_TIMEOUT.votesAimdDown())
                .as("an attempt timeout is client-side: it never votes AIMD down")
                .isFalse();
        assertThat(ThrottleException.Kind.SLOWDOWN.votesAimdDown())
                .as("a 503 IS store backpressure -- a different signal, deliberately not suppression evidence")
                .isTrue();
    }

    /**
     * INTEGRATION companion to {@link #aTimingOutStructureProbeRecordsAgainstTheVictimThroughTheThief}:
     * a delimiter probe that faults with {@code SLOWDOWN} (a 503, store backpressure) instead of
     * {@code ATTEMPT_TIMEOUT} must NOT move the timeout-suppression counters through the real
     * {@link Thief#probeStructure}, not just at the {@code Kind.votesAimdDown()} unit level above.
     *
     * <p>Drives the fetcher directly (not {@link GaugedFetcher}-wrapped): {@code
     * Kind#votesAimdDown()} faults are a deliberately UNBOUNDED ride-out in {@code GaugedFetcher}
     * (AIMD paces them, not a retry cap — see its {@code fetchPage} javadoc), so a mock that always
     * answers with {@code SLOWDOWN} would spin forever behind that wrapper. What this test isolates
     * is {@code Thief}'s own kind check, which is exactly what a SLOWDOWN would still have to clear
     * if it ever did reach {@code probeStructure} (e.g. once cap-exhaustion unwinds one).
     */
    @Test
    void aSlowdownFaultedStructureProbeDoesNotRecordAgainstTheVictimThroughTheThief() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        AtomicInteger delimiterProbes = new AtomicInteger();
        List<byte[]> siblingDirs = new ArrayList<>();
        for (int i = 0; i <= 199; i++) {
            siblingDirs.add(b("led/%04d/obj".formatted(i)));
        }
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(siblingDirs)
                .interceptor((req, idx, page) -> {
                    if (req.delimiter() != null && req.delimiter().length > 0) {
                        delimiterProbes.incrementAndGet();
                        throw ThrottleException.slowDown("wedged structure probe, 503");
                    }
                    return page;
                })
                .build();
        WorkerState victim = WorkerStates.of(1, b("led/0100/"), b("led/0100/obj"), b("led/02"));
        victim.addKeysEmitted(100);
        Thief thief = Thiefs.of(StubCheckpointStore.returning(42L), delegate, 1L, new byte[0],
                ListingMode.OBJECTS, (childNodeId, childLo, childHi) -> { }, metrics);

        thief.steal(List.of(victim));

        assertThat(delimiterProbes.get())
                .as("the structure probe path was actually exercised").isPositive();
        assertThat(victim.consecutiveTimedOutStructureProbes())
                .as("SLOWDOWN is store backpressure, not evidence about this keyspace's shape")
                .isZero();
        assertThat(counter(metrics, "STRUCTURE", "probe_timed_out"))
                .as("the timeout-suppression metric must stay silent on a non-timeout fault").isZero();
    }
}
