/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * FAST/deterministic gauge-level guards for the FLOOR RE-ARM SUPPRESSION: a multiplicative decrease
 * whose CAS produces no numeric change at the floor ({@code floor(0.7*1)=0 -> max(1,0)=1}) does not
 * re-arm the 10 s {@link ConcurrencyGauge#CLEAN_WINDOW_NANOS} cool-down. Only a decrease that actually
 * reduces {@code T} ({@code next < cur}) re-arms {@code lastThrottleNs}; every other side effect (the
 * AIMD vote, {@code markCongestion}, {@code stealingAllowed=false}, the T-band and {@code
 * floor_noop_rearm} counters) stays UNCONDITIONAL, and a {@code AIMD/floor_rearm_suppressed} counter
 * fires on the suppressed-re-arm branch. Same injected-clock / fixed-window seam as
 * {@link ConcurrencyGaugeFrozenGrowthValveTest} (no real sleep; per-commit tier, matching that sibling's
 * CONC methods).
 *
 * <p><b>Preservation guards.</b> The brake- and truthfulness-preservation guards ({@link
 * #realDecreaseAtTwoStillReArms_valveHeldInWindow}, {@link #sustainedStormCollapsesAndHolds}, {@link
 * #floorNoOpStillVotes}, {@link #floorNoOpStillPausesStealing}, {@link
 * #concurrentFloorDecreasesBoundedPermitsConservedVotesExact}) pin what must NOT change: a REAL decrease
 * still re-arms; a genuine storm still collapses to 1 and HOLDS (the valve's starvation gate, not the
 * cool-down, is the load-bearing brake under sustained overload); a floor no-op still casts its AIMD
 * vote and still pauses stealing; and concurrent floor decreases stay bounded with permits conserved.
 * Each method's javadoc states the mutant it catches.
 *
 * <p><b>Deterministic driving note (inherited from the sibling valve test).</b> The valve's progress
 * numerator {@code shedWindowSuccesses} RESETS on each shed-window roll ({@link
 * ConcurrencyGauge#SHED_WINDOW_BASE_NANOS} = 30 s), and the valve pace is the SAME 30 s; advancing the
 * clock past the pace ALSO rolls the shed window and zeroes progress. The correct technique: (1) the FIRST
 * valve step needs no clock advance ({@code lastValveGrowthNs} starts at 0 so the pace auto-passes); (2)
 * rebuild in-window progress by feeding successes AT ONE INSTANT (no advance between them) so they
 * accumulate — the first same-instant success after a roll is STARVED (count 1) and the second clears the
 * gate (count 2) and admits. Drive {@code latencyFrozen()} with {@code onAttemptLatency(100ms)} then 5x
 * {@code onAttemptLatency(300ms)} (does not touch {@code shedWindowSuccesses}). The valve must be ACTIVE
 * (latFrozen) for the floor-503-doesn't-block-growth cases to be observable at all — a non-frozen
 * gauge would grow via the ordinary paced +1, which the cool-down gate also fences, so the suppression is
 * visible either way, but pinning it through the valve exercises the tail regime this suppression targets.
 *
 * <p>Three additional guards close races in the decrease/re-arm interleavings. {@link
 * #concurrentDecreaseVsDecrease_movedWriteBounded_windowEndsArmed}: two real racing threads land on
 * DIFFERENT CAS rungs from the same starting {@code T} (e.g. {@code 10->7} then {@code 7->4}); pins the
 * bounded final outcome (fully-applied product, permits conserved) and that {@code lastThrottleNs} ends
 * ARMED regardless of write order. {@link #floorNoOpDoesNotShortenOrExtendArmedWindow}: a floor no-op
 * after a real decrease neither extends nor shortens the REAL decrease's already-armed window. {@link
 * #floorNoOpDoesNotAccelerateStealReEnable}: pins the steal-pause RE-ENABLE timing edge —
 * a floor no-op does not push re-enable later (a sibling guard already pins that the PAUSE itself stays
 * unconditional; this pins the re-enable timing directly).
 */
final class ConcurrencyGaugeFloorRearmSuppressionTest {

    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    /**
     * The valve pace. It equals {@link ConcurrencyGauge#SHED_WINDOW_BASE_NANOS} by construction (see the
     * gauge's {@code VALVE_PACE_NANOS}); this class references the shed-window constant so it stays correct
     * and compiles today. Advancing by {@code VALVE_PACE + 1} rolls the shed window AND clears the valve
     * pace in one step.
     */
    private static final long VALVE_PACE = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    /** Floor-503 batch size for the vote-truthfulness, counter-semantics and CONC guards. */
    private static final int FLOOR_BATCH = 5;
    /** Thread count for the CONC concurrent-floor-decrease burst. */
    private static final int CONC_THREADS = 8;

    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);   // non-zero base (0 = "unset" sentinel)

    private ConcurrencyGauge newGauge(int tMax, int initialT, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, initialT);
    }

    private static double stealReason(RunMetrics metrics, String outcome, String reason) {
        var c = metrics.registry().find("swath.steal_reason").tags("outcome", outcome, "reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    /** {@code AIMD/floor_rearm_suppressed} — the NEW counter (absent on current code -> reads 0.0). */
    private static double suppressed(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "floor_rearm_suppressed");
    }

    /** {@code AIMD/floor_noop_rearm} — the W0 no-op-decrease-EVENT counter (exists today; semantics unchanged). */
    private static double floorNoopRearm(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "floor_noop_rearm");
    }

    /** {@code AIMD/growth_blocked_cooldown} — a success's growth eaten by the cool-down gate. */
    private static double growthBlockedCooldown(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "growth_blocked_cooldown");
    }

    /** {@code AIMD/decrease_at_floor} — W0 T-band counter (fires at {@code T<=2}). */
    private static double decreaseAtFloor(RunMetrics metrics) {
        return stealReason(metrics, "AIMD", "decrease_at_floor");
    }

    /** {@code GROWTH/frozen_growth_valve} — one per admitted valve step. */
    private static double valveCount(RunMetrics metrics) {
        return stealReason(metrics, "GROWTH", "frozen_growth_valve");
    }

    /** {@code swath.aimd.votes} — the honest real-503 down-vote counter (unconditional in the decrease path). */
    private static double votes(RunMetrics metrics) {
        var c = metrics.registry().find("swath.aimd.votes").counter();
        return c == null ? 0.0 : c.count();
    }

    /**
     * Drive the latency-inflation rung FROZEN (so the valve is the active growth path): seed a
     * low (100 ms) rolling-minimum baseline, then feed 5x higher (300 ms) successful-attempt latencies so
     * the short EWMA climbs past {@code 2x baseline}. Uses {@code onAttemptLatency} ONLY, which does NOT
     * touch {@code shedWindowSuccesses}, so the freeze is established WITHOUT granting progress. Copied from
     * {@link ConcurrencyGaugeFrozenGrowthValveTest#driveLatencyFrozen}.
     */
    private void driveLatencyFrozen(ConcurrencyGauge gauge) {
        gauge.onAttemptLatency(100_000_000L);            // baseline := 100 ms
        for (int i = 0; i < 5; i++) {
            gauge.onAttemptLatency(300_000_000L);        // EWMA climbs > 2*100 ms = 200 ms
        }
    }

    // ---- Floor no-op 503 does not re-arm -> a same-instant success valve-admits --------------------

    /**
     * A fresh, never-throttled gauge at the floor ({@code initialT=1}, {@code lastThrottleNs=0}) sees a
     * 503 that is a NUMERIC NO-OP ({@code floor(0.7*1)=1}); it does NOT re-arm the cool-down, so two
     * same-instant latency-frozen successes rebuild progress (count 2 > gate 1) and the valve admits
     * {@code 1 -> 2}. <b>Mutant caught:</b> an impl that re-arms on the no-op 503 makes both successes hit
     * {@code growth_blocked_cooldown}, so {@code T} stays 1 and {@code frozen_growth_valve} never fires.
     */
    @Test
    void floorNoOpDoesNotReArm_valveAdmitsSameInstantSuccess() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        driveLatencyFrozen(gauge);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503: 0.7*1 -> 1 (>= cur) -> no re-arm
        gauge.reportStatus(200);   // same instant: rolls shed window -> successes=1 -> starved hold
        gauge.reportStatus(200);   // successes=2 -> progress clears -> valve admits (no cool-down in the way)

        assertThat(gauge.effectiveT())
                .as("floor no-op 503 does not re-arm the cool-down -> a same-instant progress+latFrozen "
                        + "success valve-admits 1->2; RED today (the no-op re-arms, T pinned at 1)")
                .isEqualTo(2);
        assertThat(valveCount(metrics))
                .as("the valve fires exactly once; RED today (blocked by the re-armed cool-down)")
                .isEqualTo(1.0);
        assertThat(suppressed(metrics))
                .as("the floor no-op recorded floor_rearm_suppressed once; RED today (counter absent)")
                .isEqualTo(1.0);
    }

    // ---- A REAL decrease still re-arms (brake preserved) -------------------------------------------

    /**
     * A brake-preservation guard: a decrease that ACTUALLY reduces
     * {@code T} still re-arms the cool-down. {@code initialT=2}; a 503 does a REAL {@code 2 -> 1}
     * (re-arm); two same-instant successes WITHIN the 10 s window are then blocked ({@code
     * growth_blocked_cooldown}), so {@code T} holds at 1 and the valve does not fire. After the cool-down
     * elapses the SAME latency-frozen + progress state admits {@code 1 -> 2}, proving the earlier hold was
     * specifically the cool-down the real decrease re-armed. <b>Mutant caught:</b> an impl that gated the
     * re-arm on the wrong condition (suppressing it for REAL decreases too) would let the valve fire inside
     * the cool-down -> the mid-window {@code T==1}/{@code valveCount==0} assertions fail.
     */
    @Test
    void realDecreaseAtTwoStillReArms_valveHeldInWindow() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 2, metrics);

        driveLatencyFrozen(gauge);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // REAL decrease 2->1, re-arms lastThrottleNs
        assertThat(gauge.effectiveT()).as("real decrease 2->1").isEqualTo(1);

        gauge.reportStatus(200);   // within cool-down: blocked
        gauge.reportStatus(200);   // within cool-down: blocked
        assertThat(gauge.effectiveT())
                .as("a REAL decrease still re-arms -> in-window successes are cool-down-blocked, T holds at 1")
                .isEqualTo(1);
        assertThat(valveCount(metrics)).as("no valve step inside the re-armed cool-down").isZero();
        assertThat(growthBlockedCooldown(metrics))
                .as("both in-window successes were eaten by the cool-down the real decrease re-armed")
                .isEqualTo(2.0);
        assertThat(suppressed(metrics))
                .as("a REAL decrease is NOT a floor no-op, so it records no floor_rearm_suppressed")
                .isZero();

        now.addAndGet(ConcurrencyGauge.CLEAN_WINDOW_NANOS + 1L);   // clear the cool-down (still < shed window)
        gauge.reportStatus(200);   // same accumulated progress + latFrozen -> admits now the cool-down cleared

        assertThat(gauge.effectiveT())
                .as("once the cool-down the real decrease armed elapses, the valve admits 1->2")
                .isEqualTo(2);
        assertThat(valveCount(metrics)).as("exactly one admitted step after the cool-down cleared").isEqualTo(1.0);
    }

    // ---- Brake critical case: a sustained storm collapses to 1 and HOLDS (starvation gate, not cool-down) ---

    /**
     * A genuine sustained 503 storm from {@code initialT=64} collapses to 1 (every step is a REAL
     * decrease and re-arms), and then — with floor no-op 503s, which do NOT re-arm — {@code T} still
     * HOLDS at 1 across many windows because the load-bearing brake at the floor is the valve's
     * STARVATION GATE ({@code shedWindowSuccesses <= max(1, T/32)}), not the cool-down: each window feeds
     * exactly ONE success (<= gate 1), so the valve stays shut even though the floor 503s no longer arm a
     * cool-down. Permits stay conserved throughout. <b>Mutant caught:</b> one that ALSO dropped the
     * starvation gate (or the floor-503 re-arm) would let the lone per-window success valve-grow {@code
     * 1 -> 2} here — caught by the per-window {@code T==1} assertion. Deterministic, single-threaded
     * (injected clock) -> fast tier.
     */
    @Test
    void sustainedStormCollapsesAndHolds() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 64, metrics);

        driveLatencyFrozen(gauge);
        // Phase 1: the storm collapses T to the floor (64->...->1) — every decrease is real and re-arms.
        for (int i = 0; i < 12; i++) {
            gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);
        }
        assertThat(gauge.effectiveT()).as("a sustained 503 storm collapses T to 1").isEqualTo(1);
        assertThat(gauge.availablePermits())
                .as("permit conservation through the collapse").isEqualTo(gauge.effectiveT());

        // Phase 2: sustained storm AT the floor — floor no-op 503s (do not re-arm) plus
        // exactly one starved success per window. The starvation gate must hold T at 1.
        for (int window = 0; window < 5; window++) {
            now.addAndGet(WINDOW + 1L);                              // roll the shed window (and past the cool-down)
            gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);    // floor no-op 503
            gauge.reportStatus(200);                                // exactly 1 success this window -> starved
            assertThat(gauge.effectiveT())
                    .as("window %d: the STARVATION gate (not the cool-down) holds T at 1 under a genuine "
                            + "storm even though the floor 503 no longer re-arms", window)
                    .isEqualTo(1);
            assertThat(gauge.availablePermits())
                    .as("window %d: permits conserved at the floor", window).isEqualTo(gauge.effectiveT());
        }
        assertThat(valveCount(metrics)).as("the valve never admits under a starved storm").isZero();
    }

    // ---- First-lost-valve-step discriminator — a single floor-503 costs a re-arming impl one step ---

    /**
     * This isolates the ONE in-cool-down valve step the floor-503 re-arm suppression costs a re-arming
     * implementation — it is NOT a full "sparse-5xx every window" sawtooth regime (it fires a SINGLE
     * floor no-op 503 at the transition instant, then no further 503s; see the choreography note below
     * for why an every-window form is mechanically self-defeating at the unit level). The
     * repeated-sparse-503 sawtooth tail regime — many 503s spread SPARSER than the valve pace across a
     * long run — is validated end-to-end elsewhere, not by this unit guard. From the collapsed floor
     * ({@code initialT=1}, never throttled), a floor no-op 503 lands WHILE progress + latency-freeze
     * would otherwise grow {@code T}. That 503 does not re-arm, so the SAME-instant successes valve-admit
     * in-window ({@code 1 -> 2}); the valve then ratchets one step per ~30 s window (no further 503) and
     * {@code T} ESCAPES the floor monotonically {@code 1 -> 2 -> 3 -> 4 -> 5} with {@code
     * frozen_growth_valve == 4}. <b>Mutant caught:</b> an impl that re-arms on the floor 503
     * cool-down-blocks that in-window success and LOSES the first step — pinned at {@code T == 4}, {@code
     * valveCount == 3}, {@code floor_rearm_suppressed} absent.
     *
     * <p><b>Why a single floor-503, not a per-window sawtooth.</b> A per-window "one floor 503 every
     * window" form is mechanically impossible and self-defeating: a valve step lifts {@code T} off the
     * floor, so the next window's 503 is a REAL decrease that knocks {@code T} back and (still,
     * correctly) re-arms — producing a flat {@code 1 <-> 2} oscillation that neither escapes nor
     * discriminates. Escape in the sparse regime requires the valve to get consecutive un-knocked windows
     * (503s SPARSER than the 30 s valve pace). Do not change this test to fire a floor 503 every window:
     * the single-503 form is the only one that isolates the one step the cool-down re-arm suppression
     * costs a re-arming implementation, while still reproducing the escape target exactly ({@code T == 5},
     * {@code valveCount == 4}, monotonic 1->2->3->4->5).
     */
    @Test
    void sparseFloorFiveHundredThreeWithProgress_valveEscapesFloor() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        driveLatencyFrozen(gauge);

        // Window 0 (no clock advance needed — lastValveGrowthNs==0 auto-passes the pace): the floor no-op
        // 503 lands with same-instant progress. It does NOT re-arm, so the valve admits.
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503 (no re-arm)
        gauge.reportStatus(200);   // successes=1 -> starved hold
        gauge.reportStatus(200);   // successes=2 -> valve admits 1->2
        assertThat(gauge.effectiveT())
                .as("the discriminating step: the floor 503 did not re-arm, so the in-window success "
                        + "valve-admits 1->2; RED today (re-armed cool-down blocks it, T stays 1)")
                .isEqualTo(2);
        assertThat(valveCount(metrics)).as("window 0 admitted; RED today (blocked)").isEqualTo(1.0);

        // Windows 1..3: sparse regime (no further 503) — the valve ratchets +1 per pace window, unimpeded.
        int expected = 2;
        for (int window = 1; window <= 3; window++) {
            now.addAndGet(VALVE_PACE + 1L);   // roll shed window + clear valve pace
            gauge.reportStatus(200);          // successes=1 -> starved hold
            gauge.reportStatus(200);          // successes=2 -> valve admits +1
            expected++;
            assertThat(gauge.effectiveT())
                    .as("window %d: the valve ratchets the escaped floor up by +1", window)
                    .isEqualTo(expected);
        }

        assertThat(gauge.effectiveT())
                .as("T escapes the floor 1->2->3->4->5 in the sparse-5xx tail regime; RED today (pinned at "
                        + "4 — the OLD arm loses window 0 to the re-armed cool-down)")
                .isEqualTo(5);
        assertThat(valveCount(metrics))
                .as("four admitted valve steps; RED today (three — window 0 is cool-down-blocked)")
                .isEqualTo(4.0);
        assertThat(suppressed(metrics))
                .as("the single floor no-op 503 recorded floor_rearm_suppressed once; RED today (absent)")
                .isEqualTo(1.0);
    }

    // ---- Floor no-op SHED also suppresses re-arm (kind-agnostic, shared path) ----------------------

    /**
     * The suppression lives in the SHARED {@link
     * ConcurrencyGauge#multiplicativeDecrease} path, so a floor no-op SHED ({@code TIMEOUT_SHED}, {@code
     * 0.5*1 -> 1}) suppresses its re-arm exactly like a floor no-op 503 does. {@code initialT=1}; three
     * worker attempt-timeouts at one instant meet the shed gate ({@code timeouts 3 >= max(3, ceil(0.3*1))},
     * {@code successes 0 <= max(1, 1/32)}) and fire a floor no-op shed. That shed does NOT
     * re-arm the cool-down, so a same-instant success is not cool-down-blocked ({@code
     * growth_blocked_cooldown == 0}). <b>Mutant
     * caught:</b> an impl
     * that suppressed the re-arm on ONLY the THROTTLE path fails here.
     *
     * <p>The cool-down gate ({@code onSuccess} L736) is checked BEFORE the transient growth-freeze rung, so
     * even though the 3 worker timeouts also latch {@code growthFrozen()}, the {@code
     * growth_blocked_cooldown} observable is decided purely by whether the shed re-armed {@code
     * lastThrottleNs} — the clean discriminator here.
     */
    @Test
    void floorNoOpShedAlsoSuppressesReArm() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        driveLatencyFrozen(gauge);
        for (int i = 0; i < 3; i++) {
            gauge.onTransientTimeout(true);   // worker timeouts -> shed gate met at T=1 -> floor no-op shed (0.5*1)
        }
        assertThat(gauge.effectiveT()).as("the floor no-op shed removes zero concurrency (T stays 1)").isEqualTo(1);
        assertThat(suppressed(metrics))
                .as("the floor no-op SHED records floor_rearm_suppressed (shared decrease path); RED today "
                        + "(counter absent)")
                .isGreaterThanOrEqualTo(1.0);

        gauge.reportStatus(200);   // same instant: the shed did not re-arm -> not cool-down-blocked
        assertThat(growthBlockedCooldown(metrics))
                .as("the floor no-op shed suppressed its re-arm, so a subsequent success is NOT "
                        + "cool-down-blocked; RED today (the shed re-arms -> growth_blocked_cooldown fires)")
                .isZero();
    }

    // ---- Vote accounting truthful — a floor no-op still casts its AIMD vote ------------------------

    /**
     * The {@code votes ~ throttle_events} truthfulness fence: every 503
     * casts an honest AIMD down-vote EVEN when it is a floor no-op whose re-arm is suppressed. {@code
     * initialT=1}; fire {@link #FLOOR_BATCH} floor no-op 503s and assert {@code swath.aimd.votes ==
     * FLOOR_BATCH} (and the {@code floor_noop_rearm}/{@code decrease_at_floor} band counters both {@code ==
     * FLOOR_BATCH}). <b>Mutant caught:</b> an impl that moved {@code recordAimdVote()} into the CAS-success
     * branch (accidentally gating the vote on a REAL reduction) would under-count floor votes here.
     *
     * <p><b>Gauge-seam scope note.</b> A
     * {@code THROTTLE/server5xx == N} cross-check is emitted ONLY at {@code
     * S3PageFetcher}'s classification point, never by the gauge — the {@code reportStatus(503)} seam cannot
     * produce it. That {@code votes == server5xx} cross-check is a run-level invariant checked elsewhere; at
     * the gauge unit level the truthful, observable form is {@code votes == N}, asserted here.
     */
    @Test
    void floorNoOpStillVotes() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        for (int i = 0; i < FLOOR_BATCH; i++) {
            gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503
        }
        assertThat(gauge.effectiveT()).as("all floor no-ops: T never moves").isEqualTo(1);
        assertThat(votes(metrics))
                .as("every floor no-op 503 still casts its AIMD vote (unconditional) — the votes~throttle "
                        + "truthfulness fence")
                .isEqualTo((double) FLOOR_BATCH);
        assertThat(floorNoopRearm(metrics))
                .as("W0 floor_noop_rearm counts each no-op decrease EVENT").isEqualTo((double) FLOOR_BATCH);
        assertThat(decreaseAtFloor(metrics))
                .as("W0 decrease_at_floor (T<=2 band) fires per decrease").isEqualTo((double) FLOOR_BATCH);
    }

    // ---- Steal pause preserved — a floor no-op still sets stealingAllowed=false --------------------

    /**
     * {@code stealingAllowed = false} stays UNCONDITIONAL: a floor
     * no-op 503 still pauses stealing even though it re-arms nothing. {@code initialT=1}; one floor no-op
     * 503; assert {@code isStealingAllowed() == false}. <b>Mutant caught:</b> an impl that accidentally
     * moved {@code stealingAllowed = false} into the real-decrease CAS branch would leave stealing enabled
     * after a floor no-op.
     */
    @Test
    void floorNoOpStillPausesStealing() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        assertThat(gauge.isStealingAllowed()).as("fresh gauge allows stealing").isTrue();
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503

        assertThat(gauge.isStealingAllowed())
                .as("a floor no-op 503 still pauses stealing (stealingAllowed=false stays unconditional)")
                .isFalse();
    }

    // ---- Counter semantics preserved — floor_noop_rearm == floor_rearm_suppressed ------------------

    /**
     * {@code floor_rearm_suppressed} is count-IDENTICAL to {@code floor_noop_rearm} on any run (both fire
     * once per floor no-op decrease event). After {@link #FLOOR_BATCH} floor no-op 503s, assert {@code
     * floor_noop_rearm == floor_rearm_suppressed == FLOOR_BATCH}. {@code floor_noop_rearm} counts no-op
     * decrease events, not re-arms, and keeps that semantics unchanged.
     */
    @Test
    void w0CounterSemanticsPreserved_noopRearmEqualsSuppressed() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        for (int i = 0; i < FLOOR_BATCH; i++) {
            gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503
        }

        assertThat(floorNoopRearm(metrics))
                .as("W0 floor_noop_rearm keeps its semantics (one per no-op decrease event) and count")
                .isEqualTo((double) FLOOR_BATCH);
        assertThat(suppressed(metrics))
                .as("floor_rearm_suppressed is count-identical to floor_noop_rearm; RED today (absent -> 0)")
                .isEqualTo((double) FLOOR_BATCH);
        assertThat(suppressed(metrics))
                .as("the new counter and the W0 counter fire on the SAME events")
                .isEqualTo(floorNoopRearm(metrics));
    }

    // ---- Concurrent floor decreases bounded, permits conserved, votes exact ------------------------

    /**
     * Covers the §4.1 moved-write region at the floor
     * under real concurrency:
     * {@link #CONC_THREADS} threads each call {@code reportStatus(503)} off one {@link CountDownLatch} at a
     * frozen {@code now} on a {@code initialT=1} gauge. Every 503 is a floor no-op (T never leaves 1, so no
     * thread takes the real-decrease/moved-write branch), so the outcome is fully bounded: {@code T == 1},
     * {@code availablePermits() == effectiveT()} (no permit lost or over-released — the floor break never
     * calls {@code reduceAvailable}), {@code votes == CONC_THREADS} (every 503 votes, unconditional), and
     * {@code floor_rearm_suppressed == CONC_THREADS} (each floor no-op records once). Mirrors the sibling
     * valve test's CONC structure ({@code @Timeout(10)}, latch, daemon threads, frozen clock -> fast tier).
     *
     * <p><b>Scope note.</b> This form fires 503 on every thread; the §4.1 moved-write race between a valve
     * success and a concurrent REAL decrease
     * lives on the real-decrease path (T>=2) and is already guarded by {@code
     * ConcurrencyGaugeFrozenGrowthValveTest#mixedThrottleAndValveBurst_decreaseNeverLost_valveAtMostOneStep}.
     * At the floor there is NO moved write at all (a no-op suppresses the re-arm entirely), which is exactly
     * the bounded property pinned here.
     */
    @Test
    @Timeout(10)
    void concurrentFloorDecreasesBoundedPermitsConservedVotesExact() throws InterruptedException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 1, metrics);

        CountDownLatch ready = new CountDownLatch(CONC_THREADS);
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < CONC_THREADS; i++) {
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503 (frozen now)
            }, "floor-503-" + i);
            t.setDaemon(true);
            workers.add(t);
            t.start();
        }
        ready.await();
        go.countDown();
        for (Thread t : workers) {
            t.join(4_000);
        }

        assertThat(gauge.effectiveT())
                .as("concurrent floor no-op 503s never move T off the floor").isEqualTo(1);
        assertThat(gauge.availablePermits())
                .as("permit conservation: a floor no-op calls no reduceAvailable, so permits track "
                        + "effectiveT() exactly under the concurrent burst")
                .isEqualTo(gauge.effectiveT());
        assertThat(votes(metrics))
                .as("every concurrent 503 still casts its AIMD vote exactly once")
                .isEqualTo((double) CONC_THREADS);
        assertThat(suppressed(metrics))
                .as("each concurrent floor no-op records floor_rearm_suppressed once; RED today (absent)")
                .isEqualTo((double) CONC_THREADS);
    }

    // ---- CONC decrease-vs-decrease moved-write, bounded, window ends armed -------------------------

    /**
     * The decrease-vs-decrease moved-write race:
     * two REAL threads, released off one latch at a single frozen {@code now}, both call
     * {@code reportStatus(503)} concurrently from {@code initialT=10}. Because the CAS loop retries on
     * failure, exactly two REAL reductions are applied to the SAME running value regardless of scheduling —
     * whichever thread wins the first CAS takes {@code 10->7}; the loser retries against the live (now 7)
     * value and takes {@code 7->4} — so the fully-applied product {@code T==4} is the only reachable
     * outcome (there is no interleaving that skips a rung or double-applies one). Both CAS-success branches
     * write {@code lastThrottleNs.set(now)} (§4.1); the outcome pinned here is that
     * the window ends ARMED no matter which thread's write lands last — verified indirectly via an
     * immediately-following same-instant success being {@code growth_blocked_cooldown}-blocked (a lost
     * re-arm would let it recover instead).
     *
     * <p><b>Frozen-clock scope note.</b> Both threads capture
     * {@code now} at method entry via the SAME injected, non-advancing clock, so their two {@code
     * lastThrottleNs.set(now)} writes are numerically IDENTICAL — the race collapses to write-order only,
     * and write-order is unobservable here because both writes produce the same value. This test therefore
     * pins the BOUNDED OUTCOME (final T at the fully-applied product, permits conserved, window ends
     * armed) under real thread concurrency; it does NOT (and cannot, with a frozen clock) independently
     * distinguish "re-arm with the entry-captured {@code now}" from "re-arm with a fresh
     * post-decrease re-read" — that distinction only matters against a REAL advancing clock, where a
     * re-read could arm a LATER, staler timestamp than the correct entry-captured one; residual staleness
     * there is bounded (worst case: an earlier-than-correct cool-down expiry by the
     * inter-thread delay), not testable at this seam. <b>Mutant caught:</b> a CAS-success branch that
     * DROPS the re-arm entirely (on either rung) — caught directly by the follow-up success recovering
     * instead of being blocked, and/or by {@code lastThrottleNs} staying at its stale pre-race value.
     */
    @Test
    @Timeout(10)
    void concurrentDecreaseVsDecrease_movedWriteBounded_windowEndsArmed() throws InterruptedException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 10, metrics);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // races: both real reductions at a frozen now
            }, "decrease-vs-decrease-" + i);
            t.setDaemon(true);
            workers.add(t);
            t.start();
        }
        ready.await();
        go.countDown();
        for (Thread t : workers) {
            t.join(4_000);
        }

        assertThat(gauge.effectiveT())
                .as("two racing REAL decreases from T=10 land at the fully-applied product 10->7->4 "
                        + "regardless of which thread wins which CAS rung")
                .isEqualTo(4);
        assertThat(gauge.availablePermits())
                .as("permit conservation across both real reductions (10-4=6 total reduceAvailable)")
                .isEqualTo(gauge.effectiveT());
        assertThat(votes(metrics)).as("both racing 503s cast an honest AIMD vote").isEqualTo(2.0);
        assertThat(suppressed(metrics))
                .as("neither race step was a floor no-op (10->7 and 7->4 are both real reductions) -- "
                        + "confirms this race exercises the CAS-success re-arm branch, not the no-op break")
                .isZero();

        // The window ends ARMED regardless of write order: an immediately-following same-instant success
        // is cool-down-blocked (a lost re-arm on either racing thread would let it recover instead).
        gauge.reportStatus(200);
        assertThat(growthBlockedCooldown(metrics))
                .as("the window is armed at the frozen `now` instant after the race -- a dropped re-arm "
                        + "on either racing CAS-success branch would let this follow-up success recover")
                .isEqualTo(1.0);
        assertThat(gauge.effectiveT())
                .as("the follow-up success stays blocked -- T does not recover past the fully-applied product")
                .isEqualTo(4);
    }

    // ---- A floor no-op neither extends nor shortens an armed window --------------------------------

    /**
     * A floor no-op after a REAL
     * decrease is a true no-op on the cool-down window: it neither SHORTENS the armed window (a mutant that
     * clears/zeroes {@code lastThrottleNs} on the no-op path) nor EXTENDS it (a mutant that still re-arms
     * on the no-op path). {@code initialT=2}; a REAL decrease at {@code t0}
     * ({@code 2->1}) arms the window to {@code [t0, t0+10s)}. At {@code t0+3s} a floor no-op 503 ({@code
     * T} already 1) fires — a same-instant success is still {@code growth_blocked_cooldown}-blocked (proves
     * NOT shortened: the window did not collapse to zero/unset). Advancing to {@code t0+10s+1ns} — PAST the
     * original window's expiry, but well BEFORE a would-be-extended {@code t0+3s+10s=t0+13s} expiry — the
     * SAME accumulated state now recovers with the ordinary additive {@code +1} ({@code
     * congestionSeen} already latched by the {@code t0} real decrease, so no slow-start doubling): {@code
     * T: 1->2}. That recovery at {@code t0+10s+1ns} (not still blocked, as an extending mutant would show)
     * proves NOT extended. <b>Mutant caught:</b> either direction — a no-op that zeroes {@code
     * lastThrottleNs} fails the {@code t0+3s} blocked assertion; a no-op that still re-arms fails the
     * {@code t0+10s+1ns} recovery assertion.
     */
    @Test
    void floorNoOpDoesNotShortenOrExtendArmedWindow() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 2, metrics);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // REAL decrease 2->1 at t0: arms [t0, t0+10s)
        assertThat(gauge.effectiveT()).as("real decrease 2->1 at t0").isEqualTo(1);

        now.addAndGet(3_000_000_000L);   // t0+3s
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503 at t0+3s (T already 1)
        assertThat(gauge.effectiveT()).as("floor no-op at t0+3s: T unchanged").isEqualTo(1);
        assertThat(suppressed(metrics)).as("the t0+3s decrease was a floor no-op").isEqualTo(1.0);

        gauge.reportStatus(200);   // same instant (t0+3s): still inside the ORIGINAL [t0, t0+10s) window
        assertThat(growthBlockedCooldown(metrics))
                .as("NOT SHORTENED: the t0+3s no-op did not zero/clear lastThrottleNs -- a same-instant "
                        + "success is still blocked by the t0-armed window")
                .isEqualTo(1.0);
        assertThat(gauge.effectiveT()).as("still blocked at t0+3s").isEqualTo(1);

        // Advance to t0+10s+1ns: PAST the ORIGINAL window's expiry, but well before a would-be-extended
        // t0+3s+10s=t0+13s expiry (7s + 1ns further from t0+3s == ConcurrencyGauge.CLEAN_WINDOW_NANOS -
        // 3s + 1ns).
        now.addAndGet(ConcurrencyGauge.CLEAN_WINDOW_NANOS - 3_000_000_000L + 1L);
        gauge.reportStatus(200);

        assertThat(gauge.effectiveT())
                .as("NOT EXTENDED: at t0+10s+1ns (past the ORIGINAL t0-armed window, not the wrongly-"
                        + "extended t0+13s one) the same accumulated state recovers with the ordinary "
                        + "additive +1 (congestionSeen already latched): 1->2")
                .isEqualTo(2);
    }

    // ---- Floor no-op does not accelerate the steal re-enable edge ----------------------------------

    /**
     * Pins the steal-pause RE-ENABLE timing edge — a sibling guard
     * already pins that the PAUSE itself ({@code stealingAllowed=false}) stays unconditional on a floor
     * no-op; this pins that the RE-ENABLE is bound to the real-decrease window, not pushed later by an
     * intervening no-op. {@code initialT=2}; a REAL decrease at {@code t0} ({@code 2->1}) pauses stealing
     * and arms {@code [t0, t0+10s)}. A floor no-op 503 at {@code t0+5s} keeps the pause ({@code
     * stealingAllowed} stays {@code false} — a decrease, real or no-op, always sets it false) but must NOT
     * extend the window. A success at {@code t0+11s} — PAST the ORIGINAL {@code t0+10s} expiry but still
     * INSIDE a would-be-extended {@code t0+5s+10s=t0+15s} one — re-enables stealing: {@code
     * isStealingAllowed()==true}. The SAME success's growth is asserted precisely: the cool-down clears
     * (same edge), so it takes the ordinary additive {@code +1} (no latency/growth freeze driven in this
     * fixture, {@code congestionSeen} already latched by the {@code t0} real decrease): {@code T: 1->2}.
     * <b>Mutant caught:</b> a suppression that accidentally re-arms on the no-op path (extending the pause
     * window) fails the {@code t0+11s} {@code isStealingAllowed()==true} assertion, since a re-arming
     * mutant would push re-enable to {@code t0+15s} and stealing would still read {@code false} at
     * {@code t0+11s}.
     */
    @Test
    void floorNoOpDoesNotAccelerateStealReEnable() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, 2, metrics);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // REAL decrease 2->1 at t0: pause + arm [t0, t0+10s)
        assertThat(gauge.effectiveT()).as("real decrease 2->1 at t0").isEqualTo(1);
        assertThat(gauge.isStealingAllowed()).as("t0: stealing paused by the real decrease").isFalse();

        now.addAndGet(5_000_000_000L);   // t0+5s
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // floor no-op 503 at t0+5s (T already 1)
        assertThat(gauge.effectiveT()).as("floor no-op at t0+5s: T unchanged").isEqualTo(1);
        assertThat(gauge.isStealingAllowed())
                .as("the pause STAYS on the no-op -- a decrease (real or no-op) always sets "
                        + "stealingAllowed=false unconditionally (G7's invariant), independent of this "
                        + "test's re-enable-timing question")
                .isFalse();

        now.addAndGet(6_000_000_000L);   // t0+11s: past the ORIGINAL t0+10s expiry, still inside a would-be
        // extended t0+15s expiry -- the discriminating instant.
        gauge.reportStatus(200);

        assertThat(gauge.isStealingAllowed())
                .as("stealing re-enables at t0+11s -- bound to the OLD (t0) window's expiry, not a no-op-"
                        + "extended t0+15s one; pre-change (or under a re-arming mutant) this would still "
                        + "read false here")
                .isTrue();
        assertThat(gauge.effectiveT())
                .as("the same success clears the cool-down and takes the ordinary additive +1 "
                        + "(congestionSeen already latched by the t0 real decrease, no freeze driven): 1->2")
                .isEqualTo(2);
    }

    // ---- Advancing-clock decrease-vs-decrease STALE re-arm clobber ---------------------------------

    /**
     * The advancing-clock decrease-vs-decrease staleness that {@link
     * #concurrentDecreaseVsDecrease_movedWriteBounded_windowEndsArmed} cannot catch: that guard runs a
     * FROZEN clock, so its two racing {@code lastThrottleNs.set(now)} writes are numerically identical and
     * the race collapses to an unobservable write-order (its own javadoc, the "Frozen-clock scope note",
     * says so). Here a REAL decrease's re-arm would otherwise be ERASED by a LATER real decrease that
     * carries a STALER entry-captured timestamp — prevented by writing {@code lastThrottleNs} with
     * {@code accumulateAndGet(now, Math::max)} (never move it backward).
     *
     * <p><b>The failure mode this guards against ({@link ConcurrencyGauge#multiplicativeDecrease}).</b>
     * {@code now} is captured at METHOD ENTRY. If a CAS-successful REAL decrease wrote {@code
     * lastThrottleNs} UNCONDITIONALLY as {@code lastThrottleNs.set(now)}: Thread A enters at {@code t0}
     * (captures {@code now=t0}) and stalls BEFORE its CAS (here: parked inside the injected entry-clock
     * read). Thread B then runs a COMPLETE real decrease at {@code tB>t0} and re-arms {@code
     * lastThrottleNs=tB}. A resumes and CASes its OWN real decrease; an unconditional write would then
     * write {@code lastThrottleNs} back to the stale {@code t0} — erasing/shortening the cool-down B armed
     * at {@code tB}, so a REAL decrease would fail to keep the brake armed (cf.
     * {@link #realDecreaseAtTwoStillReArms_valveHeldInWindow}). Do not write {@code lastThrottleNs}
     * unconditionally on a CAS-successful decrease: use the monotonic {@code accumulateAndGet(now,
     * Math::max)}, so A's stale {@code t0} can never move {@code lastThrottleNs} backward past B's correct
     * {@code tB}.
     *
     * <p><b>Deterministic interleave (no wall-clock, no timing ratios — CI is 2-core).</b> An injected clock
     * blocks thread A's ENTRY read (after A has captured {@code t0}) on a latch, releasing only after the
     * main thread has advanced the clock to {@code tB} and driven B's full real decrease. Both decreases are
     * REAL — from {@code initialT=8}: B does {@code 8->5} ({@code floor(0.7*8)=5}) and A does {@code 5->3}
     * ({@code floor(0.7*5)=3}); no floor no-op is involved ({@code floor_rearm_suppressed==0}), so this is a
     * pure decrease-vs-decrease staleness distinct from the floor-no-op concern the sibling guards cover.
     *
     * <p><b>Contract-exercising assertion (not implementation-peeking).</b> After A's stale-timestamped real
     * decrease lands, the clock is advanced to {@code tS} chosen INSIDE the cool-down relative to B's
     * correct re-arm ({@code tS-tB < CLEAN_WINDOW}) but OUTSIDE it relative to the stale {@code t0}
     * ({@code tS-t0 >= CLEAN_WINDOW}). A success is then driven. The contract: B's real decrease must still
     * hold the brake, so the success is cool-down-blocked and {@code T} does NOT grow (stays 3).
     * <b>Mutant caught:</b> an unconditional write that erases B's arming would let the success see
     * {@code tS-t0 >= CLEAN_WINDOW}, clear the gate, and take an additive {@code +1} ({@code T} grows to
     * 4) instead.
     */
    @Test
    @Timeout(10)
    void staleEntryTimestampDoesNotClobberLaterRealDecreaseReArm() throws InterruptedException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        final long t0 = 1_000_000_000_000L;                    // A captures this at entry
        final long tB = t0 + 6_000_000_000L;                   // B's real decrease re-arms here (t0+6s)
        final long tS = t0 + 12_000_000_000L;                  // success instant (t0+12s)
        // tS-tB = 6s < CLEAN_WINDOW (10s): still cooled vs B's CORRECT re-arm -> must block growth.
        // tS-t0 = 12s >= CLEAN_WINDOW: NOT cooled vs the STALE t0 -> HEAD wrongly grows.

        final AtomicLong clock = new AtomicLong(t0);
        final AtomicReference<Thread> aThread = new AtomicReference<>();
        final AtomicBoolean aBlockArmed = new AtomicBoolean(true);
        final CountDownLatch aCapturedT0 = new CountDownLatch(1);
        final CountDownLatch releaseA = new CountDownLatch(1);

        // Injected clock: parks ONLY thread A's FIRST read (its multiplicativeDecrease entry, L677), AFTER it
        // has captured t0, so A stalls before its CAS while the main thread (B) advances the clock and runs a
        // complete real decrease. Every other read (B's entry, the later success) returns the live clock.
        LongSupplier interleavingClock = () -> {
            if (Thread.currentThread() == aThread.get() && aBlockArmed.compareAndSet(true, false)) {
                long captured = clock.get();                   // == t0 (main has not advanced yet)
                aCapturedT0.countDown();                       // A has captured its entry timestamp
                try {
                    releaseA.await(5, TimeUnit.SECONDS);       // park BEFORE A's CAS
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return captured;                               // A's `now` = the STALE t0
            }
            return clock.get();
        };

        ConcurrencyGauge gauge = new ConcurrencyGauge(64, metrics, interleavingClock, () -> WINDOW, 8);

        // Thread A: enters multiplicativeDecrease, captures now=t0, then parks before its CAS.
        Thread a = new Thread(() -> {
            aThread.set(Thread.currentThread());
            gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // real 503; blocks in the entry clock read
        }, "stale-rearm-A");
        a.setDaemon(true);
        a.start();

        assertThat(aCapturedT0.await(5, TimeUnit.SECONDS))
                .as("thread A captured its entry timestamp t0 and parked before its CAS").isTrue();

        // Main thread is B: advance the clock to tB and run a COMPLETE real decrease 8->5, arming tB.
        clock.set(tB);
        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);
        assertThat(gauge.effectiveT()).as("B's complete real decrease 8->5 at tB").isEqualTo(5);

        // Release A: it resumes and CASes its OWN real decrease 5->3, writing lastThrottleNs back to t0.
        releaseA.countDown();
        a.join(5_000);
        assertThat(a.isAlive()).as("thread A finished its decrease").isFalse();
        assertThat(gauge.effectiveT()).as("A's real decrease 5->3 landed after B's").isEqualTo(3);
        assertThat(votes(metrics)).as("both racing 503s cast an honest AIMD vote").isEqualTo(2.0);
        assertThat(suppressed(metrics))
                .as("both decreases were REAL (8->5, 5->3); no floor no-op -> pure decrease-vs-decrease staleness")
                .isZero();

        // Drive a success INSIDE B's cool-down (tS-tB=6s<10s) but OUTSIDE the stale t0 window (tS-t0=12s>=10s).
        clock.set(tS);
        gauge.reportStatus(200);

        assertThat(gauge.effectiveT())
                .as("CONTRACT: B's real decrease at tB must keep the brake armed, so a success 6s later is "
                        + "cool-down-blocked and T holds at 3 -- the monotonic re-arm write (Math::max) "
                        + "prevents A's stale t0 from ever moving lastThrottleNs backward past B's tB.")
                .isEqualTo(3);
        assertThat(growthBlockedCooldown(metrics))
                .as("the blocked success recorded growth_blocked_cooldown -- the monotonic write keeps the "
                        + "window armed instead of letting it recover early")
                .isEqualTo(1.0);
    }
}
