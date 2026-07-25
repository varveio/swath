/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.ConcurrencyGauges;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Guards the AIMD gauge's slow-start ramp.
 *
 * <p><b>The contract under test.</b> A concurrent-fleet launch that
 * sees an ApiCallAttemptTimeout storm from {@code t=0} must NOT open at {@code Tmax} and hammer a
 * saturated path. So the gauge starts at {@code effectiveT = min(}{@link
 * ConcurrencyGauge#SLOW_START_INITIAL_T}{@code , Tmax)} instead of {@code Tmax}, and while NO congestion
 * signal has EVER been observed this run (no attempt-timeout, no real-503 down-vote, no shed) each
 * <i>paced</i> growth step DOUBLES {@code T} (min with {@code Tmax}). The FIRST congestion signal flips
 * growth back to the additive {@code +1} for the rest of the run. Engagement is instrumented:
 * {@code recordStealReason("AIMD","slow_start_double")} per doubling and {@code
 * recordStealReason("AIMD","slow_start_exit_congestion")} EXACTLY ONCE on the flip.
 *
 * <p>They drive the {@linkplain ConcurrencyGauge#ConcurrencyGauge(int, RunMetrics,
 * java.util.function.LongSupplier, java.util.function.LongSupplier) injected-clock seam} — a fake
 * nano clock plus a fixed shed-window length — so pacing/windows advance deterministically with NO real
 * sleep. Existing pacing ({@link ConcurrencyGauge#GROWTH_PACE_NANOS}), the shed, and the growth-freeze
 * are unchanged; each method names the mutant it kills.
 */
final class SlowStartConcurrencyGaugeContractTest {

    /** A fixed shed-window length so any window roll is driven purely by the injected clock. */
    private static final long WINDOW = ConcurrencyGauge.SHED_WINDOW_BASE_NANOS;
    /** One full growth-pace tick on the injected clock — advance this before each paced success. */
    private static final long PACE = ConcurrencyGauge.GROWTH_PACE_NANOS;

    /** Injected fake nano clock — non-zero base (0 is the gauge's "unset" sentinel). */
    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);

    private ConcurrencyGauge newGauge(int tMax, RunMetrics metrics) {
        return new ConcurrencyGauge(tMax, metrics, now::get, () -> WINDOW, ConcurrencyGauge.defaultInitialT(tMax));
    }

    private static double stealReason(RunMetrics m, String outcome, String reason) {
        Counter c = m.registry().find("swath.steal_reason")
                .tag("outcome", outcome).tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double doubles(RunMetrics m) {
        return stealReason(m, "AIMD", "slow_start_double");
    }

    private static double exitCongestion(RunMetrics m) {
        return stealReason(m, "AIMD", "slow_start_exit_congestion");
    }

    private static double timeoutSheds(RunMetrics m) {
        Counter c = m.registry().find("swath.aimd.timeout_shed").counter();
        return c == null ? 0.0 : c.count();
    }

    /** Feed one paced success: advance the clock a full pace first so the growth step is not paced out. */
    private void pacedSuccess(ConcurrencyGauge gauge) {
        now.addAndGet(PACE + 1);
        gauge.reportStatus(200);
    }

    // ---- (a) initial effectiveT == min(SLOW_START_INITIAL_T, Tmax) ------------------------------

    /**
     * The gauge opens at {@code min(4, Tmax)} — NOT {@code Tmax} — across the scale. Kills the mutant
     * that reverts the slow-start seed (initial {@code == Tmax}); at {@code Tmax=64} that mutant reads
     * {@code 64} here.
     */
    @Test
    void initialEffectiveT_isMinOfSlowStartAndTmax() {
        assertThat(ConcurrencyGauge.SLOW_START_INITIAL_T)
                .as("the slow-start seed constant is the pinned value").isEqualTo(4);
        int[][] cases = {{1, 1}, {2, 2}, {4, 4}, {64, 4}};
        for (int[] c : cases) {
            int tMax = c[0];
            int expected = c[1];
            ConcurrencyGauge gauge = ConcurrencyGauges.of(tMax);
            assertThat(gauge.effectiveT())
                    .as("Tmax=%d opens at min(4,Tmax)=%d, not Tmax", tMax, expected)
                    .isEqualTo(expected);
        }
    }

    // ---- (b) a clean run DOUBLES 4→8→16→32→64 and stops at Tmax ---------------------------------

    /**
     * With no congestion EVER, paced successes DOUBLE {@code T}: {@code 4→8→16→32→64}, capped at
     * {@code Tmax}. Exactly four doublings are recorded and the flip is never observed. Kills:
     * <b>no-slow-start</b> (a flat {@code +1} crawls to 5,6,7… and never reaches 64 within four paced
     * successes), <b>wrong-double-count</b> (a doubling that also fires at/after the cap would over-count),
     * and any mutant that emits {@code slow_start_exit_congestion} without a real congestion signal.
     */
    @Test
    void cleanRun_doublesEachPacedStep_upToTmax_recordsFourDoubles_noExitCongestion() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);
        assertThat(gauge.effectiveT()).as("opens at slow-start seed").isEqualTo(4);

        int[] expected = {8, 16, 32, 64};   // exactly ×2 per paced step, min Tmax
        for (int i = 0; i < expected.length; i++) {
            pacedSuccess(gauge);
            assertThat(gauge.effectiveT())
                    .as("paced doubling step #%d (×2, not +1)", i + 1)
                    .isEqualTo(expected[i]);
        }
        // Further paced successes at the cap do nothing (cur >= Tmax early-returns): no extra doublings.
        for (int i = 0; i < 4; i++) {
            pacedSuccess(gauge);
        }
        assertThat(gauge.effectiveT()).as("held at Tmax").isEqualTo(64);
        assertThat(doubles(metrics)).as("exactly four doublings 4→8→16→32→64").isEqualTo(4.0);
        assertThat(exitCongestion(metrics))
                .as("no congestion signal was ever seen — the flip must NOT have fired").isEqualTo(0.0);
    }

    // ---- (c) an attempt-timeout before any doubling flips growth to +1, records the flip once -----

    /**
     * A single {@link ConcurrencyGauge#onTransientTimeout()} before any doubling is a congestion signal:
     * it flips growth to the additive {@code +1} for the rest of the run and records the flip
     * EXACTLY ONCE. (At {@code T=4} one timeout is below the shed gate {@code max(3, ceil(0.3·4))=3}, so
     * {@code T} does not shed — the flip is observed cleanly, not confounded by a shed.) Kills:
     * <b>timeout-doesn't-exit-slow-start</b> (growth would keep doubling {@code 4→8→16}); <b>flip fires
     * per step</b> ({@code slow_start_exit_congestion} would exceed 1).
     */
    @Test
    void attemptTimeoutBeforeAnyDoubling_flipsToPlusOne_recordsExitCongestionOnce() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);
        assertThat(gauge.effectiveT()).isEqualTo(4);

        gauge.onTransientTimeout();   // the FIRST congestion signal — one timeout, below the shed gate
        assertThat(gauge.effectiveT()).as("one timeout at T=4 is below the shed gate — no shed").isEqualTo(4);

        int[] expected = {5, 6, 7};   // additive +1 per paced step, NOT ×2 (which would give 8,16,32)
        for (int i = 0; i < expected.length; i++) {
            pacedSuccess(gauge);
            assertThat(gauge.effectiveT())
                    .as("post-congestion growth is +1, step #%d", i + 1)
                    .isEqualTo(expected[i]);
        }
        assertThat(doubles(metrics)).as("no doubling ever fired (congestion came first)").isEqualTo(0.0);
        assertThat(exitCongestion(metrics)).as("the flip is recorded EXACTLY once").isEqualTo(1.0);
    }

    // ---- (d) a pure storm from t=0 never grows above the seed and the shed drives 4→2→1 ----------

    /**
     * A pure attempt-timeout storm from {@code t=0}: slow-start capped the open at {@code 4} (not
     * {@code Tmax=64}), there is no success so growth never fires, and the existing progress-gated shed
     * halves {@code T} once per window {@code 4→2→1}. Kills: <b>no-slow-start</b> (a {@code Tmax=64} open
     * would storm at full width — the seed assertion catches it), and any regression that let the shed
     * stop functioning under slow-start.
     */
    @Test
    void pureStormFromZero_neverGrowsAboveSeed_shedDrivesFourTwoOne() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);
        assertThat(gauge.effectiveT()).as("a fleet launch opens at the low seed, not Tmax").isEqualTo(4);

        int[] expected = {2, 1};
        for (int i = 0; i < expected.length; i++) {
            for (int j = 0; j < 100; j++) {
                gauge.onTransientTimeout();
            }
            now.addAndGet(WINDOW + 1);   // roll the shed window
            assertThat(gauge.effectiveT()).as("shed window #%d halves T", i + 1).isEqualTo(expected[i]);
        }
        assertThat(gauge.effectiveT()).as("shed floors at 1").isEqualTo(1);
        assertThat(doubles(metrics)).as("a starved storm never doubles").isEqualTo(0.0);
        assertThat(timeoutSheds(metrics)).as("the shed did the work (two windows)").isEqualTo(2.0);
    }

    // ---- (e) a real-503 down-vote also exits slow-start --------------------------------------------

    /**
     * A real {@code 503 SlowDown} is a congestion signal too: it multiplies {@code T} down ({@code 4→2})
     * AND exits slow-start, so subsequent recovery is {@code +1} ({@code 2→3→4→5…}), never a resumed
     * doubling ({@code 2→4→8}). The flip is recorded once. Kills: <b>only-timeouts-exit-slow-start</b> (a
     * mutant that flipped on an attempt-timeout but not on a 503 would resume doubling after a real
     * throttle and re-storm).
     */
    @Test
    void realSlowdownDownVote_alsoExitsSlowStart_recoveryIsPlusOne() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        ConcurrencyGauge gauge = newGauge(64, metrics);
        assertThat(gauge.effectiveT()).isEqualTo(4);

        gauge.reportStatus(ConcurrencyGauge.SLOWDOWN_STATUS);   // real 503: down-vote AND congestion
        assertThat(gauge.effectiveT()).as("503 multiplies T down (floor(0.7·4)=2)").isEqualTo(2);

        // Clear the 10 s throttle cool-down, then recover: growth must be +1, not a resumed ×2.
        now.addAndGet(ConcurrencyGauge.CLEAN_WINDOW_NANOS + PACE + 1);
        gauge.reportStatus(200);
        assertThat(gauge.effectiveT()).as("recovery is +1 (2→3), not doubling to 4").isEqualTo(3);
        pacedSuccess(gauge);
        assertThat(gauge.effectiveT()).as("recovery stays +1 (3→4), not doubling to 8").isEqualTo(4);

        assertThat(doubles(metrics)).as("a 503 exited slow-start — no doublings after it").isEqualTo(0.0);
        assertThat(exitCongestion(metrics)).as("the flip is recorded EXACTLY once").isEqualTo(1.0);
    }
}
