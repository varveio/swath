/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sim.kernel.SimKernel;
import io.varve.swath.sim.kernel.SimRng;
import io.varve.swath.sim.kernel.SimRngStream;
import io.varve.swath.sim.model.EngineTimeBudgets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The adaptive-concurrency port, checked against the shape the shipped controller documents as its
 * guarantees. Every test here drives the controller by hand at explicit instants, which is the whole
 * benefit of a port whose every signal carries its own timestamp: the windows, the pacing and the
 * freezes are exercised at their exact boundaries with no sleeping and no flakiness.
 *
 * <p>What these cannot do is prove the port matches the shipped controller. The two share no code, by
 * design — the real one is a machine of CAS loops built to survive concurrency this simulator does not
 * have — so agreement rests on review against its documented guarantees, and these tests pin the parts
 * of that reading which are mechanically checkable.
 */
class SimConcurrencyPolicyTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final EngineTimeBudgets BUDGETS = EngineTimeBudgets.engineDefaults()
            // A fixed shed window keeps the boundary arithmetic below exact; the jitter's own effect is
            // asserted separately.
            .withAimd(EngineTimeBudgets.AimdBudgets.engineDefaults().withFixedShedWindow(30 * SECOND));

    /**
     * The first success of a run must be able to grow the target, at whatever instant it arrives.
     *
     * <p>This is a virtual clock, so a run begins at zero and its first page can complete a microsecond
     * later. The shipped controller expresses "no growth step has happened yet" as a zero timestamp,
     * which is unreachably far in the past on a clock that counts from process start and is the *current
     * instant* on a clock that counts from the run's. Reusing that sentinel here would pace out the first
     * step of every run and, worse, would make a valve paced at thirty seconds unable to open at all in a
     * run shorter than thirty virtual seconds — which is the length of exactly the runs that need it.
     */
    @Test
    void theFirstGrowthStepIsNotPacedOutWhenTheRunBeginsAtVirtualZero() {
        SimConcurrencyPolicy gauge = gauge(64);

        gauge.onSuccess(0L);

        assertThat(gauge.effectiveT())
                .as("a success at the very start of a run is a growth opportunity, not a paced-out one")
                .isEqualTo(8);

        gauge.onSuccess(1L);

        assertThat(gauge.effectiveT()).as("and the pace applies normally from there").isEqualTo(8);
    }

    /**
     * The same inversion, seen where it actually bites: the latency-freeze valve is paced at the shed
     * window's own length, so on a zero sentinel it could never admit a step inside a run shorter than
     * that — and a poisoned-store run that ends in twenty virtual seconds is exactly such a run.
     */
    @Test
    void theLatencyValveCanOpenInsideARunShorterThanItsOwnPacingInterval() {
        SimConcurrencyPolicy gauge = gauge(64);
        gauge.onAttemptLatency(0L, TimeUnit.MILLISECONDS.toNanos(20));
        for (int i = 0; i < 20; i++) {
            gauge.onAttemptLatency(i, TimeUnit.MILLISECONDS.toNanos(400));
        }
        int before = gauge.effectiveT();

        for (int i = 0; i < 8; i++) {
            gauge.onSuccess(i);   // still inside the first second of the run
        }

        assertThat(gauge.counters()).containsKey("FREEZE.latency_inflation");
        assertThat(gauge.effectiveT())
                .as("the valve's first step is owed immediately, not one valve interval into the run")
                .isEqualTo(before + 1);
    }

    @Test
    void growthDoublesUntilTheFirstCongestionSignalAndIsAdditiveForeverAfter() {
        SimConcurrencyPolicy gauge = gauge(64);

        assertThat(gauge.effectiveT()).as("slow start begins at four, not at the ceiling").isEqualTo(4);
        gauge.onSuccess(SECOND);
        assertThat(gauge.effectiveT()).isEqualTo(8);
        gauge.onSuccess(2 * SECOND);
        assertThat(gauge.effectiveT()).isEqualTo(16);
        gauge.onSuccess(2 * SECOND + 1);
        assertThat(gauge.effectiveT()).as("growth is paced at one step per second, not per success")
                .isEqualTo(16);

        gauge.onTransientTimeout(3 * SECOND, true);   // the run's first congestion signal
        gauge.onSuccess(20 * SECOND);

        assertThat(gauge.effectiveT()).as("doubling is latched off for the rest of the run").isEqualTo(17);
    }

    @Test
    void aThrottleIsTheOnlyMultiplicativeDecreaseThatPausesStealingUntilTheCleanWindowElapses() {
        SimConcurrencyPolicy gauge = gauge(64);
        rampTo(gauge, 32);

        gauge.onThrottle(10 * SECOND);

        assertThat(gauge.effectiveT()).as("T := max(1, floor(0.7T))").isEqualTo(22);
        assertThat(gauge.isStealingAllowed()).isFalse();
        gauge.onSuccess(15 * SECOND);
        assertThat(gauge.effectiveT()).as("no growth inside the cool-down").isEqualTo(22);
        assertThat(gauge.isStealingAllowed()).as("still inside the clean window").isFalse();

        gauge.onSuccess(21 * SECOND);

        assertThat(gauge.isStealingAllowed()).as("the clean window elapsed").isTrue();
        assertThat(gauge.effectiveT()).isEqualTo(23);
    }

    @Test
    void theDecreaseFloorsAtOneAndAFlooredDecreaseBuysNoFreshCoolDown() {
        SimConcurrencyPolicy gauge = gauge(1);

        gauge.onThrottle(SECOND);

        assertThat(gauge.effectiveT()).isEqualTo(1);
        assertThat(gauge.counters()).containsKey("AIMD.floor_rearm_suppressed");
        // Nothing was removed, so nothing is owed a recovery window: the very next success may grow,
        // which at the ceiling means only that the flag is restored.
        gauge.onSuccess(SECOND + 1);
        assertThat(gauge.isStealingAllowed()).isTrue();
    }

    @Test
    void aSustainedTimeoutStormShedsOncePerWindowAndOnlyWhileProgressIsStarved() {
        SimConcurrencyPolicy gauge = gauge(64);
        rampTo(gauge, 32);

        // The timeout gate scales with the target -- max(3, ceil(0.3 x T)), so ten at T = 32 -- and no
        // success falls inside the window, so the starvation gate stays closed.
        for (int i = 0; i < 10; i++) {
            gauge.onTransientTimeout(40 * SECOND + i, true);
        }

        assertThat(gauge.effectiveT()).as("T := max(1, floor(0.5T))").isEqualTo(16);
        assertThat(gauge.counters()).containsKey("SHED.timeout_storm");

        for (int i = 0; i < 10; i++) {
            gauge.onTransientTimeout(45 * SECOND + i, true);
        }

        assertThat(gauge.effectiveT()).as("at most one shed per real window").isEqualTo(16);
    }

    @Test
    void progressInTheWindowClearsTheStarvationGateSoATimeoutTailNeverSheds() {
        SimConcurrencyPolicy gauge = gauge(64);
        rampTo(gauge, 32);
        for (int i = 0; i < 8; i++) {
            gauge.onSuccess(40 * SECOND + i);   // real page completions coexisting with the timeouts
        }
        int before = gauge.effectiveT();

        for (int i = 0; i < 10; i++) {
            gauge.onTransientTimeout(41 * SECOND + i, true);
        }

        assertThat(gauge.effectiveT())
                .as("a timeout tail alongside real progress is not a storm").isEqualTo(before);
        assertThat(gauge.counters()).doesNotContainKey("SHED.timeout_storm");
    }

    @Test
    void aProbeTimeoutFeedsNeitherTheShedGateNorTheGrowthFreeze() {
        SimConcurrencyPolicy gauge = gauge(64);
        rampTo(gauge, 32);
        int before = gauge.effectiveT();

        for (int i = 0; i < 10; i++) {
            gauge.onTransientTimeout(40 * SECOND + i, false);
        }
        gauge.onSuccess(50 * SECOND);

        assertThat(gauge.effectiveT())
                .as("a probe carries no backpressure signal: it can neither shed nor freeze, and it "
                        + "does not even latch slow start off -- this success still doubles")
                .isEqualTo(Math.min(64, before * 2));
        assertThat(gauge.counters()).containsKey("GROWTH.probe_timeout_excluded");
        assertThat(gauge.counters()).doesNotContainKey("SHED.timeout_storm");
    }

    @Test
    void theGrowthFreezeSuppressesGrowthWithoutEverLoweringTheTarget() {
        SimConcurrencyPolicy gauge = gauge(64);
        rampTo(gauge, 32);
        // One early timeout latches slow start off, so growth below is the additive +1 the freeze acts
        // on rather than a doubling that would run straight into the ceiling and hide it.
        gauge.onTransientTimeout(20 * SECOND, true);
        for (int i = 0; i < 8; i++) {
            gauge.onSuccess(35 * SECOND + i);   // past that window: the freeze has thawed, T grows by one
        }
        int frozenAt = gauge.effectiveT();
        // Three worker timeouts inside one freeze window, but far below the shed's own volume gate and
        // alongside real progress -- so the freeze is the only thing acting here.
        gauge.onTransientTimeout(36 * SECOND, true);
        gauge.onTransientTimeout(37 * SECOND, true);
        gauge.onTransientTimeout(38 * SECOND, true);

        gauge.onSuccess(40 * SECOND);

        assertThat(gauge.effectiveT()).as("frozen, never decreased").isEqualTo(frozenAt);
        assertThat(gauge.counters()).containsKey("FREEZE.transient_timeouts");

        gauge.onSuccess(50 * SECOND);   // the freeze window has elapsed with no fresh timeouts

        assertThat(gauge.effectiveT()).as("the freeze thaws on its own").isEqualTo(frozenAt + 1);
    }

    @Test
    void anInflatedSuccessLatencyFreezesGrowthAndTheValveAdmitsOnePacedStep() {
        SimConcurrencyPolicy gauge = gauge(64);
        rampTo(gauge, 32);
        gauge.onAttemptLatency(40 * SECOND, TimeUnit.MILLISECONDS.toNanos(20));   // the baseline
        for (int i = 0; i < 20; i++) {
            // Inflate the trailing EWMA well past twice the baseline.
            gauge.onAttemptLatency(41 * SECOND + i, TimeUnit.MILLISECONDS.toNanos(400));
        }
        int frozenAt = gauge.effectiveT();

        for (int i = 0; i < 8; i++) {
            gauge.onSuccess(42 * SECOND + i);   // progress, so the valve's own gate is open
        }

        assertThat(gauge.counters()).containsKey("FREEZE.latency_inflation");
        assertThat(gauge.effectiveT())
                .as("the valve is a damper, not a latch: exactly one step across a burst of successes, "
                        + "and never a doubling")
                .isEqualTo(frozenAt + 1);

        gauge.onSuccess(50 * SECOND);

        assertThat(gauge.effectiveT()).as("and then it is paced shut for a whole valve interval")
                .isEqualTo(frozenAt + 1);

        for (int i = 0; i < 4; i++) {
            // Past the valve interval, and past the shed window too -- which resets the progress count
            // the valve's own gate reads, so the run has to demonstrate progress again before the valve
            // will admit anything.
            gauge.onSuccess(75 * SECOND + i);
        }

        assertThat(gauge.effectiveT()).as("one more step, once the valve interval has elapsed")
                .isEqualTo(frozenAt + 2);
    }

    @Test
    void theShedWindowLengthIsDrawnInsideTheDeclaredBoundsAndReallyVaries() {
        EngineTimeBudgets jittered = EngineTimeBudgets.engineDefaults();
        long min = jittered.aimd().shedWindowMinNanos();
        long max = jittered.aimd().shedWindowMaxNanos();
        List<Long> lengths = new ArrayList<>();

        for (long seed = 1; seed <= 12; seed++) {
            SimConcurrencyPolicy gauge = new SimConcurrencyPolicy(64, jittered, SimRng.of(seed));
            // The first window is rolled by the first signal, not by construction: until then there is
            // no window, and a controller that drew one up front would be taking a value off the tape
            // that nothing could ever measure against.
            gauge.onSuccess(0L);
            lengths.add(gauge.shedWindowLengthNanos());
        }

        assertThat(lengths).as("a window outside its declared bounds is not jitter, it is a bug")
                .allSatisfy(length -> assertThat(length).isBetween(min, max));
        assertThat(lengths).as("a constant length would satisfy the bounds and desynchronise nothing")
                .doesNotHaveDuplicates();
    }

    @Test
    void twoControllersOnOneTapeRollIdenticalWindows() {
        EngineTimeBudgets jittered = EngineTimeBudgets.engineDefaults();
        SimConcurrencyPolicy first = new SimConcurrencyPolicy(64, jittered,
                SimRng.forStream(99L, SimKernel.FLEET_ACTOR, SimRngStream.AIMD_JITTER));
        SimConcurrencyPolicy second = new SimConcurrencyPolicy(64, jittered,
                SimRng.forStream(99L, SimKernel.FLEET_ACTOR, SimRngStream.AIMD_JITTER));

        // The jitter desynchronises windows within a run; it does not make a run irreproducible.
        for (int i = 0; i < 50; i++) {
            first.onSuccess(i * SECOND);
            second.onSuccess(i * SECOND);
        }

        assertThat(first.effectiveT()).isEqualTo(second.effectiveT());
        assertThat(first.counters()).isEqualTo(second.counters());
    }

    private static SimConcurrencyPolicy gauge(int tMax) {
        return new SimConcurrencyPolicy(tMax, BUDGETS, SimRng.of(4242L));
    }

    /**
     * Drives the slow-start ramp to at least {@code target}, one paced step per second, <b>starting at
     * virtual zero</b> — where a run really starts, and where a pacing sentinel that means "long ago" on
     * a wall clock means "right now" on this one.
     */
    private static void rampTo(SimConcurrencyPolicy gauge, int target) {
        for (int second = 0; gauge.effectiveT() < target && second < 40; second++) {
            gauge.onSuccess(second * SECOND);
        }
        assertThat(gauge.effectiveT()).isGreaterThanOrEqualTo(target);
    }
}
