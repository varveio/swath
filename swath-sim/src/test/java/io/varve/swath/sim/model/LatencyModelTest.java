/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.sim.kernel.SimRng;
import io.varve.swath.sim.kernel.SimRngStream;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The two latency model forms shipped here, and the partial-specification trap they both refuse. */
class LatencyModelTest {

    private static final int DRAWS = 20_000;

    @Test
    void theConstantModelDrawsNothingAndAnswersPerClass() {
        Map<CallClass, Long> nanos = new EnumMap<>(CallClass.class);
        nanos.put(CallClass.WORKER_PAGE, 30_000_000L);
        nanos.put(CallClass.PIVOT_PROBE, 9_000_000L);
        nanos.put(CallClass.STRUCTURE_PROBE, 120_000_000L);
        nanos.put(CallClass.SEED_PROBE, 40_000_000L);
        ConstantLatencyModel model = ConstantLatencyModel.perClass(nanos);
        SimRng rng = SimRng.forStream(1L, 0, SimRngStream.LATENCY);
        long before = rng.nextLong();

        assertThat(model.drawNanos(CallClass.PIVOT_PROBE, rng)).isEqualTo(9_000_000L);
        assertThat(model.drawNanos(CallClass.STRUCTURE_PROBE, rng)).isEqualTo(120_000_000L);
        assertThat(SimRng.forStream(1L, 0, SimRngStream.LATENCY).nextLong())
                .as("the constant model must consume no draws, or it would shift every later one")
                .isEqualTo(before);
    }

    /**
     * A latency model missing one call class would answer a default for it, and a default here is a
     * measurement nobody took. Both forms therefore demand every class up front.
     */
    @Test
    void bothFormsRefuseAPartiallySpecifiedSetOfClasses() {
        Map<CallClass, Long> incomplete = Map.of(CallClass.WORKER_PAGE, 1L);
        Map<CallClass, FittedLatencyModel.Params> incompleteParams =
                Map.of(CallClass.WORKER_PAGE, new FittedLatencyModel.Params(1L, 2L));

        assertThatThrownBy(() -> ConstantLatencyModel.perClass(incomplete))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("PIVOT_PROBE");
        assertThatThrownBy(() -> FittedLatencyModel.of(incompleteParams))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("PIVOT_PROBE");
    }

    @Test
    void theFittedModelRespectsItsFloorAndReproducesItsMean() {
        long floor = 5_000_000L;
        long mean = 20_000_000L;
        FittedLatencyModel model = uniformlyFitted(floor, mean);
        SimRng rng = SimRng.of(4242L);
        long total = 0;
        long minimum = Long.MAX_VALUE;
        long maximum = Long.MIN_VALUE;
        for (int i = 0; i < DRAWS; i++) {
            long draw = model.drawNanos(CallClass.WORKER_PAGE, rng);
            total += draw;
            minimum = Math.min(minimum, draw);
            maximum = Math.max(maximum, draw);
        }

        assertThat(minimum).as("no draw beats the floor").isGreaterThanOrEqualTo(floor);
        assertThat(total / DRAWS).as("the sample mean recovers the fitted mean")
                .isBetween((long) (mean * 0.95), (long) (mean * 1.05));
        assertThat(maximum).as("the tail must actually be long, or this is a constant in disguise")
                .isGreaterThan(mean * 3);
    }

    @Test
    void aFittedModelWhoseMeanIsItsFloorIsConstant() {
        FittedLatencyModel model = uniformlyFitted(7L, 7L);
        SimRng rng = SimRng.of(1L);

        Set<Long> draws = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            draws.add(model.drawNanos(CallClass.SEED_PROBE, rng));
        }

        assertThat(draws).containsExactly(7L);
    }

    /**
     * The interface promises a non-negative service time, and a negative one would schedule an event
     * into the past. An absurd tail mean saturates the {@code double} to {@code long} conversion at
     * {@code Long.MAX_VALUE}, which then overflows when the floor is added to it — so the promise is
     * clamped rather than inferred from the arithmetic.
     */
    @Test
    void anAbsurdTailMeanCannotProduceANegativeServiceTime() {
        FittedLatencyModel model = uniformlyFitted(1_000L, Long.MAX_VALUE);
        SimRng rng = SimRng.of(123L);

        for (int i = 0; i < DRAWS; i++) {
            assertThat(model.drawNanos(CallClass.WORKER_PAGE, rng)).isGreaterThanOrEqualTo(1_000L);
        }
    }

    @Test
    void negativeAndInvertedParametersAreRejected() {
        assertThatThrownBy(() -> ConstantLatencyModel.uniform(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FittedLatencyModel.Params(-1L, 5L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FittedLatencyModel.Params(10L, 5L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(">= minNanos");
    }

    private static FittedLatencyModel uniformlyFitted(long floor, long mean) {
        Map<CallClass, FittedLatencyModel.Params> params = new EnumMap<>(CallClass.class);
        for (CallClass callClass : CallClass.values()) {
            params.put(callClass, new FittedLatencyModel.Params(floor, mean));
        }
        return FittedLatencyModel.of(params);
    }
}
