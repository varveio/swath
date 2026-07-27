/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The declared engine time budgets, and the reference values a scenario varies from.
 *
 * <p>Pinning the reference numbers here is the point: these are the engine's own defaults restated as
 * simulation inputs, and a silent drift between the two would make every scenario that says "the
 * engine's defaults" quietly mean something else. If the engine changes one of these, this test is
 * where the restatement is updated, deliberately.
 */
class EngineTimeBudgetsTest {

    @Test
    void theDefaultsRestateTheEnginesOwnValues() {
        EngineTimeBudgets defaults = EngineTimeBudgets.engineDefaults();

        assertThat(defaults.seedProbeBudget())
                .as("min(256, max(1, min(1000, 4 x 64))) at the default concurrency ceiling").isEqualTo(256);
        assertThat(defaults.probeAttemptTimeoutNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(3));
        assertThat(defaults.workerAttemptTimeoutNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(10));
        assertThat(defaults.idleStealBaseParkNanos()).isEqualTo(TimeUnit.MILLISECONDS.toNanos(5));
        assertThat(defaults.idleStealBackoffCapNanos()).isEqualTo(TimeUnit.MILLISECONDS.toNanos(50));
        assertThat(defaults.idleStealAttemptParkNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(1));
        assertThat(defaults.concurrencyCleanWindowNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(10));
        assertThat(defaults.maxDurationNanos())
                .as("the engine ships with no run ceiling").isEqualTo(EngineTimeBudgets.UNBOUNDED_DURATION);
    }

    @Test
    void aMaxDurationCanBeDeclaredWithoutDisturbingTheOtherBudgets() {
        EngineTimeBudgets defaults = EngineTimeBudgets.engineDefaults();

        EngineTimeBudgets bounded = defaults.withMaxDuration(TimeUnit.MINUTES.toNanos(5));

        assertThat(bounded.maxDurationNanos()).isEqualTo(TimeUnit.MINUTES.toNanos(5));
        assertThat(bounded.withMaxDuration(EngineTimeBudgets.UNBOUNDED_DURATION)).isEqualTo(defaults);
    }

    @Test
    void nonsensicalBudgetsAreRejectedAtConstruction() {
        EngineTimeBudgets defaults = EngineTimeBudgets.engineDefaults();

        assertThatThrownBy(() -> defaults.withMaxDuration(-1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxDurationNanos");
        assertThatThrownBy(() -> new EngineTimeBudgets(0, 1, 1, 1, 1, 1, 1, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("seedProbeBudget");
        assertThatThrownBy(() -> new EngineTimeBudgets(1, 1, 1, 100, 50, 1, 1, 0))
                .as("a backoff cap below its own base step is a typo, not a configuration")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idleStealBackoffCapNanos");
    }
}
