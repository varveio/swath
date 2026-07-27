/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.sim.model.EngineTimeBudgets;
import org.junit.jupiter.api.Test;

/** A declared input that the protocol would silently change is refused, not accepted and altered. */
class PolicyScenarioTest {

    /**
     * A page size above the protocol's own ceiling is the one input a run could accept and then not
     * honour: {@code ListObjectsV2} caps {@code max-keys} at 1,000, so a scenario declaring 2,000 would
     * list in pages of 1,000 while its result described itself as a 2,000-key-page run — a number
     * answering a different question from the one asked, which is exactly what this module refuses
     * everywhere else it takes a stated input.
     */
    @Test
    void aPageSizeAboveTheProtocolCeilingIsRefusedRatherThanClamped() {
        assertThatThrownBy(() -> scenarioWithPageSize(PolicyScenario.MAX_PAGE_SIZE + 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pageSize must be <= 1000");
    }

    private static PolicyScenario scenarioWithPageSize(int pageSize) {
        return new PolicyScenario(20260727L, 1, pageSize, new byte[0], PolicyScenario.SimSeedMode.NONE,
                EngineToggles.DEFAULT, PolicyRunFixtures.perClass(1L, 1L),
                PolicyRunFixtures.zeroedCost("a refused input never reaches a cost model"),
                EngineTimeBudgets.engineDefaults(), PolicyScenario.FaultDisposition.RIDE_OUT, 0, false,
                PolicyScenario.DEFAULT_MAX_EVENTS);
    }
}
