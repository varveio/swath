/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The pure, injectable fan-in clamp arithmetic ({@link FileDescriptorBudget}). Exercised with
 * injected limits so the fd clamp is proven without depending on the real process {@code ulimit}.
 */
class FileDescriptorBudgetTest {

    @Test
    void fdBoundedFanInReservesHeadroomBelowTheSoftLimit() {
        assertThat(FileDescriptorBudget.fdBoundedFanIn(1024, 128)).isEqualTo(896);
        assertThat(FileDescriptorBudget.fdBoundedFanIn(200, 128)).isEqualTo(72);
    }

    @Test
    void fdBoundedFanInFloorsAtTwoUnderAnExtremelyLowLimit() {
        assertThat(FileDescriptorBudget.fdBoundedFanIn(129, 128)).isEqualTo(2);   // 1 would be below the floor
        assertThat(FileDescriptorBudget.fdBoundedFanIn(10, 128)).isEqualTo(2);    // negative before the floor
    }

    @Test
    void fdBoundedFanInTreatsANegativeSoftLimitAsUnlimited() {
        // RLIM_INFINITY sentinel: no fd constraint at all.
        assertThat(FileDescriptorBudget.fdBoundedFanIn(-1, 128)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void clampedFanInReducesFanInWhenTheSoftLimitIsLow() {
        // static fan-in 512, but a soft limit of 200 (fd bound 72) forces the merge down to 72.
        int clamped = FileDescriptorBudget.clampedFanIn(512, 200, 128, Integer.MAX_VALUE);
        assertThat(clamped).isEqualTo(72);
    }

    @Test
    void clampedFanInIsUnaffectedByAHealthyFdLimit() {
        int clamped = FileDescriptorBudget.clampedFanIn(512, 10_000, 128, Integer.MAX_VALUE);
        assertThat(clamped).isEqualTo(512);   // fd bound 9872 never binds
    }

    @Test
    void clampedFanInTakesTheRecordSizedPlanningBoundWhenItIsTighter() {
        // fd healthy (bound 9872), static 512, but the exact per-segment memory bound is 40 ⇒ 40 wins.
        int clamped = FileDescriptorBudget.clampedFanIn(512, 10_000, 128, 40);
        assertThat(clamped).isEqualTo(40);
    }

    @Test
    void clampedFanInNeverReturnsBelowTwo() {
        assertThat(FileDescriptorBudget.clampedFanIn(512, 129, 128, Integer.MAX_VALUE)).isEqualTo(2);
        assertThat(FileDescriptorBudget.clampedFanIn(512, 10_000, 128, 1)).isEqualTo(2);
    }
}
