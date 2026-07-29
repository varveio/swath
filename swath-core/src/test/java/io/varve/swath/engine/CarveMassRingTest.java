/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Boundary + fill/wrap coverage for {@link CarveMassRing} — the carve brake's signal (campaign
 * memo §5) — parallel in style to {@code OwnerSplitChildMassFloorTest}: exercises the pure
 * arithmetic directly (package-private, no engine machinery) so the exact fill/wrap/average
 * boundary is pinned without driving the whole engine to a precise sequence of completions.
 */
final class CarveMassRingTest {

    @Test
    void freshRingHasNoSignal() {
        CarveMassRing ring = new CarveMassRing();
        assertThat(ring.windowAverage()).as("pre-warmup: no basis to average").isNaN();
    }

    @Test
    void oneShortOfTheWindowIsStillNoSignal() {
        CarveMassRing ring = new CarveMassRing();
        for (int i = 0; i < CarveMassRing.SIZE - 1; i++) {
            ring.record(100L);
        }
        assertThat(ring.windowAverage())
                .as("SIZE - 1 samples: still below the warmup floor")
                .isNaN();
    }

    @Test
    void exactlyTheWindowProducesTheAverage() {
        CarveMassRing ring = new CarveMassRing();
        // 1..SIZE (8): average = (1+2+...+8)/8 = 4.5
        for (long i = 1; i <= CarveMassRing.SIZE; i++) {
            ring.record(i);
        }
        assertThat(ring.windowAverage()).isEqualTo(4.5);
    }

    @Test
    void windowAverageIsUniformWhenEveryMassIsIdentical() {
        CarveMassRing ring = new CarveMassRing();
        for (int i = 0; i < CarveMassRing.SIZE; i++) {
            ring.record(250L);
        }
        assertThat(ring.windowAverage()).isEqualTo(250.0);
    }

    /**
     * Past the window, the OLDEST sample is evicted — a ring, not a running/cumulative average.
     * SIZE=8 fills with 1..8 (average 4.5), then one more record(1000) evicts the oldest (1),
     * leaving {2,3,4,5,6,7,8,1000}: average = (2+3+4+5+6+7+8+1000)/8 = 129.375.
     */
    @Test
    void oneMoreRecordPastTheWindowEvictsOnlyTheOldestSample() {
        CarveMassRing ring = new CarveMassRing();
        for (long i = 1; i <= CarveMassRing.SIZE; i++) {
            ring.record(i);
        }
        ring.record(1000L);

        assertThat(ring.windowAverage()).isEqualTo(129.375);
    }

    /** A full wrap (2 * SIZE records) leaves only the second batch in the window. */
    @Test
    void aFullWrapLeavesOnlyTheMostRecentBatch() {
        CarveMassRing ring = new CarveMassRing();
        for (int i = 0; i < CarveMassRing.SIZE; i++) {
            ring.record(1L);   // evicted entirely by the second batch below
        }
        for (long i = 1; i <= CarveMassRing.SIZE; i++) {
            ring.record(i * 10);
        }

        // Second batch: 10,20,...,80 -> average 45.0, with no trace of the first batch's 1s.
        assertThat(ring.windowAverage()).isEqualTo(45.0);
    }

    @Test
    void windowSizeIsPinnedToTheGatesWarmupFloor() {
        assertThat((long) CarveMassRing.SIZE).isEqualTo(ConfettiFeedbackGate.MIN_SAMPLE);
    }
}
