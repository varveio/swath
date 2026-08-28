/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MergeRunTrackerTest {

    @Test
    void runCountsSaturateAtTwoWithoutOverflow() {
        MergeRunTracker tracker = new MergeRunTracker(2);
        assertThat(tracker.count(0)).isZero();
        assertThat(tracker.count(1)).isZero();
        tracker.emittedFrom(0);
        assertThat(tracker.count(0)).isEqualTo(1);
        tracker.emittedFrom(1);
        tracker.emittedFrom(0);
        assertThat(tracker.count(0)).isEqualTo(2);
        tracker.emittedFrom(1);
        tracker.emittedFrom(0);
        assertThat(tracker.count(0)).isEqualTo(2);
        tracker.seedCountForTesting(0, Integer.MAX_VALUE);
        tracker.emittedFrom(1);
        tracker.emittedFrom(0);
        assertThat(tracker.count(0)).isEqualTo(Integer.MAX_VALUE);
    }
}
