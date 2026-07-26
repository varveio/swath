/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.policy;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Direct, table-driven unit tests for {@link FutilityPacingPolicy}'s per-victim futility-cooldown
 * arithmetic: one test per named boundary (threshold, bounded-exponential max, decay, reset-on-
 * carve) — mirrors {@code ThiefPolicyCascadeTest}/{@code OwnerSplitGovernorTest}'s shape. Drives
 * the pure functions directly against hand-picked {@code int}s — no {@code WorkerState}, no lock,
 * no I/O. {@code WorkerState}'s own {@code AtomicInteger} call sequencing (still this class's job
 * to preserve, per {@link FutilityPacingPolicy}'s javadoc) is separately pinned by {@code
 * FutilityPacingTest}/{@code FutilityPacingContractTest}, which this suite does not touch.
 */
class FutilityPacingPolicyTest {

    /** Below {@code FUTILITY_PACE_THRESHOLD}, a futile outcome never trips the cooldown. */
    @Test
    void tripsIsFalseBelowTheThreshold() {
        for (int consecutive = 0; consecutive < FutilityPacingPolicy.FUTILITY_PACE_THRESHOLD; consecutive++) {
            assertThat(FutilityPacingPolicy.trips(consecutive))
                    .as("consecutive=%d", consecutive)
                    .isFalse();
        }
    }

    /** At and above {@code FUTILITY_PACE_THRESHOLD}, a futile outcome trips the cooldown. */
    @Test
    void tripsIsTrueAtAndAboveTheThreshold() {
        assertThat(FutilityPacingPolicy.trips(FutilityPacingPolicy.FUTILITY_PACE_THRESHOLD)).isTrue();
        assertThat(FutilityPacingPolicy.trips(FutilityPacingPolicy.FUTILITY_PACE_THRESHOLD + 1)).isTrue();
        assertThat(FutilityPacingPolicy.trips(1_000)).isTrue();
    }

    /** The bounded-exponential cooldown doubles per trip, from the 1st trip through the cap. */
    @Test
    void cooldownForTripsGrowsExponentiallyUntilItSaturates() {
        assertThat(FutilityPacingPolicy.cooldownForTrips(1)).isEqualTo(2);
        assertThat(FutilityPacingPolicy.cooldownForTrips(2)).isEqualTo(4);
        assertThat(FutilityPacingPolicy.cooldownForTrips(3)).isEqualTo(8);
        assertThat(FutilityPacingPolicy.cooldownForTrips(4)).isEqualTo(16);
        assertThat(FutilityPacingPolicy.cooldownForTrips(5)).isEqualTo(32);
        // 1<<6 == 64 == the cap itself: the last trip the raw shift and the cap agree on.
        assertThat(FutilityPacingPolicy.cooldownForTrips(6)).isEqualTo(FutilityPacingPolicy.FUTILITY_PACE_MAX_COOLDOWN);
    }

    /** Past the trip where the raw shift would exceed the cap, the cooldown saturates at the cap. */
    @Test
    void cooldownForTripsNeverExceedsTheMaxCooldown() {
        assertThat(FutilityPacingPolicy.cooldownForTrips(7)).isEqualTo(FutilityPacingPolicy.FUTILITY_PACE_MAX_COOLDOWN);
        assertThat(FutilityPacingPolicy.cooldownForTrips(20)).isEqualTo(FutilityPacingPolicy.FUTILITY_PACE_MAX_COOLDOWN);
        // The shift itself is clamped at 20 (an int-overflow guard for a persistently-racing victim
        // that has tripped thousands of times over a long run) — still saturates at the cap.
        assertThat(FutilityPacingPolicy.cooldownForTrips(1_000)).isEqualTo(FutilityPacingPolicy.FUTILITY_PACE_MAX_COOLDOWN);
    }

    /** {@code paced} is a plain threshold check: any positive skip count is paced, zero/negative is not. */
    @Test
    void pacedIsTrueOnlyForAPositiveSkipCount() {
        assertThat(FutilityPacingPolicy.paced(-1)).isFalse();
        assertThat(FutilityPacingPolicy.paced(0)).isFalse();
        assertThat(FutilityPacingPolicy.paced(1)).isTrue();
        assertThat(FutilityPacingPolicy.paced(FutilityPacingPolicy.FUTILITY_PACE_MAX_COOLDOWN)).isTrue();
    }

    /** Decay: repeatedly applying it to a tripped cooldown drains it back to unpaced, one skip at a time. */
    @Test
    void decayDrainsATrippedCooldownBackToUnpaced() {
        int skips = FutilityPacingPolicy.cooldownForTrips(3);   // 8
        int decays = 0;
        while (FutilityPacingPolicy.paced(skips)) {
            skips = FutilityPacingPolicy.decay(skips);
            decays++;
        }
        assertThat(decays).as("exactly one decay per armed skip").isEqualTo(8);
        assertThat(skips).as("decay lands exactly on zero, never past it on the paced path").isEqualTo(0);
    }

    /** Decay is unconditional — consuming past zero goes negative, and {@link FutilityPacingPolicy#paced} still reads it as unpaced. */
    @Test
    void decayIsUnconditionalAndGoingNegativeStaysUnpaced() {
        int skips = FutilityPacingPolicy.decay(0);
        assertThat(skips).isEqualTo(-1);
        assertThat(FutilityPacingPolicy.paced(skips)).isFalse();
    }

    /** Reset-on-carve: the value every counter is set to when a victim yields a child. */
    @Test
    void resetIsZero() {
        assertThat(FutilityPacingPolicy.RESET).isZero();
        assertThat(FutilityPacingPolicy.trips(FutilityPacingPolicy.RESET)).isFalse();
        assertThat(FutilityPacingPolicy.paced(FutilityPacingPolicy.RESET)).isFalse();
    }
}
