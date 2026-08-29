/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MergeDiskPlanTest {

    private static final long GIB = 1L << 30;

    @Test
    void sharedStorePreservesThreeTimesStagedPolicyAndAddsExactProof() {
        long staged = 2L * GIB;
        long proof = 123_456L;
        MergeDiskPlan.Reservation reservation = MergeDiskPlan.reservation(staged, proof);

        assertThat(reservation.finalOutputReserveBytes()).isEqualTo(staged);
        assertThat(reservation.cascadeReserveBytes()).isEqualTo(staged);
        assertThat(reservation.safetyReserveBytes()).isEqualTo(staged);
        assertThat(reservation.filesystemFloorBytes()).isEqualTo(GIB);
        assertThat(reservation.proofBytes()).isEqualTo(proof);
        assertThat(reservation.sharedRequiredBytes()).isEqualTo(6L * GIB + proof);
    }

    @Test
    void safetyFloorRetainsHistoricalMaxSemanticsForSmallRuns() {
        MergeDiskPlan.Reservation reservation = MergeDiskPlan.reservation(100L << 20, 99L);

        assertThat(reservation.sharedRequiredBytes()).isEqualTo(GIB + 99L);
    }

    @Test
    void exactProofExtentClampsRangesOneAtATime() {
        int segments = 100;
        long staged = GIB;
        long base = 3L * GIB;
        long threeRangeProof = PageRunProofSpool.logicalBytes(3, segments);
        MergeDiskPolicy.Snapshot space = new MergeDiskPolicy.Snapshot(
                base + threeRangeProof, base + threeRangeProof, true);

        MergeDiskPlan.Decision decision = MergeDiskPlan.decide(4, segments, staged, space);

        assertThat(decision.ranges()).isEqualTo(3);
        assertThat(decision.diskLimited()).isTrue();
        assertThat(decision.reservation().proofBytes()).isEqualTo(threeRangeProof);
    }

    @Test
    void serialIsAdmittedWithoutProofWhenParallelProofDoesNotFit() {
        long staged = GIB;
        MergeDiskPolicy.Snapshot space = new MergeDiskPolicy.Snapshot(
                3L * GIB, 3L * GIB, true);

        MergeDiskPlan.Decision decision = MergeDiskPlan.decide(8, 10_000, staged, space);

        assertThat(decision.ranges()).isEqualTo(1);
        assertThat(decision.reservation().proofBytes()).isZero();
    }

    @Test
    void serialRefusesWhenBaseReservationsDoNotFit() {
        MergeDiskPolicy.Snapshot space = new MergeDiskPolicy.Snapshot(
                3L * GIB - 1L, 3L * GIB - 1L, true);

        MergeDiskPlan.Decision decision = MergeDiskPlan.decide(1, 10, GIB, space);

        assertThat(decision.refused()).isTrue();
    }

    @Test
    void separateStoresAreGatedIndependently() {
        long staged = 2L * GIB;
        long proof = PageRunProofSpool.logicalBytes(2, 100);
        MergeDiskPlan.Reservation reservation = MergeDiskPlan.reservation(staged, proof);

        assertThat(reservation.stagingRequiredBytes()).isEqualTo(6L * GIB + proof);
        assertThat(reservation.outputRequiredBytes()).isEqualTo(4L * GIB);
        assertThat(reservation.fits(new MergeDiskPolicy.Snapshot(
                6L * GIB + proof, 4L * GIB, false))).isTrue();
        assertThat(reservation.fits(new MergeDiskPolicy.Snapshot(
                6L * GIB + proof, 4L * GIB - 1L, false))).isFalse();
    }

    @Test
    void unknownUsableSpaceFailsOpenLikeExistingDiskGuard() {
        MergeDiskPlan.Reservation reservation = MergeDiskPlan.reservation(Long.MAX_VALUE, Long.MAX_VALUE);

        assertThat(reservation.fits(new MergeDiskPolicy.Snapshot(-1L, -1L, false))).isTrue();
    }
}
