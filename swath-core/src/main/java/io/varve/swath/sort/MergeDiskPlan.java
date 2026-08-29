/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.Locale;

/** Pure, overflow-safe merge-start disk reservation and range-admission arithmetic. */
final class MergeDiskPlan {

    private MergeDiskPlan() {
    }

    /**
     * Preserve the established 3x-staged merge headroom as three named policy components: one
     * staged extent each for final output, live cascade intermediates, and estimation safety. The
     * fixed filesystem floor retains its historical max-floor semantics. Only {@code proofBytes}
     * is byte-exact.
     */
    static Reservation reservation(long stagedBytes, long proofBytes) {
        long staged = Math.max(0L, stagedBytes);
        long proof = Math.max(0L, proofBytes);
        return new Reservation(
                staged,
                staged,
                staged,
                SortDiskGuard.DEFAULT_MIN_FREE_BYTES,
                proof);
    }

    static Decision decide(int candidateRanges, int segments, long stagedBytes,
            MergeDiskPolicy.Snapshot space) {
        int candidate = Math.max(1, candidateRanges);
        for (int ranges = candidate; ranges >= 1; ranges--) {
            long proofBytes = ranges > 1
                    ? PageRunProofSpool.logicalBytes(ranges, segments)
                    : 0L;
            Reservation reservation = reservation(stagedBytes, proofBytes);
            if (reservation.fits(space)) {
                return new Decision(ranges, reservation, ranges < candidate);
            }
        }
        Reservation serial = reservation(stagedBytes, 0L);
        return new Decision(0, serial, candidate > 1);
    }

    static String refusalReason(Reservation reservation, MergeDiskPolicy.Snapshot space) {
        if (space.sharedStore()) {
            return String.format(Locale.ROOT,
                    "merge needs ~%.2f GiB free on the shared staging/output filesystem "
                            + "(final reserve ~%.2f GiB, cascade reserve ~%.2f GiB, "
                            + "staged-size safety ~%.2f GiB, filesystem floor ~%.2f GiB, "
                            + "exact proof ~%.2f GiB), have ~%.2f GiB",
                    gib(reservation.sharedRequiredBytes()), gib(reservation.finalOutputReserveBytes()),
                    gib(reservation.cascadeReserveBytes()), gib(reservation.safetyReserveBytes()),
                    gib(reservation.filesystemFloorBytes()), gib(reservation.proofBytes()),
                    gib(space.sharedUsableBytes()));
        }
        return String.format(Locale.ROOT,
                "merge needs staging ~%.2f GiB (final-temp reserve ~%.2f GiB, cascade reserve ~%.2f GiB, staged-size safety "
                        + "~%.2f GiB, filesystem floor ~%.2f GiB, "
                        + "exact proof ~%.2f GiB; have ~%.2f GiB) and output ~%.2f GiB "
                        + "(final reserve ~%.2f GiB, staged-size safety ~%.2f GiB, filesystem floor "
                        + "~%.2f GiB; have ~%.2f GiB)",
                gib(reservation.stagingRequiredBytes()), gib(reservation.finalOutputReserveBytes()),
                gib(reservation.cascadeReserveBytes()),
                gib(reservation.safetyReserveBytes()), gib(reservation.filesystemFloorBytes()),
                gib(reservation.proofBytes()),
                gib(space.stagingUsableBytes()), gib(reservation.outputRequiredBytes()),
                gib(reservation.finalOutputReserveBytes()), gib(reservation.safetyReserveBytes()),
                gib(reservation.filesystemFloorBytes()), gib(space.outputUsableBytes()));
    }

    private static long saturatedAdd(long first, long second) {
        try {
            return Math.addExact(first, second);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static double gib(long bytes) {
        return bytes / (1024.0 * 1024.0 * 1024.0);
    }

    record Reservation(long finalOutputReserveBytes, long cascadeReserveBytes,
            long safetyReserveBytes, long filesystemFloorBytes, long proofBytes) {

        long sharedRequiredBytes() {
            long stagedPolicy = saturatedAdd(finalOutputReserveBytes, cascadeReserveBytes);
            stagedPolicy = saturatedAdd(stagedPolicy, safetyReserveBytes);
            return saturatedAdd(proofBytes, Math.max(stagedPolicy, filesystemFloorBytes));
        }

        long stagingRequiredBytes() {
            // Final Parquet files are written under staging first; a cross-store non-atomic move can
            // then temporarily require the same final bytes on output as well.
            long stagingPolicy = saturatedAdd(finalOutputReserveBytes, cascadeReserveBytes);
            stagingPolicy = saturatedAdd(stagingPolicy, safetyReserveBytes);
            return saturatedAdd(proofBytes, Math.max(stagingPolicy, filesystemFloorBytes));
        }

        long outputRequiredBytes() {
            long outputPolicy = saturatedAdd(finalOutputReserveBytes, safetyReserveBytes);
            return Math.max(outputPolicy, filesystemFloorBytes);
        }

        boolean fits(MergeDiskPolicy.Snapshot space) {
            if (space.sharedStore()) {
                return unknownOrAtLeast(space.sharedUsableBytes(), sharedRequiredBytes());
            }
            return unknownOrAtLeast(space.stagingUsableBytes(), stagingRequiredBytes())
                    && unknownOrAtLeast(space.outputUsableBytes(), outputRequiredBytes());
        }

        private static boolean unknownOrAtLeast(long usable, long required) {
            return usable < 0 || usable >= required;
        }
    }

    record Decision(int ranges, Reservation reservation, boolean diskLimited) {
        boolean refused() {
            return ranges == 0;
        }
    }
}
