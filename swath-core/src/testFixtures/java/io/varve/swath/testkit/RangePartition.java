/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reconstructs and verifies the <b>range-set partition</b> a sequence of splits produces
 * (algorithms.md §6 Invariant I, I2/I3). Each range is the half-open {@code (lo, hi]} with
 * {@code null} = ⊥ (lo) / ⊤ (hi). Replaying the splits from the single root range
 * {@code (⊥, ⊤]} and asserting the result is a gap-free, overlap-free tiling is the
 * structural no-gap/no-overlap proof, complementary to the emitted-key set check.
 */
public final class RangePartition {

    private RangePartition() {
    }

    /** A committed split: victim {@code (lo, oldHi]} → victim {@code (lo, pivot]} + child {@code (pivot, oldHi]}. */
    public record Split(long victimId, byte[] pivot, byte[] oldHi, long childId) {
    }

    /** A half-open interval {@code (lo, hi]}; {@code lo == null} = ⊥, {@code hi == null} = ⊤. */
    public record Interval(byte[] lo, byte[] hi) {
    }

    private static int cmp(byte[] a, byte[] b) {
        return KeyBytes.compareUnsigned(a, b);
    }

    /**
     * Replay {@code splits} (in arrival/commit order) from the root range {@code (⊥, ⊤]}
     * and return the resulting interval set, asserting along the way that each split is
     * structurally sound: the victim currently owns {@code (lo, oldHi]} (the CAS bound
     * matched), and the pivot {@code m} lies strictly between the victim's {@code lo} and
     * {@code oldHi} (I3: {@code m} belongs LEFT — the child gets {@code (m, oldHi]}).
     */
    public static List<Interval> replay(long rootId, List<Split> splits) {
        Map<Long, Interval> live = new HashMap<>();
        live.put(rootId, new Interval(null, null));   // (⊥, ⊤]
        for (Split s : splits) {
            Interval v = live.get(s.victimId());
            assertThat(v).as("split victim %d must be a live range", s.victimId()).isNotNull();
            // The CAS guard matched range_end IS oldHi, so the victim's current hi == oldHi.
            assertThat(Arrays.equals(v.hi(), s.oldHi()))
                    .as("split oldHi must equal the victim's current hi").isTrue();
            // pivot strictly between lo and oldHi (I3, boundary belongs LEFT).
            if (v.lo() != null) {
                assertThat(cmp(v.lo(), s.pivot())).as("lo < pivot").isLessThan(0);
            }
            if (s.oldHi() != null) {
                assertThat(cmp(s.pivot(), s.oldHi())).as("pivot < oldHi").isLessThan(0);
            }
            live.put(s.victimId(), new Interval(v.lo(), s.pivot()));   // victim keeps (lo, m]
            live.put(s.childId(), new Interval(s.pivot(), s.oldHi())); // child gets (m, oldHi]
        }
        return new ArrayList<>(live.values());
    }

    /**
     * Assert the intervals tile {@code (⊥, ⊤]} exactly: sorted by {@code lo}, the first
     * starts at ⊥, each interval's {@code hi} equals the next interval's {@code lo}
     * (boundary-belongs-left, so no gap and no overlap), and the last ends at ⊤.
     */
    public static void assertTiles(List<Interval> intervals) {
        assertThat(intervals).isNotEmpty();
        List<Interval> sorted = new ArrayList<>(intervals);
        sorted.sort((x, y) -> {
            if (x.lo() == null) {
                return y.lo() == null ? 0 : -1;   // ⊥ sorts first
            }
            if (y.lo() == null) {
                return 1;
            }
            return cmp(x.lo(), y.lo());
        });
        assertThat(sorted.get(0).lo()).as("partition starts at ⊥").isNull();
        for (int i = 0; i < sorted.size() - 1; i++) {
            byte[] hi = sorted.get(i).hi();
            byte[] nextLo = sorted.get(i + 1).lo();
            assertThat(hi).as("interval %d hi must be non-⊤ (it is not last)", i).isNotNull();
            assertThat(Arrays.equals(hi, nextLo))
                    .as("interval %d hi must equal interval %d lo (no gap, no overlap)", i, i + 1)
                    .isTrue();
        }
        assertThat(sorted.get(sorted.size() - 1).hi()).as("partition ends at ⊤").isNull();
    }

    /** Convenience: replay {@code splits} from {@code rootId} and assert the result tiles. */
    public static void assertTilesFromSplits(long rootId, List<Split> splits) {
        assertTiles(replay(rootId, splits));
    }
}
