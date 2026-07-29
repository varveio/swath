/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Boundary, fill/wrap, split-aware, and coherence-under-concurrency coverage for {@link
 * CarveCompletionWindow} — the E-20 redesign of {@code CarveMassRing} (campaign memo §5, punch-list
 * rows 24/25). Exercises the pure component directly (package-private, no engine machinery), parallel
 * in style to {@code OwnerSplitChildMassFloorTest}.
 */
final class CarveCompletionWindowTest {

    private static final int MAX_KEYS = 100;

    // -------------------------------------------------------------------------------------------
    // Zero warmup: NaN only when nothing has completed yet; a single completion already averages.
    // -------------------------------------------------------------------------------------------

    @Test
    void freshWindowHasNoSignal() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        assertThat(window.windowAverage(2, MAX_KEYS)).as("nothing recorded yet").isNaN();
    }

    @Test
    void aSingleCompletionAlreadyAverages() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        window.record(50L, false);
        assertThat(window.windowAverage(8, MAX_KEYS))
                .as("zero warmup: n=1 is enough to produce a real average, not NaN")
                .isEqualTo(50.0);
    }

    // -------------------------------------------------------------------------------------------
    // Fill/wrap: prefix average up to CAPACITY, then the oldest sample evicts on the next record.
    // -------------------------------------------------------------------------------------------

    @Test
    void oneShortOfCapacityAveragesOverWhatHasArrived() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        for (int i = 0; i < CarveCompletionWindow.CAPACITY - 1; i++) {
            window.record(100L, false);
        }
        assertThat(window.windowAverage(8, MAX_KEYS)).isEqualTo(100.0);
    }

    @Test
    void exactlyCapacityProducesTheFullWindowAverage() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        for (long i = 1; i <= CarveCompletionWindow.CAPACITY; i++) {
            window.record(i, false);
        }
        // 1..8 -> average 4.5
        assertThat(window.windowAverage(8, MAX_KEYS)).isEqualTo(4.5);
    }

    /**
     * Past the window, the OLDEST sample is evicted — a ring, not a running/cumulative average.
     * CAPACITY=8 fills with 1..8 (average 4.5), then one more record(1000) evicts the oldest (1),
     * leaving {2,3,4,5,6,7,8,1000}: average = (2+3+4+5+6+7+8+1000)/8 = 129.375.
     */
    @Test
    void oneMoreRecordPastCapacityEvictsOnlyTheOldestSample() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        for (long i = 1; i <= CarveCompletionWindow.CAPACITY; i++) {
            window.record(i, false);
        }
        window.record(1000L, false);

        assertThat(window.windowAverage(8, MAX_KEYS)).isEqualTo(129.375);
    }

    /** A full wrap (2 * CAPACITY records) leaves only the second batch in the window. */
    @Test
    void aFullWrapLeavesOnlyTheMostRecentBatch() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        for (int i = 0; i < CarveCompletionWindow.CAPACITY; i++) {
            window.record(1L, false);   // evicted entirely by the second batch below
        }
        for (long i = 1; i <= CarveCompletionWindow.CAPACITY; i++) {
            window.record(i * 10, false);
        }

        assertThat(window.windowAverage(8, MAX_KEYS))
                .as("second batch 10,20,...,80 -> average 45.0, no trace of the first batch's 1s")
                .isEqualTo(45.0);
    }

    @Test
    void countTracksEveryRecordUncapped() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        for (int i = 0; i < CarveCompletionWindow.CAPACITY * 3; i++) {
            window.record(1L, false);
        }
        assertThat(window.count()).isEqualTo((long) CarveCompletionWindow.CAPACITY * 3);
    }

    // -------------------------------------------------------------------------------------------
    // Split-aware effective mass (E-20, punch-list row 25): a child that itself split further is
    // never negative evidence, floored at K*maxKeys rather than counted at its own (irrelevant) tally.
    // -------------------------------------------------------------------------------------------

    @Test
    void splitChildAtLowMassDoesNotDragTheAverageDown() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        // One split child with a tiny own tally (mass=1, but hasSplit=true) alongside seven
        // healthy, never-split children at mass 1000 each.
        window.record(1L, true);
        for (int i = 0; i < CarveCompletionWindow.CAPACITY - 1; i++) {
            window.record(1000L, false);
        }
        // K=2 -> floor = 2*100 = 200. The split child's effective mass is max(1, 200) = 200, not 1.
        double expected = (200.0 + 1000.0 * 7) / CarveCompletionWindow.CAPACITY;
        assertThat(window.windowAverage(2, MAX_KEYS))
                .as("the split child is floored at K*maxKeys, not counted at its tiny own tally")
                .isEqualTo(expected);
    }

    @Test
    void splitChildAboveTheFloorKeepsItsOwnMass() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        // A split child whose own tally (500) already exceeds K*maxKeys (200 at K=2): the max()
        // is a floor, not a ceiling -- it never DEFLATES a healthy split child's own reading either.
        window.record(500L, true);
        for (int i = 0; i < CarveCompletionWindow.CAPACITY - 1; i++) {
            window.record(500L, true);
        }
        assertThat(window.windowAverage(2, MAX_KEYS))
                .as("every entry already clears the floor, so effective mass == own mass throughout")
                .isEqualTo(500.0);
    }

    @Test
    void nonSplitChildIsNeverFloored() {
        CarveCompletionWindow window = new CarveCompletionWindow();
        // A never-split child at a tiny mass is genuine confetti-signal evidence -- the floor
        // applies ONLY to split children (hasSplit=false must count at its raw, unfloored mass).
        for (int i = 0; i < CarveCompletionWindow.CAPACITY; i++) {
            window.record(1L, false);
        }
        assertThat(window.windowAverage(8, MAX_KEYS))
                .as("K's floor never applies to a non-split entry, however large K is")
                .isEqualTo(1.0);
    }

    @Test
    void kIsResolvedAtReadTimeNotBakedIntoStorage() {
        // One window, raw pairs recorded once; different K read back different effective averages
        // from the SAME stored entries -- proving K is applied at windowAverage(), not at record().
        CarveCompletionWindow window = new CarveCompletionWindow();
        window.record(50L, true);
        for (int i = 0; i < CarveCompletionWindow.CAPACITY - 1; i++) {
            window.record(50L, true);
        }
        assertThat(window.windowAverage(2, MAX_KEYS)).as("K=2 floor=200").isEqualTo(200.0);
        assertThat(window.windowAverage(4, MAX_KEYS)).as("K=4 floor=400, same stored entries").isEqualTo(400.0);
        assertThat(window.windowAverage(8, MAX_KEYS)).as("K=8 floor=800, same stored entries").isEqualTo(800.0);
    }

    // -------------------------------------------------------------------------------------------
    // Coherence under concurrent hammer (E-20, punch-list row 24 -- the ring's non-linearizable
    // publication defect this class replaces). A single monitor guards both record and
    // windowAverage, so a reader must never observe a torn mix of pre- and post-write state.
    // -------------------------------------------------------------------------------------------

    /**
     * <b>Attack:</b> many writer threads hammer {@link CarveCompletionWindow#record} with the SAME
     * fixed value (mass=777, never split) while a reader thread concurrently hammers {@link
     * CarveCompletionWindow#windowAverage}. Since EVERY entry ever written is 777, any COHERENT read
     * (whatever point in the writers' progress it lands on) must average to EXACTLY 777.0 once at
     * least one write has landed — a torn read (e.g. an unsynchronized implementation blending a
     * not-yet-written zero-default slot with real 777s, or reading {@code masses[i]} and {@code
     * hasSplit[i]} from two different generations) would instead surface as a value other than
     * 777.0/NaN at some point during the hammer.
     */
    @Test
    void windowAverageNeverObservesATornMixUnderConcurrentHammer() throws InterruptedException {
        CarveCompletionWindow window = new CarveCompletionWindow();
        int writerThreads = 16;
        int recordsPerWriter = 5_000;
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.List<Double> observed = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        Thread reader = new Thread(() -> {
            while (!stop.get()) {
                double avg = window.windowAverage(2, MAX_KEYS);
                if (!Double.isNaN(avg)) {
                    observed.add(avg);
                }
            }
        });
        Thread[] writers = new Thread[writerThreads];
        for (int i = 0; i < writerThreads; i++) {
            writers[i] = new Thread(() -> {
                for (int c = 0; c < recordsPerWriter; c++) {
                    window.record(777L, false);
                }
            });
        }

        reader.start();
        for (Thread w : writers) {
            w.start();
        }
        for (Thread w : writers) {
            w.join();
        }
        stop.set(true);
        reader.join();

        assertThat(observed).as("the reader must have observed at least one non-NaN average").isNotEmpty();
        assertThat(observed)
                .as("every single one of %d reads taken DURING concurrent writes is exactly 777.0 -- a "
                        + "torn/non-coherent read would surface a different value at least once", observed.size())
                .containsOnly(777.0);
        assertThat(window.count()).isEqualTo((long) writerThreads * recordsPerWriter);
    }

    /**
     * The sibling of the hammer above with MIXED entries (half the writer threads record a split
     * child floored well above its own tiny mass; half record a non-split child at a different
     * fixed mass) — genuinely concurrent, so the window's actual composition at any instant is
     * unpredictable (anywhere from 0 to {@link CarveCompletionWindow#CAPACITY} of each kind), which
     * is exactly why the assertion is a RANGE rather than an enumerated set: every real composition
     * of up to {@code CAPACITY} entries each equal to either {@code splitEffective} or {@code
     * nonSplitRaw} is a weighted average of the two, so it must fall within
     * {@code [min, max]} inclusive. A torn read that exposes a not-yet-written slot's zero default
     * alongside real entries would pull the average below that minimum — exactly the failure mode
     * this bound catches (a torn read blending generations, {@link CarveCompletionWindow}'s
     * replaced-{@code CarveMassRing} defect, punch-list row 24).
     */
    @Test
    void windowAverageStaysWithinTheAchievableRangeUnderMixedConcurrentWrites() throws InterruptedException {
        CarveCompletionWindow window = new CarveCompletionWindow();
        int writerThreads = 8;
        int recordsPerWriter = 4_000;
        double splitEffective = 8.0 * MAX_KEYS;   // K=8 floor -- the split child's floored value
        long nonSplitRaw = 55L;
        double low = Math.min(splitEffective, nonSplitRaw);
        double high = Math.max(splitEffective, nonSplitRaw);

        Thread[] writers = new Thread[writerThreads];
        for (int i = 0; i < writerThreads; i++) {
            boolean split = i % 2 == 0;
            writers[i] = new Thread(() -> {
                for (int c = 0; c < recordsPerWriter; c++) {
                    if (split) {
                        window.record(1L, true);   // effective mass floored to splitEffective at K=8
                    } else {
                        window.record(nonSplitRaw, false);
                    }
                }
            });
        }
        java.util.List<Double> observed = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        Thread reader = new Thread(() -> {
            while (!stop.get()) {
                double avg = window.windowAverage(8, MAX_KEYS);
                if (!Double.isNaN(avg)) {
                    observed.add(avg);
                }
            }
        });

        reader.start();
        for (Thread w : writers) {
            w.start();
        }
        for (Thread w : writers) {
            w.join();
        }
        stop.set(true);
        reader.join();

        assertThat(observed).isNotEmpty();
        for (double v : observed) {
            assertThat(v)
                    .as("every observed average must be a weighted mix of splitEffective (%s) and "
                            + "nonSplitRaw (%s), so within [%s, %s] -- a torn read exposing a "
                            + "not-yet-written zero slot would pull it below the minimum",
                            splitEffective, nonSplitRaw, low, high)
                    .isBetween(low, high);
        }
    }
}
