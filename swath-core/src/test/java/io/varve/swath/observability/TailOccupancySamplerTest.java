/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

/**
 * {@link TailOccupancySampler}: the bounded sample buffer that derives the tail-occupancy
 * screen (last-{@code pct}% window avg-in-flight + wall-time share) post-hoc. Two scenarios: (1)
 * an exact, hand-computed check with a capacity comfortably larger than the sample count (no
 * decimation) — the correctness spine; (2) a decimation-crossing scenario (far more {@link
 * TailOccupancySampler#record} calls than the initial {@code capacity × stride}) that still
 * reports the same DIRECTIONAL signal after the buffer has compacted/doubled its stride several
 * times over.
 */
final class TailOccupancySamplerTest {

    /**
     * No decimation (capacity 4096 ≫ 1000 samples recorded). 899 "busy" emits (in-flight 50, 1ns
     * apart) followed by 101 "serial tail" emits (in-flight 1, 100ns apart) — the classic
     * wide-then-collapsed-tail shape. The pct=10 window boundary (900/1000) lands EXACTLY on the
     * first serial-tail sample, so both windows are pure (no high-in-flight sample leaks in).
     */
    @Test
    void lastWindowReportsTheCollapsedTailDespiteAHighWholeRunAverage() {
        TailOccupancySampler sampler = new TailOccupancySampler(TailOccupancySampler.DEFAULT_CAPACITY);
        long elapsed = 0L;
        for (long k = 1; k <= 899; k++) {
            elapsed += 1L;
            sampler.record(k, elapsed, 50);
        }
        for (long k = 900; k <= 1000; k++) {
            elapsed += 100L;
            sampler.record(k, elapsed, 1);
        }
        long totalEmitted = 1000L;
        long totalElapsedNanos = elapsed;   // 899 + 101*100 = 10_999

        // pct=10: window starts exactly at the first serial-tail sample (emitIndex=900) -- every
        // sample in the window is the collapsed in-flight=1 tail.
        assertThat(sampler.avgInFlightForWindow(10, totalEmitted, totalElapsedNanos))
                .as("last-10% avg in-flight collapses to the serial tail's value")
                .isEqualTo(1.0);
        double wallShare10 = sampler.wallShareForWindow(10, totalEmitted, totalElapsedNanos);
        assertThat(wallShare10).as("a 10%-of-keys window eating ~91% of wall time -- the serial-tail signature")
                .isCloseTo(10_000.0 / 10_999.0, Offset.offset(1e-9));
        assertThat(wallShare10).as("wall share far exceeds the window's own 10% key share").isGreaterThan(0.5);

        // pct=5: still entirely inside the serial tail (900..1000 spans the last 10%, so the last
        // 5% -- emitIndex 950..1000 -- is a strict subset).
        assertThat(sampler.avgInFlightForWindow(5, totalEmitted, totalElapsedNanos)).isEqualTo(1.0);
        double wallShare5 = sampler.wallShareForWindow(5, totalEmitted, totalElapsedNanos);
        assertThat(wallShare5).as("wall share far exceeds the window's own 5% key share").isGreaterThan(0.05 * 5);

        // Contrast: the naive whole-run mean in-flight (899*50 + 101*1)/1000 stays high (~45),
        // exactly the blind spot the tail-occupancy window exists to see through.
        double wholeRunMean = (899.0 * 50 + 101.0 * 1) / 1000.0;
        assertThat(wholeRunMean).isGreaterThan(40.0);
    }

    /** No samples recorded yet -- both derivations report {@code NaN}, never a fabricated 0. */
    @Test
    void reportsNanBeforeAnySampleIsRecorded() {
        TailOccupancySampler sampler = new TailOccupancySampler(TailOccupancySampler.DEFAULT_CAPACITY);
        assertThat(sampler.avgInFlightForWindow(10, 1000L, 1_000_000L)).isNaN();
        assertThat(sampler.wallShareForWindow(10, 1000L, 1_000_000L)).isNaN();
    }

    /**
     * Decimation-crossing: capacity=64, but 3000 emits are recorded (comfortably more than the
     * initial {@code capacity × stride}=64), forcing several stride-doubling compactions. The last
     * 301 emits (>10%) still hold the collapsed in-flight=1 shape and cost 100ns/emit versus the
     * dense region's 1ns/emit -- the ring/doubling-bucket decimation must not destroy that signal,
     * even though only a handful of the tail's samples survive compaction. The dense/tail split
     * (k=2699/2700) sits one emit BEFORE the pct=10 boundary (emitIndex 2700, exactly {@code
     * totalEmitted * 0.9}), so — unlike a split placed exactly ON the boundary — no dense sample can
     * ever land in either window regardless of which samples decimation happens to keep: EVERY
     * surviving sample with {@code emitIndex >= 2700} is, by construction, from the collapsed tail.
     */
    @Test
    void decimationAcrossManyCompactionsStillSurfacesTheCollapsedTail() {
        TailOccupancySampler sampler = new TailOccupancySampler(64);
        long elapsed = 0L;
        for (long k = 1; k <= 2699; k++) {
            elapsed += 1L;
            sampler.record(k, elapsed, 50);
        }
        for (long k = 2700; k <= 3000; k++) {
            elapsed += 100L;
            sampler.record(k, elapsed, 1);
        }
        long totalEmitted = 3000L;
        long totalElapsedNanos = elapsed;   // 2699 + 301*100 = 32_799

        // pct=10: EVERY surviving sample in the window is a pure collapsed-tail sample (see the
        // javadoc's boundary-placement note), so the average is exactly 1.0, not merely "low" --
        // decimation thins out WHICH tail samples survive, never which REGION they come from.
        assertThat(sampler.avgInFlightForWindow(10, totalEmitted, totalElapsedNanos))
                .as("the last-10% window is pure collapsed tail even under heavy decimation")
                .isEqualTo(1.0);
        double wallShare10 = sampler.wallShareForWindow(10, totalEmitted, totalElapsedNanos);
        assertThat(wallShare10)
                .as("the collapsed tail's 100ns/emit cost still dominates wall time despite surviving only a "
                        + "few decimated samples")
                .isGreaterThan(0.5);

        // pct=5: same purity argument, a strict subset of the pct=10 window.
        assertThat(sampler.avgInFlightForWindow(5, totalEmitted, totalElapsedNanos)).isEqualTo(1.0);
        assertThat(sampler.wallShareForWindow(5, totalEmitted, totalElapsedNanos))
                .as("wall share still far exceeds the window's own 5% key share").isGreaterThan(0.25);
    }

    /**
     * Regression: a {@code --sort --resume} reattach backfills {@code entriesEmitted} with a
     * pre-crash count BEFORE this process lists a single key of its own ({@code
     * RunMetrics#recordRecoveredObjects}). Without {@link TailOccupancySampler#recordBaseline}, the
     * backfilled jump pollutes {@code totalEmitted}: {@code windowStartEmit} can fall at/below the
     * FIRST real sample, so both {@code pct} windows collapse onto this process's ENTIRE relisted
     * span (identical result regardless of {@code pct}) and {@code wall_share} reads ≈1.0
     * regardless of {@code pct} — exactly the symptom this test pins. {@code recordBaseline}
     * fixes it: the windows are then derived over {@code totalListed = totalEmitted - baseline}
     * (THIS process's own 100 relisted keys), landing cleanly inside the pure collapsed-tail
     * sub-region instead.
     *
     * <p>Scenario: a 2000-key pre-crash backfill (large relative to this process's own relisted
     * span, so a NAIVE {@code totalEmitted}-only threshold falls below the first real sample for
     * BOTH pct=5 and pct=10), then this process relists 100 keys of its own (emitIndex
     * 2001..2100) -- 89 "busy" (in-flight 50, 1ns apart) followed by 11 "collapsed tail" (in-flight
     * 1, 100ns apart). With the baseline excluded, the pct=10 boundary ({@code baseline +
     * totalListed*0.9 = 2090}) lands exactly on the first collapsed-tail sample (k=2090); pct=5
     * ({@code 2095}) lands inside it.
     */
    @Test
    void recordBaselineExcludesTheResumeBackfillFromTheWindowDenominator() {
        long baseline = 2000L;
        long totalEmitted = 2100L;   // baseline (2000) + this process's own 100 relisted keys

        TailOccupancySampler withoutBaseline = new TailOccupancySampler(TailOccupancySampler.DEFAULT_CAPACITY);
        TailOccupancySampler withBaseline = new TailOccupancySampler(TailOccupancySampler.DEFAULT_CAPACITY);
        withBaseline.recordBaseline(baseline);

        long elapsed = 0L;
        for (long k = 2001; k <= 2089; k++) {
            elapsed += 1L;
            withoutBaseline.record(k, elapsed, 50);
            withBaseline.record(k, elapsed, 50);
        }
        for (long k = 2090; k <= 2100; k++) {
            elapsed += 100L;
            withoutBaseline.record(k, elapsed, 1);
            withBaseline.record(k, elapsed, 1);
        }
        long totalElapsedNanos = elapsed;   // 89 + 11*100 = 1_189

        // BUGGY (no baseline): windowStartEmit(pct=10)=1890 and (pct=5)=1995 both sit BELOW the
        // first real sample (2001), so both windows collapse to the whole 100-key relisted span --
        // identical result regardless of pct, a blend of 89 busy + 11 collapsed samples, not the
        // pure tail -- and wall_share reads ≈1.0 regardless of pct.
        double buggyAvg10 = withoutBaseline.avgInFlightForWindow(10, totalEmitted, totalElapsedNanos);
        double buggyAvg5 = withoutBaseline.avgInFlightForWindow(5, totalEmitted, totalElapsedNanos);
        assertThat(buggyAvg10).as("collapses to the whole relisted span, not the pure tail")
                .isCloseTo(44.61, Offset.offset(0.01));
        assertThat(buggyAvg5).as("pct=5 and pct=10 collapse to the SAME span without a baseline")
                .isEqualTo(buggyAvg10);
        assertThat(withoutBaseline.wallShareForWindow(10, totalEmitted, totalElapsedNanos))
                .as("wall_share reads ~1.0 regardless of pct without a baseline")
                .isGreaterThan(0.99);
        assertThat(withoutBaseline.wallShareForWindow(5, totalEmitted, totalElapsedNanos))
                .isGreaterThan(0.99);

        // FIXED (recordBaseline told about the 900-key backfill): both windows land purely inside
        // the collapsed tail, and differ correctly by pct.
        assertThat(withBaseline.avgInFlightForWindow(10, totalEmitted, totalElapsedNanos))
                .as("pct=10 window is the pure collapsed tail once the backfill is excluded")
                .isEqualTo(1.0);
        assertThat(withBaseline.avgInFlightForWindow(5, totalEmitted, totalElapsedNanos)).isEqualTo(1.0);
        double wallShare10 = withBaseline.wallShareForWindow(10, totalEmitted, totalElapsedNanos);
        double wallShare5 = withBaseline.wallShareForWindow(5, totalEmitted, totalElapsedNanos);
        assertThat(wallShare10).isCloseTo(1000.0 / 1189.0, Offset.offset(1e-9));
        assertThat(wallShare5).isCloseTo(500.0 / 1189.0, Offset.offset(1e-9));
        assertThat(wallShare5).as("the pct=5 window is strictly narrower in wall time than pct=10")
                .isLessThan(wallShare10);
    }
}
