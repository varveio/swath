/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.testkit.EfficiencyHarness;
import io.varve.swath.testkit.Keyspaces;
import java.nio.file.Path;
import java.time.Duration;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The bounded time-bin {@code trajectory} rollup (in-flight concurrency + progress rate) and
 * its derived {@code serial_frac}/{@code collapse_at_frac}/{@code peak_workers}/{@code
 * final_workers} scalars.
 *
 * <p>Two tests: (1) an exact, hand-computed check of {@code RunMetrics}' bin/derived-scalar math,
 * driven on the deterministic fake-nanoClock seam {@code RunMetrics} already exposes for avg-in-
 * flight tests — this is the correctness spine; (2) an end-to-end wiring check via the
 * offline {@link EfficiencyHarness} that a REAL engine run actually populates the block.
 */
final class TrajectoryRollupTest {

    /**
     * Hand-computed scenario on a fake clock: 3 workers ramp up instantly at t=0 (no bin
     * contribution — the very first transition's window is always zero-elapsed), then drain one at
     * a time every second: 3 in-flight over [0s,1s], 2 over [1s,2s], 1 over [2s,3s]. Bin width starts
     * at 1s, so each second lands in its own bin (indices 1, 2, 3 — bin 0 is never touched since the
     * ramp-up windows are all zero-elapsed).
     */
    @Test
    void trajectoryBinsAndDerivedScalarsMatchHandComputedValues() {
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();

        metrics.incrementInFlight();   // 0 -> 1 @ t=0 (zero-elapsed window, no bin contribution)
        metrics.incrementInFlight();   // 1 -> 2 @ t=0
        metrics.incrementInFlight();   // 2 -> 3 @ t=0

        clock[0] = 1_000_000_000L;     // t = 1s
        metrics.decrementInFlight();   // 3 -> 2: bin[1] gets (value=3, dt=1s)

        clock[0] = 2_000_000_000L;     // t = 2s
        metrics.decrementInFlight();   // 2 -> 1: bin[2] gets (value=2, dt=1s)

        clock[0] = 3_000_000_000L;     // t = 3s
        metrics.decrementInFlight();   // 1 -> 0: bin[3] gets (value=1, dt=1s)

        // No page was ever fetched in this hand-driven scenario, so `shape`/`trajectory` would both
        // be null under the shared rawPageCount>0 gate (see trajectoryIsNullWhenNoPageWasEverFetched
        // below) — record one page so the block is actually populated.
        metrics.recordListingPageShape(1L, false, 1000);
        RunSummary summary = metrics.summary(Duration.ofSeconds(3), "WORK_STEALING", 0L, 0L);

        assertThat(summary.trajectory()).isNotNull();
        RunSummary.TrajectorySummary trajectory = summary.trajectory();
        assertThat(trajectory.inFlight()).hasSize(4);
        assertThat(trajectory.inFlight()[0]).as("bin 0 never touched by the zero-elapsed ramp-up").isEqualTo(0.0);
        assertThat(trajectory.inFlight()[1]).isEqualTo(3.0);
        assertThat(trajectory.inFlight()[2]).isEqualTo(2.0);
        assertThat(trajectory.inFlight()[3]).isEqualTo(1.0);

        // serial_frac: bins at or below the <=2 threshold (bins 2 and 3) over total dt (bins 1-3).
        assertThat(trajectory.serialFrac()).isCloseTo(2.0 / 3.0, Offset.offset(1e-9));
        // collapse_at_frac: the trailing low-concurrency run starts at bin 2 (bin 1 is >2, busy) —
        // 2 / 4 bins used.
        assertThat(trajectory.collapseAtFrac()).isCloseTo(0.5, Offset.offset(1e-9));
        assertThat(trajectory.peakWorkers()).isEqualTo(3);
        assertThat(trajectory.finalWorkers()).isEqualTo(1);
    }

    /** A run that never transitions in-flight (no page fetched) omits the block, like {@code shape}. */
    @Test
    void trajectoryIsNullWhenNoPageWasEverFetched() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "WORK_STEALING", 0L, 0L);
        assertThat(summary.trajectory()).isNull();
    }

    /** A never-collapsed run (still busy at the very end) reports collapse_at_frac as -1.0. */
    @Test
    void collapseAtFracIsMinusOneWhenTheRunNeverCollapsesToSerial() {
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();
        metrics.incrementInFlight();
        metrics.incrementInFlight();
        metrics.incrementInFlight();   // 3 in-flight, still busy for the whole recorded span
        clock[0] = 1_000_000_000L;
        metrics.incrementInFlight();   // another transition @ t=1s folds bin[1] = (value=3, dt=1s)
        metrics.recordListingPageShape(1L, false, 1000);

        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "WORK_STEALING", 0L, 0L);
        assertThat(summary.trajectory().collapseAtFrac())
                .as("never collapsed to <=2 in-flight -- sentinel -1.0").isEqualTo(-1.0);
    }

    /**
     * End-to-end wiring: a real {@link io.varve.swath.engine.WorkStealingScan} run (via the offline
     * {@link EfficiencyHarness}) actually populates the trajectory block with plausible values —
     * structural invariants only (bin count positive, scalars in-range), since real wall-clock
     * timing through a virtual-thread engine is not exactly reproducible run to run.
     */
    @Test
    void engineHarnessRunPopulatesATrajectoryWithPlausibleValues(@TempDir Path dir) throws Exception {
        EfficiencyHarness.Result eff = EfficiencyHarness.run(
                Keyspaces.flatRandom(1L, 4000), 8, 200, Duration.ofMillis(2), dir);

        RunSummary.TrajectorySummary trajectory = eff.trajectory();
        assertThat(trajectory).isNotNull();
        assertThat(trajectory.inFlight().length).as("at least one bin used").isGreaterThan(0);
        assertThat(trajectory.inFlight().length).isEqualTo(trajectory.progressRate().length);
        assertThat(trajectory.serialFrac()).isBetween(0.0, 1.0);
        double collapseAtFrac = trajectory.collapseAtFrac();
        assertThat(collapseAtFrac == -1.0 || (collapseAtFrac >= 0.0 && collapseAtFrac <= 1.0))
                .as("collapse_at_frac is -1.0 (never collapsed) or a valid in-range fraction").isTrue();
        assertThat(trajectory.peakWorkers()).as("peak_workers reused from the existing peakInFlight tracker")
                .isEqualTo(eff.peakInFlight());
        assertThat(trajectory.finalWorkers()).isGreaterThanOrEqualTo(0);
    }
}
