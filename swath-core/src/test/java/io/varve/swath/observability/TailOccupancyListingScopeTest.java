/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The tail-occupancy gauges are scoped to the LISTING phase, not the whole run — the window and its
 * wall-share denominator both end where the samples end ({@link RunMetrics#markListingFinished()}),
 * never at gauge-read time. Driven over a fake clock ({@code RunMetrics(MeterRegistry,
 * LongSupplier)}), so a simulated post-listing merge is a clock advance rather than a sleep.
 *
 * <p>What this pins: {@link TailOccupancySampler} only ever samples listing-time page emits, so a
 * whole-run elapsed would stretch the window past the last sample and — on a {@code --sort} run,
 * whose merge/publish tail can be a majority of wall time — report the merge instead of the serial
 * listing tail these gauges exist to screen for.
 */
final class TailOccupancyListingScopeTest {

    private static final long MS = 1_000_000L;

    /** Pages of 100 keys, one per millisecond, at a steady 2 in flight. */
    private static final int PAGES = 20;
    private static final long KEYS_PER_PAGE = 100L;

    private static double gauge(RunMetrics metrics, String name, int pct) {
        return metrics.registry().get(name).tag("pct", Integer.toString(pct)).gauge().value();
    }

    @Test
    void postListingWallTimeNeverMovesTheTailWindow() {
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();
        metrics.incrementInFlight();
        metrics.incrementInFlight();
        for (int page = 0; page < PAGES; page++) {
            clock[0] += MS;
            metrics.recordEntriesEmitted(KEYS_PER_PAGE);
        }

        // At the boundary: the last 10% of 2000 keys starts at key 1800, i.e. the sample taken at
        // t=18ms, so the window is the final 2ms of a 20ms listing.
        double wallShareAtBoundary = gauge(metrics, "swath.tail_occupancy.wall_share", 10);
        double avgInFlightAtBoundary = gauge(metrics, "swath.tail_occupancy.avg_in_flight", 10);
        assertThat(wallShareAtBoundary).isCloseTo(0.1, within(1e-9));
        assertThat(avgInFlightAtBoundary).isCloseTo(2.0, within(1e-9));

        metrics.markListingFinished();
        clock[0] += 180 * MS;   // a merge/publish tail 9x the listing it followed

        assertThat(gauge(metrics, "swath.tail_occupancy.wall_share", 10))
                .as("the wall share is a share of LISTING wall time — a merge that emits no key "
                        + "cannot move it")
                .isEqualTo(wallShareAtBoundary);
        assertThat(gauge(metrics, "swath.tail_occupancy.avg_in_flight", 10))
                .as("the window is unchanged, so its mean in-flight is too")
                .isEqualTo(avgInFlightAtBoundary);
        assertThat(gauge(metrics, "swath.tail_occupancy.wall_share", 5))
                .as("a whole-run denominator would drive every window's share toward 1 — the "
                        + "serial-tail signal these gauges exist for, drowned by the merge")
                .isLessThan(0.5);
    }

    @Test
    void anUnsortedRunKeepsTheLiveListingClock() {
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();
        metrics.incrementInFlight();
        for (int page = 0; page < PAGES; page++) {
            clock[0] += MS;
            metrics.recordEntriesEmitted(KEYS_PER_PAGE);
        }

        // No boundary is ever stamped on a run that never merges, so elapsed keeps growing with the
        // run clock: the same 2ms window now sits inside a 40ms listing.
        assertThat(gauge(metrics, "swath.tail_occupancy.wall_share", 10)).isCloseTo(0.1, within(1e-9));
        clock[0] += 20 * MS;
        assertThat(gauge(metrics, "swath.tail_occupancy.wall_share", 10)).isCloseTo(0.55, within(1e-9));
    }

    @Test
    void theListingBoundaryStampIsIdempotent() {
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();
        metrics.incrementInFlight();
        clock[0] = 10 * MS;
        metrics.recordEntriesEmitted(KEYS_PER_PAGE);

        metrics.markListingFinished();
        clock[0] = 500 * MS;
        metrics.markListingFinished();   // a second crossing must not re-stamp the boundary

        assertThat(metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 0L, 0L)
                .listingDuration().toMillis())
                .as("first call wins — listing_duration_ms stays the first crossing")
                .isEqualTo(10L);
    }

    /**
     * A boundary crossed at clock 0 is a real crossing. The stamp is claimed by a separate flag, not
     * by a CAS from a 0 sentinel on the timestamp — a fake clock may legitimately read 0 there, and
     * a sentinel scheme would silently leave the boundary unstamped and hand the win to the first
     * NON-ZERO crossing instead (the same reasoning {@code sessionClaimed} is built on).
     */
    @Test
    void aBoundaryStampedAtClockZeroSticks() {
        long[] clock = {0L};
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry(), () -> clock[0]);
        metrics.markRunStarted();       // run start and the boundary both land at clock 0
        metrics.incrementInFlight();
        metrics.recordEntriesEmitted(KEYS_PER_PAGE);
        metrics.markListingFinished();

        clock[0] = 500 * MS;
        metrics.markListingFinished();   // a later crossing must not take a stamp already claimed

        assertThat(metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 0L, 0L)
                .listingDuration())
                .as("a zero-length listing is a real listing — not an unstamped boundary")
                .isEqualTo(Duration.ZERO);
        // The listing window has zero elapsed, so the gauges report the sampler's "unobserved" NaN
        // rather than picking the merge's wall time back up as the run clock advances.
        assertThat(gauge(metrics, "swath.tail_occupancy.wall_share", 10)).isNaN();
        assertThat(gauge(metrics, "swath.tail_occupancy.avg_in_flight", 10)).isNaN();
    }
}
