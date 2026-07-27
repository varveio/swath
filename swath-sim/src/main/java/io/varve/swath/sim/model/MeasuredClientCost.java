/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.model.ClientCostTerm.Provenance;

/**
 * The measured client-side cost parameters, as published — one place, so a scenario says which sink it
 * is modelling and gets numbers whose provenance travels with them instead of a constant somebody
 * typed.
 *
 * <h2>Where the numbers come from, and how far they carry</h2>
 * Direct span instrumentation of a real listing client against a real store, over one fixture on one
 * eight-core arm64 host, at three concurrency levels and two sink kinds, with two predeclared
 * cross-checks: that the spans account for the process's own CPU, and that the consumer stage's
 * service rate reproduces an independently measured page-rate plateau. Both passed inside their
 * predeclared band, which is what {@link Provenance#FINAL} records here.
 *
 * <p><b>A final label is not a validated model.</b> What passed is CPU accounting and the reproduction
 * of one consumer-side plateau. A third check — the span model predicting a closed-loop page rate —
 * remains unpassed, limited by the replay instrument's own ceiling rather than by these terms. Read a
 * simulated result accordingly: the client's cost structure is measured, the loop it closes is not.
 *
 * <p>Known limits that travel with the numbers, stated rather than left to be rediscovered: one
 * fixture, one host, one profile; concurrency beyond roughly fifteen in flight is unexplored, so the
 * concurrency-sensitive checkpoint wait is not extrapolable past it; and the write-backpressure span
 * was never driven at a representative rate, so it is not modelled here at all rather than modelled
 * badly.
 */
public final class MeasuredClientCost {

    /** Which sink a scenario is modelling — they differ by more than a constant. */
    public enum SinkKind {
        /**
         * A text sink writing rows on the consumer stage itself: about a millisecond a page, and a
         * genuine ceiling of roughly a thousand pages a second.
         */
        TEXT,
        /**
         * A columnar sink whose consumer stage only dispatches (about forty microseconds a page) and
         * whose real encode work runs on its own pool, measured parallel and off the critical path at
         * about two milliseconds a page. Per page it costs about twice the text sink; it takes that
         * cost off the serial stage, whose occupancy it cuts by more than an order of magnitude.
         */
        COLUMNAR
    }

    private static final String SOURCE = "client-cost span measurement (one fixture, 8-vCPU arm64 host)";

    /** Lanes the columnar sink's encode pool runs; the measurement never approached its ceiling. */
    private static final int OFFLOAD_LANES = 4;

    private MeasuredClientCost() {
    }

    /**
     * The composite for {@code sink}, at the measured concurrency the fixture ran at.
     *
     * <p>The worker term is sampled — its mean sits well above its median — and so is the columnar
     * sink's offload pool, for the same reason. The checkpoint and consumer terms are charged at their
     * means: what the sink's own service time does to a page is dominated by the queue in front of it,
     * which this model produces structurally rather than from a distribution.
     */
    public static CompositeClientCost composite(SinkKind sink) {
        return new CompositeClientCost(worker(), checkpoint(), sink(sink),
                sink == SinkKind.COLUMNAR ? offload() : null, OFFLOAD_LANES);
    }

    /**
     * The fetch worker's own per-page cost, at the low-concurrency end of the measured range: three
     * spans, summed, because all three are that worker's own serial work on that page.
     *
     * <ul>
     *   <li><b>SDK response unmarshalling</b> — 5.2 ms/page uncontended, rising to 7.4–7.5 at ~15
     *       concurrent. The <b>uncontended</b> value is taken, not the middle of that range, because
     *       contention is what the simulator produces for itself: the fleet's own concurrency is the
     *       model's variable, so feeding in a figure that already contains someone else's concurrency
     *       would count it twice. The dominant term by an order of magnitude, and the only one of the
     *       three with a published distribution (p50 3.65–6.02, p90 8.90–14.41, p99 17.8–30.9), so it is
     *       the one the ladder below is built from.</li>
     *   <li><b>Response parsing into the page model</b> — measured band 0.098–0.178 ms/page. The low end
     *       for the same reason: it is the value from the uncontended arm the unmarshal figure above is
     *       taken from, so the three spans here describe one consistent operating point rather than
     *       three different ones.</li>
     *   <li><b>The narrowed residual</b> — what the worker's page time still holds after the two spans
     *       above and the time-to-first-byte are removed; measured band 0.487–0.617 ms/page, flat in
     *       concurrency, published as a mean only. 0.55 ms is its midpoint, which is the whole of the
     *       justification available: with no trend across arms to prefer an end, the middle is the least
     *       committal reading of the band.</li>
     * </ul>
     *
     * <p>The two smaller spans have no ladders of their own, so they are carried as a flat shift on the
     * unmarshal ladder rather than sampled independently — which is the honest treatment of a term
     * published as a mean, and immaterial at a twentieth of the dominant span.
     */
    public static SampledClientCostTerm worker() {
        long responseParseNanos = 100_000L;
        long narrowedResidualNanos = 550_000L;
        long parseAndResidualNanos = responseParseNanos + narrowedResidualNanos;
        ClientCostTerm term = new ClientCostTerm(Provenance.FINAL,
                5_200_000L + parseAndResidualNanos, 0L, SOURCE + " — worker page service");
        return SampledClientCostTerm.quantiles(term,
                new double[] {0.50, 0.90, 0.99},
                new long[] {
                    3_650_000L + parseAndResidualNanos,
                    8_900_000L + parseAndResidualNanos,
                    17_800_000L + parseAndResidualNanos,
                });
    }

    /**
     * The serial checkpoint writer's per-batch service time at the low-concurrency end: measured
     * 0.109 ms/batch at eight-way concurrency, rising to 0.240 at 128-way. The uncontended end again, and
     * here the reason is structural rather than conventional: only the service time belongs in this
     * parameter, and the growth across that range is produced by the queue in front of it, which this
     * model builds. Taking a contended figure would charge that queueing twice. The measurement says the
     * same thing directly — the per-page <em>wait</em> grows several-fold across the range while the
     * service time barely moves.
     */
    public static SampledClientCostTerm checkpoint() {
        return SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 109_000L, 0L,
                SOURCE + " — checkpoint commit service"));
    }

    /** The consumer stage's per-page service time for {@code sink}. */
    public static SampledClientCostTerm sink(SinkKind sink) {
        return switch (sink) {
            // Text: measured band 0.93-1.07 ms/page at ~165 KB/page, midpoint 1.03 -- a spread across
            // arms with no trend in concurrency, so the midpoint is the operating point rather than
            // either end. Its reciprocal is a real serial ceiling, independently corroborated against a
            // measured page-rate plateau.
            case TEXT -> SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 1_030_000L, 0L,
                    SOURCE + " — text consumer stage"));
            // Columnar: measured band 0.042-0.046 ms/page, midpoint taken -- the band is a spread across
            // arms rather than a trend in concurrency, so no end of it is the "right" operating point.
            // It is dispatch only: the sink's real work is the pool below, off this stage entirely.
            case COLUMNAR -> SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 44_000L, 0L,
                    SOURCE + " — columnar consumer dispatch"));
        };
    }

    /**
     * The columnar sink's encode pool: measured 1.88–2.11 ms/page and flat across a sixteen-fold range of
     * concurrency, because the work is per row and rows per page are set by the fixture rather than by
     * the fleet. Its mean is 3.2x its median (p50 0.62–0.65, p90 1.74–2.03, p99 16.0–19.4 ms), so it is
     * sampled rather than averaged. Parallel to everything and off the page's critical path; the
     * measurement ran the pool at a tenth of a core, so it establishes the service cost and says nothing
     * about the pool's own ceiling.
     */
    public static SampledClientCostTerm offload() {
        ClientCostTerm term = new ClientCostTerm(Provenance.FINAL, 2_000_000L, 0L,
                SOURCE + " — columnar encode pool");
        return SampledClientCostTerm.quantiles(term,
                new double[] {0.50, 0.90, 0.99},
                new long[] {630_000L, 1_850_000L, 18_000_000L});
    }
}
