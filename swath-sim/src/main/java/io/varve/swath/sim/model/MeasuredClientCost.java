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
     * The fetch worker's own per-page cost: response unmarshalling plus page-model conversion plus the
     * narrowed residual around them, at the low-concurrency end of the measured range. Sampled through
     * the measured unmarshal quantiles, shifted so the ladder carries the other two stages' means with
     * it — they are small, flat, and were published without their own ladders.
     */
    public static SampledClientCostTerm worker() {
        long parseAndResidualNanos = 100_000L + 550_000L;
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

    /** The serial checkpoint writer's per-batch service time at the low-concurrency end. */
    public static SampledClientCostTerm checkpoint() {
        return SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 109_000L, 0L,
                SOURCE + " — checkpoint commit service"));
    }

    /** The consumer stage's per-page service time for {@code sink}. */
    public static SampledClientCostTerm sink(SinkKind sink) {
        return switch (sink) {
            case TEXT -> SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 1_030_000L, 0L,
                    SOURCE + " — text consumer stage"));
            case COLUMNAR -> SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 46_000L, 0L,
                    SOURCE + " — columnar consumer dispatch"));
        };
    }

    /** The columnar sink's encode pool: parallel, off the page's critical path, heavy-tailed. */
    public static SampledClientCostTerm offload() {
        ClientCostTerm term = new ClientCostTerm(Provenance.FINAL, 2_000_000L, 0L,
                SOURCE + " — columnar encode pool");
        return SampledClientCostTerm.quantiles(term,
                new double[] {0.50, 0.90, 0.99},
                new long[] {630_000L, 1_850_000L, 18_000_000L});
    }
}
