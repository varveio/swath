/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import io.varve.swath.sim.model.ClientCostTerm.Provenance;

/**
 * Published client-cost calibration for one fixture, profile, and 8-vCPU arm64 host.
 *
 * <p>{@link Provenance#FINAL} means its stated CPU-accounting and consumer-plateau checks passed; it
 * does not validate the closed-loop model. It excludes unrepresentative write backpressure and does
 * not support extrapolating the concurrency-sensitive checkpoint wait beyond roughly 15 in flight.
 */
public final class MeasuredClientCost {

    /** Which sink a scenario is modelling — they differ by more than a constant. */
    public enum SinkKind {
        /** Text rows are consumed serially. */
        TEXT,
        /** Columnar dispatch is serial; encoding is parallel offload. */
        COLUMNAR
    }

    private static final String SOURCE = "client-cost span measurement (one fixture, 8-vCPU arm64 host)";

    /** Fixed columnar encode lanes; the measurement did not establish the pool's ceiling. */
    private static final int OFFLOAD_LANES = 4;

    private MeasuredClientCost() {
    }

    /**
     * Returns the calibrated composite for {@code sink}. Worker and columnar offload use their
     * published quantile ladders; checkpoint and consumer stages use published service costs.
     */
    public static CompositeClientCost composite(SinkKind sink) {
        return new CompositeClientCost(worker(), checkpoint(), sink(sink),
                sink == SinkKind.COLUMNAR ? offload() : null, OFFLOAD_LANES);
    }

    /**
     * Worker service at the uncontended end: one sampled unmarshal ladder plus flat parse and residual
     * terms. The low-end spans avoid double-counting fleet contention; the 0.55 ms residual is the
     * midpoint of its flat 0.487–0.617 ms band. Only unmarshalling has a published ladder.
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
     * Serial checkpoint service at the low-concurrency end (0.109 ms/batch). Queue wait is modelled by
     * the server, rather than folded into this scalar service term.
     */
    public static SampledClientCostTerm checkpoint() {
        return SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 109_000L, 0L,
                SOURCE + " — checkpoint commit service"));
    }

    /** The consumer stage's per-page service time for {@code sink}. */
    public static SampledClientCostTerm sink(SinkKind sink) {
        return switch (sink) {
            // Text service is the 1.03 ms midpoint of a 0.93–1.07 ms band with no concurrency trend.
            case TEXT -> SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 1_030_000L, 0L,
                    SOURCE + " — text consumer stage"));
            // Columnar's 0.044 ms midpoint is serial dispatch; encoding is offloaded below.
            case COLUMNAR -> SampledClientCostTerm.scalar(new ClientCostTerm(Provenance.FINAL, 44_000L, 0L,
                    SOURCE + " — columnar consumer dispatch"));
        };
    }

    /**
     * Parallel columnar encode service, sampled because its mean is 3.2× its median. The 1.88–2.11 ms
     * range was flat across the measured concurrency range; it establishes service cost, not capacity.
     */
    public static SampledClientCostTerm offload() {
        ClientCostTerm term = new ClientCostTerm(Provenance.FINAL, 2_000_000L, 0L,
                SOURCE + " — columnar encode pool");
        return SampledClientCostTerm.quantiles(term,
                new double[] {0.50, 0.90, 0.99},
                new long[] {630_000L, 1_850_000L, 18_000_000L});
    }
}
