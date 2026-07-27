/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.sim.kernel.SimRng;
import io.varve.swath.sim.model.ClientCostTerm.Provenance;
import org.junit.jupiter.api.Test;

/**
 * The sampled cost term: what its quantile ladder does between the measured points, and — just as
 * important — what it refuses to do beyond them.
 */
class SampledClientCostTermTest {

    private static final ClientCostTerm TERM =
            new ClientCostTerm(Provenance.FINAL, 2_000_000L, 0L, "a heavy-tailed stage");

    private static final SampledClientCostTerm LADDER = SampledClientCostTerm.quantiles(TERM,
            new double[] {0.50, 0.90, 0.99},
            new long[] {600_000L, 1_800_000L, 18_000_000L});

    @Test
    void theLadderInterpolatesBetweenMeasuredQuantilesAndIsFlatOutsideThem() {
        assertThat(LADDER.sample(0.0)).as("nothing below the lowest measured point was measured")
                .isEqualTo(600_000L);
        assertThat(LADDER.sample(0.50)).isEqualTo(600_000L);
        assertThat(LADDER.sample(0.70)).as("halfway from p50 to p90").isEqualTo(1_200_000L);
        assertThat(LADDER.sample(0.90)).isEqualTo(1_800_000L);
        assertThat(LADDER.sample(1.0))
                .as("the tail beyond the last measurement is not extrapolated, deliberately")
                .isEqualTo(18_000_000L);
    }

    @Test
    void aSampledTermDrawsAndAScalarOneDoesNot() {
        SimRng first = SimRng.of(7L);
        SimRng second = SimRng.of(7L);
        SampledClientCostTerm scalar = SampledClientCostTerm.scalar(TERM);

        assertThat(LADDER.isSampled()).isTrue();
        assertThat(LADDER.drawNanos(100, first))
                .as("one tape, one seed: the draw is reproducible")
                .isEqualTo(LADDER.drawNanos(100, second));
        assertThat(scalar.isSampled()).isFalse();
        assertThat(scalar.drawNanos(100, SimRng.of(1L))).isEqualTo(TERM.perPageNanos());
        assertThat(scalar.drawNanos(100, SimRng.of(2L)))
                .as("a scalar term consumes no randomness at all").isEqualTo(TERM.perPageNanos());
    }

    @Test
    void thePerKeyComponentIsAddedUnsampled() {
        ClientCostTerm perKey = new ClientCostTerm(Provenance.FINAL, 2_000_000L, 1_000L, "per key too");
        SampledClientCostTerm term = SampledClientCostTerm.quantiles(perKey,
                new double[] {0.5}, new long[] {600_000L});

        assertThat(term.drawNanos(10, SimRng.of(3L)))
                .as("a page's size is not a random quantity")
                .isEqualTo(600_000L + 10 * 1_000L);
    }

    @Test
    void aMalformedLadderIsRejected() {
        assertThatThrownBy(() -> SampledClientCostTerm.quantiles(TERM, new double[] {0.5}, new long[] {1, 2}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("one measured value");
        assertThatThrownBy(() -> SampledClientCostTerm.quantiles(TERM, new double[] {0.0}, new long[] {1}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("strictly inside");
        assertThatThrownBy(() -> SampledClientCostTerm.quantiles(TERM,
                new double[] {0.9, 0.5}, new long[] {1, 2}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ascend");
        assertThatThrownBy(() -> SampledClientCostTerm.scalar(null))
                .isInstanceOf(MissingSimDependencyException.class);
    }

    /**
     * The disclosed bias, pinned. Holding the ladder flat outside its measured ends overstates the lower
     * half (no minimum was ever published, so everything below the median is charged at the median) and
     * understates the extreme tail. The javadoc quotes the net effect on the measured worker term; this
     * is what makes that quote a fact rather than a claim, and what fails if the ladder's shape changes.
     */
    @Test
    void theWorkerTermsDrawnMeanMatchesTheBiasItsJavadocDiscloses() {
        SampledClientCostTerm worker = MeasuredClientCost.worker();
        SimRng tape = SimRng.of(20260727L);
        long total = 0;
        int draws = 200_000;

        for (int i = 0; i < draws; i++) {
            total += worker.drawNanos(0, tape);
        }

        double drawnMeanMillis = total / (double) draws / 1_000_000.0;
        double publishedMeanMillis = worker.term().perPageNanos() / 1_000_000.0;
        assertThat(drawnMeanMillis).as("the disclosed ~6.36 ms/page drawn mean").isBetween(6.30, 6.42);
        assertThat(publishedMeanMillis).as("against the published 5.85 ms/page mean").isEqualTo(5.85);
        assertThat((drawnMeanMillis - publishedMeanMillis) / publishedMeanMillis)
                .as("net +8.8%: overstated below the median, understated beyond the top quantile")
                .isBetween(0.075, 0.100);
    }

    @Test
    void theMeasuredCompositeCarriesItsProvenanceIntoEverySink() {
        for (MeasuredClientCost.SinkKind sink : MeasuredClientCost.SinkKind.values()) {
            CompositeClientCost composite = MeasuredClientCost.composite(sink);

            assertThat(composite.term().provenance()).isEqualTo(Provenance.FINAL);
            assertThat(composite.term().sourceLabel()).contains("span measurement");
            assertThat(composite.checkpointTerm().provenance()).isEqualTo(Provenance.FINAL);
            assertThat(composite.sinkTerm().perPageNanos()).isPositive();
        }
        assertThat(MeasuredClientCost.composite(MeasuredClientCost.SinkKind.COLUMNAR).offloadTerm())
                .as("the columnar sink's encode work is a stage of its own").isNotNull();
        assertThat(MeasuredClientCost.composite(MeasuredClientCost.SinkKind.TEXT).offloadTerm())
                .as("the text sink does its work on the consumer stage itself").isNull();
        assertThat(MeasuredClientCost.composite(MeasuredClientCost.SinkKind.TEXT).sinkTerm().perPageNanos())
                .as("the text sink costs the serial stage far more per page than a dispatch does")
                .isGreaterThan(MeasuredClientCost.composite(MeasuredClientCost.SinkKind.COLUMNAR)
                        .sinkTerm().perPageNanos() * 10);
    }
}
