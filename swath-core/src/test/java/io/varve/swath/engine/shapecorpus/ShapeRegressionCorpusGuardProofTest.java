/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine.shapecorpus;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.SeedMode;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.SeedSteps;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The shape-regression tier's Hive envelope is SHARP, not vacuous. Deliberately NOT
 * {@code @Tag("deep")}: this must run on every commit, proving live that
 * {@link ShapeRegressionCorpusTest#hiveDensePartitions_seedsWide_lowSerialFrac_byteExact} would
 * actually catch a regression in fanout_tiling, rather than resting on a one-off manual
 * check. Seed-time only (no engine run needed — the seed-range-count half of the envelope is
 * decided entirely at seed time, with zero timing dependency), so this stays fast and always-on.
 *
 * <p>Toggling {@code fanout_tiling} OFF reproduces the pre-fix Hive collapse: the SAME Hive
 * keyspace that the ON envelope requires to reach {@code >= 0.8W} seed ranges collapses to a
 * couple of ranges instead — violating the envelope's seed-count bound. That is the guard's whole
 * point: if a future regression silently disables/breaks the tiling path, this bound is what
 * fails.
 */
final class ShapeRegressionCorpusGuardProofTest {

    @Test
    void fanoutTilingOffViolatesTheHiveEnvelope() throws Exception {
        int workers = 24;
        List<byte[]> keyspace = ShapeRegressionCorpusTest.hiveDensePartitions(1100, 150);
        // Explicit massAwareSeed=false (12th arg): the 10-arg back-compat constructor now resolves
        // massAwareSeed=true (its default), which would let mass-aware sampling BAND this fixture's
        // heavy `date=` partitions (150 objs/partition, easily sample-proven heavy) instead of
        // collapsing to a couple of ranges — defeating this guard's whole point (proving
        // fanout_tiling=off alone violates the envelope). Pin it explicitly off.
        EngineToggles fanoutOff =
                EngineToggles.DEFAULT.withFanoutTiling(false).withMassAwareSeed(false);

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<NodeSpec> specs = SeedSteps.of(fetcher, new byte[0], workers, null, fanoutOff)
                .seedSpecs(1L, SeedMode.SHALLOW);

        int envelopeFloor = (int) Math.floor(0.8 * workers);
        assertThat(specs.size())
                .as("fanout_tiling OFF (a revert) collapses well below the ON envelope's "
                        + ">= 0.8W seed-range floor (%d) — proving the envelope is sharp, not vacuous",
                        envelopeFloor)
                .isLessThan(envelopeFloor);
    }
}
