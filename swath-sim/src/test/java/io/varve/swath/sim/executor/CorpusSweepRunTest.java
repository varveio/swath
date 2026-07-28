/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link CorpusSweep} driven from the command line</b> — the corpus counterpart to
 * {@link RealListingRunTest}, which runs the same arms against one captured listing. Opt-in and
 * fixture-free for the same reason: the corpus is a local directory the operator supplies, the repo
 * names none of it, and with the property unset the run <em>skips</em> rather than fails.
 *
 * <pre>{@code ./gradlew :swath-sim:test -PonlyPerf \
 *     -Dswath.sim.listing.corpus=/path/to/staged/captures \
 *     -Dswath.sim.listing.results=/path/to/sweep.tsv}</pre>
 *
 * <p>The results path is required rather than optional: a sweep over a corpus produces thousands of
 * numbers, its per-fixture tables scroll past on the way, and a run whose output existed only in a
 * console buffer would be a run that has to be repeated to be read. It is written as it goes, so the
 * file is complete for every fixture finished so far at any moment.
 *
 * <p><b>Nothing here asserts a magnitude.</b> What a corpus of real buckets does is the measurement,
 * and a threshold invented here would be one fitted to it. What is asserted is what a table of numbers
 * is worthless without: that the sweep measured something at all, and that no leg in it produced a
 * number nobody may use — a leg that stalled, or one that did not emit its fixture's own key total.
 * A fixture the streaming tier <em>refused</em> is not such a failure: that is the sort guard doing its
 * job, and the exclusion list is one of the sweep's outputs.
 */
@Tag("perf")
class CorpusSweepRunTest {

    /** System property naming the local directory of staged captures to sweep. */
    static final String CORPUS_PROPERTY = "swath.sim.listing.corpus";

    /** System property naming the TSV the sweep writes its per-leg rows to. */
    static final String RESULTS_PROPERTY = "swath.sim.listing.results";

    @Test
    void everySensingArmOverEveryFixtureInTheCorpus() throws IOException {
        String corpus = System.getProperty(CORPUS_PROPERTY);
        assumeTrue(corpus != null && !corpus.isBlank(),
                "-D" + CORPUS_PROPERTY + " is unset; no corpus to sweep");
        String results = System.getProperty(RESULTS_PROPERTY);
        assertThat(results).as("-D" + RESULTS_PROPERTY + " (where the sweep writes its rows)")
                .isNotNull().isNotBlank();
        Path root = Path.of(corpus);
        assertThat(Files.isDirectory(root)).as("corpus root at %s", root).isTrue();

        CorpusSweep.Result swept = CorpusSweep.sweep(root, Path.of(results));

        assertThat(swept.rows()).as("the sweep measured at least one leg").isNotEmpty();
        assertThat(swept.problems()).as("legs whose numbers are unusable").isEmpty();
    }
}
