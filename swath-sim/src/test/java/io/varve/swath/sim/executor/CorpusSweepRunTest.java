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
 * file is complete for every fixture finished so far at any moment, and it <b>must not already
 * exist</b> — the results of a sweep are cited raw, and a re-run that truncated them would destroy
 * the evidence for a finding already published.
 *
 * <p>A third, optional property caps the size of capture the sweep will open
 * ({@link CorpusSweep#MAX_KEYS_PROPERTY}): a staged corpus outlives the tier staged into it, and a
 * leftover fixture an order of magnitude larger than the rest costs more than the other hundred put
 * together while producing rows comparable with none of them. Every directory the sweep passes over,
 * for that reason or for holding no capture at all, is named with its reason in the result.
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

    /**
     * The two paths every round in this package is invoked with: the staged tree it reads and the file
     * it writes. Returned together because they are one decision — a round with a tree and nowhere to
     * write is a round whose output exists only in a console buffer.
     */
    record Staged(Path root, Path results) {
    }

    /**
     * The invocation gate every round here shares: with {@link #CORPUS_PROPERTY} unset the round is
     * <em>skipped</em> rather than failed, because the tree is a local one the operator supplies and the
     * repo names none of it; with it set, a results path is required and the tree has to exist.
     *
     * @param tree what the staged root is to this round — a {@code corpus} or a {@code roster}
     * @param verb what this round does with it — it {@code sweep}s or {@code race}s
     */
    static Staged staged(String tree, String verb) {
        String staged = System.getProperty(CORPUS_PROPERTY);
        assumeTrue(staged != null && !staged.isBlank(),
                "-D" + CORPUS_PROPERTY + " is unset; no " + tree + " to " + verb);
        String results = System.getProperty(RESULTS_PROPERTY);
        assertThat(results).as("-D" + RESULTS_PROPERTY + " (where the " + verb + " writes)")
                .isNotNull().isNotBlank();
        Path root = Path.of(staged);
        assertThat(Files.isDirectory(root)).as("%s root at %s", tree, root).isTrue();
        return new Staged(root, Path.of(results));
    }

    @Test
    void everySensingArmOverEveryFixtureInTheCorpus() throws IOException {
        Staged staged = staged("corpus", "sweep");

        CorpusSweep.Result swept = CorpusSweep.sweep(staged.root(), staged.results());

        assertThat(swept.rows()).as("the sweep measured at least one leg").isNotEmpty();
        assertThat(swept.problems()).as("legs whose numbers are unusable").isEmpty();
    }
}
