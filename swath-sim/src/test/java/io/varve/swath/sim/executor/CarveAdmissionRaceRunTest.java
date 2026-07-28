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
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link CarveAdmissionRaceProtocol}, run.</b> The same machinery {@link CorpusSweepRunTest} drives,
 * over a roster rather than a corpus and with the screening tier switched off: every fixture the
 * operator stages under the roster root is raced by all three arms at all four seeds, because this
 * round exists to resolve verdicts rather than to find candidates for one.
 *
 * <pre>{@code ./gradlew :swath-sim:test -PonlyPerf \
 *     -Dswath.sim.listing.corpus=/path/to/staged/roster \
 *     -Dswath.sim.listing.results=/path/to/race.tsv}</pre>
 *
 * <p>Opt-in and fixture-free on the same two properties and for the same reasons: the roster is a
 * local directory the operator supplies, the repo names none of it, the results file must not already
 * exist, and with the corpus property unset the run <em>skips</em> rather than fails. Which fixtures
 * carry which of the protocol's roles is the operator's staging decision, stated in the round's record
 * — nothing here knows a fixture's name, so nothing here can quietly weight one.
 *
 * <p><b>Nothing asserts a magnitude.</b> The protocol's criteria are read off the verdict table this
 * prints; a threshold asserted here would be one fitted to the numbers it is judging. What is asserted
 * is what the corpus sweep asserts: that the race measured something, and that no leg produced a
 * number nobody may use.
 */
@Tag("perf")
class CarveAdmissionRaceRunTest {

    @Test
    void theCarveAdmissionCandidateAgainstTheIncumbentAndTheControl() throws IOException {
        String roster = System.getProperty(CorpusSweepRunTest.CORPUS_PROPERTY);
        assumeTrue(roster != null && !roster.isBlank(),
                "-D" + CorpusSweepRunTest.CORPUS_PROPERTY + " is unset; no roster to race");
        String results = System.getProperty(CorpusSweepRunTest.RESULTS_PROPERTY);
        assertThat(results).as("-D" + CorpusSweepRunTest.RESULTS_PROPERTY + " (where the race writes)")
                .isNotNull().isNotBlank();
        Path root = Path.of(roster);
        assertThat(Files.isDirectory(root)).as("roster root at %s", root).isTrue();

        CorpusSweep.Race race = new CorpusSweep.Race(
                List.of(SensingVariant.CURRENT, SensingVariant.RATE_CURSOR_ANCHORED,
                        SensingVariant.RATE_ANCHORED_LIFT_ONLY), true);
        CorpusSweep.Result raced = CorpusSweep.sweep(root, Path.of(results), race);

        CarveAdmissionRaceProtocol.printVerdicts(
                "carve-admission race — paired relative duration against the shipped sensor, same seed",
                CarveAdmissionRaceProtocol.verdicts(raced.rows(), SensingVariant.CURRENT));
        CarveAdmissionRaceProtocol.printVerdicts(
                "carve-admission race — and against the incumbent candidate, same seed",
                CarveAdmissionRaceProtocol.verdicts(raced.rows(), SensingVariant.RATE_CURSOR_ANCHORED));

        assertThat(raced.rows()).as("the race measured at least one leg").isNotEmpty();
        assertThat(raced.problems()).as("legs whose numbers are unusable").isEmpty();
    }
}
