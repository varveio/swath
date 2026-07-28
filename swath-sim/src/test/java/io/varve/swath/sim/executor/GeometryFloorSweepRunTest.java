/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>{@link GeometryFloorSweepProtocol}, run.</b> The machinery {@link CarveAdmissionRaceRunTest}
 * drives, over the same kind of operator-staged roster and with the screening tier switched off, with
 * the geometry band's ladder added to its arms: the control, the incumbent combination, the lift-only
 * end, and the three interior floors, every one at all four seeds.
 *
 * <pre>{@code ./gradlew :swath-sim:test -PonlyPerf \
 *     -Dswath.sim.listing.corpus=/path/to/staged/roster \
 *     -Dswath.sim.listing.results=/path/to/floors.tsv}</pre>
 *
 * <p>Six arms rather than three because the two ends are what the floors are read against, and an end
 * re-run here at the same seeds against the same handle is an end the reader does not have to join to
 * another round's table to trust. Opt-in and fixture-free on the same two properties and for the same
 * reasons: the roster is a local directory the operator supplies, the repo names none of it, the
 * results file must not already exist, and with the corpus property unset the run <em>skips</em>
 * rather than fails. Which fixtures carry which of the protocol's five roles is the operator's staging
 * decision, stated in the round's record — nothing here knows a fixture's name, so nothing here can
 * quietly weight one.
 *
 * <p><b>Nothing asserts a magnitude.</b> The protocol's F1–F3 are read off the two verdict tables this
 * prints, and F4 is a separate reading taken on the synthetic bench and guards for the winning floor
 * alone. A threshold asserted here would be one fitted to the numbers it is judging. What is asserted
 * is what every round of this campaign asserts: that the race measured something, and that no leg
 * produced a number nobody may use.
 */
@Tag("perf")
class GeometryFloorSweepRunTest {

    /**
     * The ladder and the two ends it is read between, in floor order, the control first — the table
     * order of every reading this round reports.
     */
    static final List<SensingVariant> ARMS = List.of(SensingVariant.CURRENT,
            SensingVariant.RATE_CURSOR_ANCHORED, SensingVariant.RATE_ANCHORED_FLOOR_EIGHTH,
            SensingVariant.RATE_ANCHORED_FLOOR_QUARTER, SensingVariant.RATE_ANCHORED_FLOOR_HALF,
            SensingVariant.RATE_ANCHORED_LIFT_ONLY);

    @Test
    void everyInteriorGeometryFloorAgainstBothEndsOverTheRoster() throws IOException {
        CorpusSweepRunTest.Staged staged = CorpusSweepRunTest.staged("roster", "sweep");

        CorpusSweep.Result swept = CorpusSweep.sweep(staged.root(), staged.results(),
                new CorpusSweep.Race(ARMS, true));

        CarveAdmissionRaceProtocol.printVerdicts(
                "geometry-floor sweep — paired relative duration against the shipped sensor, same seed",
                CarveAdmissionRaceProtocol.verdicts(swept.rows(), SensingVariant.CURRENT));
        CarveAdmissionRaceProtocol.printVerdicts(
                "geometry-floor sweep — and against the incumbent combination, same seed",
                CarveAdmissionRaceProtocol.verdicts(swept.rows(), SensingVariant.RATE_CURSOR_ANCHORED));

        assertThat(swept.rows()).as("the sweep measured at least one leg").isNotEmpty();
        assertThat(swept.problems()).as("legs whose numbers are unusable").isEmpty();
    }
}
