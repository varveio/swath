/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <b>The quarter geometry floor, over a whole corpus.</b> {@link GeometryFloorSweepRunTest} answers
 * the ladder's question on an operator-staged roster at all four seeds; this is the same floor's own
 * {@link CorpusSweepRunTest}-shaped round — a corpus sweep like the campaign's N2 rounds, screened at
 * two seeds and escalated to four on divergence, rather than an all-seeds roster round. That is a
 * deliberate difference from {@link GeometryFloorSweepRunTest}: a quarter floor that only ever earns a
 * verdict on fixtures somebody hand-picked for it is a floor that has never been asked whether it
 * matters on a fixture nobody picked.
 *
 * <p>The quarter floor sits outside {@link CorpusSweep#ARMS} — {@code CorpusSweep.ARMS} names the
 * campaign's own four, and the geometry ladder's interior floors are extended in per invocation
 * rather than folded into the default; see {@link CorpusSweep.Race}. This round is the extension:
 * the default four plus {@link SensingVariant#RATE_ANCHORED_FLOOR_QUARTER}, in that table order,
 * over its own custom {@link CorpusSweep.Race}. What makes the screen correct over a fifth arm
 * outside the default four is {@link CorpusSweep.Race#candidateArms()} reading the invocation's own
 * arms rather than a list fixed to {@link CorpusSweep#ARMS} — a fixture divergent only at the
 * quarter floor now earns its confirmation seeds exactly as one divergent at any of the other four
 * would.
 *
 * <pre>{@code ./gradlew :swath-sim:test -PonlyPerf -PsimTestTimeout=60 \
 *     -Dswath.sim.listing.corpus=/path/to/staged/captures \
 *     -Dswath.sim.listing.results=/path/to/quarter-floor.tsv}</pre>
 *
 * <p>{@code -PsimTestTimeout} because five arms over a whole corpus run well past the module's
 * ten-minute default, and the task timeout kills the JVM mid-fixture — the results file then holds a
 * covered prefix of the corpus, honestly, but the round it was for has to be re-run.
 *
 * <p>Opt-in and fixture-free on the same two properties and for the same reasons as every other round
 * in this package: the corpus is a local directory the operator supplies, the repo names none of it,
 * the results file must not already exist, and with the corpus property unset the run <em>skips</em>
 * rather than fails.
 *
 * <p><b>Nothing here asserts a magnitude, and not every printed row is a verdict.</b> The three
 * tables this prints — against the shipped sensor, against the incumbent combination, and against
 * the lift-only end the quarter floor must beat to justify itself — mix escalated four-seed fixtures
 * with two-seed screens, and only the former carry verdicts; a screened row prints as
 * {@code partial} and may be quoted as a lead, never as a finding. Two limits are the screen's own,
 * and a reading of this round has to carry both: escalation only ever measures a candidate against
 * {@link SensingVariant#CURRENT}, so the lift-only table is conditioned on CURRENT-divergence — a
 * fixture where the quarter floor and the lift-only end differ sharply while both track the shipped
 * sensor is never escalated, and that comparison stays two-seed there. A threshold asserted here
 * would be one fitted to the numbers it is judging. What is asserted is what every round of this
 * campaign asserts: that the sweep measured something, and that no leg in it produced a number
 * nobody may use.
 */
@Tag("perf")
class QuarterFloorCorpusSweepRunTest {

    /**
     * The four campaign arms plus the quarter floor, in table order, the control first — the order
     * every reading this round reports. Derived from {@link CorpusSweep#ARMS} rather than restated,
     * so there is one list of the campaign's own four to keep in step. Distinct because that list is
     * the one that may grow: a floor folded into it later would otherwise be raced twice here, at
     * twice the cost, for a table with a duplicated column.
     */
    static final List<SensingVariant> ARMS = Stream.concat(CorpusSweep.ARMS.stream(),
            Stream.of(SensingVariant.RATE_ANCHORED_FLOOR_QUARTER)).distinct().toList();

    @Test
    void theQuarterGeometryFloorScreenedOverTheCorpusAndEscalatedOnDivergence() throws IOException {
        CorpusSweepRunTest.Staged staged = CorpusSweepRunTest.staged("corpus", "sweep");

        CorpusSweep.Result swept = CorpusSweep.sweep(staged.root(), staged.results(),
                new CorpusSweep.Race(ARMS, false));

        CarveAdmissionRaceProtocol.printVerdicts(
                "quarter-floor corpus sweep — paired relative duration against the shipped sensor, "
                        + "same seed",
                CarveAdmissionRaceProtocol.verdicts(swept.rows(), SensingVariant.CURRENT));
        CarveAdmissionRaceProtocol.printVerdicts(
                "quarter-floor corpus sweep — and against the incumbent combination, same seed",
                CarveAdmissionRaceProtocol.verdicts(swept.rows(), SensingVariant.RATE_CURSOR_ANCHORED));
        CarveAdmissionRaceProtocol.printVerdicts(
                "quarter-floor corpus sweep — and against the lift-only end it must beat to justify "
                        + "itself, same seed",
                CarveAdmissionRaceProtocol.verdicts(swept.rows(), SensingVariant.RATE_ANCHORED_LIFT_ONLY));

        assertThat(swept.rows()).as("the sweep measured at least one leg").isNotEmpty();
        assertThat(swept.problems()).as("legs whose numbers are unusable").isEmpty();
    }
}
