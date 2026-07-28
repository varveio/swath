/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.varve.swath.sim.kernel.SimEventLog;
import io.varve.swath.sim.kernel.SimRunResult;
import io.varve.swath.sim.kernel.SimStopReason;
import io.varve.swath.sim.store.SimStoreBackend;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * <b>The scoring the carve-admission round's verdicts are read off</b>, over rows written here rather
 * than run — because every way this function can be wrong produces a plausible verdict table rather
 * than an error, and the round's own record is that table. A pairing that quietly compared a candidate
 * at one seed against the control at another, a fixture promoted on two seeds of four, a set of
 * per-seed readings that disagree in sign averaged into a win: all three read as ordinary rows.
 *
 * <p>Nothing here runs a fleet. The only field of a leg the scoring reads is its duration, so the rows
 * carry a duration and defaults for everything else, and the numbers in them are chosen to land on the
 * boundaries of {@link CarveAdmissionRaceProtocol#NEUTRAL_BAND} rather than to resemble a run.
 */
class CarveAdmissionRaceProtocolTest {

    private static final String FIXTURE = "fx";

    private static final long[] SEEDS = SensingRaceProtocol.SEEDS;

    /** A control leg every paired reading below is taken against — one virtual second, at every seed. */
    private static final double CONTROL_SECONDS = 1.0;

    @Test
    void everyCandidateArmIsReadAgainstTheControlsOwnLegAtTheSameSeedAndTheControlIsNotAVerdict() {
        List<CorpusSweep.Row> rows = new ArrayList<>();
        // A control that doubles at every seed, so a reading taken against the wrong seed's control
        // cannot accidentally agree with one taken against the right seed's. In the protocol's own
        // seed order -- 20260727, 1, 424242, 987654321 -- these are deltas of +0.5, +0.1, -0.1, 0.
        double[] control = {1.0, 2.0, 4.0, 8.0};
        double[] candidate = {0.5, 1.8, 4.4, 8.0};
        for (int i = 0; i < SEEDS.length; i++) {
            rows.add(row(SensingVariant.CURRENT, SEEDS[i], control[i]));
            rows.add(row(SensingVariant.RATE_ANCHORED_LIFT_ONLY, SEEDS[i], candidate[i]));
        }

        List<CarveAdmissionRaceProtocol.Verdict> verdicts =
                CarveAdmissionRaceProtocol.verdicts(rows, SensingVariant.CURRENT);

        assertThat(verdicts).as("the control is the yardstick, not one of the readings").singleElement()
                .satisfies(verdict -> {
                    assertThat(verdict.fixture()).isEqualTo(FIXTURE);
                    assertThat(verdict.arm())
                            .isEqualTo(SensingRaceProtocol.label(SensingVariant.RATE_ANCHORED_LIFT_ONLY));
                    // Ascending seed, which is the order the printed column is in and NOT the order the
                    // protocol lists its seeds in -- pinned because a reader lining the four values up
                    // against that list would otherwise attribute every one of them to the wrong seed.
                    assertThat(verdict.perSeed()).as("paired, same seed, relative to that seed's control")
                            .containsExactly(0.1, -0.1, 0.5, 0.0);
                    assertThat(verdict.delta()).as("the mean over the four, never a ratio of means")
                            .isCloseTo(0.125, within(1e-12));
                });
    }

    /**
     * The pairing is the yardstick, so a leg the control never ran at is not a reading — and the
     * fixture it belongs to is then short of the protocol's four seeds and carries no verdict at all.
     * The alternative, scoring it against whatever control legs did run, is the cherry-pick the round
     * forbids wearing a mean's clothing.
     */
    @Test
    void aLegWithNoControlAtItsSeedIsNotAReadingAndLeavesTheFixturePartial() {
        List<CorpusSweep.Row> rows = new ArrayList<>();
        for (long seed : SEEDS) {
            rows.add(row(SensingVariant.RATE_ANCHORED_LIFT_ONLY, seed, 0.5));
        }
        rows.add(row(SensingVariant.CURRENT, SEEDS[0], CONTROL_SECONDS));

        CarveAdmissionRaceProtocol.Verdict verdict =
                CarveAdmissionRaceProtocol.verdicts(rows, SensingVariant.CURRENT).getFirst();

        assertThat(verdict.perSeed()).as("only the seed the control also ran at").containsExactly(0.5);
        assertThat(verdict.partial()).isTrue();
        assertThat(verdict.label())
                .as("a subset is not a result — and a 50%% reading on one seed of four least of all")
                .isEqualTo("partial");
    }

    @Test
    void aFixtureReadAtEveryOneOfTheProtocolsSeedsCarriesAVerdictAndNothingShortOfThatDoes() {
        assertThat(verdictOf(0.2, 0.2, 0.2, 0.2).label()).isEqualTo("win");
        assertThat(verdictOf(-0.2, -0.2, -0.2, -0.2).label()).isEqualTo("loss");
        assertThat(verdictOf(0.02, -0.03, 0.01, -0.01).label()).isEqualTo("neutral");
        for (int seeds = 1; seeds < SEEDS.length; seeds++) {
            double[] deltas = new double[seeds];
            Arrays.fill(deltas, 0.2);
            assertThat(verdictOf(deltas).label())
                    .as("%d seed(s) of %d is not a verdict", seeds, SEEDS.length).isEqualTo("partial");
        }
    }

    /**
     * The rule as the protocol pre-registered it — per-seed deltas that <b>do not share a sign</b> —
     * against the reading it exists for. Three seeds far ahead and one behind is the exact shape a
     * four-seed rule is written to catch, and the first cut of the scoring called it a win: it required
     * a seed past the band in <em>each</em> direction, so a fixture whose fourth seed was merely
     * negative averaged into a promotion.
     */
    @Test
    void perSeedDeltasThatDisagreeInSignAreASplitWhateverTheirMeanSays() {
        CarveAdmissionRaceProtocol.Verdict verdict = verdictOf(0.30, 0.30, 0.30, -0.02);

        assertThat(verdict.delta()).as("the mean alone would read as a decisive win").isGreaterThan(0.2);
        assertThat(verdict.split()).isTrue();
        assertThat(verdict.label()).isEqualTo("split");
        assertThat(verdictOf(-0.30, -0.30, -0.30, 0.02).label())
                .as("and the same shape the other way up is not a loss either").isEqualTo("split");
    }

    /**
     * The one qualification on that rule, and the reason it is not simply "any two signs": the neutral
     * band is this protocol's own statement of which differences are not differences, so four readings
     * that all sit inside it are a fixture that did not move. Reporting the signs of readings the
     * protocol has already declared to be noise would file every quiet fixture in the corpus as
     * unstable.
     */
    @Test
    void fourReadingsAllInsideTheNeutralBandAreNeutralRatherThanASplit() {
        double band = CarveAdmissionRaceProtocol.NEUTRAL_BAND;
        CarveAdmissionRaceProtocol.Verdict verdict = verdictOf(band, -band, band / 2, -band / 2);

        assertThat(verdict.split()).isFalse();
        assertThat(verdict.label()).isEqualTo("neutral");
        assertThat(verdictOf(band + 0.01, -band / 2, band / 2, band / 2).split())
                .as("but one seed leaving the band, against seeds that lean the other way, is a split")
                .isTrue();
    }

    // ---- rows ---------------------------------------------------------------------------

    /** One arm's four legs against a one-second control, at the deltas given, in seed order. */
    private static CarveAdmissionRaceProtocol.Verdict verdictOf(double... deltas) {
        List<CorpusSweep.Row> rows = new ArrayList<>();
        for (int i = 0; i < deltas.length; i++) {
            rows.add(row(SensingVariant.CURRENT, SEEDS[i], CONTROL_SECONDS));
            rows.add(row(SensingVariant.RATE_ANCHORED_LIFT_ONLY, SEEDS[i],
                    CONTROL_SECONDS * (1.0 - deltas[i])));
        }
        return CarveAdmissionRaceProtocol.verdicts(rows, SensingVariant.CURRENT).getFirst();
    }

    /**
     * A row of the sweep carrying one duration. Every other field is a default: the scoring reads a
     * leg's fixture, arm, seed and virtual duration and nothing else, and filling the rest with
     * plausible run numbers would suggest it does.
     */
    private static CorpusSweep.Row row(SensingVariant arm, long seed, double virtualSeconds) {
        long nanos = (long) (virtualSeconds * 1e9);
        PolicyRunTimeline timeline = new PolicyRunTimeline(0L, 0L, nanos, nanos, 0L, 0L, 0L, 0L, 0L, 0);
        PolicyRunResult result = new PolicyRunResult(
                new SimRunResult(nanos, 0L, SimStopReason.QUIESCED, SimEventLog.disabled(), new TreeMap<>()),
                PolicyRunFixtures.scenario(SensingRaceProtocol.WORKERS,
                                PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE,
                                PolicyRunFixtures.MEASURED_TAIL_LATENCY, PolicyRunFixtures.measuredCost())
                        .withSeed(seed),
                "written by hand", new TreeMap<>(), 0L, 0L, 0L, 0, false, timeline, arm);
        return new CorpusSweep.Row(FIXTURE, 0L, SimStoreBackend.STREAMING,
                new CorpusSweep.Fleet(SensingRaceProtocol.WORKERS, CorpusSweep.FleetSource.CAPTURE),
                Duration.ZERO,
                new SensingRaceProtocol.Leg(SensingRaceProtocol.label(arm), FIXTURE, seed,
                        PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE, result),
                Duration.ZERO, 0.0, true);
    }
}
