/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The position-sensor race. <b>Its protocol — bench, seeds, criteria, guards, and the rules about
 * cherry-picking and regime disclosure — is written down in {@link SensingRaceProtocol}, and was
 * committed before the first variant was implemented.</b> Read that file first; this one only runs
 * what it declares.
 *
 * <p>This class starts life holding a single leg: the <b>control</b>, which is the algorithm as it
 * ships, measured on the bench and on both regression guards at all four protocol seeds and at both
 * page regimes. That leg is the race's baseline and also its precondition — if the bench does not
 * hold still under re-seeding before any variant exists, nothing measured against it afterwards means
 * anything. Only the constancy the protocol relies on is asserted; every magnitude is printed.
 *
 * <p>Opt-in ({@code @Tag("perf")}) for memory and time, like every at-scale fixture here: the bench
 * is a million-key keyspace and the race runs it once per seed per variant.
 */
@Tag("perf")
class SensingRaceTest {

    @Test
    void theBenchAndItsGuardsHoldStillBeforeAnyVariantExists() {
        List<SensingRaceProtocol.Leg> legs = new ArrayList<>();
        for (long seed : SensingRaceProtocol.SEEDS) {
            legs.add(SensingRaceProtocol.runLeg("current", "leaf-conc", SensingRaceProtocol.bench(), seed,
                    SensingRaceProtocol.BENCH_PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY));
        }
        for (long seed : SensingRaceProtocol.SEEDS) {
            legs.add(SensingRaceProtocol.runLeg("current", "hash-fanned",
                    SensingRaceProtocol.hashFannedGuard(), seed, SensingRaceProtocol.BENCH_PAGE_SIZE,
                    PolicyRunFixtures.REMOTE_LATENCY));
        }
        for (long seed : SensingRaceProtocol.SEEDS) {
            legs.add(SensingRaceProtocol.runLeg("current", "uniform", SensingRaceProtocol.uniformGuard(),
                    seed, SensingRaceProtocol.BENCH_PAGE_SIZE, PolicyRunFixtures.REMOTE_LATENCY));
        }
        for (long seed : SensingRaceProtocol.SEEDS) {
            legs.add(SensingRaceProtocol.runLeg("current", "leaf-conc", SensingRaceProtocol.bench(), seed,
                    PolicyRunFixtures.MEASURED_TAIL_PAGE_SIZE, PolicyRunFixtures.MEASURED_TAIL_LATENCY));
        }
        SensingRaceProtocol.printTable("== control: the algorithm as it ships", legs);

        List<SensingRaceProtocol.Leg> bench = legs.subList(0, 4);
        List<SensingRaceProtocol.Leg> hashFanned = legs.subList(4, 8);
        List<SensingRaceProtocol.Leg> uniform = legs.subList(8, 12);

        // The bench is a constant, which is the whole reason it is the bench: a cure has to move a
        // number that re-seeding does not.
        for (SensingRaceProtocol.Leg leg : bench) {
            assertThat(leg.serialFraction()).as("bench serial fraction at seed %d", leg.seed())
                    .isBetween(0.25, 0.40);
            assertThat(leg.tailFraction()).as("bench tail fraction at seed %d", leg.seed())
                    .isGreaterThan(0.25);
        }
        // And the two guards are healthy at every seed, so a variant that damages them is visible.
        for (SensingRaceProtocol.Leg leg : hashFanned) {
            assertThat(leg.tailFraction()).as("hash-fanned tail at seed %d", leg.seed()).isLessThan(0.05);
        }
        for (SensingRaceProtocol.Leg leg : uniform) {
            assertThat(leg.serialFraction()).as("uniform serial at seed %d", leg.seed()).isLessThan(0.05);
        }
    }
}
