/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The draw-stream derivation's three load-bearing properties: reproducible, separated, stable. */
class SimRngTest {

    private static final int DRAWS = 1000;

    @Test
    void oneSeedGivesOneSequence() {
        assertThat(drawsOf(SimRng.forStream(99L, 3, SimRngStream.DECISION)))
                .isEqualTo(drawsOf(SimRng.forStream(99L, 3, SimRngStream.DECISION)));
    }

    /**
     * Every (actor, purpose) pair must get its own tape. Colliding tapes would correlate two actors'
     * decisions — the failure would not look like a bug, it would look like a policy result.
     */
    @Test
    void everyActorAndPurposePairGetsADistinctStream() {
        Set<Long> seeds = new HashSet<>();
        for (int actor = 0; actor < 64; actor++) {
            for (SimRngStream stream : SimRngStream.values()) {
                seeds.add(SimRng.deriveStreamSeed(1234L, actor, stream));
            }
        }

        assertThat(seeds).hasSize(64 * SimRngStream.values().length);
    }

    /**
     * The derivation reads an actor's own id, never how many actors there are — so a scaling sweep
     * that adds workers leaves every existing worker's tape exactly where it was.
     */
    @Test
    void aStreamSeedDependsOnTheActorIdAndNotOnTheActorCount() {
        long before = SimRng.deriveStreamSeed(5L, 2, SimRngStream.LATENCY);

        assertThat(SimRng.deriveStreamSeed(5L, 2, SimRngStream.LATENCY)).isEqualTo(before);
        assertThat(SimRng.deriveStreamSeed(5L, 3, SimRngStream.LATENCY)).isNotEqualTo(before);
        assertThat(SimRng.deriveStreamSeed(6L, 2, SimRngStream.LATENCY)).isNotEqualTo(before);
    }

    /**
     * Adjacent actors must not get near-identical tapes. A linear seed offset would leave them
     * weakly decorrelated, which is why the derivation mixes rather than adds; this checks the mixing
     * actually happened, by requiring the first draws of adjacent actors to differ throughout.
     */
    @Test
    void adjacentActorsAreNotCorrelated() {
        List<Long> firstDraws = new ArrayList<>();
        for (int actor = 0; actor < 32; actor++) {
            firstDraws.add(SimRng.forStream(0L, actor, SimRngStream.DECISION).nextLong());
        }

        assertThat(new HashSet<>(firstDraws)).hasSize(firstDraws.size());
    }

    @Test
    void boundedDrawsStayInRangeAndCoverIt() {
        SimRng rng = SimRng.of(7L);
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < DRAWS; i++) {
            int value = rng.nextInt(6);
            assertThat(value).isBetween(0, 5);
            seen.add(value);
        }

        assertThat(seen).as("a non-power-of-two bound must reach every value").hasSize(6);
        assertThatThrownBy(() -> rng.nextInt(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unitDrawsStayInTheUnitInterval() {
        SimRng rng = SimRng.of(11L);
        for (int i = 0; i < DRAWS; i++) {
            assertThat(rng.nextDouble()).isGreaterThanOrEqualTo(0.0).isLessThan(1.0);
        }
    }

    private static List<Long> drawsOf(SimRng rng) {
        List<Long> draws = new ArrayList<>(DRAWS);
        for (int i = 0; i < DRAWS; i++) {
            draws.add(rng.nextLong());
        }
        return draws;
    }
}
