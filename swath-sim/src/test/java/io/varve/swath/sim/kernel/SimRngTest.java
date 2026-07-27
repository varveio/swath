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
     *
     * <p>Distinctness alone is necessary but nowhere near sufficient — {@code seed = base + actor}
     * also gives 32 distinct first draws and is exactly the derivation this property exists to rule
     * out. So distinctness is checked here and the mixing itself is pinned by
     * {@link #theDerivationIsPinnedToItsMixedForm()}.
     */
    @Test
    void adjacentActorsAreNotCorrelated() {
        List<Long> firstDraws = new ArrayList<>();
        for (int actor = 0; actor < 32; actor++) {
            firstDraws.add(SimRng.forStream(0L, actor, SimRngStream.DECISION).nextLong());
        }

        assertThat(new HashSet<>(firstDraws)).hasSize(firstDraws.size());
    }

    /**
     * Golden vectors for the derivation and the first draw off each derived tape.
     *
     * <p>These are the check that has teeth. A distinctness assertion passes for any injective
     * derivation, including the additive one the mixing exists to avoid; pinning the actual values
     * fails the moment {@code deriveStreamSeed} stops being {@code mix64(mix64(base + actor*gamma) +
     * ordinal*gamma)} — an offset, a dropped mix round, a reordered {@link SimRngStream} constant, or
     * a swapped multiplier. That is also why the vectors are written out rather than recomputed from
     * the constants: a test that recomputed them would agree with whatever the implementation became.
     *
     * <p>These numbers are a compatibility surface, not an implementation detail. Every recorded run
     * replays against them, so a failure here means old traces no longer reproduce — the fix is
     * ordinarily to restore the derivation, and re-pinning is a deliberate break of every trace on
     * disk.
     */
    @Test
    void theDerivationIsPinnedToItsMixedForm() {
        assertPinned(0L, 0, SimRngStream.DECISION, 0L, -2152535657050944081L);
        assertPinned(0L, 1, SimRngStream.DECISION, 5197578548964807871L, 6235967106033911276L);
        assertPinned(0L, 2, SimRngStream.DECISION, -3642288131749336026L, -8675236160134566309L);
        assertPinned(42L, 7, SimRngStream.LATENCY, -4026654402003481814L, 2620904494501322228L);
        assertPinned(42L, 7, SimRngStream.CLIENT_COST, 3833205896053839730L, 3613847663784274578L);
    }

    private static void assertPinned(long baseSeed, int actorId, SimRngStream stream, long expectedSeed,
            long expectedFirstDraw) {
        String where = "base=" + baseSeed + " actor=" + actorId + " stream=" + stream;
        assertThat(SimRng.deriveStreamSeed(baseSeed, actorId, stream)).as("derived seed, " + where)
                .isEqualTo(expectedSeed);
        assertThat(SimRng.forStream(baseSeed, actorId, stream).nextLong()).as("first draw, " + where)
                .isEqualTo(expectedFirstDraw);
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
