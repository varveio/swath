/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.fixture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.varve.swath.engine.StealMath;
import io.varve.swath.sim.fixture.KeyspaceFixtures.SubtreeMass;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The generated shapes' own contract: a generator produces the same keys every time, and the
 * deep-nested one produces the byte geometry it claims.
 *
 * <p>The geometry is asserted rather than described because it is the entire reason that fixture
 * exists. A keyspace that merely <em>looks</em> deep — long names, many slashes — proves nothing about
 * a policy if its bytes still vary inside the window position is measured over. What has to hold is the
 * gap: sibling subtrees differing high up, their contents differing far below, and the measured
 * fraction consequently standing still while a cursor crosses a whole subtree.
 */
class KeyspaceFixturesTest {

    private static final byte[] NO_KEY = new byte[0];

    @Test
    void aGeneratorProducesTheSameKeysEveryTime() {
        List<byte[]> first = KeyspaceFixtures.deepNestedSharedPrefix(4, 4, 2, 40, SubtreeMass.UNIFORM);
        List<byte[]> second = KeyspaceFixtures.deepNestedSharedPrefix(4, 4, 2, 40, SubtreeMass.UNIFORM);

        assertThat(second).usingElementComparator(Arrays::compareUnsigned)
                .containsExactlyElementsOf(first);
        assertThat(KeyspaceFixtures.deepNestedSharedPrefix(4, 4, 2, 40, SubtreeMass.HEAVY_TAILED))
                .usingElementComparator(Arrays::compareUnsigned)
                .containsExactlyElementsOf(
                        KeyspaceFixtures.deepNestedSharedPrefix(4, 4, 2, 40, SubtreeMass.HEAVY_TAILED));
    }

    /**
     * The class's own claim, checked against every generator it has: keys come out in ascending
     * unsigned byte order, which is what lets a fixture store take them without a sort. One generator
     * quietly violating it would be found by a store's precondition, but only in whichever test
     * happened to use that shape.
     */
    @Test
    void everyGeneratedKeyAscendsInUnsignedByteOrder() {
        for (List<byte[]> keys : List.of(
                KeyspaceFixtures.observationArchive(3, 4, 5, 6),
                KeyspaceFixtures.hashFannedCorpus(4, 4, 30),
                KeyspaceFixtures.oneObjectPerDirectory(500),
                KeyspaceFixtures.denseFlatLeaf(500),
                KeyspaceFixtures.deepNestedSharedPrefix(8, 8, 2, 3, SubtreeMass.UNIFORM),
                KeyspaceFixtures.deepNestedSharedPrefix(8, 8, 9, 64, SubtreeMass.HEAVY_TAILED))) {
            for (int i = 1; i < keys.size(); i++) {
                assertThat(Arrays.compareUnsigned(keys.get(i - 1), keys.get(i)))
                        .as("entry %d must be strictly greater than its predecessor", i).isNegative();
            }
        }
    }

    @Test
    void aFileCountTheNameFieldCannotHoldIsRefused() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> KeyspaceFixtures.deepNestedSharedPrefix(1, 1, 1, 1_000_000,
                        SubtreeMass.UNIFORM))
                .withMessageContaining("filesPerLeafDir");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> KeyspaceFixtures.deepNestedSharedPrefix(1, 1, 1, 0,
                        SubtreeMass.HEAVY_TAILED));
    }

    /**
     * The worked example the shape is built around: two adjacent species subtrees whose directories
     * diverge ten bytes in, holding files that differ only from byte 39 on. The twenty-nine bytes
     * between those two numbers are the whole defect — {@code fracIn} measures twelve bytes from the
     * first, and every byte a draining cursor changes is past the second.
     */
    @Test
    void adjacentSubtreesDivergeFarAboveTheDepthTheirContentsVaryAt() {
        List<byte[]> keys = KeyspaceFixtures.deepNestedSharedPrefix(1, 2, 1, 4, SubtreeMass.UNIFORM);
        byte[] firstOfSubtreeA = keys.getFirst();
        byte[] firstOfSubtreeB = keys.get(keys.size() / 2);

        assertThat(new String(firstOfSubtreeA, StandardCharsets.UTF_8))
                .isEqualTo("species/Bacephalura_australis/aBacAus1/"
                        + "assembly_vgp_standard_1.6/evaluation/000000.fastq.gz");
        assertThat(commonPrefixLength(firstOfSubtreeA, firstOfSubtreeB))
                .as("sibling species diverge inside the directory name").isEqualTo(10);
        assertThat(commonPrefixLength(firstOfSubtreeA, keys.get(keys.size() / 2 - 1)))
                .as("two keys in one subtree share everything down to the leaf directory")
                .isGreaterThanOrEqualTo(39);
    }

    /**
     * The consequence, stated in the arithmetic the policies actually run: over a range spanning two
     * sibling subtrees, draining the first one from end to end moves neither the position fraction nor
     * the consumed span — so {@code estRemaining} discards the emitted keys entirely and returns the
     * same number for a worker that has finished a subtree as for one that has not started.
     */
    @Test
    void drainingAWholeSubtreeMovesNeitherTheFractionNorTheConsumedSpan() {
        List<byte[]> keys = KeyspaceFixtures.deepNestedSharedPrefix(1, 2, 1, 100, SubtreeMass.UNIFORM);
        byte[] lo = keys.getFirst();
        byte[] hi = keys.getLast();
        byte[] lastKeyOfFirstSubtree = keys.get(keys.size() / 2 - 1);

        assertThat(StealMath.fracIn(lastKeyOfFirstSubtree, lo, hi))
                .isEqualTo(StealMath.fracIn(lo, lo, hi));
        assertThat(StealMath.spanIn(lo, lastKeyOfFirstSubtree, lo, hi)).isZero();
        assertThat(StealMath.estRemaining(lastKeyOfFirstSubtree, lo, hi, keys.size() / 2))
                .as("half the range emitted, and the estimate is the one it started with")
                .isEqualTo(StealMath.estRemaining(lo, lo, hi, 0L));
    }

    @Test
    void theHeavyTailedMassLawSpreadsFilesAsOneOverRank() {
        List<byte[]> uniform = KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 64, SubtreeMass.UNIFORM);
        List<byte[]> heavy = KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 64,
                SubtreeMass.HEAVY_TAILED);

        assertThat(uniform).hasSize(2 * 4 * 4 * 64);
        // Ranks 1..8 dealt over eight species: sum of 64/r, over the four leaf directories each has.
        int expected = 0;
        for (int rank = 1; rank <= 8; rank++) {
            expected += 4 * (64 / rank);
        }
        assertThat(heavy).hasSize(expected);
        // Rank 1 of eight: 64 files per leaf directory where rank 8 has 8, which over the harmonic
        // series is a little over a third of the whole keyspace.
        assertThat(largestSubtree(heavy)).isBetween(heavy.size() / 3, heavy.size() / 2);
        assertThat(firstSubtree(heavy))
                .as("the deal starts half way along, so the heaviest species is not the first")
                .isLessThan(largestSubtree(heavy));
    }

    /**
     * The depth law's own claim: species for species the mass is the one the heavy-tailed law deals, and
     * all of it sits in a single directory instead of being shared out among an accession's four.
     *
     * <p>The two are compared at file counts in that ratio — a quarter each way — because the comparison
     * they exist for is a controlled one: a run over either keyspace faces the same subtrees holding the
     * same number of keys, and differs only in how deep it has to go before the keys stop being
     * separated by anything a structure probe can find. The counts are multiples of 840 so that the rank
     * law's integer division lands exactly on both sides and "the same number" is literal.
     */
    @Test
    void theDepthLawPutsOneSpeciesWorthOfFilesInOneDirectory() {
        List<byte[]> spread = KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 840,
                SubtreeMass.HEAVY_TAILED);
        List<byte[]> concentrated = KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 4 * 840,
                SubtreeMass.LEAF_CONCENTRATED);

        // Every species holds what it held, to the three token files the other leaf directories keep.
        assertThat(largestSubtree(concentrated)).isEqualTo(largestSubtree(spread) + 3);
        assertThat(concentrated).hasSize(spread.size() + 3 * 8);
        // And it is one directory deep rather than four wide.
        assertThat(largestLeafDirectory(concentrated)).isEqualTo(4 * 840);
        assertThat(largestLeafDirectory(spread)).isEqualTo(840);
    }

    /** Keys under the busiest deepest {@code .../<dir>/} directory. */
    private static int largestLeafDirectory(List<byte[]> keys) {
        int largest = 0;
        int current = 0;
        byte[] currentDir = NO_KEY;
        for (byte[] key : keys) {
            byte[] dir = Arrays.copyOf(key, lastSlash(key));
            if (!Arrays.equals(dir, currentDir)) {
                largest = Math.max(largest, current);
                current = 0;
                currentDir = dir;
            }
            current++;
        }
        return Math.max(largest, current);
    }

    private static int lastSlash(byte[] key) {
        for (int i = key.length - 1; i >= 0; i--) {
            if (key[i] == '/') {
                return i + 1;
            }
        }
        return 0;
    }

    /** Keys under the first {@code species/<name>/} directory. */
    private static int firstSubtree(List<byte[]> keys) {
        byte[] first = Arrays.copyOf(keys.getFirst(), 30);
        return (int) keys.stream().filter(k -> Arrays.equals(Arrays.copyOf(k, 30), first)).count();
    }

    /** Keys under the busiest {@code species/<name>/} directory. */
    private static int largestSubtree(List<byte[]> keys) {
        int largest = 0;
        int current = 0;
        byte[] currentPrefix = NO_KEY;
        for (byte[] key : keys) {
            byte[] prefix = Arrays.copyOf(key, 30);   // "species/" + the 21-byte binomial + "/"
            if (!Arrays.equals(prefix, currentPrefix)) {
                largest = Math.max(largest, current);
                current = 0;
                currentPrefix = prefix;
            }
            current++;
        }
        return Math.max(largest, current);
    }

    private static int commonPrefixLength(byte[] a, byte[] b) {
        int i = 0;
        while (i < Math.min(a.length, b.length) && a[i] == b[i]) {
            i++;
        }
        return i;
    }
}
