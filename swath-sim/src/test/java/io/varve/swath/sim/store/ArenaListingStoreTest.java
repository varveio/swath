/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ArenaListingStoreTest {

    private static final long GENEROUS_BUDGET = 1L << 20;

    /** Enough for the multi-batch set below (~131k short keys), whose footprint clears 1 MiB. */
    private static final long MULTI_BATCH_BUDGET = 1L << 26;

    @Test
    void loadStreamsEveryKeyOutOfTheSourceInOrder() {
        ArenaListingStore arena = load(FakeListingStore.ofKeys("c", "a", "b"));

        assertThat(arena.keyCount()).isEqualTo(3);
        assertThat(keys(arena.rows(null, true, null, 10, Projection.KEYS_ONLY)))
                .containsExactly("a", "b", "c");
    }

    @Test
    void loadCrossesManySourceBatchesWithoutLosingOrRepeatingAKey() {
        // Two batch boundaries' worth of keys, so the cursor hand-off (last key of a batch, then
        // exclusive-from) is exercised rather than a single one-shot read.
        String[] many = new String[2 * ArenaListingStore.LOAD_BATCH_ROWS + 7];
        for (int i = 0; i < many.length; i++) {
            many[i] = String.format("key-%08d", i);
        }
        ArenaListingStore arena =
                ArenaListingStore.loadWithin(FakeListingStore.ofKeys(many), MULTI_BATCH_BUDGET).orElseThrow();

        assertThat(arena.keyCount()).isEqualTo(many.length);
        assertThat(keys(arena.rows(null, true, null, 3, Projection.KEYS_ONLY)))
                .containsExactly(many[0], many[1], many[2]);
        ByteKey nearEnd = key(many[many.length - 2]);
        assertThat(keys(arena.rows(nearEnd, false, null, 10, Projection.KEYS_ONLY)))
                .containsExactly(many[many.length - 1]);
    }

    @Test
    void loadDeclinesWhenTheFixtureWouldExceedTheBudget() {
        FakeListingStore source = FakeListingStore.ofKeys("aaaa", "bbbb", "cccc");

        assertThat(ArenaListingStore.loadWithin(source, KeyArena.encodedBytes(8, 2))).isEmpty();
    }

    @Test
    void loadFailsLoudlyWhenTheSourceIsNotStrictlyAscending() {
        // A silently-accepted inversion makes every later binary search wrong, so the store would
        // report confident wrong answers instead of failing.
        ScriptedSource source = new ScriptedSource(List.of(rows("b", "a")));

        assertThatThrownBy(() -> ArenaListingStore.loadWithin(source, GENEROUS_BUDGET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly ascending");
    }

    @Test
    void loadFailsLoudlyWhenABatchRepeatsThePreviousBatchesLastKey() {
        // The exclusive-from resume is what makes the load correct: batch n+1 is fetched from the
        // last key of batch n, EXCLUSIVE. A source that re-emits that key breaks the assumption,
        // and the duplicate must surface here rather than inside the arena.
        ScriptedSource source = new ScriptedSource(List.of(rows("a", "b"), rows("b", "c")));

        assertThatThrownBy(() -> ArenaListingStore.loadWithin(source, GENEROUS_BUDGET))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
    }

    @Test
    void anEmptyFixtureLoadsToAnEmptyArena() {
        ArenaListingStore arena = load(FakeListingStore.ofKeys(new String[0]));

        assertThat(arena.keyCount()).isZero();
        assertThat(arena.rows(null, true, null, 10, Projection.KEYS_ONLY)).isEmpty();
    }

    @Test
    void rangeBoundsAreHalfOpenAndHonourFromInclusive() {
        ArenaListingStore arena = load(FakeListingStore.ofKeys("a", "b", "c", "d"));

        assertThat(keys(arena.rows(key("b"), true, key("d"), 10, Projection.KEYS_ONLY)))
                .containsExactly("b", "c");
        assertThat(keys(arena.rows(key("b"), false, key("d"), 10, Projection.KEYS_ONLY)))
                .containsExactly("c");
        assertThat(keys(arena.rows(null, true, key("a"), 10, Projection.KEYS_ONLY))).isEmpty();
        assertThat(keys(arena.rows(key("d"), false, null, 10, Projection.KEYS_ONLY))).isEmpty();
    }

    @Test
    void limitCapsTheRowsReturnedAndANonPositiveLimitReturnsNothing() {
        ArenaListingStore arena = load(FakeListingStore.ofKeys("a", "b", "c"));

        assertThat(keys(arena.rows(null, true, null, 2, Projection.KEYS_ONLY))).containsExactly("a", "b");
        assertThat(arena.rows(null, true, null, 0, Projection.KEYS_ONLY)).isEmpty();
    }

    @Test
    void everyMetadataColumnIsStubbedUnderBothProjections() {
        // The sim-mode projection: metadata is not loaded at all, so WITH_OWNER cannot and does not
        // resurrect it. This is the documented difference from Projection.KEYS_ONLY, which only
        // drops the owner fields while a store still materialises size/etag/dates.
        ArenaListingStore arena = load(FakeListingStore.ofKeys("a"));

        for (Projection projection : List.of(Projection.KEYS_ONLY, Projection.WITH_OWNER)) {
            ListedObject row = arena.rows(null, true, null, 1, projection).getFirst();
            assertThat(row.key()).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
            assertThat(row.size()).isEqualTo(SimModeRows.STUB_SIZE);
            assertThat(row.lastModifiedEpochMicros())
                    .isEqualTo(SimModeRows.STUB_LAST_MODIFIED_EPOCH_MICROS);
            assertThat(row.etag()).isNull();
            assertThat(row.storageClass()).isNull();
            assertThat(row.ownerId()).isNull();
            assertThat(row.ownerDisplayName()).isNull();
            assertThat(row.checksumAlgorithm()).isNull();
            assertThat(row.checksumType()).isNull();
        }
    }

    @Test
    void delimitedRollupIsDeclinedSoThePagerKeepsEveryDelimiterRule() {
        ArenaListingStore arena = load(FakeListingStore.ofKeys("a/1", "a/2", "b"));

        assertThat(arena.delimitedRollup(null, true, null, new byte[0],
                "/".getBytes(StandardCharsets.UTF_8), 10, Projection.KEYS_ONLY)).isNull();
    }

    /**
     * A deliberately contract-breaking source: it replays a fixed script of batches, ignoring the
     * requested bounds, so the load path can be handed key sequences no honest store would produce.
     */
    private static final class ScriptedSource implements ListingStore {

        private final Iterator<List<ListedObject>> batches;

        private ScriptedSource(List<List<ListedObject>> batches) {
            this.batches = batches.iterator();
        }

        @Override
        public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                       Projection projection) {
            return batches.hasNext() ? batches.next() : List.of();
        }

        @Override
        public void close() {
        }
    }

    private static List<ListedObject> rows(String... keys) {
        return Arrays.stream(keys)
                .map(key -> new ListedObject(key.getBytes(StandardCharsets.UTF_8), 0, 0,
                        null, null, null, null, null, null))
                .toList();
    }

    private static ArenaListingStore load(ListingStore source) {
        return ArenaListingStore.loadWithin(source, GENEROUS_BUDGET).orElseThrow();
    }

    private static ByteKey key(String value) {
        return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> keys(List<ListedObject> rows) {
        return rows.stream().map(row -> new String(row.key(), StandardCharsets.UTF_8)).toList();
    }
}
