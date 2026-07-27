/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The arena tier answers the same key sequence as the Parquet-backed tier, over the same fixture,
 * the same pager, and the same request sequence — including delimiter rollups, the edge-case keys
 * of {@code docs/internals/algorithms.md} §11, and the truncation boundaries around a fixture's
 * exact key count.
 *
 * <p><b>What is compared, and what deliberately is not.</b> The transcript is keys, common
 * prefixes, page boundaries, {@code IsTruncated} and the continuation token. Object <b>metadata is
 * not compared, because the arena does not load it</b>: its sim-mode projection stubs size,
 * last-modified, etag, storage class, owner and checksum on every row (see
 * {@link ArenaListingStore}). That is by design — a simulator decides splits, steals and
 * pagination from keys alone, and loading metadata for every key of every fixture would defeat the
 * tier. {@link #metadataIsStubbedOnTheArenaAndFullOnParquet} pins the difference so it stays a
 * documented contract rather than an undetected regression; full byte-for-byte comparison
 * including metadata is the replay module's own sorted-vs-DuckDB differential suite.
 *
 * <p>Both backends are driven through the identical {@link ListObjectsV2Pager}, so any
 * disagreement is attributable to a store by construction. Backends are selected explicitly
 * ({@link SimStoreBackend#ARENA} / {@link SimStoreBackend#PARQUET}) rather than through
 * {@link SimStoreBackend#AUTO}, which would resolve to one tier and compare it against itself.
 */
class ArenaDifferentialTest {

    private static final String BUCKET = "bucket";

    private static final SimStoreConfig GENEROUS = new SimStoreConfig(1L << 20);

    /** Beyond the pager's default seek-scan threshold (32), so a rollup takes the seek path. */
    private static final int WIDE_DIRECTORY_CHILDREN = 150;

    private static final String OBJECT_MARK = "O:";
    private static final String COMMON_PREFIX_MARK = "P:";

    @Test
    void arenaAndParquetAgreeOnKeysPaginationAndTruncation(@TempDir Path dir) throws IOException {
        List<byte[]> keys = edgeCaseKeys();
        Path fixture = writeCapture(dir, keys);

        List<Scenario> scenarios = scenarios(keys.size());
        List<List<String>> arena = transcripts(fixture, SimStoreBackend.ARENA, scenarios);
        List<List<String>> parquet = transcripts(fixture, SimStoreBackend.PARQUET, scenarios);

        for (int i = 0; i < scenarios.size(); i++) {
            assertThat(arena.get(i)).as("%s", scenarios.get(i)).isEqualTo(parquet.get(i));
        }
    }

    @Test
    void theFlatWalkEnumeratesEveryFixtureKeyExactlyOnce(@TempDir Path dir) throws IOException {
        // Without this, the differential above would pass just as happily on two stores that both
        // returned nothing. Small max-keys, so this is many continuation hops, not one page.
        List<byte[]> keys = edgeCaseKeys();
        Path fixture = writeCapture(dir, keys);

        SimStoreFactory.Result result = SimStoreFactory.open(fixture, SimStoreBackend.ARENA, GENEROUS);
        try (var store = result.store()) {
            ListObjectsV2Pager pager = new ListObjectsV2Pager(store, result.metrics());
            List<String> walked = new ArrayList<>();
            for (String page : walk(pager, new Scenario(null, null, null, 3, false))) {
                walked.addAll(objectKeysOf(page));
            }
            assertThat(walked).containsExactlyElementsOf(keys.stream().map(ByteKeys::percentEncode).toList());
        }
    }

    @Test
    void metadataIsStubbedOnTheArenaAndFullOnParquet(@TempDir Path dir) throws IOException {
        Path fixture = writeCapture(dir, List.of(utf8("solo")));

        SimStoreFactory.Result arena = SimStoreFactory.open(fixture, SimStoreBackend.ARENA, GENEROUS);
        SimStoreFactory.Result parquet = SimStoreFactory.open(fixture, SimStoreBackend.PARQUET, GENEROUS);
        try (var arenaStore = arena.store(); var parquetStore = parquet.store()) {
            ListedObject fromArena = arenaStore.rows(null, true, null, 1, Projection.WITH_OWNER).getFirst();
            ListedObject fromParquet = parquetStore.rows(null, true, null, 1, Projection.WITH_OWNER).getFirst();

            assertThat(fromArena.key()).isEqualTo(fromParquet.key());
            assertThat(fromArena.size()).isEqualTo(ArenaListingStore.STUB_SIZE);
            assertThat(fromArena.etag()).isNull();
            assertThat(fromArena.ownerId()).isNull();
            assertThat(fromParquet.etag()).isEqualTo("etag-solo");
            assertThat(fromParquet.ownerId()).isEqualTo("owner-id");
        }
    }

    // --- the fixture ----------------------------------------------------------

    /**
     * The store-visible half of the algorithms.md §11 checklist, plus the key shapes the pager's
     * delimiter walk switches on. The set is byte-sorted and de-duplicated on the way out.
     */
    private static List<byte[]> edgeCaseKeys() {
        Comparator<byte[]> unsigned = Arrays::compareUnsigned;
        TreeSet<byte[]> keys = new TreeSet<>(unsigned);
        // §11.11 — directory-marker keys are ordinary objects, including a bare delimiter and an
        // empty path segment; each is also a key that EQUALS a common prefix the rollup emits.
        addUtf8(keys, "/", "a", "a/", "a//b", "a/b", "a/b/", "a/b/c", "ab");
        // §11.3 / §11.13 — prefix-of and NUL cases around a boundary: "a" < "a\0" < "a/" < "ab".
        keys.add(new byte[]{'a', 0x00});
        // §11.1 / §11.2 — arbitrary key bytes in UNSIGNED order: '~' (0x7E) sorts BEFORE every
        // 0x80+ byte, which UTF-16 order would get wrong.
        addUtf8(keys, "~tilde", "é-accent", "日本");
        keys.add(new byte[]{'k', 0x01});
        keys.add(new byte[]{'k', (byte) 0x80});
        keys.add(new byte[]{'k', (byte) 0xFF});
        keys.add(new byte[]{'k', (byte) 0xFF, (byte) 0xFF});
        // §11.13 — 0xFF runs, including the all-0xFF key whose successor is end-of-keyspace.
        keys.add(new byte[]{(byte) 0xFF});
        keys.add(new byte[]{(byte) 0xFF, (byte) 0xFF});
        // §11.13 — very long keys at and just under the 1024-byte ceiling, one a prefix of the other.
        keys.add(longKey(KeyArena.MAX_KEY_BYTES - 1));
        keys.add(longKey(KeyArena.MAX_KEY_BYTES));
        // A rollup run LONGER than the pager's seek-scan threshold: the seek-to-successor path.
        for (int i = 0; i < WIDE_DIRECTORY_CHILDREN; i++) {
            keys.add(utf8(String.format("wide/%03d", i)));
        }
        // successor("wide/") == "wide0" is a REAL key that must follow the rolled-up prefix.
        addUtf8(keys, "wide0", "wide00");
        // A rollup run SHORTER than the threshold: the scan-in-batch path, no seek.
        addUtf8(keys, "narrow/0", "narrow/1");
        return new ArrayList<>(keys);
    }

    private static List<Scenario> scenarios(int keyCount) {
        List<Scenario> scenarios = new ArrayList<>();
        // Flat listing at page sizes that force many continuation hops, and one that does not.
        addProjections(scenarios, null, null, null, 1, 2, 3, 7, 1000);
        // max-keys=0 ⇒ empty and NOT truncated; then the exact-fit and off-by-one truncation
        // boundaries, where "is there one more row" must agree across backends.
        addProjections(scenarios, null, null, null, 0, keyCount - 1, keyCount, keyCount + 1);
        // Prefix windows: a wide one, a one-character one whose successor keys are adjacent, a
        // prefix that is itself a key, and one that matches nothing.
        addProjections(scenarios, utf8("wide/"), null, null, 1, 7, 1000);
        addProjections(scenarios, utf8("a"), null, null, 1, 3, 1000);
        addProjections(scenarios, utf8("a/b"), null, null, 2);
        addProjections(scenarios, utf8("zzz-absent"), null, null, 5);
        // Delimiter rollups: whole-keyspace, inside a prefix, and with a non-'/' delimiter.
        addProjections(scenarios, null, utf8("/"), null, 1, 2, 3, 1000);
        addProjections(scenarios, utf8("a/"), utf8("/"), null, 1, 2, 1000);
        addProjections(scenarios, utf8("wide/"), utf8("/"), null, 2, 1000);
        addProjections(scenarios, null, utf8("0"), null, 3);
        // §11.3 — start-after is exclusive: at a key, between keys, and past the last key of all
        // (0xFF 0xFF is a real key here, so only 0xFF 0xFF 0xFF is genuinely past the end).
        addProjections(scenarios, null, null, utf8("a/b"), 1, 3);
        addProjections(scenarios, null, null, utf8("wide/074"), 2, 1000);
        addProjections(scenarios, null, null, new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, 5);
        // §11.10 — start-after BELOW the prefix window is ignored; inside it, it bounds the page.
        addProjections(scenarios, utf8("wide/"), null, utf8("A"), 3);
        addProjections(scenarios, utf8("wide/"), null, utf8("wide/100"), 3);
        // start-after exactly equal to a common-prefix boundary: the whole subtree must still roll up.
        addProjections(scenarios, null, utf8("/"), utf8("wide/"), 2, 1000);
        return scenarios;
    }

    // --- the differential engine ----------------------------------------------

    /** One request shape; {@link #walk} re-issues it page by page until the listing ends. */
    private record Scenario(byte[] prefix, byte[] delimiter, byte[] startAfter, int maxKeys, boolean fetchOwner) {

        @Override
        public String toString() {
            return "prefix=" + show(prefix) + " delimiter=" + show(delimiter) + " startAfter=" + show(startAfter)
                    + " maxKeys=" + maxKeys + " fetchOwner=" + fetchOwner;
        }

        private static String show(byte[] value) {
            return value == null ? "-" : ByteKeys.percentEncode(value);
        }
    }

    private static void addProjections(List<Scenario> scenarios, byte[] prefix, byte[] delimiter,
                                       byte[] startAfter, int... maxKeysValues) {
        for (int maxKeys : maxKeysValues) {
            for (boolean fetchOwner : new boolean[]{false, true}) {
                scenarios.add(new Scenario(prefix, delimiter, startAfter, maxKeys, fetchOwner));
            }
        }
    }

    private static List<List<String>> transcripts(Path fixture, SimStoreBackend backend,
                                                  List<Scenario> scenarios) {
        SimStoreFactory.Result result = SimStoreFactory.open(fixture, backend, GENEROUS);
        assertThat(result.resolvedBackend()).as("forced backend").isEqualTo(backend);
        try (var store = result.store()) {
            ListObjectsV2Pager pager = new ListObjectsV2Pager(store, result.metrics());
            List<List<String>> transcripts = new ArrayList<>(scenarios.size());
            for (Scenario scenario : scenarios) {
                transcripts.add(walk(pager, scenario));
            }
            return transcripts;
        }
    }

    /**
     * Pages a scenario to completion, rendering each page as one line: its entries (objects and
     * rolled-up common prefixes, percent-encoded so raw bytes survive), whether it was truncated,
     * and the continuation token that resumes it. Metadata is deliberately absent — see the class
     * javadoc.
     */
    private static List<String> walk(ListObjectsV2Pager pager, Scenario scenario) {
        List<String> pages = new ArrayList<>();
        String token = null;
        for (int guard = 0; guard <= 4096; guard++) {
            S3ListResult result = pager.list(new S3ListRequest(BUCKET, scenario.prefix(), scenario.delimiter(),
                    // continuation-token and start-after are mutually exclusive: from page 2 on,
                    // the token is the resume boundary.
                    token == null ? scenario.startAfter() : null,
                    token, scenario.maxKeys(), true, scenario.fetchOwner()));
            StringBuilder page = new StringBuilder();
            for (S3ResultEntry entry : result.entries()) {
                page.append(entry instanceof S3ResultEntry.CommonPrefixResult ? COMMON_PREFIX_MARK : OBJECT_MARK)
                        .append(ByteKeys.percentEncode(entry.key())).append(' ');
            }
            page.append("| truncated=").append(result.truncated())
                    .append(" next=").append(result.nextContinuationToken());
            pages.add(page.toString());
            if (!result.truncated()) {
                return pages;
            }
            token = result.nextContinuationToken();
        }
        throw new AssertionError("listing did not terminate: " + scenario);
    }

    /** The object keys of one rendered page, in order (rolled-up common prefixes excluded). */
    private static List<String> objectKeysOf(String page) {
        List<String> keys = new ArrayList<>();
        for (String entry : page.substring(0, page.indexOf('|')).trim().split(" ")) {
            if (entry.startsWith(OBJECT_MARK)) {
                keys.add(entry.substring(OBJECT_MARK.length()));
            }
        }
        return keys;
    }

    private static Path writeCapture(Path dir, List<byte[]> keys) throws IOException {
        Path capture = dir.resolve("part-0.parquet");
        try (var writer = ParquetFixtures.open(capture)) {
            for (byte[] key : keys) {
                writer.write(ObjectEntries.withOwner(key, "etag-" + ByteKeys.percentEncode(key)));
            }
        }
        return capture;
    }

    private static void addUtf8(TreeSet<byte[]> keys, String... values) {
        for (String value : values) {
            keys.add(utf8(value));
        }
    }

    private static byte[] longKey(int length) {
        return utf8("long/" + "x".repeat(length - "long/".length()));
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
