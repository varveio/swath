/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListObjectsV2Pager;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The arena tier and the windowed tier each answer the same key sequence as the Parquet-backed
 * tier, over the same fixture, the same pager, and the same request sequence — including
 * delimiter rollups, the edge-case keys of {@code docs/internals/algorithms.md} §11, and the
 * truncation boundaries around a fixture's exact key count.
 *
 * <p>Every fixture here is written through the production sorter ({@link #writeCapture}), never a
 * bare unsorted capture: the windowed tier requires a sorted-eligible fixture, and running the
 * SAME sorted fixture through the arena and Parquet tiers too keeps all three comparisons over
 * identical bytes on disk. {@link #theEdgeCaseInventoryFixtureSpansMultipleRowGroups} pins that the
 * largest fixture is genuinely multi-row-group, so the windowed tier's window-boundary and
 * window-refill paths are actually exercised here, not vacuously true of a single-group file.
 *
 * <p><b>What is compared, and what deliberately is not.</b> The transcript is keys, common
 * prefixes, page boundaries, {@code IsTruncated} and the continuation token. Object <b>metadata is
 * not compared for the arena, because the arena does not load it</b>: its sim-mode projection
 * stubs size, last-modified, etag, storage class, owner and checksum on every row (see
 * {@link ArenaListingStore}). That is by design — a simulator decides splits, steals and
 * pagination from keys alone, and loading metadata for every key of every fixture would defeat the
 * tier. {@link #metadataIsStubbedOnTheArenaAndFullOnParquet} pins the difference so it stays a
 * documented contract rather than an undetected regression; the windowed tier carries full
 * metadata, exactly like the Parquet tier it wraps. Full byte-for-byte comparison of metadata
 * across every field is the replay module's own sorted-vs-DuckDB differential suite.
 *
 * <p>Every backend is driven through the identical {@link ListObjectsV2Pager}, so any
 * disagreement is attributable to a store by construction. Backends are selected explicitly
 * ({@link SimStoreBackend#ARENA} / {@link SimStoreBackend#WINDOWED} / {@link SimStoreBackend#PARQUET})
 * rather than through {@link SimStoreBackend#AUTO}, which would resolve to one tier and compare it
 * against itself.
 */
class ArenaDifferentialTest {

    private static final String BUCKET = "bucket";

    private static final SimStoreConfig GENEROUS = new SimStoreConfig(1L << 20);

    /** Beyond the pager's default seek-scan threshold (32), so a rollup takes the seek path. */
    private static final int WIDE_DIRECTORY_CHILDREN = 150;

    /**
     * The smallest legal segment. Loading the arena with it puts most fixture keys across a segment
     * boundary, so the segmented layout is exercised through the pager and not only in
     * {@link KeyArenaTest}; production always uses {@link KeyArena#SEGMENT_BYTES}.
     */
    private static final int TIGHT_SEGMENT_BYTES = KeyArena.MAX_KEY_BYTES;

    private static final String OBJECT_MARK = "O:";
    private static final String COMMON_PREFIX_MARK = "P:";

    @TempDir
    private Path dir;

    /**
     * The degenerate fixtures matter as much as the interesting one: an empty capture and a
     * single-key capture are where a first-page, a lower bound and a truncation probe all collapse
     * to the same edge, and they must collapse identically on both backends.
     */
    static List<Arguments> fixtures() {
        return List.of(
                Arguments.of("empty", List.of()),
                Arguments.of("single-key", List.of(utf8("solo"))),
                Arguments.of("edge-case-inventory", edgeCaseKeys()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtures")
    void arenaAndWindowedAgreeWithParquetOnKeysPaginationAndTruncation(String name, List<byte[]> keys)
            throws IOException {
        Path fixture = writeCapture(dir, keys);
        List<Scenario> scenarios = scenarios(keys.size());

        List<List<String>> parquet = transcripts(scenarios, parquetStore(fixture));
        // Both the production segment size and a tight one, so a cross-segment key that the arena
        // reassembled wrongly would show up as a pager-visible disagreement, not just a unit failure.
        List<List<String>> arena = transcripts(scenarios, arenaStore(fixture));
        List<List<String>> tightlySegmented = transcripts(scenarios, tightlySegmentedArenaStore(fixture));
        List<List<String>> windowed = transcripts(scenarios, windowedStore(fixture));

        for (int i = 0; i < scenarios.size(); i++) {
            assertThat(arena.get(i)).as("%s / %s", name, scenarios.get(i)).isEqualTo(parquet.get(i));
            assertThat(tightlySegmented.get(i))
                    .as("%s / tight segments / %s", name, scenarios.get(i)).isEqualTo(parquet.get(i));
            assertThat(windowed.get(i)).as("%s / windowed / %s", name, scenarios.get(i)).isEqualTo(parquet.get(i));
        }
    }

    /**
     * Windowing over a single row group would be vacuously correct — a miss always covers the
     * whole fixture. The edge-case-inventory fixture (the largest of {@link #fixtures}) must
     * actually split into more than one row group under the production sorter's small-row-group
     * config, so the differential above genuinely exercises window-boundary and window-refill
     * behaviour, not just a full-fixture single window.
     */
    @Test
    void theEdgeCaseInventoryFixtureSpansMultipleRowGroups() throws IOException {
        Path fixture = writeCapture(dir, edgeCaseKeys());
        assertThat(rowGroupCount(fixture)).isGreaterThan(1);
    }

    @Test
    void theFlatWalkEnumeratesEveryFixtureKeyExactlyOnce() throws IOException {
        // Without this, the differential above would pass just as happily on two stores that both
        // returned nothing. Small max-keys, so this is many continuation hops, not one page.
        List<byte[]> keys = edgeCaseKeys();
        Path fixture = writeCapture(dir, keys);

        try (Opened opened = arenaStore(fixture)) {
            ListObjectsV2Pager pager = new ListObjectsV2Pager(opened.store(), opened.metrics());
            List<String> walked = new ArrayList<>();
            for (String page : walk(pager, new Scenario(null, null, null, 3, false))) {
                walked.addAll(objectKeysOf(page));
            }
            assertThat(walked).containsExactlyElementsOf(keys.stream().map(ByteKeys::percentEncode).toList());
        }
    }

    @Test
    void metadataIsStubbedOnTheArenaFullOnParquetAndFullOnWindowed() throws IOException {
        Path fixture = writeCapture(dir, List.of(utf8("solo")));

        try (Opened arena = arenaStore(fixture); Opened parquet = parquetStore(fixture);
             Opened windowed = windowedStore(fixture)) {
            ListedObject fromArena = arena.store().rows(null, true, null, 1, Projection.WITH_OWNER).getFirst();
            ListedObject fromParquet = parquet.store().rows(null, true, null, 1, Projection.WITH_OWNER).getFirst();
            ListedObject fromWindowed = windowed.store().rows(null, true, null, 1, Projection.WITH_OWNER).getFirst();

            assertThat(fromArena.key()).isEqualTo(fromParquet.key());
            assertThat(fromArena.size()).isEqualTo(ArenaListingStore.STUB_SIZE);
            assertThat(fromArena.etag()).isNull();
            assertThat(fromArena.ownerId()).isNull();
            assertThat(fromParquet.etag()).isEqualTo("etag-solo");
            assertThat(fromParquet.ownerId()).isEqualTo("owner-id");
            assertThat(fromWindowed.etag()).isEqualTo(fromParquet.etag());
            assertThat(fromWindowed.ownerId()).isEqualTo(fromParquet.ownerId());
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
        // boundaries, where "is there one more row" must agree across backends. keyCount-1 only
        // exists as a boundary once there is a key to be one short of.
        addProjections(scenarios, null, null, null, 0, keyCount, keyCount + 1);
        if (keyCount > 0) {
            addProjections(scenarios, null, null, null, keyCount - 1);
        }
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

    /** An opened store and the metrics its pager needs, closed as one. */
    private record Opened(ListingStore store, ReplayMetrics metrics) implements AutoCloseable {

        @Override
        public void close() {
            store.close();
        }
    }

    private static Opened parquetStore(Path fixture) {
        return forced(fixture, SimStoreBackend.PARQUET);
    }

    private static Opened arenaStore(Path fixture) {
        return forced(fixture, SimStoreBackend.ARENA);
    }

    private static Opened windowedStore(Path fixture) {
        return forced(fixture, SimStoreBackend.WINDOWED);
    }

    /**
     * The arena again, but packed into {@value #TIGHT_SEGMENT_BYTES}-byte segments. The segment size
     * is an internal encoding detail with no operator knob, so this reaches past the factory to the
     * load seam rather than inventing a configuration surface for a test.
     */
    private static Opened tightlySegmentedArenaStore(Path fixture) {
        SimStoreFactory.Result source = SimStoreFactory.open(fixture, SimStoreBackend.PARQUET, GENEROUS);
        try (var parquet = source.store()) {
            ArenaListingStore arena = ArenaListingStore
                    .loadWithin(parquet, GENEROUS.arenaMaxEncodedBytes(), TIGHT_SEGMENT_BYTES)
                    .orElseThrow();
            return new Opened(arena, source.metrics());
        }
    }

    private static Opened forced(Path fixture, SimStoreBackend backend) {
        SimStoreFactory.Result result = SimStoreFactory.open(fixture, backend, GENEROUS);
        assertThat(result.resolvedBackend()).as("forced backend").isEqualTo(backend);
        return new Opened(result.store(), result.metrics());
    }

    private static List<List<String>> transcripts(List<Scenario> scenarios, Opened opened) {
        try (opened) {
            ListObjectsV2Pager pager = new ListObjectsV2Pager(opened.store(), opened.metrics());
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

    /**
     * Writes {@code keys} to an unsorted capture, then runs it through the production sorter with a
     * small final-row-group-bytes config — the same shape {@code SortedParquetStoreTest} uses — so
     * the result is a stamped, {@code mode=objects} fixture every backend here can serve, and one
     * large enough to span several row groups (see {@link #theEdgeCaseInventoryFixtureSpansMultipleRowGroups}).
     */
    private static Path writeCapture(Path dir, List<byte[]> keys) throws IOException {
        Path capture = Files.createDirectory(dir.resolve("cap"));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (byte[] key : keys) {
                writer.write(ObjectEntries.withOwner(key, "etag-" + ByteKeys.percentEncode(key)));
            }
        }
        Path out = Files.createDirectory(dir.resolve("out"));
        new CaptureSorter(SortConfigs.manySmallRowGroups()).sort(capture, out);
        return out;
    }

    private static int rowGroupCount(Path fixtureDir) throws IOException {
        List<Path> files = SortedFixtures.resolveFiles(fixtureDir);
        IndexLoadResult result = SortedFixtures.loadIndex(files, new FixtureMetrics());
        return ((IndexLoadResult.Loaded) result).entries().size();
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
