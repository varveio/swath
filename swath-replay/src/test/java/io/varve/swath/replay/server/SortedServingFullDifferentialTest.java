/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.testkit.HttpProbe;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The full token-walk differential — {@code pager + SortedParquetStore} (role 2) vs {@code pager +
 * DuckDbListingStore} (role 1 oracle) over the <b>same real HTTP surface</b>, asserting
 * byte-identical XML page-for-page across adversarial synthetic keyspaces and the full
 * request-projection matrix. It trusts the S3 contract, not the routing code, so a too-tight bound, a
 * mis-encoded delimiter seek, or a projection leak surfaces as a page-body divergence attributable to
 * the sorted store by construction (both stores are driven through the identical pager).
 *
 * <p>Keyspaces cover: flat listings; delimiter'd deep hierarchies; a giant single prefix spanning
 * many row groups; keys straddling row-group boundaries at various {@code max-keys}; a
 * {@code successor(P)}-is-a-real-key case; {@code 0xFF}-laden keys; and a multi-byte delimiter. Each
 * runs the {@code encoding-type=url} on/off × {@code fetch-owner} on/off matrix, and the whole thing
 * runs once more over a <b>rolled multi-file</b> sorted fixture (tiny {@code final-file-bytes}) to
 * prove the file-aware index serves identically.
 *
 * <p>Two request shapes cannot come from the page walk, which drops {@code start-after} once it holds
 * a token, so they are driven directly: a token sent alongside a conflicting {@code start-after}, and
 * a blank {@code continuation-token=} that only the query string can express.
 */
class SortedServingFullDifferentialTest {

    /** Tiny final row groups so a few hundred keys form many row groups — the multi-RG path is real. */
    private static SortConfig manySmallGroups() {
        return SortConfigs.manySmallRowGroups();
    }

    /** Tiny final-file-bytes and tiny row groups: ordered multi-file output with many groups. */
    private static SortConfig rolledSmallFiles() {
        return SortConfigs.rolledPerEntry()
                .withSegmentEntries(1)
                .withFinalRowGroupBytes(1024L)
                .withMergeBudgetBytes(64L << 20);
    }

    @Test
    void flatListingIsByteIdentical(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 180; i++) {
            keys.add(utf8(String.format("key-%05d", i)));
        }
        differential(dir, keys, manySmallGroups(), projectionMatrix(null, null, null,
                new int[]{0, 1, 3, 7, 50, 1000}));
    }

    @Test
    void deepDelimitedHierarchyIsByteIdentical(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int a = 0; a < 3; a++) {
            for (int b = 0; b < 3; b++) {
                for (int c = 0; c < 4; c++) {
                    keys.add(utf8(String.format("d/a%d/b%d/c%d.txt", a, b, c)));
                }
            }
        }
        for (int n = 0; n < 6; n++) {
            keys.add(utf8("d/top-" + n + ".txt"));   // objects directly under the prefix, no deeper delimiter
            keys.add(utf8("e-" + n));                 // outside the prefix entirely
        }
        List<Scenario> scenarios = new ArrayList<>();
        scenarios.addAll(projectionMatrix(null, utf8("/"), null, new int[]{1, 2, 3, 7}));
        scenarios.addAll(projectionMatrix(utf8("d/"), utf8("/"), null, new int[]{1, 2, 1000}));
        scenarios.addAll(projectionMatrix(utf8("d/a1/"), utf8("/"), null, new int[]{1, 3}));
        differential(dir, keys, manySmallGroups(), scenarios);
    }

    /**
     * This combines delimiter'd CommonPrefix rollup WITH a start-after resume boundary over a deep
     * hierarchy, in both boundary shapes S3 treats differently: a boundary strictly INSIDE a
     * CommonPrefix's subtree (only that prefix's remaining tail must still roll up and be emitted
     * once) and a boundary EXACTLY EQUAL to a CommonPrefix
     * string itself (never a real key here — every one of that prefix's children sorts after it, so
     * the WHOLE subtree must still roll up). Small {@code max-keys} values force the walk through
     * several {@code continuation-token} hops after the initial {@code start-after} page, so the
     * pager's boundary handoff from "resolved start-after" to "resolved token" is exercised too.
     */
    @Test
    void startAfterInsideAndAtACommonPrefixBoundaryWalksIdenticallyWithTokens(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int a = 0; a < 3; a++) {
            for (int b = 0; b < 3; b++) {
                for (int c = 0; c < 4; c++) {
                    keys.add(utf8(String.format("d/a%d/b%d/c%d.txt", a, b, c)));
                }
            }
        }
        for (int n = 0; n < 6; n++) {
            keys.add(utf8("d/top-" + n + ".txt"));
        }
        List<Scenario> scenarios = new ArrayList<>();
        // start-after strictly INSIDE "d/a1/"'s subtree: only the surviving tail of a1's children
        // (b1/c3, all of b2) plus a2's subtree and the top-level objects roll up/emit.
        scenarios.addAll(projectionMatrix(utf8("d/"), utf8("/"), utf8("d/a1/b1/c2.txt"), new int[]{1, 2, 3, 1000}));
        // start-after EXACTLY EQUAL to a CommonPrefix boundary string ("d/a1/" is never a literal key
        // here — every child of that prefix is lexicographically greater than the bare prefix string
        // itself): the WHOLE "d/a1/" subtree must still roll up as one CommonPrefix.
        scenarios.addAll(projectionMatrix(utf8("d/"), utf8("/"), utf8("d/a1/"), new int[]{1, 2, 3, 1000}));
        differential(dir, keys, manySmallGroups(), scenarios);
    }

    /**
     * A client that keeps its original {@code start-after} while paging sends both parameters from
     * page two on; real S3 resumes at the token and ignores the start-after. Both conflict directions
     * run here: {@code a/1} sorts before the token's boundary (honoring it would re-emit keys page one
     * already returned) and {@code a/3} sorts after it (honoring it would drop {@code a/3}). The
     * walk-based tests cannot cover this — they drop start-after once they hold a token — so it is
     * proven over the whole query-parser-to-XML path instead of the pager alone.
     */
    @Test
    void continuationTokenWinsOverConflictingStartAfterOnHttp(@TempDir Path dir) throws Exception {
        Fixture fixture = writeSorted(dir, List.of(
                utf8("a/1"), utf8("a/2"), utf8("a/3"), utf8("a/4")), manySmallGroups());
        withServers(fixture, (sorted, duck, client) -> {
            for (String conflicting : new String[]{"a/1", "a/3"}) {
                String response = agreedResume(sorted, duck, client,
                        "&start-after=" + HttpProbe.percentEncode(utf8(conflicting)));

                assertThat(response).as("start-after=%s must not move the token's boundary", conflicting)
                        .contains("<Key>a/3</Key>", "<Key>a/4</Key>")
                        .doesNotContain("<Key>a/1</Key>", "<Key>a/2</Key>", "<StartAfter>");
            }
        });
    }

    /**
     * {@code continuation-token=} reaches the pager as an empty string rather than an absent
     * parameter, which only the wire can produce. Blank is absent: {@code start-after} stays the
     * boundary and is still echoed. Treating blank as a present token would restart the listing at
     * {@code a/1} and hand back keys the client asked to skip.
     */
    @Test
    void blankContinuationTokenLeavesStartAfterAsTheBoundaryOnHttp(@TempDir Path dir) throws Exception {
        Fixture fixture = writeSorted(dir, List.of(
                utf8("a/1"), utf8("a/2"), utf8("a/3"), utf8("a/4")), manySmallGroups());
        withServers(fixture, (sorted, duck, client) -> {
            String query = "/bucket?list-type=2&max-keys=1000&continuation-token=&start-after="
                    + HttpProbe.percentEncode(utf8("a/1"));

            String response = HttpProbe.body(sorted, client, query);

            assertThat(response).isEqualTo(HttpProbe.body(duck, client, query));
            assertThat(response)
                    .contains("<Key>a/2</Key>", "<Key>a/3</Key>", "<Key>a/4</Key>",
                            "<StartAfter>a/1</StartAfter>")
                    .doesNotContain("<Key>a/1</Key>", "<ContinuationToken>");
        });
    }

    @Test
    void giantSinglePrefixSpanningManyRowGroupsIsByteIdentical(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 220; i++) {
            keys.add(utf8(String.format("big/%04d", i)));
        }
        Collections.addAll(keys, utf8("aaa"), utf8("aab"), utf8("zzy"), utf8("zzz"));
        List<Scenario> scenarios = new ArrayList<>();
        // delimiter rolls the whole "big/" span to a single CommonPrefix — one seek past many groups.
        scenarios.addAll(projectionMatrix(null, utf8("/"), null, new int[]{1, 2, 3}));
        // flat listing over the same giant prefix straddles row-group boundaries at every max-keys.
        scenarios.addAll(projectionMatrix(utf8("big/"), null, null, new int[]{1, 2, 3, 7, 37}));
        scenarios.addAll(projectionMatrix(utf8("big/"), null, utf8("big/0100"), new int[]{5}));
        differential(dir, keys, manySmallGroups(), scenarios);
    }

    @Test
    void successorOfPrefixIsARealEmittedKey(@TempDir Path dir) throws Exception {
        // successor("photos/") == "photos0" — a REAL key that must be emitted right after the "photos/"
        // CommonPrefix is rolled up (§9.2), the inclusive seek-to-successor case.
        List<byte[]> keys = new ArrayList<>();
        Collections.addAll(keys, utf8("album/1"), utf8("album/2"));
        for (int i = 1; i <= 9; i++) {
            keys.add(utf8("photos/" + i));
        }
        Collections.addAll(keys, utf8("photos0"), utf8("photos00"), utf8("photos1"), utf8("zeta"));
        List<Scenario> scenarios = new ArrayList<>();
        scenarios.addAll(projectionMatrix(null, utf8("/"), null, new int[]{1, 2, 3, 1000}));
        scenarios.addAll(projectionMatrix(null, null, utf8("photos/9"), new int[]{1, 2}));
        differential(dir, keys, manySmallGroups(), scenarios);
    }

    @Test
    void ffLadenKeysAreByteIdentical(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            keys.add(new byte[]{'k', (byte) i});
            keys.add(new byte[]{'k', (byte) i, (byte) 0xFF});
            keys.add(new byte[]{'k', (byte) i, (byte) 0xFF, (byte) 0x01});
        }
        keys.add(new byte[]{(byte) 0xFF});                        // all-0xFF: successor is end-of-keyspace
        keys.add(new byte[]{(byte) 0xFF, (byte) 0xFF});
        // 0xFF is not valid UTF-8, so only encoding-type=url is safe here; both projections of it.
        List<Scenario> scenarios = new ArrayList<>();
        scenarios.add(new Scenario(null, null, null, 1, true, false));
        scenarios.add(new Scenario(null, null, null, 3, true, true));
        scenarios.add(new Scenario(null, null, null, 1000, true, true));
        scenarios.add(new Scenario(new byte[]{'k'}, null, null, 2, true, false));
        differential(dir, keys, manySmallGroups(), scenarios);
    }

    /**
     * The fast-path shape introduced for the O(prefixes) root rollup (public issue #77): a no-prefix
     * request (the request shape whose open upper bound used to fall through to the O(prefixes)
     * per-directory range walk even on the sorted store), many prefixes each spanning several row
     * groups (tiny {@code manySmallGroups} row groups over 25 objects/prefix — the "prefix run spans
     * row-group boundaries" case), small {@code max-keys} forcing a continuation-token resume mid the
     * prefix sequence, and plain keys sorted after the very last delimiter-bearing prefix (no {@code /}
     * at all — the "trailing plain keys" case). Also pins the fast path actually engaged for the
     * no-prefix scenario, not merely that its output happens to match: a silent fallback to the range
     * walk would still pass the byte-identical assertion below.
     */
    @Test
    void wideNoPrefixRollupWithRowGroupSpanningPrefixesAndTrailingPlainKeysIsByteIdentical(@TempDir Path dir)
            throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int p = 0; p < 12; p++) {
            for (int i = 0; i < 25; i++) {
                keys.add(utf8(String.format("wide%02d/obj-%03d", p, i)));
            }
        }
        for (int n = 0; n < 4; n++) {
            keys.add(utf8("zzz-plain-" + n));   // sorts after every "wideNN/" prefix; no delimiter at all
        }
        Fixture fixture = writeSorted(dir, keys, manySmallGroups());
        List<Scenario> scenarios = new ArrayList<>(projectionMatrix(null, utf8("/"), null, new int[]{1, 2, 3, 5}));
        withServers(fixture, (sorted, duck, client) -> {
            walkAll(sorted, duck, client, scenarios);
            assertThat(sorted.metrics().registry().find("swath.replay.delimiter.path")
                    .tag("path", ReplayMetrics.DELIMITER_PATH_ROLLUP).counter().count())
                    .as("no-prefix delimiter requests must be served by the store's native rollup, "
                            + "not silently fall back to the range walk")
                    .isGreaterThan(0.0);
        });
    }

    @Test
    void multiByteDelimiterIsByteIdentical(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        Collections.addAll(keys,
                utf8("a--1"), utf8("a--2"), utf8("a--3"),
                utf8("b--1"), utf8("b--2"),
                utf8("plain-1"), utf8("plain-2"), utf8("z"));
        differential(dir, keys, manySmallGroups(),
                projectionMatrix(null, utf8("--"), null, new int[]{1, 2, 1000}));
    }

    @Test
    void rolledMultiFileFixtureServesIdentically(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add(utf8(String.format("shard/%05d/obj", i)));
        }
        List<Scenario> scenarios = new ArrayList<>();
        scenarios.addAll(projectionMatrix(null, null, null, new int[]{1, 3, 7, 1000}));
        scenarios.addAll(projectionMatrix(null, utf8("/"), null, new int[]{2, 3}));
        scenarios.addAll(projectionMatrix(utf8("shard/00050/"), null, null, new int[]{1, 2}));
        Fixture fixture = writeSorted(dir, keys, rolledSmallFiles());
        assertThat(fixture.rolledIntoMultipleFiles()).as("fixture must genuinely roll into >1 file").isTrue();
        runAll(fixture, scenarios);
    }

    // --- differential engine --------------------------------------------------

    private record Scenario(byte[] prefix, byte[] delimiter, byte[] startAfter, int maxKeys,
                            boolean encodingUrl, boolean fetchOwner) {
    }

    private static List<Scenario> projectionMatrix(byte[] prefix, byte[] delimiter, byte[] startAfter,
                                                   int[] maxKeysValues) {
        List<Scenario> scenarios = new ArrayList<>();
        for (int maxKeys : maxKeysValues) {
            for (boolean encodingUrl : new boolean[]{true, false}) {
                for (boolean fetchOwner : new boolean[]{true, false}) {
                    scenarios.add(new Scenario(prefix, delimiter, startAfter, maxKeys, encodingUrl, fetchOwner));
                }
            }
        }
        return scenarios;
    }

    private void differential(Path dir, List<byte[]> keys, SortConfig config, List<Scenario> scenarios)
            throws Exception {
        runAll(writeSorted(dir, keys, config), scenarios);
    }

    private void runAll(Fixture fixture, List<Scenario> scenarios) throws Exception {
        withServers(fixture, (sorted, duck, client) -> walkAll(sorted, duck, client, scenarios));
    }

    /** The body of a differential: both servers started over one fixture, plus the client driving them. */
    @FunctionalInterface
    private interface Differential {
        void run(ReplayServer sorted, ReplayServer duck, HttpClient client) throws Exception;
    }

    /**
     * Serves one fixture through both stores and hands both servers to {@code body}. The resolved-mode
     * assertions live here rather than in each caller because they are what makes a differential one:
     * were either server to resolve to the other's store, every comparison below would be a response
     * against itself and would pass for the wrong reason.
     */
    private static void withServers(Fixture fixture, Differential body) throws Exception {
        try (ReplayServer sorted = new ReplayServer(
                "127.0.0.1", 0, "bucket", fixture.path(), 2, ServingMode.SORTED);
             ReplayServer duck = new ReplayServer(
                     "127.0.0.1", 0, "bucket", fixture.path(), 2, ServingMode.DUCKDB)) {
            sorted.start();
            duck.start();
            assertThat(sorted.resolvedServingMode()).isEqualTo(ServingMode.SORTED);
            assertThat(duck.resolvedServingMode()).isEqualTo(ServingMode.DUCKDB);
            body.run(sorted, duck, HttpClient.newHttpClient());
        }
    }

    private static void walkAll(ReplayServer sorted, ReplayServer duck, HttpClient client,
                                List<Scenario> scenarios) throws Exception {
        for (Scenario scenario : scenarios) {
            assertThat(walk(sorted, client, scenario))
                    .as("sorted vs duckdb differ for %s", describe(scenario))
                    .isEqualTo(walk(duck, client, scenario));
        }
    }

    private static List<String> walk(ReplayServer server, HttpClient client, Scenario scenario)
            throws Exception {
        List<String> pages = new ArrayList<>();
        String token = null;
        for (int guard = 0; guard < 100_000; guard++) {
            StringBuilder q = new StringBuilder("/bucket?list-type=2&max-keys=").append(scenario.maxKeys());
            if (scenario.encodingUrl()) {
                q.append("&encoding-type=url");
            }
            if (scenario.fetchOwner()) {
                q.append("&fetch-owner=true");
            }
            if (scenario.prefix() != null) {
                q.append("&prefix=").append(HttpProbe.percentEncode(scenario.prefix()));
            }
            if (scenario.delimiter() != null) {
                q.append("&delimiter=").append(HttpProbe.percentEncode(scenario.delimiter()));
            }
            // This walk sends only the token after page one, the shape a well-behaved client uses.
            // continuationTokenWinsOverConflictingStartAfterOnHttp covers the shape that carries both.
            if (token != null) {
                q.append("&continuation-token=").append(HttpProbe.percentEncode(token));
            } else if (scenario.startAfter() != null) {
                q.append("&start-after=").append(HttpProbe.percentEncode(scenario.startAfter()));
            }
            String body = HttpProbe.body(server, client, q.toString());
            pages.add(body);
            if (!"true".equals(HttpProbe.extractTag(body, "IsTruncated"))) {
                return pages;
            }
            token = HttpProbe.extractTag(body, "NextContinuationToken");
            assertThat(token).as("a truncated page must carry a continuation token").isNotNull();
        }
        throw new AssertionError("listing did not terminate for " + describe(scenario));
    }

    /** Resumes through both stores and returns the response they must agree on byte for byte. */
    private static String agreedResume(ReplayServer sorted, ReplayServer duck, HttpClient client,
                                       String extraQuery) throws Exception {
        String response = resume(sorted, client, extraQuery);
        assertThat(response).as("sorted vs duckdb differ for resume%s", extraQuery)
                .isEqualTo(resume(duck, client, extraQuery));
        return response;
    }

    /** Takes a real token from a truncated first page, then resends it with {@code extraQuery} appended. */
    private static String resume(ReplayServer server, HttpClient client, String extraQuery) throws Exception {
        String first = HttpProbe.body(server, client, "/bucket?list-type=2&max-keys=2");
        String token = HttpProbe.extractTag(first, "NextContinuationToken");
        assertThat(token).as("the first page must carry a continuation token").isNotNull();
        return HttpProbe.body(server, client, "/bucket?list-type=2&max-keys=1000&continuation-token="
                + HttpProbe.percentEncode(token) + extraQuery);
    }

    private static String describe(Scenario s) {
        return "prefix=" + (s.prefix() == null ? "-" : new String(s.prefix(), StandardCharsets.UTF_8))
                + " delimiter=" + (s.delimiter() == null ? "-" : new String(s.delimiter(), StandardCharsets.UTF_8))
                + " startAfter=" + (s.startAfter() == null ? "-" : new String(s.startAfter(), StandardCharsets.UTF_8))
                + " maxKeys=" + s.maxKeys() + " url=" + s.encodingUrl() + " fetchOwner=" + s.fetchOwner();
    }

    // --- fixture construction -------------------------------------------------

    private record Fixture(Path path, int fileCount) {
        boolean rolledIntoMultipleFiles() {
            return fileCount > 1;
        }
    }

    private static Fixture writeSorted(Path dir, List<byte[]> keys, SortConfig config) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture-" + System.nanoTime()));
        List<byte[]> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, new Random(1234));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (byte[] key : shuffled) {
                writer.write(object(key));
            }
        }
        Path out = Files.createDirectories(dir.resolve("sorted-" + System.nanoTime()));
        var result = new CaptureSorter(config).sort(capture, out);
        return new Fixture(out, result.finalFiles().size());
    }

    private static ObjectEntry object(byte[] key) {
        return ObjectEntries.withOwner(key, "etag");
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
