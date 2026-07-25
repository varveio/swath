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
 */
class SortedServingFullDifferentialTest {

    /** Tiny final row groups so a few hundred keys form many row groups — the multi-RG path is real. */
    private static SortConfig manySmallGroups() {
        return SortConfigs.manySmallRowGroups();
    }

    /** Tiny final-file-bytes AND tiny row groups: range-disjoint multi-file output with many groups. */
    private static SortConfig rolledSmallFiles() {
        return SortConfigs.base().withFinalFileBytes(8192L).withFinalRowGroupBytes(1024L).withMergeBudgetBytes(64L << 20);
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
        try (ReplayServer sorted = new ReplayServer(
                "127.0.0.1", 0, "bucket", fixture.path(), 2, ServingMode.SORTED);
             ReplayServer duck = new ReplayServer(
                     "127.0.0.1", 0, "bucket", fixture.path(), 2, ServingMode.DUCKDB)) {
            sorted.start();
            duck.start();
            assertThat(sorted.resolvedServingMode()).isEqualTo(ServingMode.SORTED);
            assertThat(duck.resolvedServingMode()).isEqualTo(ServingMode.DUCKDB);

            HttpClient client = HttpClient.newHttpClient();
            for (Scenario scenario : scenarios) {
                assertThat(walk(sorted, client, scenario))
                        .as("sorted vs duckdb differ for %s", describe(scenario))
                        .isEqualTo(walk(duck, client, scenario));
            }
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
            // start-after and continuation-token are mutually exclusive (a real client drops
            // start-after once it holds a token); prefix/delimiter carry across every page.
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
            for (byte[] k : shuffled) {
                writer.write(object(k));
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
