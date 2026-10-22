/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.store.DuckDbListingStore;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.store.WindowedListingStore;
import io.varve.swath.replay.testkit.FakeListingStore;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Azure lists in UTF-16 code-unit order while the fixture is byte-sorted. These tests compare the
 * Azure pager on every store mode with an in-memory model that sorts decoded names with
 * {@link String#compareTo} (UTF-16 code units) and shares no code with the pager.
 */
class AzureProviderOrderTest {
    private static final String U = "u/";
    /** The captured azure-06 corpus under {@code u/}, in fixture (byte) order. */
    private static final List<String> CAPTURED = List.of("E", "e", "e\u0301", "z", "\u00e9", "\u00ff",
            "\ud7ff", "\ue000", "\ufffd", "\ufffe", "\uffff", "\ud800\udc00", "\ud83d\ude00", "\udbff\udffd");
    private static final String[] PIECES = {"a", "b", "z", "/", "\u00e9", "\u0800", "\ud7ff", "\ue000",
            "\uff21", "\ufffd", "\ud800\udc00", "\ud83d\ude00", "\udbff\udffd", "\ue000/", "a/"};

    @Test
    void capturedOrderingCasesMatchNativeAzure(@TempDir Path dir) throws Exception {
        List<String> keys = new ArrayList<>();
        for (String name : CAPTURED) keys.add(U + name);
        keys.addAll(List.of("t/a", "v/b"));
        List<String> utf16 = List.of("E", "e", "e\u0301", "z", "\u00e9", "\u00ff", "\ud7ff", "\ud800\udc00",
                "\ud83d\ude00", "\udbff\udffd", "\ue000", "\ufffd", "\ufffe", "\uffff");
        for (Store store : stores(dir, keys)) {
            try (store) {
                AzureListPager pager = new AzureListPager(store.store(), "fixture");
                // azure-06a: the whole window in one page.
                assertThat(walk(pager, U, null, null, 5000)).as(store.mode())
                        .containsExactlyElementsOf(blobs(utf16));
                // azure-06b: startFrom at U+E000 skips the supplementary names that sort before it.
                assertThat(walk(pager, U, null, U + "\ue000", 5000)).as(store.mode())
                        .containsExactlyElementsOf(blobs(utf16.subList(10, 14)));
                // azure-06e: a supplementary startFrom keeps the BMP names above U+E000 after it.
                assertThat(walk(pager, U, null, U + "\ud83d\ude00", 5000)).as(store.mode())
                        .containsExactlyElementsOf(blobs(utf16.subList(8, 14)));
                // azure-06f: delimiter é rolls up u/é; every other name stays a blob in UTF-16 order.
                List<String> rolled = new ArrayList<>(blobs(utf16));
                rolled.set(4, "P:" + U + "\u00e9");
                assertThat(walk(pager, U, "\u00e9", null, 5000)).as(store.mode()).containsExactlyElementsOf(rolled);
                // azure-10a: the SDK walk with five entries per page keeps the same page boundaries.
                List<List<String>> pages = pages(pager, U, null, null, 5);
                assertThat(pages).as(store.mode()).containsExactly(blobs(utf16.subList(0, 5)),
                        blobs(utf16.subList(5, 10)), blobs(utf16.subList(10, 14)));
            }
        }
    }

    @Test
    void randomizedWalksMatchUtf16Model(@TempDir Path dir) throws Exception {
        Random random = new Random(0x239);
        Set<String> unique = new LinkedHashSet<>();
        while (unique.size() < 160) unique.add(randomName(random, 1 + random.nextInt(4)));
        List<String> keys = new ArrayList<>(unique);
        for (Store store : stores(dir, keys)) {
            try (store) {
                AzureListPager pager = new AzureListPager(store.store(), "fixture");
                Random cases = new Random(0x2390);
                // DuckDB answers each read in tens of milliseconds; its share of cases stays smaller.
                int caseCount = store.mode().equals("duckdb") ? 25 : 100;
                for (int caseNumber = 0; caseNumber < caseCount; caseNumber++) {
                    String prefix = pick(cases, null, "", "a", "a/", "\ue000", "\ud83d\ude00",
                            firstCharacter(keys.get(cases.nextInt(keys.size()))));
                    String delimiter = pick(cases, null, null, "/", "\u00e9", "\ue000", "\ud83d\ude00",
                            "/\ue000");
                    String start = delimiter != null ? null : pick(cases, null, keys.get(cases.nextInt(keys.size())),
                            randomName(cases, 2), "\ue000", "\ud800\udc00");
                    int pageSize = pick(cases, 1, 2, 3, 7, 5000);
                    List<String> expected = expected(keys, prefix, delimiter, start);
                    List<List<String>> pages = pages(pager, prefix, delimiter, start, pageSize);
                    List<String> actual = new ArrayList<>();
                    for (int i = 0; i < pages.size(); i++) {
                        if (i < pages.size() - 1) {
                            assertThat(pages.get(i)).as("%s case %d page %d", store.mode(), caseNumber, i)
                                    .hasSize(pageSize);
                        }
                        actual.addAll(pages.get(i));
                    }
                    assertThat(actual).as("%s case %d prefix=%s delimiter=%s start=%s size=%d", store.mode(),
                            caseNumber, ascii(prefix), ascii(delimiter), ascii(start), pageSize)
                            .containsExactlyElementsOf(expected);
                }
            }
        }
    }

    @Test
    void comparatorIsUtf16OrderForValidNames() {
        Random random = new Random(7);
        for (int i = 0; i < 20_000; i++) {
            String a = randomName(random, 1 + random.nextInt(3));
            String b = randomName(random, 1 + random.nextInt(3));
            assertThat(Integer.signum(AzureOrder.compare(bytes(a), bytes(b))))
                    .as("%s vs %s", ascii(a), ascii(b)).isEqualTo(Integer.signum(a.compareTo(b)));
        }
    }

    @Test
    void namesWithoutLeadBytesAtOrAboveEeKeepOneRangeAndOneRead() {
        assertThat(AzureOrder.rangesFrom(bytes("root/a/b/c"), false, bytes("root/"))).hasSize(1);
        assertThat(AzureOrder.rangesFrom(bytes("root/\ue000\ud83d\ude00"), false, bytes("root/"))).hasSize(4);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) keys.add(String.format("k%03d", i));
        try (FakeListingStore store = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            AzureListPager pager = new AzureListPager(store, "fixture");
            AzureListResult first = pager.list(request(null, null, "k010", null, 10));
            assertThat(store.calls()).isEqualTo(1);
            assertThat(store.lastLimit()).isEqualTo(11);
            pager.list(request(null, null, "k010", first.nextMarker(), 10));
            assertThat(store.calls()).isEqualTo(2);
        }
    }

    @Test
    void swappedBranchesReuseRowsAlreadyRead() {
        // One read returns the EE branch, the F0 branch and the tail; the swap re-reads nothing.
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "\ue000", "\uffff", "\ud800\udc00", "z\u00ff")) {
            AzureListResult page = new AzureListPager(store, "fixture").list(request(null, null, null, null, 5000));
            assertThat(names(page)).containsExactly("O:a", "O:z\u00ff", "O:\ud800\udc00", "O:\ue000", "O:\uffff");
            assertThat(store.calls()).isEqualTo(1);
        }
    }

    @Test
    void invalidUtf8NamesKeepATotalOrderWithEveryNameOnce() {
        // Not valid UTF-8, so not renderable on the Azure route; the pager still orders them by the
        // same byte permutation (00..ED, F0..F4, EE..EF, F5..FF) and never loses or repeats one.
        byte[][] keys = {{(byte) 0xee}, {(byte) 0xef, 0x41}, {(byte) 0xf0}, {(byte) 0xf4, (byte) 0xff},
                {(byte) 0xf5}, {(byte) 0xff}, {0x41, (byte) 0xee, (byte) 0xf0}, {0x41, (byte) 0xf0, (byte) 0xee},
                {(byte) 0xc0, (byte) 0x80}, {(byte) 0xed, (byte) 0xa0, (byte) 0x80}};
        int[] order = new int[256];
        int next = 0;
        for (int b = 0x00; b <= 0xed; b++) order[b] = next++;
        for (int b = 0xf0; b <= 0xf4; b++) order[b] = next++;
        for (int b = 0xee; b <= 0xef; b++) order[b] = next++;
        for (int b = 0xf5; b <= 0xff; b++) order[b] = next++;
        Comparator<byte[]> permuted = (a, b) -> {
            int at = Arrays.mismatch(a, b);
            if (at < 0) return 0;
            if (at == a.length || at == b.length) return Integer.compare(a.length, b.length);
            return Integer.compare(order[a[at] & 0xff], order[b[at] & 0xff]);
        };
        List<byte[]> expected = new ArrayList<>(List.of(keys));
        expected.sort(permuted);
        try (FakeListingStore store = FakeListingStore.ofKeys(keys)) {
            AzureListPager pager = new AzureListPager(store, "fixture");
            for (int size : List.of(1, 2, 3, 5000)) {
                List<byte[]> actual = new ArrayList<>();
                String marker = null;
                do {
                    AzureListResult page = pager.list(request(null, null, null, marker, size));
                    for (AzureListResult.Entry entry : page.entries()) {
                        actual.add(((AzureListResult.Entry.Blob) entry).object().key());
                    }
                    marker = page.nextMarker();
                } while (marker != null);
                assertThat(actual).as("page size %d", size).usingElementComparator(Arrays::compare)
                        .containsExactlyElementsOf(expected);
            }
        }
    }

    /** One store mode over the same keys. */
    record Store(String mode, ListingStore store, AutoCloseable owner) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            store.close();
            if (owner != null) owner.close();
        }
    }

    /** The range-only fake, the sorted Parquet store, the windowed cache over it, and DuckDB. */
    static List<Store> stores(Path dir, List<String> keys) throws Exception {
        Path capture = Files.createDirectories(dir.resolve("capture-" + keys.hashCode()));
        List<String> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, new Random(44));
        try (var writer = ParquetFixtures.open(capture.resolve("part.parquet"))) {
            for (String key : shuffled) writer.write(ObjectEntries.key(key).size(1).isLatest(true).build());
        }
        Path sortedDir = Files.createDirectories(dir.resolve("sorted-" + keys.hashCode()));
        new CaptureSorter(SortConfigs.manySmallRowGroups()).sort(capture, sortedDir);
        var files = SortedFixtures.resolveFiles(sortedDir);
        var loaded = SortedFixtures.loadIndex(files, new FixtureMetrics());
        var index = ((SortedFixtures.IndexLoadResult.Loaded) loaded).entries();
        SortedParquetStore sorted = new SortedParquetStore(files, index, new ReplayMetrics(), 2);
        SortedParquetStore backing = new SortedParquetStore(files, index, new ReplayMetrics(), 2);
        return List.of(
                new Store("range-only", FakeListingStore.ofKeys(keys.toArray(String[]::new)), null),
                new Store("sorted", sorted, null),
                new Store("windowed", new WindowedListingStore(backing, new ReplayMetrics(), 7, 4), backing),
                new Store("duckdb", new DuckDbListingStore(capture.resolve("part.parquet"), new ReplayMetrics(), 1),
                        null));
    }

    /** Native semantics over decoded names: UTF-16 order, inclusive startFrom, one entry per rolled prefix. */
    static List<String> expected(List<String> keys, String prefix, String delimiter, String start) {
        Map<String, String> entries = new TreeMap<>();
        for (String key : keys) {
            if (prefix != null && !key.startsWith(prefix) || start != null && key.compareTo(start) < 0) continue;
            int at = delimiter == null ? -1 : key.indexOf(delimiter, prefix == null ? 0 : prefix.length());
            if (at >= 0) {
                String rolled = key.substring(0, at + delimiter.length());
                entries.put(rolled, "P:" + rolled);
            } else {
                entries.put(key, "O:" + key);
            }
        }
        return new ArrayList<>(entries.values());
    }

    static List<String> walk(AzureListPager pager, String prefix, String delimiter, String start, int size) {
        List<String> out = new ArrayList<>();
        pages(pager, prefix, delimiter, start, size).forEach(out::addAll);
        return out;
    }

    static List<List<String>> pages(AzureListPager pager, String prefix, String delimiter, String start,
                                    int size) {
        List<List<String>> pages = new ArrayList<>();
        Set<String> markers = new HashSet<>();
        String marker = null;
        for (int page = 0; page < 2000; page++) {
            AzureListResult result = pager.list(request(prefix, delimiter, start, marker, size));
            assertThat(result.entries().size()).isLessThanOrEqualTo(size);
            pages.add(names(result));
            marker = result.nextMarker();
            if (marker == null) return pages;
            assertThat(markers.add(marker)).as("marker cycle").isTrue();
        }
        throw new AssertionError("Azure marker walk did not stop");
    }

    static AzureListRequest request(String prefix, String delimiter, String start, String marker, int size) {
        return new AzureListRequest("replay", "bucket", "2026-06-06", prefix == null || prefix.isEmpty() ? null : prefix,
                delimiter, start, marker, size, Integer.toString(size), prefix != null, delimiter != null,
                marker != null, true, null);
    }

    static List<String> names(AzureListResult result) {
        List<String> names = new ArrayList<>();
        for (AzureListResult.Entry entry : result.entries()) {
            names.add(switch (entry) {
                case AzureListResult.Entry.Blob blob -> "O:" + new String(blob.object().key(), StandardCharsets.UTF_8);
                case AzureListResult.Entry.BlobPrefix rolled -> "P:" + new String(rolled.name(), StandardCharsets.UTF_8);
            });
        }
        return names;
    }

    private static List<String> blobs(List<String> names) {
        return names.stream().map(name -> "O:" + U + name).toList();
    }

    static String randomName(Random random, int pieces) {
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < pieces; i++) name.append(PIECES[random.nextInt(PIECES.length)]);
        return name.toString();
    }

    @SafeVarargs
    static <T> T pick(Random random, T... values) {
        return values[random.nextInt(values.length)];
    }

    private static String firstCharacter(String value) {
        return value.substring(0, value.offsetByCodePoints(0, 1));
    }

    static String ascii(String value) {
        if (value == null) return "null";
        StringBuilder out = new StringBuilder();
        value.chars().forEach(c -> out.append(c < 0x80 ? Character.toString(c) : String.format("\\u%04x", c)));
        return out.toString();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
