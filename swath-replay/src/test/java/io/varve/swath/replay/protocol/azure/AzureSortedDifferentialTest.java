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
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.testkit.FakeListingStore;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AzureSortedDifferentialTest {
    @Test
    void indexedHierarchyAndFlatStartFromMatchRangeOnlyStore(@TempDir Path dir) throws Exception {
        List<String> keys = new ArrayList<>();
        for (String branch : List.of("a", "b", "c")) {
            for (int i = 0; i < 350; i++) {
                keys.add("root/" + branch + "/" + String.format("%03d", i));
            }
        }
        keys.addAll(List.of("root/direct", "root/z"));
        Path capture = Files.createDirectories(dir.resolve("capture"));
        List<String> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, new Random(44));
        try (var writer = ParquetFixtures.open(capture.resolve("part.parquet"))) {
            for (String key : shuffled) {
                writer.write(ObjectEntries.key(key).size(1).isLatest(true).build());
            }
        }
        Path sortedDir = Files.createDirectories(dir.resolve("sorted"));
        new CaptureSorter(SortConfigs.manySmallRowGroups()).sort(capture, sortedDir);
        var files = SortedFixtures.resolveFiles(sortedDir);
        var loaded = SortedFixtures.loadIndex(files, new FixtureMetrics());
        var index = ((SortedFixtures.IndexLoadResult.Loaded) loaded).entries();
        assertThat(index).hasSizeGreaterThan(1);
        try (SortedParquetStore sorted = new SortedParquetStore(files, index, new ReplayMetrics(), 2);
             FakeListingStore oracle = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            AzureListPager indexed = new AzureListPager(sorted, "fixture");
            AzureListPager ranged = new AzureListPager(oracle, "fixture");
            for (String prefix : List.of("root/", "root/a/")) {
                for (String delimiter : List.of("/", "0")) {
                    for (int pageSize : List.of(1, 2, 7)) {
                        AzureListRequest request = request(prefix, delimiter, null, null, pageSize);
                        assertThat(walk(indexed, request)).isEqualTo(walk(ranged, request));
                    }
                }
            }
            for (String start : List.of("root/a/", "root/a/175", "root/b/", "root/c/349")) {
                AzureListRequest request = request("root/", null, start, null, 17);
                assertThat(walk(indexed, request)).isEqualTo(walk(ranged, request));
            }
        }
    }

    private static AzureListRequest request(String prefix, String delimiter, String start,
                                            String marker, int size) {
        return new AzureListRequest("replay", "bucket", "2026-06-06", prefix, delimiter,
                start, marker, size, Integer.toString(size), prefix != null, delimiter != null,
                marker != null, true, null);
    }

    private static List<String> walk(AzureListPager pager, AzureListRequest first) {
        List<String> out = new ArrayList<>();
        String marker = null;
        for (int page = 0; page < 1100; page++) {
            AzureListResult result = pager.list(request(first.prefix(), first.delimiter(), first.startFrom(),
                    marker, first.pageSize()));
            for (AzureListResult.Entry entry : result.entries()) {
                out.add(switch (entry) {
                    case AzureListResult.Entry.Blob blob -> "O:"
                            + new String(blob.object().key(), StandardCharsets.UTF_8);
                    case AzureListResult.Entry.BlobPrefix prefix -> "P:"
                            + new String(prefix.name(), StandardCharsets.UTF_8);
                });
            }
            marker = result.nextMarker();
            if (marker == null) return out;
        }
        throw new AssertionError("Azure marker walk did not terminate");
    }
}
