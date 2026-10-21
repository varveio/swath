/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
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

class GcsSortedDifferentialTest {
    @Test
    void indexedDelimiterMatchesIndependentRangeWalkAcrossOffsetIntersections(@TempDir Path dir) throws Exception {
        List<String> keys = new ArrayList<>();
        for (String branch : List.of("a", "b", "c")) {
            for (int i = 0; i < 500; i++) {
                keys.add("root/" + branch + "/" + String.format("%03d", i));
            }
        }
        keys.addAll(List.of("root/direct", "root/z"));
        Path capture = Files.createDirectories(dir.resolve("capture"));
        List<String> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, new Random(42));
        try (var writer = ParquetFixtures.open(capture.resolve("part.parquet"))) {
            for (String key : shuffled) {
                writer.write(object(key));
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
            // Pick an actual row-group boundary inside root/a/ and ask for the empty gap just
            // before it. The index shortcut must not infer the prefix from bounding keys alone.
            boolean testedBoundaryGap = false;
            for (int i = 1; i < index.size(); i++) {
                String nextFirst = new String(index.get(i).firstKey().toByteArray(), StandardCharsets.UTF_8);
                String previousFirst = new String(index.get(i - 1).firstKey().toByteArray(), StandardCharsets.UTF_8);
                if (nextFirst.startsWith("root/a/") && previousFirst.startsWith("root/a/")) {
                    List<String> ordered = new ArrayList<>(keys);
                    ordered.sort(String::compareTo);
                    String prior = ordered.get(ordered.indexOf(nextFirst) - 1);
                    GcsListRequest gap = new GcsListRequest("bucket", "root/", "/", prior + "x",
                            nextFirst, 1, null, false);
                    assertThat(walk(new GcsPager(sorted, "fixture"), gap))
                            .isEqualTo(walk(new GcsPager(oracle, "fixture"), gap)).isEmpty();
                    testedBoundaryGap = true;
                    break;
                }
            }
            assertThat(testedBoundaryGap).isTrue();
            for (String start : List.of("root/", "root/a/", "root/a/040", "root/b/", "root/b/099")) {
                for (String end : List.of("root/z", "root/b/050", "root/c/090")) {
                    for (int size : List.of(1, 2, 5)) {
                        GcsListRequest request = new GcsListRequest("bucket", "root/", "/", start,
                                end, size, null, false);
                        assertThat(walk(new GcsPager(sorted, "fixture"), request))
                                .as("start=%s end=%s size=%d", start, end, size)
                                .isEqualTo(walk(new GcsPager(oracle, "fixture"), request));
                    }
                }
            }
        }
    }

    private static List<String> walk(GcsPager pager, GcsListRequest first) {
        List<String> out = new ArrayList<>();
        String token = null;
        for (int guard = 0; guard < 400; guard++) {
            GcsPage page = pager.list(new GcsListRequest(first.bucket(), first.prefix(), first.delimiter(),
                    first.startOffset(), first.endOffset(), first.pageSize(), token, first.fullProjection()));
            for (var object : page.objects()) {
                out.add("O:" + new String(object.key(), StandardCharsets.UTF_8));
            }
            for (byte[] prefix : page.prefixes()) {
                out.add("P:" + new String(prefix, StandardCharsets.UTF_8));
            }
            token = page.nextPageToken();
            if (token == null) {
                return out;
            }
        }
        throw new AssertionError("GCS walk did not terminate");
    }

    private static ObjectEntry object(String key) {
        return ObjectEntries.key(key).size(1).lastModifiedEpochMicros(1_767_225_600_000_000L)
                .etag("etag").storageClass("STANDARD").isLatest(true).build();
    }
}
