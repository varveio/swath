/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Independent model over a complete in-memory inventory; no pager, store, or rollup helper calls. */
class GcsIndependentOracleTest {
    @Test
    void randomizedRangesAndDelimiterWalksHaveNoGapsOverlapsOrTokenLoops() {
        Random random = new Random(0x220223);
        List<String> keys = new ArrayList<>(List.of("a/", "a/1", "a/2", "\uE000", "\uD800\uDC00"));
        for (int i = 0; i < 70; i++) {
            keys.add("root/" + (char) ('a' + random.nextInt(5)) + "/" + String.format("%03d", i));
        }
        keys.addAll(List.of("root/z", "root/z/1", "root/z/2"));
        try (FakeListingStore store = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            GcsPager pager = new GcsPager(store, "fixture");
            for (int caseNumber = 0; caseNumber < 300; caseNumber++) {
                String prefix = pick(random, null, "a/", "root/", "root/b/");
                String delimiter = pick(random, null, "/", "0");
                String start = pick(random, null, "a/", "a/1", "root/a/", "root/b/035",
                        "root/z/1", "\uE000");
                String end = pick(random, null, "root/b/", "root/c/045", "root/z/2", "\uD800\uDC00");
                Expected expected = expected(keys, prefix, delimiter, start, end);
                List<String> objects = new ArrayList<>();
                List<String> prefixes = new ArrayList<>();
                Set<String> seenTokens = new HashSet<>();
                String token = null;
                for (int page = 0; page <= keys.size() + 1; page++) {
                    int pageSize = 1 + (page % 7);
                    GcsPage actual = pager.list(new GcsListRequest("bucket", prefix, delimiter, start, end,
                            pageSize, token, false));
                    assertThat(actual.objects().size() + actual.prefixes().size()).isLessThanOrEqualTo(pageSize);
                    actual.objects().forEach(row -> objects.add(new String(row.key(), StandardCharsets.UTF_8)));
                    actual.prefixes().forEach(raw -> prefixes.add(new String(raw, StandardCharsets.UTF_8)));
                    token = actual.nextPageToken();
                    if (token == null) {
                        break;
                    }
                    assertThat(seenTokens.add(token)).as("token cycle in case %d", caseNumber).isTrue();
                    if (page == keys.size() + 1) {
                        throw new AssertionError("GCS walk exceeded inventory bound");
                    }
                }
                assertThat(objects).as("objects case %d", caseNumber).containsExactlyElementsOf(expected.objects());
                assertThat(prefixes).as("prefixes case %d", caseNumber).containsExactlyElementsOf(expected.prefixes());
            }
        }
    }

    private static Expected expected(List<String> inventory, String prefix, String delimiter,
                                     String start, String end) {
        List<String> sorted = new ArrayList<>(inventory);
        sorted.sort((a, b) -> Arrays.compareUnsigned(bytes(a), bytes(b)));
        List<String> objects = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        Set<String> already = new HashSet<>();
        for (String name : sorted) {
            if (prefix != null && !name.startsWith(prefix)
                    || start != null && Arrays.compareUnsigned(bytes(name), bytes(start)) < 0
                    || end != null && Arrays.compareUnsigned(bytes(name), bytes(end)) >= 0) {
                continue;
            }
            int cut = delimiter == null ? -1 : name.indexOf(delimiter, prefix == null ? 0 : prefix.length());
            if (cut >= 0) {
                String rolled = name.substring(0, cut + delimiter.length());
                if (already.add(rolled)) {
                    prefixes.add(rolled);
                }
            } else {
                objects.add(name);
            }
        }
        return new Expected(objects, prefixes);
    }

    private static String pick(Random random, String... values) {
        return values[random.nextInt(values.length)];
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private record Expected(List<String> objects, List<String> prefixes) {
    }
}
