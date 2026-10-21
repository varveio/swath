/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

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

/** Complete-inventory model independent of AzureListPager and store rollup code. */
class AzureIndependentOracleTest {
    @Test
    void randomizedFlatAndHierarchyWalksKeepInterleavedOrderAndCompleteInventory() {
        Random random = new Random(0x220224);
        List<String> keys = new ArrayList<>(List.of("A", "a/", "a/1", "a/2", "\uFF21", "\uD83D\uDE00"));
        for (int i = 0; i < 90; i++) {
            keys.add("root/" + (char) ('a' + random.nextInt(5)) + "/" + String.format("%03d", i));
        }
        try (FakeListingStore store = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            AzureListPager pager = new AzureListPager(store, "fixture");
            for (int caseNumber = 0; caseNumber < 250; caseNumber++) {
                String prefix = pick(random, null, "a/", "root/", "root/b/");
                String delimiter = pick(random, null, "/", "0");
                String start = delimiter == null ? pick(random, null, "A", "a/1", "root/b/032", "\uFF21") : null;
                List<String> expected = expected(keys, prefix, delimiter, start);
                List<String> actual = new ArrayList<>();
                Set<String> seenTokens = new HashSet<>();
                String marker = null;
                for (int page = 0; page <= keys.size() + 1; page++) {
                    AzureListRequest request = new AzureListRequest("replay", "bucket", "2026-06-06",
                            prefix, delimiter, start, marker, 1 + page % 9, null,
                            prefix != null, delimiter != null, marker != null, false, null);
                    AzureListResult result = pager.list(request);
                    assertThat(result.entries().size()).isLessThanOrEqualTo(request.pageSize());
                    result.entries().forEach(entry -> actual.add(label(entry)));
                    marker = result.nextMarker();
                    if (marker == null) break;
                    assertThat(seenTokens.add(marker)).as("case %d token cycle", caseNumber).isTrue();
                    if (page == keys.size() + 1) throw new AssertionError("Azure marker walk did not stop");
                }
                assertThat(actual).as("case %d", caseNumber).containsExactlyElementsOf(expected);
            }
        }
    }

    private static List<String> expected(List<String> inventory, String prefix, String delimiter, String start) {
        List<String> ordered = new ArrayList<>(inventory);
        ordered.sort((a, b) -> Arrays.compareUnsigned(bytes(a), bytes(b)));
        Set<String> emitted = new HashSet<>();
        List<String> result = new ArrayList<>();
        for (String key : ordered) {
            if (prefix != null && !key.startsWith(prefix)
                    || start != null && Arrays.compareUnsigned(bytes(key), bytes(start)) < 0) continue;
            int at = delimiter == null ? -1 : key.indexOf(delimiter, prefix == null ? 0 : prefix.length());
            if (at >= 0) {
                String rolled = key.substring(0, at + delimiter.length());
                if (emitted.add(rolled)) result.add("P:" + rolled);
            } else result.add("O:" + key);
        }
        return result;
    }

    private static String label(AzureListResult.Entry entry) {
        return switch (entry) {
            case AzureListResult.Entry.Blob blob -> "O:" + new String(blob.object().key(), StandardCharsets.UTF_8);
            case AzureListResult.Entry.BlobPrefix prefix -> "P:" + new String(prefix.name(), StandardCharsets.UTF_8);
        };
    }

    private static String pick(Random random, String... values) {
        return values[random.nextInt(values.length)];
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
