/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.metrics.ObservationShape;
import io.varve.swath.replay.protocol.PaginationTestProfile;
import io.varve.swath.replay.server.ListingHttpRequest;
import io.varve.swath.replay.server.ReplayRequestException;
import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzurePagerTest {
    @Test
    void parserPinsVersionAndRefusesUnmeasuredIntersection() {
        assertThatThrownBy(() -> parse("2026-04-06", "restype=container&comp=list"))
                .isInstanceOf(ReplayRequestException.class);
        assertThatThrownBy(() -> parse("2026-06-06", "restype=container&comp=list&startFrom=a%2F1&delimiter=%2F"))
                .isInstanceOf(ReplayRequestException.class);
        assertThatThrownBy(() -> parse("2026-06-06", "restype=container&comp=list&maxresults=0"))
                .isInstanceOf(ReplayRequestException.class);
        assertThat(parse("2026-10-06", "restype=container&comp=list&maxresults=5001").pageSize())
                .isEqualTo(5000);
        assertThatThrownBy(() -> parse("2026-06-06", "restype=container&comp=list&prefix=%FF"))
                .isInstanceOf(ReplayRequestException.class);
    }

    @Test
    void flatStartFromIsInclusiveAndMarkerOwnsContinuation() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "b", "c", "d")) {
            AzureListPager pager = new AzureListPager(store, "fixture-1");
            AzureListRequest request = parse("2026-06-06",
                    "restype=container&comp=list&startFrom=b&maxresults=1");
            AzureListResult first = pager.list(request);
            assertThat(names(first)).containsExactly("O:b");
            assertThat(first.nextMarker()).startsWith("az1.");
            AzureListRequest continued = parse("2026-06-06", "restype=container&comp=list&startFrom=b"
                    + "&maxresults=5000&marker=" + first.nextMarker());
            AzureListResult second = pager.list(continued);
            assertThat(names(second)).containsExactly("O:c", "O:d");
            assertThat(second.nextMarker()).isNull();
            assertThatThrownBy(() -> new AzureListPager(store, "fixture-2").list(continued))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void delimiterEntriesInterleaveAndPageEndsAfterPrefixSubtree() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a/1", "a/2", "b", "c/1")) {
            AzureListPager pager = new AzureListPager(store, "fixture");
            AzureListResult first = pager.list(parse("2026-06-06",
                    "restype=container&comp=list&delimiter=%2F&maxresults=1"));
            assertThat(names(first)).containsExactly("P:a/");
            AzureListResult second = pager.list(parse("2026-06-06",
                    "restype=container&comp=list&delimiter=%2F&maxresults=2&marker=" + first.nextMarker()));
            assertThat(names(second)).containsExactly("O:b", "P:c/");
            assertThat(second.nextMarker()).isNull();
        }
    }

    @Test
    void fallbackBatchesFlatEntriesUnderDelimiter() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) keys.add(String.format("flat-%04d", i));
        try (FakeListingStore store = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            AzureListResult page = new AzureListPager(store, "fixture").list(parse("2026-06-06",
                    "restype=container&comp=list&delimiter=%2F&maxresults=1000"));
            assertThat(page.entries()).hasSize(1000);
            assertThat(store.calls()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void parserRejectsDuplicatesEscapesOverflowAndNegativeSize() {
        for (String query : List.of(
                "restype=container&comp=list&comp=list",
                "restype=container&comp=list&prefix=%GG",
                "restype=container&comp=list&maxresults=2147483648",
                "restype=container&comp=list&maxresults=-2")) {
            assertThatThrownBy(() -> parse("2026-06-06", query))
                    .as(query).isInstanceOf(ReplayRequestException.class);
        }
        assertThat(parse("2026-06-06", "restype=container&comp=list&prefix=a+b").prefix())
                .isEqualTo("a+b");
        assertThatThrownBy(() -> parse("2026-06-06", "restype=container&comp=list&delimiter=%EF%BF%BE"))
                .isInstanceOf(ReplayRequestException.class);
    }

    @Test
    void nativeFiveThousandPageLimitAndMarkerScope() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 5001; i++) keys.add(String.format("key-%05d", i));
        try (FakeListingStore store = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            AzureListPager pager = new AzureListPager(store, "fixture");
            AzureListRequest first = parse("2026-06-06", "restype=container&comp=list&maxresults=5001");
            AzureListResult page = pager.list(first);
            assertThat(page.entries()).hasSize(5000);
            assertThat(page.nextMarker()).isNotNull();
            AzureListRequest next = parse("2026-06-06", "restype=container&comp=list&maxresults=5001"
                    + "&marker=" + page.nextMarker());
            assertThat(names(pager.list(next))).containsExactly("O:key-05000");
            assertThatThrownBy(() -> pager.list(parse("2026-10-06",
                    "restype=container&comp=list&marker=" + page.nextMarker())))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void constructorOnlyEmptyAndShortPagesCarryProgress() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "b")) {
            AzureListPager pager = new AzureListPager(store, "fixture", new PaginationTestProfile(1, true), null);
            AzureListResult empty = pager.list(parse("2026-06-06", "restype=container&comp=list"));
            assertThat(empty.entries()).isEmpty();
            assertThat(empty.nextMarker()).startsWith("az1.");
            AzureListResult first = pager.list(parse("2026-06-06",
                    "restype=container&comp=list&marker=" + empty.nextMarker()));
            assertThat(names(first)).containsExactly("O:a");
            AzureListResult second = pager.list(parse("2026-06-06",
                    "restype=container&comp=list&marker=" + first.nextMarker()));
            assertThat(names(second)).containsExactly("O:b");
            assertThat(second.nextMarker()).isNull();
        }
    }

    @Test
    void preparedPageReportsExactBlobAndPrefixCounts() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a/1", "b")) {
            AzureHandler handler = new AzureHandler("replay", "bucket",
                    () -> "http://127.0.0.1/replay/", store, "fixture", null);
            var delimiter = handler.parse(new ListingHttpRequest("GET", "/replay/bucket",
                    "restype=container&comp=list&delimiter=%2F&maxresults=1",
                    Map.of("x-ms-version", "2026-06-06"))).page().observation();
            assertThat(delimiter.shape()).isEqualTo(ObservationShape.DELIMITER);
            assertThat(delimiter.objects()).isZero();
            assertThat(delimiter.prefixes()).isEqualTo(1);
            var seek = handler.parse(new ListingHttpRequest("GET", "/replay/bucket",
                    "restype=container&comp=list&maxresults=1",
                    Map.of("x-ms-version", "2026-06-06"))).page().observation();
            assertThat(seek.shape()).isEqualTo(ObservationShape.SEEK);
            assertThat(seek.objects()).isEqualTo(1);
            assertThat(seek.prefixes()).isZero();
        }
    }

    private static AzureListRequest parse(String version, String query) {
        return AzureQuery.parse("replay", "bucket", new ListingHttpRequest("GET", "/replay/bucket",
                query, Map.of("x-ms-version", version)));
    }

    private static List<String> names(AzureListResult result) {
        List<String> names = new ArrayList<>();
        for (AzureListResult.Entry entry : result.entries()) {
            if (entry instanceof AzureListResult.Entry.Blob blob) {
                names.add("O:" + new String(blob.object().key(), StandardCharsets.UTF_8));
            } else if (entry instanceof AzureListResult.Entry.BlobPrefix prefix) {
                names.add("P:" + new String(prefix.name(), StandardCharsets.UTF_8));
            }
        }
        return names;
    }
}
