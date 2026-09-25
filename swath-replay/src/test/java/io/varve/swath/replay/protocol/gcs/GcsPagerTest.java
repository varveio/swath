/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.fixture.FixtureIdentity;
import io.varve.swath.replay.metrics.ObservationShape;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.PaginationTestProfile;
import io.varve.swath.replay.server.ListingHttpRequest;
import io.varve.swath.replay.testkit.FakeListingStore;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GcsPagerTest {
    @Test
    void strictQueryRejectsDuplicateMalformedAndZeroButCapsLargePages() {
        assertThatThrownBy(() -> GcsQuery.parse("bucket", "prefix=x&prefix=y"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcsQuery.parse("bucket", "prefix=%GG"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcsQuery.parse("bucket", "prefix=%FF"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GcsQuery.parse("bucket", "maxResults=0"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(GcsQuery.parse("bucket", "maxResults=9223372036854775807").pageSize()).isEqualTo(1000);
        assertThat(GcsQuery.parse("bucket", "maxResults=9223372036854775807").pageSizeClamped()).isTrue();
        assertThat(GcsQuery.parse("bucket", "maxResults=1000").pageSizeClamped()).isFalse();
        assertThat(GcsQuery.parse("bucket", "prefix=a+b").prefix()).isEqualTo("a+b");
    }

    @Test
    void clampedPageLimitReportsEngagementReason() {
        ReplayMetrics metrics = new ReplayMetrics();
        try (FakeListingStore store = FakeListingStore.ofKeys("a")) {
            new GcsPager(store, "fixture", PaginationTestProfile.NONE, metrics)
                    .list(GcsQuery.parse("bucket", "maxResults=5000"));
            assertThat(metrics.registry().get("swath.replay.provider.path")
                    .tags("protocol", "gcs", "path", "page_limit_clamped", "reason", "requested_above_1000")
                    .counter().count()).isEqualTo(1.0);
        } finally {
            metrics.registry().close();
        }
    }

    @Test
    void preparedPageReportsExactProtocolShapeCounts() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a/1", "b")) {
            GcsHandler handler = new GcsHandler("bucket", store, "fixture");
            var delimiter = handler.parse(new ListingHttpRequest("GET", "/storage/v1/b/bucket/o",
                    "delimiter=%2F&maxResults=1", Map.of())).page().observation();
            assertThat(delimiter.shape()).isEqualTo(ObservationShape.DELIMITER);
            assertThat(delimiter.objects()).isZero();
            assertThat(delimiter.prefixes()).isEqualTo(1);
            var seek = handler.parse(new ListingHttpRequest("GET", "/storage/v1/b/bucket/o",
                    "maxResults=1", Map.of())).page().observation();
            assertThat(seek.shape()).isEqualTo(ObservationShape.SEEK);
            assertThat(seek.objects()).isEqualTo(1);
            assertThat(seek.prefixes()).isZero();
        }
    }

    @Test
    void flatBoundsAreInclusiveThenExclusiveAndTokenSurvivesPageSizeChange() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "b", "c", "d")) {
            GcsPager pager = new GcsPager(store, "fixture-1");
            GcsPage first = pager.list(new GcsListRequest("bucket", null, null, "b", "d", 1, null, false));
            assertThat(names(first.objects())).containsExactly("b");
            assertThat(first.nextPageToken()).isNotNull();
            GcsPage second = pager.list(new GcsListRequest("bucket", null, null, "b", "d", 99,
                    first.nextPageToken(), false));
            assertThat(names(second.objects())).containsExactly("c");
            assertThat(second.nextPageToken()).isNull();
        }
    }

    @Test
    void delimiterOffsetInsideSubtreeEmitsPrefixAndResumesAfterWholeSubtree() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a/1", "a/2", "a/3", "b/1", "c")) {
            GcsPager pager = new GcsPager(store, "fixture-1");
            GcsPage first = pager.list(new GcsListRequest("bucket", null, "/", "a/2", null, 1,
                    null, false));
            assertThat(strings(first.prefixes())).containsExactly("a/");
            GcsPage second = pager.list(new GcsListRequest("bucket", null, "/", "a/2", null, 1,
                    first.nextPageToken(), false));
            assertThat(strings(second.prefixes())).containsExactly("b/");
            GcsPage third = pager.list(new GcsListRequest("bucket", null, "/", "a/2", null, 1,
                    second.nextPageToken(), false));
            assertThat(names(third.objects())).containsExactly("c");
            assertThat(third.nextPageToken()).isNull();
        }
    }

    @Test
    void tokenRejectsDifferentScopeAndFixture() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "b")) {
            String token = new GcsPager(store, "fixture-1")
                    .list(new GcsListRequest("bucket", null, null, null, null, 1, null, false))
                    .nextPageToken();
            assertThatThrownBy(() -> new GcsPager(store, "fixture-2")
                    .list(new GcsListRequest("bucket", null, null, null, null, 1, token, false)))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new GcsPager(store, "fixture-1")
                    .list(new GcsListRequest("bucket", "b", null, null, null, 1, token, false)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void delimiterFallbackBatchesDirectObjects() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            keys.add(String.format("flat-%04d", i));
        }
        try (FakeListingStore store = FakeListingStore.ofKeys(keys.toArray(String[]::new))) {
            GcsPage page = new GcsPager(store, "fixture").list(
                    new GcsListRequest("bucket", null, "/", null, null, 1000, null, false));
            assertThat(page.objects()).hasSize(1000);
            assertThat(page.nextPageToken()).isNull();
            assertThat(store.calls()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void delimiterFallbackPageOneFetchesOnlyOneEntryAndLookahead() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "b", "c", "d")) {
            GcsPager pager = new GcsPager(store, "fixture");
            GcsPage first = pager.list(new GcsListRequest("bucket", null, "/", null, null,
                    1, null, false));
            assertThat(names(first.objects())).containsExactly("a");
            assertThat(first.nextPageToken()).isNotNull();
            assertThat(store.calls()).isEqualTo(1);
            assertThat(store.lastLimit()).isEqualTo(2);
            GcsPage second = pager.list(new GcsListRequest("bucket", null, "/", null, null,
                    1, first.nextPageToken(), false));
            assertThat(names(second.objects())).containsExactly("b");
            assertThat(store.lastLimit()).isEqualTo(2);
        }
        try (FakeListingStore store = FakeListingStore.ofKeys("a/1", "a/2", "a/3", "b")) {
            GcsPage page = new GcsPager(store, "fixture").list(
                    new GcsListRequest("bucket", null, "/", null, null, 1, null, false));
            assertThat(strings(page.prefixes())).containsExactly("a/");
            assertThat(page.nextPageToken()).isNotNull();
            assertThat(store.lastLimit()).isEqualTo(1);
        }
    }

    @Test
    void optInEmptyAndShortPagesCarryBoundedProgressInToken() {
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "b", "c")) {
            GcsPager pager = new GcsPager(store, "fixture", new PaginationTestProfile(1, true));
            GcsListRequest firstRequest = new GcsListRequest("bucket", null, null, null, null, 100,
                    null, false);
            GcsPage empty = pager.list(firstRequest);
            assertThat(empty.objects()).isEmpty();
            assertThat(empty.nextPageToken()).isNotNull();
            GcsPage first = pager.list(new GcsListRequest("bucket", null, null, null, null, 100,
                    empty.nextPageToken(), false));
            assertThat(names(first.objects())).containsExactly("a");
            GcsPage second = pager.list(new GcsListRequest("bucket", null, null, null, null, 100,
                    first.nextPageToken(), false));
            assertThat(names(second.objects())).containsExactly("b");
            GcsPage third = pager.list(new GcsListRequest("bucket", null, null, null, null, 100,
                    second.nextPageToken(), false));
            assertThat(names(third.objects())).containsExactly("c");
            assertThat(third.nextPageToken()).isNull();
        }
    }

    @Test
    void fixtureIdentitySurvivesRelocationAndInvalidatesChangedManifest(@TempDir Path dir) throws Exception {
        Path original = Files.createDirectories(dir.resolve("original"));
        Path relocated = Files.createDirectories(dir.resolve("relocated"));
        Path originalPart = original.resolve("part.parquet");
        Path relocatedPart = relocated.resolve("part.parquet");
        ParquetFixtures.write(originalPart, ObjectEntries.key("a").isLatest(true).build());
        Files.copy(originalPart, relocatedPart);
        var originalMtime = Files.getLastModifiedTime(originalPart);
        Files.setLastModifiedTime(relocatedPart, originalMtime);
        String firstIdentity = FixtureIdentity.of(original);
        assertThat(FixtureIdentity.of(relocated)).isEqualTo(firstIdentity);
        try (FakeListingStore store = FakeListingStore.ofKeys("a", "b")) {
            GcsListRequest first = new GcsListRequest("bucket", null, null, null, null, 1, null, false);
            String token = new GcsPager(store, firstIdentity).list(first).nextPageToken();
            assertThat(new GcsPager(store, FixtureIdentity.of(relocated)).list(
                    new GcsListRequest("bucket", null, null, null, null, 1, token, false)).objects())
                    .hasSize(1);
            Files.setLastModifiedTime(relocatedPart,
                    java.nio.file.attribute.FileTime.fromMillis(originalMtime.toMillis() + 1000));
            assertThat(FixtureIdentity.of(relocated)).isNotEqualTo(firstIdentity);
            assertThatThrownBy(() -> new GcsPager(store, FixtureIdentity.of(relocated)).list(
                    new GcsListRequest("bucket", null, null, null, null, 1, token, false)))
                    .isInstanceOf(IllegalArgumentException.class);
            Files.setLastModifiedTime(relocatedPart, originalMtime);
            ParquetFixtures.write(relocatedPart, ObjectEntries.key("changed").isLatest(true).build());
            assertThat(FixtureIdentity.of(relocated)).isNotEqualTo(firstIdentity);
            assertThatThrownBy(() -> new GcsPager(store, FixtureIdentity.of(relocated)).list(
                    new GcsListRequest("bucket", null, null, null, null, 1, token, false)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static List<String> names(List<ListedObject> rows) {
        return rows.stream().map(row -> new String(row.key(), StandardCharsets.UTF_8)).toList();
    }

    private static List<String> strings(List<byte[]> values) {
        List<String> result = new ArrayList<>();
        for (byte[] value : values) {
            result.add(new String(value, StandardCharsets.UTF_8));
        }
        return result;
    }
}
