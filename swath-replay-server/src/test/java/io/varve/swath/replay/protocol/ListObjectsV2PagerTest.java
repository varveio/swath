/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Drives {@link ListObjectsV2Pager} over an in-memory range-only {@link ListingStore} — the point of
 * the seam: every protocol rule is exercised without DuckDB or Jetty.
 */
class ListObjectsV2PagerTest {

    @Test
    void listsObjectsByPrefixAndExclusiveStartAfter() {
        ListObjectsV2Pager pager = pager("a/1", "a/2", "a/3", "b/1");
        S3ListResult result = pager.list(new S3ListRequest(
                "bucket", bytes("a/"), null, bytes("a/1"), null, 2, true, false));

        assertThat(keys(result)).containsExactly("a/2", "a/3");
        assertThat(result.truncated()).isFalse();
        assertThat(result.nextContinuationToken()).isNull();
    }

    @Test
    void truncatesAndResumesFromOpaqueContinuationToken() {
        ListObjectsV2Pager pager = pager("a/1", "a/2", "a/3");
        S3ListResult first = pager.list(new S3ListRequest(
                "bucket", bytes("a/"), null, null, null, 2, true, false));
        S3ListResult second = pager.list(new S3ListRequest(
                "bucket", bytes("a/"), null, null, first.nextContinuationToken(), 2, true, false));

        assertThat(keys(first)).containsExactly("a/1", "a/2");
        assertThat(first.truncated()).isTrue();
        assertThat(first.nextContinuationToken()).startsWith("v2:");
        assertThat(second.truncated()).isFalse();
        assertThat(keys(second)).containsExactly("a/3");
    }

    @Test
    void maxKeysZeroReturnsEmptyNonTruncatedPage() {
        ListObjectsV2Pager pager = pager("a/1", "a/2");
        S3ListResult result = pager.list(new S3ListRequest(
                "bucket", null, null, null, null, 0, true, false));

        assertThat(result.entries()).isEmpty();
        assertThat(result.truncated()).isFalse();
        assertThat(result.nextContinuationToken()).isNull();
    }

    @Test
    void rollsUpCommonPrefixesAndResumesPastRolledUpPrefix() {
        ListObjectsV2Pager pager = pager(
                "a/0.txt", "a/dir/1.txt", "a/dir/2.txt", "a/dir0", "a/dir2/1.txt", "a/z.txt");
        S3ListResult first = pager.list(new S3ListRequest(
                "bucket", bytes("a/"), bytes("/"), null, null, 2, true, false));
        S3ListResult second = pager.list(new S3ListRequest(
                "bucket", bytes("a/"), bytes("/"), null, first.nextContinuationToken(), 10, true, false));

        assertThat(render(first.entries())).containsExactly("O:a/0.txt", "P:a/dir/");
        assertThat(first.truncated()).isTrue();
        assertThat(render(second.entries())).containsExactly("O:a/dir0", "P:a/dir2/", "O:a/z.txt");
        assertThat(second.truncated()).isFalse();
    }

    @Test
    void startAfterFiltersCommonPrefixesByPrefixLexicographicOrder() {
        ListObjectsV2Pager pager = pager("a/a", "a/c", "b");
        S3ListResult result = pager.list(new S3ListRequest(
                "bucket", null, bytes("/"), bytes("a/b"), null, 10, true, false));

        assertThat(render(result.entries())).containsExactly("O:b");
        assertThat(result.truncated()).isFalse();
    }

    @Test
    void rejectsBadContinuationToken() {
        ListObjectsV2Pager pager = pager("a/1");
        assertThatThrownBy(() -> pager.list(new S3ListRequest(
                "bucket", null, null, null, "not-a-token", 1000, true, false)))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("invalid continuation-token");
    }

    @Test
    void rejectsContinuationTokenAndStartAfterTogether() {
        ListObjectsV2Pager pager = pager("a/1");
        assertThatThrownBy(() -> pager.list(new S3ListRequest(
                "bucket", null, null, bytes("a/1"), "v2:AGEvMg", 1000, true, false)))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void recordsFixtureListLatencyMetricOnList() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ListObjectsV2Pager pager = new ListObjectsV2Pager(
                FakeListingStore.ofKeys("a/1", "a/2"), new ReplayMetrics(registry));

        pager.list(new S3ListRequest("bucket", null, null, null, null, 1000, true, false));

        assertThat(registry.find("swath.replay.fixture.list.latency").timer().count()).isEqualTo(1);
    }

    private static ListObjectsV2Pager pager(String... keys) {
        return new ListObjectsV2Pager(FakeListingStore.ofKeys(keys), new ReplayMetrics());
    }

    private static List<String> keys(S3ListResult result) {
        return result.entries().stream().map(e -> ByteKeys.utf8(e.key())).toList();
    }

    private static List<String> render(List<S3ResultEntry> entries) {
        return entries.stream().map(e -> switch (e) {
            case S3ResultEntry.ObjectResult object -> "O:" + ByteKeys.utf8(object.key());
            case S3ResultEntry.CommonPrefixResult prefix -> "P:" + ByteKeys.utf8(prefix.key());
        }).toList();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
