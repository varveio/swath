/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.server.ReplayMetrics;
import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The delimiter seek-to-successor hybrid: the pager scans a common-prefix run already in
 * the fetched batch up to a threshold before paying for a fresh seek to {@code successor(P)}. The
 * threshold changes only <em>cost</em>, never output — so the two sides must emit byte-identical
 * pages while the seek side issues strictly more store reads. Also covers the two carry corners:
 * {@code successor(P)} that is a real key, and an all-{@code 0xFF} prefix that ends the listing.
 */
class ListObjectsV2PagerDelimiterHybridTest {

    @Test
    void shortRunIsPureScanLongRunSeeksButOutputIsIdentical() {
        // A "d/" run of five keys then "e"; delimiter "/", no prefix, big page.
        byte[][] keys = utf8("d/00", "d/01", "d/02", "d/03", "d/04", "e");

        FakeListingStore scanStore = FakeListingStore.ofKeys(keys);
        S3ListResult scan = new ListObjectsV2Pager(scanStore, new ReplayMetrics(), 32)
                .list(delimited(10));

        FakeListingStore seekStore = FakeListingStore.ofKeys(keys);
        S3ListResult seek = new ListObjectsV2Pager(seekStore, new ReplayMetrics(), 1)
                .list(delimited(10));

        assertThat(render(scan.entries())).containsExactly("P:d/", "O:e");
        assertThat(render(seek.entries())).isEqualTo(render(scan.entries()));
        assertThat(scan.truncated()).isEqualTo(seek.truncated());
        assertThat(seekStore.calls()).isGreaterThan(scanStore.calls());   // the seek path costs more reads
    }

    @Test
    void resumeTokenIsInclusiveAtSuccessorAndPagesThroughCorrectly() {
        // Two prefixes, page size 1 so the first page ends on the rolled-up prefix P="a/".
        byte[][] keys = utf8("a/1", "a/2", "b/1", "b/2");
        ListObjectsV2Pager pager = new ListObjectsV2Pager(FakeListingStore.ofKeys(keys), new ReplayMetrics(), 32);

        S3ListResult first = pager.list(delimited(1));
        assertThat(render(first.entries())).containsExactly("P:a/");
        assertThat(first.truncated()).isTrue();
        assertThat(first.nextContinuationToken()).startsWith("v2:");

        // The token must resume PAST a/ entirely (inclusive-at-successor("a/")="a0"), landing on b/.
        S3ListResult second = pager.list(new S3ListRequest(
                "bucket", null, bytes("/"), null, first.nextContinuationToken(), 10, true, false));
        assertThat(render(second.entries())).containsExactly("P:b/");
        assertThat(second.truncated()).isFalse();
    }

    @Test
    void successorThatIsARealKeyIsEmitted() {
        // successor("a/") == "a0", which is a real object here and must appear after the CP.
        ListObjectsV2Pager pager = new ListObjectsV2Pager(
                FakeListingStore.ofKeys(utf8("a/1", "a/2", "a0", "b")), new ReplayMetrics(), 32);

        S3ListResult result = pager.list(delimited(10));

        assertThat(render(result.entries())).containsExactly("P:a/", "O:a0", "O:b");
        assertThat(result.truncated()).isFalse();
    }

    @Test
    void allFfPrefixEndsTheListingWithoutReopening() {
        byte delimiter = (byte) 0xFF;
        byte[][] keys = {
                {0x41},                       // "A"
                {(byte) 0xFF, 0x01},
                {(byte) 0xFF, 0x02}};
        ListObjectsV2Pager pager = new ListObjectsV2Pager(FakeListingStore.ofKeys(keys), new ReplayMetrics(), 32);

        S3ListResult result = pager.list(new S3ListRequest(
                "bucket", null, new byte[]{delimiter}, null, null, 10, true, false));

        // A, then the all-0xFF common prefix; the listing ends there — never reopened from the start.
        assertThat(result.entries()).hasSize(2);
        assertThat(result.entries().get(0)).isInstanceOf(S3ResultEntry.ObjectResult.class);
        assertThat(result.entries().get(1)).isInstanceOf(S3ResultEntry.CommonPrefixResult.class);
        assertThat(result.truncated()).isFalse();
        assertThat(result.nextContinuationToken()).isNull();
    }

    private static S3ListRequest delimited(int maxKeys) {
        return new S3ListRequest("bucket", null, bytes("/"), null, null, maxKeys, true, false);
    }

    private static byte[][] utf8(String... values) {
        byte[][] out = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            out[i] = bytes(values[i]);
        }
        return out;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> render(List<S3ResultEntry> entries) {
        return entries.stream().map(e -> switch (e) {
            case S3ResultEntry.ObjectResult object -> "O:" + ByteKeys.utf8(object.key());
            case S3ResultEntry.CommonPrefixResult prefix -> "P:" + ByteKeys.utf8(prefix.key());
        }).toList();
    }
}
