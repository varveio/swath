/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.RangeScanners;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The AWS SDK's own (always-on) {@code
 * DecodeUrlEncodedResponseInterceptor} already percent-decodes {@code
 * encoding-type=url} response fields ({@code contents[].Key},
 * {@code CommonPrefixes[].Prefix}) before {@link S3PageFetcher} ever sees them —
 * so {@code o.key()} / {@code cp.prefix()} are already the decoded key string.
 * Percent-decoding them a second time corrupts any key whose literal bytes
 * happen to contain a {@code %XX} pattern (e.g. a real key segment like {@code
 * site_name=Coffs%20Harbour}), turning {@code %20} into a literal space and
 * silently corrupting both the emitted key and the {@code start_after} cursor
 * derived from it. These tests drive the fetcher entirely through the public
 * {@link S3PageFetcher#fetchPage} seam with a fake {@link S3Client} that returns
 * an already-SDK-decoded key/prefix, and assert the literal {@code %XX} bytes
 * survive verbatim.
 */
class S3PageFetcherKeyDecodeTest {

    @Test
    void objectKeyWithLiteralPercentEncodedLookingSegmentIsNotDoubleDecoded() throws Exception {
        String key = "dir/site_name=Coffs%20Harbour/x";
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder()
                        .isTruncated(false)
                        .contents(S3Object.builder().key(key).size(1L).build())
                        .build());

        ListPage page = new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));

        assertThat(page.entries()).hasSize(1);
        ObjectEntry entry = (ObjectEntry) page.entries().getFirst();
        assertThat(entry.key().raw()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void objectKeyWithLiteralPercentThreeDIsNotDoubleDecoded() throws Exception {
        String key = "dir/x%3Dy";
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder()
                        .isTruncated(false)
                        .contents(S3Object.builder().key(key).size(1L).build())
                        .build());

        ListPage page = new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));

        assertThat(page.entries()).hasSize(1);
        ObjectEntry entry = (ObjectEntry) page.entries().getFirst();
        assertThat(entry.key().raw()).isEqualTo(key.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void commonPrefixWithLiteralPercentEncodedLookingSegmentIsNotDoubleDecoded() throws Exception {
        String prefix = "dir/site_name=Coffs%20Harbour/";
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder()
                        .isTruncated(false)
                        .commonPrefixes(CommonPrefix.builder().prefix(prefix).build())
                        .build());

        ListPage page = new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));

        assertThat(page.commonPrefixes()).hasSize(1);
        assertThat(page.commonPrefixes().getFirst().raw()).isEqualTo(prefix.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The full response-decode matrix (extends the three cases above): every raw key shape
     * that must survive the SDK's already-decoded {@code o.key()} converted straight to UTF-8
     * bytes, verbatim, with no second percent-decode.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("rawKeyCases")
    void objectKeyMatrixIsNotDoubleDecoded(String caseName, byte[] raw) throws Exception {
        String sdkDecodedKey = new String(raw, StandardCharsets.UTF_8);
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder()
                        .isTruncated(false)
                        .contents(S3Object.builder().key(sdkDecodedKey).size(1L).build())
                        .build());

        ListPage page = new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));

        assertThat(page.entries()).hasSize(1);
        ObjectEntry entry = (ObjectEntry) page.entries().getFirst();
        assertThat(entry.key().raw()).isEqualTo(raw);
    }

    /** Same matrix, but through the common-prefix field (delimiter-mode listing). */
    @ParameterizedTest(name = "{0}")
    @MethodSource("rawKeyCases")
    void commonPrefixMatrixIsNotDoubleDecoded(String caseName, byte[] raw) throws Exception {
        String sdkDecodedPrefix = new String(raw, StandardCharsets.UTF_8);
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder()
                        .isTruncated(false)
                        .commonPrefixes(CommonPrefix.builder().prefix(sdkDecodedPrefix).build())
                        .build());

        ListPage page = new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));

        assertThat(page.commonPrefixes()).hasSize(1);
        assertThat(page.commonPrefixes().getFirst().raw()).isEqualTo(raw);
    }

    private static Stream<Arguments> rawKeyCases() {
        return Stream.of(
                Arguments.of("literal %20", bytes("dir/site_name=Coffs%20Harbour/x")),
                Arguments.of("literal %3D", bytes("dir/x%3Dy")),
                Arguments.of("literal %25", bytes("dir/z%25raw")),
                Arguments.of("trailing %", bytes("dir/trailing%")),
                Arguments.of("incomplete %2", bytes("dir/incomplete%2")),
                Arguments.of("real space (0x20)", bytes("dir/real space")),
                Arguments.of("real + (0x2B)", bytes("dir/real+plus")),
                Arguments.of("control bytes {0x01,0x02}", new byte[]{0x01, 0x02}),
                Arguments.of("3-byte UTF-8 (world)", bytes("dir/世")),
                Arguments.of("4-byte UTF-8 (supplementary plane)", bytes("dir/𝔘"))
        );
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * T-REQ-echo: the pagination half of the response-decode contract above — {@code start_after}
     * must be re-derived from the previously-emitted key's <b>raw bytes</b> and echoed back
     * byte-exactly as the next request's {@code start-after}, even when the key contains a literal
     * {@code %XX} pattern. A double-decoded response would corrupt the cursor derived from it,
     * silently stalling or repeating pagination; here a stateful fake {@link
     * S3Client} serves one already-SDK-decoded key per call (mimicking {@code maxKeys=1}
     * pagination) and captures every request's {@code start-after} so it can be checked against
     * the previous page's last emitted key.
     */
    @Test
    void paginationEchoesStartAfterByteExactAcrossLiteralPercentKeys() throws Exception {
        List<String> sortedKeys = List.of(
                "dir/site_name=Coffs%20Harbour/a",
                "dir/site_name=Coffs%20Harbour/b",
                "dir/site_name=Sydney/c",
                "dir/site_name=Sydney/pct%3Dequals",
                "dir/z%25raw");
        FakeS3Client client = paginatingOver(sortedKeys);
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        List<KeyBytes> emitted = new ArrayList<>();
        RangeScanners.of(fetcher, 1).runRange(new byte[0], ListingMode.OBJECTS, null, null, null,
                batch -> batch.forEach(e -> emitted.add(e.key())));

        // All keys emitted exactly once, byte-exact, in unsigned-sorted order.
        assertThat(emitted).hasSize(sortedKeys.size());
        for (int i = 0; i < emitted.size(); i++) {
            assertThat(emitted.get(i).raw()).isEqualTo(sortedKeys.get(i).getBytes(StandardCharsets.UTF_8));
        }
        for (int i = 1; i < emitted.size(); i++) {
            assertThat(KeyBytes.compareUnsigned(emitted.get(i - 1).raw(), emitted.get(i).raw()))
                    .as("unsigned-sorted, no dupes").isLessThan(0);
        }

        // Every request after the first echoes the PREVIOUS page's last emitted key byte-exactly
        // as start-after — the assertion that would fail if the response were double-decoded (a
        // double-decoded cursor never advances past a literal-%XX key, tripping RangeScanner's
        // "no forward progress" abort).
        List<String> capturedStartAfters = client.capturedRequests().stream()
                .map(ListObjectsV2Request::startAfter)
                .toList();
        assertThat(capturedStartAfters).hasSize(sortedKeys.size());
        assertThat(capturedStartAfters.getFirst()).isNull();
        for (int i = 1; i < capturedStartAfters.size(); i++) {
            assertThat(capturedStartAfters.get(i).getBytes(StandardCharsets.UTF_8))
                    .isEqualTo(emitted.get(i - 1).raw());
        }
    }

    /**
     * A fake {@link S3Client} that mimics {@code maxKeys=1} ListObjectsV2 pagination over a
     * fixed sorted keyset, returning keys ALREADY-decoded (as the SDK's {@code
     * DecodeUrlEncodedResponseInterceptor} would hand them to {@link S3PageFetcher}); every
     * request's {@code start-after} is available via {@link FakeS3Client#capturedRequests()} for
     * the pagination-echo assertion above.
     */
    private static FakeS3Client paginatingOver(List<String> sortedKeys) {
        return FakeS3Client.respondingWith(request -> {
            String startAfter = request.startAfter();
            String next = null;
            for (String k : sortedKeys) {
                if (startAfter == null || k.compareTo(startAfter) > 0) {
                    next = k;
                    break;
                }
            }
            if (next == null) {
                return ListObjectsV2Response.builder().isTruncated(false).build();
            }
            boolean hasMore = sortedKeys.indexOf(next) < sortedKeys.size() - 1;
            return ListObjectsV2Response.builder()
                    .isTruncated(hasMore)
                    .contents(S3Object.builder().key(next).size(1L).build())
                    .build();
        });
    }
}
