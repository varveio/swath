/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.EncodingType;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The true end-to-end guard spec carried in {@code swath-model}'s {@code
 * ByteMidpointPercentEchoSafetyTest#trueEndToEndGuardSpec()}.
 *
 * <p>{@code ByteMidpointPercentEchoSafetyTest} proves — hermetically, using the exact JDK {@code
 * URLDecoder} the AWS SDK's response interceptor calls — that swath never SYNTHESIZES a pivot
 * that would crash a verbatim-echo endpoint. It cannot prove the wire-level path: that a real
 * LocalStack/MinIO endpoint really does echo {@code start-after} verbatim (unlike real S3, which
 * re-encodes; docs/internals/s3-implementation-compatibility.md), and that the SDK's always-on
 * {@code DecodeUrlEncodedResponseInterceptor} really does crash on that echo pre-fix and stays
 * clean post-fix. This class closes that gap against a real LocalStack container.
 *
 * <p>A full {@code WORK_STEALING} listing (production {@code ListRunner}/{@code S3PageFetcher}
 * path) over keys containing literal {@code %}-sequences is not exercised here at engine scale:
 * LocalStack double-URL-decodes the {@code start-after} request parameter, so a cursor whose key
 * contains a literal {@code %25} loses one decode ({@code %25} to {@code %}) and the effective
 * cursor sorts PAST the remaining {@code %25...} keys, silently skipping them with no error. Real
 * S3 (and MinIO, verified) decode {@code start-after} exactly once and round-trip losslessly;
 * swath's cursor logic — sending the raw decoded key as {@code start-after} — is correct against
 * real S3 semantics, so this is a LocalStack limitation, not a product defect, and is pinned
 * instead as a raw-SDK positive control below ({@link
 * #localStackDoubleDecodesAPercentContainingStartAfterAndSilentlySkipsKeys}). Full evidence and
 * wire trace: docs/internals/s3-implementation-compatibility.md.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class PercentEchoLocalStackIT {

    private static final String BUCKET = "percent-echo-it";

    @Container
    static final LocalStackContainer LOCALSTACK = LocalStackSupport.s3Container();

    private static S3Client s3;

    @BeforeAll
    static void setUp() {
        s3 = LocalStackSupport.client(LOCALSTACK);
        LocalStackSupport.createBucket(s3, BUCKET);
    }

    @AfterAll
    static void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    /**
     * Positive control, at the real SDK/HTTP seam: the wire-level percent-echo crash mechanism
     * is real against LocalStack. A {@code start-after} value ending in a lone {@code
     * '%'} is percent-encoded on the way OUT (so the wire carries a well-formed query string), but
     * LocalStack echoes the DECODED value verbatim (unmodified, not re-percent-encoded) into the
     * response's {@code <StartAfter>} field. The SDK's always-on {@code
     * DecodeUrlEncodedResponseInterceptor} then strict-decodes that field and throws — proving
     * the crash this whole guard chain defends against is a genuine LocalStack behavior, not just
     * a hermetic proxy of one.
     */
    @Test
    @Timeout(60)
    void verbatimEchoedLonePercentStartAfterCrashesTheRealSdkResponsePath() {
        assertThatThrownBy(() -> s3.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(BUCKET)
                        .encodingType(EncodingType.URL)
                        .startAfter("boundary%")
                        .maxKeys(10)
                        .build()))
                .satisfies(e -> assertThat(rootCause(e))
                        .as("the real LocalStack echo path throws the exact URLDecoder crash")
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Incomplete trailing escape"));
    }

    /**
     * The fixed, normal swath path: keys containing {@code %}-sequences and {@code +} characters,
     * listed through the PRODUCTION {@link S3PageFetcher} (real {@code encoding-type=url} decode
     * of the {@code <Key>} field) against the same verbatim-echo LocalStack, round-trip byte-exact.
     * Single page (no {@code start_after} cursor in play) so this test is purely about the SDK's
     * response-key decode of {@code %}/{@code +} content, independent of the cursor-echo mechanism
     * exercised by the positive control above.
     */
    @Test
    @Timeout(60)
    void percentAndPlusKeysRoundTripByteExactThroughTheProductionFetcher() throws Exception {
        List<byte[]> keys = new ArrayList<>();
        keys.add("pct/a%25b".getBytes(StandardCharsets.UTF_8));
        keys.add("pct/lone-percent-in-key%".getBytes(StandardCharsets.UTF_8));
        keys.add("pct/c%2Fd".getBytes(StandardCharsets.UTF_8));
        keys.add("pct/plus+sign+key".getBytes(StandardCharsets.UTF_8));
        keys.add("pct/x+y%20z".getBytes(StandardCharsets.UTF_8));
        keys.add("pct/multi%25%25%25".getBytes(StandardCharsets.UTF_8));
        keys.add("pct/plain".getBytes(StandardCharsets.UTF_8));

        for (byte[] key : keys) {
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET)
                            .key(new String(key, StandardCharsets.UTF_8)).build(),
                    RequestBody.fromBytes(new byte[]{1}));
        }

        S3PageFetcher fetcher = new S3PageFetcher(s3, BUCKET);
        ListPage page = fetcher.fetchPage(
                PageRequest.objects("pct/".getBytes(StandardCharsets.UTF_8), null, 100));
        assertThat(page.truncated()).as("all %/+ keys fit in one page").isFalse();

        TreeSet<byte[]> distinct = new TreeSet<>(KeyBytes::compareUnsigned);
        for (var entry : page.entries()) {
            distinct.add(entry.key().raw());
        }
        assertThat(distinct).as("every %/+ key emitted exactly once, byte-exact").hasSize(keys.size());
        for (byte[] key : keys) {
            assertThat(distinct.contains(key))
                    .as("byte-exact round trip for %s", new String(key, StandardCharsets.UTF_8))
                    .isTrue();
        }
    }

    /**
     * Positive control (raw SDK/HTTP seam): LocalStack double-URL-decodes the {@code
     * start-after} request parameter, silently skipping keys. swath sends a cursor as the raw
     * decoded key {@code sa/%25pct-0006} (correct per S3 semantics: {@code start-after} is a raw
     * key, and against real S3 or MinIO it round-trips losslessly). The SDK percent-encodes it on
     * the wire ({@code %} to {@code %25}); real S3 decodes once and compares against {@code
     * sa/%25pct-0006}, returning the later {@code sa/%25pct-0009}/{@code 0012} keys. LocalStack
     * instead decodes ONE extra time, so its effective cursor is {@code sa/%pct-0006} — which, byte
     * for byte, sorts AFTER every {@code sa/%25pct-*} key ({@code p}=0x70 &gt; {@code 2}=0x32 at the
     * position after {@code sa/%}). It therefore returns NOTHING for a start-after that should have
     * yielded two keys, with no error. The SDK-decoded echo of {@code <StartAfter>} exposes the
     * double-decode directly ({@code
     * sa/%pct-0006}). This pins the artifact so a future LocalStack fix (or a real-S3/MinIO run,
     * which never skips) is caught as a change; swath itself is unchanged. See
     * docs/internals/s3-implementation-compatibility.md.
     */
    @Test
    @Timeout(60)
    void localStackDoubleDecodesAPercentContainingStartAfterAndSilentlySkipsKeys() {
        List<String> keys = List.of("sa/%25pct-0006", "sa/%25pct-0009", "sa/%25pct-0012");
        for (String k : keys) {
            s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(k).build(),
                    RequestBody.fromBytes(new byte[]{1}));
        }

        // Control: all three %-keys exist and list correctly with no cursor (LocalStack encodes the
        // response <Key> fields faithfully — it is only the start-after ECHO/decode path that is wrong).
        ListObjectsV2Response noCursor = s3.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(BUCKET).encodingType(EncodingType.URL).prefix("sa/").maxKeys(10).build());
        assertThat(noCursor.contents().stream().map(S3Object::key))
                .as("all three %-keys are present in the bucket").containsExactlyInAnyOrderElementsOf(keys);

        // The artifact: a start-after that is itself a real %-containing key skips the later %-keys.
        ListObjectsV2Response withCursor = s3.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(BUCKET).encodingType(EncodingType.URL).prefix("sa/")
                .startAfter("sa/%25pct-0006").maxKeys(10).build());
        assertThat(withCursor.contents().stream().map(S3Object::key))
                .as("LocalStack double-decodes start-after and skips sa/%%25pct-0009 and 0012 "
                        + "(real S3 / MinIO would return both)")
                .doesNotContain("sa/%25pct-0009", "sa/%25pct-0012");
        assertThat(withCursor.startAfter())
                .as("the SDK-decoded <StartAfter> echo exposes LocalStack's extra decode: %%25 -> %%")
                .isEqualTo("sa/%pct-0006");
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }
}
