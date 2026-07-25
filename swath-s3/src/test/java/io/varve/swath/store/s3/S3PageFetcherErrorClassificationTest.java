/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static io.varve.swath.store.s3.S3ExceptionFixtures.redirectException;
import static io.varve.swath.store.s3.S3ExceptionFixtures.s3Exception;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.AccessDeniedException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.NoSuchBucketException;
import io.varve.swath.error.RegionRedirectException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.error.UnauthorizedException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageRequest;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A fresh run's seed/fetch that hits a WELL-KNOWN PERMANENT S3 failure ({@code 403 AccessDenied},
 * {@code 401 Unauthorized}, {@code 404 NoSuchBucket}, {@code 301 PermanentRedirect}) must surface
 * as a TYPED {@link ListingException} subtype carrying a distinct, greppable
 * {@link ListingException#errorClass()} — NOT the generic untyped {@link ListingException} that is
 * {@link S3PageFetcher}'s fallback for every other fault, whose run summary reads
 * {@code error_class=null}, which a fleet supervisor cannot tell apart from an in-process crash.
 *
 * <p><b>Disposition contract.</b> All four are FATAL/non-resumable (exit 1) — the same
 * client/credentials/bucket gets the identical response forever — so each is a {@link
 * ListingException} that is NOT a retryable {@link ThrottleException}. This mirrors the
 * {@link S3PageFetcherFaultTaxonomyTest} FATAL rows, refined from "some plain ListingException" to
 * "exactly this typed subtype with this error_class".
 */
class S3PageFetcherErrorClassificationTest {

    private static final PageRequest REQ = PageRequest.objects(null, null, 1000);

    private record Row(String name, S3Exception fault,
                       Class<? extends ListingException> type, String errorClass) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static Stream<Row> permanentRows() {
        return Stream.of(
                new Row("403 AccessDenied", s3Exception(403, "AccessDenied"),
                        AccessDeniedException.class, "access_denied"),
                new Row("401 Unauthorized", s3Exception(401, "InvalidAccessKeyId"),
                        UnauthorizedException.class, "unauthorized"),
                new Row("404 NoSuchBucket", s3Exception(404, "NoSuchBucket"),
                        NoSuchBucketException.class, "no_such_bucket"),
                new Row("301 PermanentRedirect", redirectException("eu-west-1"),
                        RegionRedirectException.class, "region_redirect"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("permanentRows")
    void permanentFailureClassifiesToTypedSubtypeWithErrorClass(Row row) {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(row.fault()), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).as("%s must throw", row.name()).isNotNull();
        // Fatal, non-retryable: a ListingException that is NEVER a retryable throttle.
        assertThat(thrown).as("%s -> fatal ListingException, never a throttle", row.name())
                .isInstanceOf(ListingException.class)
                .isNotInstanceOf(ThrottleException.class);
        // The core assertion: the EXACT typed subtype (not the generic untyped ListingException) ...
        assertThat(thrown).as("%s must be the typed %s subtype", row.name(), row.type().getSimpleName())
                .isInstanceOf(row.type());
        // ... carrying its distinct, greppable snake_case error_class.
        assertThat(((ListingException) thrown).errorClass())
                .as("%s must carry error_class=%s", row.name(), row.errorClass())
                .isEqualTo(row.errorClass());
        // The exit disposition is terminal (exit 1) for every permanent class.
        assertThat(((ListingException) thrown).exitCode())
                .as("%s is fatal — exit 1", row.name()).isEqualTo(1);
    }

    /**
     * Codomain guard: a 404 whose code is {@code NoSuchKey} (NOT a bucket-level failure) must NOT
     * be reclassified as {@link NoSuchBucketException} — it stays the generic untyped {@link
     * ListingException} (error_class=null). This pins the narrow (status, code) match so the
     * classification never over-reaches a sibling code on the same status.
     */
    @Test
    void noSuchKey404IsNotReclassifiedAsNoSuchBucket() {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(s3Exception(404, "NoSuchKey")), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).isInstanceOf(ListingException.class)
                .isNotInstanceOf(NoSuchBucketException.class)
                .isNotInstanceOf(ThrottleException.class);
        assertThat(((ListingException) thrown).errorClass())
                .as("a 404 NoSuchKey is not a permanent class — error_class stays null")
                .isNull();
    }

    /**
     * The classifier must enforce the (status, code) PAIR, not either signal alone. A 400 carrying
     * the {@code AccessDenied} code (a mismatched/synthetic combination) must NOT be reclassified
     * as {@link AccessDeniedException} — it stays the generic untyped {@link ListingException}
     * (error_class=null). (A 5xx status is deliberately avoided here: {@link
     * io.varve.swath.store.s3.S3FaultClassifier#isServerError5xx} already routes any 5xx to the
     * retryable {@link ThrottleException} arm before these classifiers run, which would validate
     * that pre-existing precedence rather than the (status, code) pairing this test pins.)
     */
    @Test
    void accessDeniedCodeOnWrongStatusIsNotReclassified() {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(s3Exception(400, "AccessDenied")), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).isInstanceOf(ListingException.class)
                .isNotInstanceOf(AccessDeniedException.class)
                .isNotInstanceOf(ThrottleException.class);
        assertThat(((ListingException) thrown).errorClass())
                .as("a 400 AccessDenied is not a permanent class — error_class stays null")
                .isNull();
    }

    /**
     * A 403 carrying a code other than {@code AccessDenied} must NOT be reclassified as
     * {@link AccessDeniedException} — it stays the generic untyped {@link ListingException}
     * (error_class=null).
     */
    @Test
    void non403AccessDeniedCodeOn403StatusIsNotReclassified() {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(s3Exception(403, "SomeOtherCode")), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).isInstanceOf(ListingException.class)
                .isNotInstanceOf(AccessDeniedException.class)
                .isNotInstanceOf(ThrottleException.class);
        assertThat(((ListingException) thrown).errorClass())
                .as("a 403 SomeOtherCode is not a permanent class — error_class stays null")
                .isNull();
    }

    /**
     * A 400 carrying the {@code NoSuchBucket} code (a mismatched/synthetic combination) must
     * NOT be reclassified as {@link NoSuchBucketException} — it stays the generic untyped
     * {@link ListingException} (error_class=null). (A non-5xx status is used for the same reason
     * as {@link #accessDeniedCodeOnWrongStatusIsNotReclassified()} above.)
     */
    @Test
    void noSuchBucketCodeOnWrongStatusIsNotReclassified() {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(s3Exception(400, "NoSuchBucket")), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).isInstanceOf(ListingException.class)
                .isNotInstanceOf(NoSuchBucketException.class)
                .isNotInstanceOf(ThrottleException.class);
        assertThat(((ListingException) thrown).errorClass())
                .as("a 400 NoSuchBucket is not a permanent class — error_class stays null")
                .isNull();
    }

    @Test
    void endpointControlledDiagnosticsCannotInjectNewlinesOrTabs() {
        S3Exception fault = (S3Exception) S3Exception.builder()
                .statusCode(301)
                .requestId("request\tid")
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("Permanent\nRedirect")
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .putHeader("x-amz-bucket-region", "us-east-1\nforged")
                                .build())
                        .build())
                .message("redirect")
                .build();
        S3FaultClassifier classifier = new S3FaultClassifier(
                "bucket", new RunMetrics(new SimpleMeterRegistry()));

        // A region redirect is a terminal one-shot fault -- no request context on its log line.
        ListingException classified = classifier.classify(fault, S3FaultClassifier.FaultContext.NONE);

        assertThat(classified).isInstanceOf(RegionRedirectException.class);
        assertThat(((RegionRedirectException) classified).correctRegion())
                .isEqualTo("us-east-1\\x0aforged");
        assertThat(S3FaultClassifier.s3ErrorCode(fault)).isEqualTo("Permanent\\x0aRedirect");
        assertThat(S3FaultClassifier.requestId(fault)).isEqualTo("request\\x09id");
        assertThat(classified.getMessage()).doesNotContain("\n").doesNotContain("\t");
    }

}
