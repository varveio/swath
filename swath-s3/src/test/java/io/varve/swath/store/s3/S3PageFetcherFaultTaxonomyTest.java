/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static io.varve.swath.store.s3.S3ExceptionFixtures.redirectException;
import static io.varve.swath.store.s3.S3ExceptionFixtures.s3Exception;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.varve.swath.error.ListingException;
import io.varve.swath.error.RegionRedirectException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.store.PageRequest;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Exhaustive fault-taxonomy guard for {@link S3PageFetcher}'s single {@code catch (SdkException e)}
 * classifier: a single top-of-classifier interrupt guard
 * ({@code if (Thread.interrupted()) throw new InterruptedException(...)}) — "only OUR cancel counts
 * as an interrupt" — runs before any fault-specific classification. When the thread's own interrupt
 * flag is set the fault unwinds as a cooperative {@link InterruptedException}; otherwise it proceeds
 * to fault-specific classification, yielding a {@link ThrottleException} for the retryable families.
 *
 * <p>This test drives the REAL {@link S3PageFetcher} over the full fault taxonomy at the
 * {@code S3Client} seam (below the classification code), asserts the disposition of every family
 * (including {@code AbortedException}), and exercises an interrupt dimension and a totality check —
 * unlike {@link S3PageFetcherUnitTest}'s per-family unit checks, which drive the classifier through
 * individual fake {@code S3Client} responses rather than an exhaustive taxonomy.
 *
 * <p>Reverting {@link S3FaultClassifier}'s {@code AbortedException} arm to an unconditional
 * {@code throw new InterruptedException(...)} MUST turn the {@code AbortedException} row RED here:
 * {@link #faultClassifiesToExpectedDisposition} expects a {@link ThrottleException} of kind
 * {@link ThrottleException.Kind#ATTEMPT_TIMEOUT}, not a bare {@link InterruptedException}.
 */
class S3PageFetcherFaultTaxonomyTest {

    private static final PageRequest REQ = PageRequest.objects(null, null, 1000);

    /** The expected terminal disposition for a fault family. */
    private enum Expect {
        THROTTLE_SLOWDOWN(ThrottleException.Kind.SLOWDOWN),
        THROTTLE_SERVER_5XX(ThrottleException.Kind.SERVER_5XX),
        THROTTLE_ATTEMPT_TIMEOUT(ThrottleException.Kind.ATTEMPT_TIMEOUT),
        THROTTLE_NETWORK(ThrottleException.Kind.NETWORK),
        REGION_REDIRECT(null),
        /** Fatal, non-retryable: a {@link ListingException} that is NOT a throttle/redirect. */
        FATAL(null);

        final ThrottleException.Kind kind;

        Expect(ThrottleException.Kind kind) {
            this.kind = kind;
        }
    }

    private record Row(String name, SdkException fault, Expect expect) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static Stream<Row> faultRows() {
        SdkClientException connectionReset = SdkClientException.create(
                "Unable to execute HTTP request: Connection reset",
                new IOException("Connection reset"));
        SdkClientException credentialFailure = SdkClientException.builder()
                .message("Unable to load credentials from any of the providers in the chain")
                .build();
        return Stream.of(
                // The AbortedException-without-interrupt cell: an SDK-side abort with no thread interrupt
                // is transient, NOT a bare InterruptedException and NOT a plain/fatal ListingException.
                new Row("AbortedException(no-interrupt)", AbortedException.create("aborted"),
                        Expect.THROTTLE_ATTEMPT_TIMEOUT),
                new Row("ApiCallAttemptTimeoutException", ApiCallAttemptTimeoutException.create(5000),
                        Expect.THROTTLE_ATTEMPT_TIMEOUT),
                new Row("ApiCallTimeoutException", ApiCallTimeoutException.create(5000),
                        Expect.THROTTLE_ATTEMPT_TIMEOUT),
                new Row("S3Exception 503 SlowDown", s3Exception(503, "SlowDown"),
                        Expect.THROTTLE_SLOWDOWN),
                new Row("S3Exception 503 ServiceUnavailable", s3Exception(503, "ServiceUnavailable"),
                        Expect.THROTTLE_SLOWDOWN),
                new Row("S3Exception 500 InternalError", s3Exception(500, "InternalError"),
                        Expect.THROTTLE_SERVER_5XX),
                new Row("S3Exception 403 AccessDenied", s3Exception(403, "AccessDenied"),
                        Expect.FATAL),
                new Row("S3Exception 404 NoSuchKey", s3Exception(404, "NoSuchKey"),
                        Expect.FATAL),
                new Row("connection-reset SdkClientException", connectionReset,
                        Expect.THROTTLE_NETWORK),
                new Row("credential/config SdkClientException", credentialFailure,
                        Expect.FATAL),
                new Row("S3Exception 301 PermanentRedirect", redirectException("eu-west-1"),
                        Expect.REGION_REDIRECT),
                // Totality anchor: a generic/unknown SdkException subclass (no S3Exception,
                // no timeout, no abort, no IOException cause) must still map to a LEGAL fatal
                // disposition via the default arm — never escape unclassified. An SDK upgrade that
                // adds a new exception type lands here until it is explicitly classified.
                new Row("generic unknown SdkException", SdkException.builder().message("mystery").build(),
                        Expect.FATAL),
                // UNKNOWN-SUBTYPE anchors. The two branches
                // of the SdkException hierarchy a future SDK release is most likely to add a new type
                // under — client-side (SdkClientException) and service-side (SdkServiceException) —
                // must each land on a LEGAL codomain member even when the classifier has never seen
                // the concrete type. These PIN the CURRENT default: an unknown SdkClientException with
                // no IOException cause and no timeout/abort marker, and an unknown SdkServiceException
                // with a non-5xx, non-throttle status, BOTH fall to the fatal ListingException default
                // arm (S3FaultClassifier#classify's final default) — never an unclassified escape.
                // RESIDUAL GAP (follow-up,
                // NOT closed here): a new subtype that SHOULD be liveness-transient would ALSO read as
                // fatal here and the suite would stay green; whether such a subtype is transient is a
                // classification-default design decision, tracked separately, not a test change.
                new Row("unknown SdkClientException(no-cause)",
                        SdkClientException.builder().message("unknown-client").build(), Expect.FATAL),
                new Row("unknown SdkServiceException(non-5xx)",
                        SdkServiceException.builder().message("unknown-service").statusCode(400).build(),
                        Expect.FATAL));
    }

    /**
     * The taxonomy table: drive the real {@link S3PageFetcher} over every fault family (thread NOT
     * interrupted) and assert the exact disposition.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("faultRows")
    void faultClassifiesToExpectedDisposition(Row row) {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(row.fault()), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).as("fault %s must throw", row.name()).isNotNull();
        // A classified fault is NEVER a bare InterruptedException outside the cooperative-cancel path.
        assertThat(thrown).as("%s must not surface as a bare InterruptedException", row.name())
                .isNotInstanceOf(InterruptedException.class);

        switch (row.expect()) {
            case THROTTLE_SLOWDOWN, THROTTLE_SERVER_5XX, THROTTLE_ATTEMPT_TIMEOUT, THROTTLE_NETWORK -> {
                assertThat(thrown).as("%s -> ThrottleException", row.name())
                        .isInstanceOf(ThrottleException.class)
                        .isInstanceOf(ListingException.class);
                ThrottleException te = (ThrottleException) thrown;
                assertThat(te.kind()).as("%s kind", row.name()).isEqualTo(row.expect().kind);
                boolean expectVotes = row.expect() == Expect.THROTTLE_SLOWDOWN
                        || row.expect() == Expect.THROTTLE_SERVER_5XX;
                assertThat(te.kind().votesAimdDown())
                        .as("%s AIMD-voting", row.name()).isEqualTo(expectVotes);
            }
            case REGION_REDIRECT -> assertThat(thrown).as("%s -> RegionRedirectException", row.name())
                    .isInstanceOf(RegionRedirectException.class)
                    .isInstanceOf(ListingException.class)
                    .isNotInstanceOf(ThrottleException.class);
            case FATAL -> assertThat(thrown).as("%s -> fatal ListingException", row.name())
                    .isInstanceOf(ListingException.class)
                    .isNotInstanceOf(ThrottleException.class)
                    .isNotInstanceOf(RegionRedirectException.class);
        }
    }

    /**
     * The interrupt dimension: when the worker thread's interrupt flag IS set (our own cancel
     * interrupts workers), the single top-of-classifier guard converts ANY fault family into a
     * cooperative {@link InterruptedException} — the classification below the guard never runs.
     * This is the discriminator that keeps a genuine cancel from being mis-reported as a transient
     * throttle, and (symmetrically) an SDK-side abort with no interrupt from being mis-reported as
     * a cancel.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("faultRows")
    void interruptFlagSetTurnsEveryFamilyIntoCooperativeCancel(Row row) {
        try {
            S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(row.fault()), "b");
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> fetcher.fetchPage(REQ))
                    .as("%s with interrupt flag set -> InterruptedException", row.name())
                    .isInstanceOf(InterruptedException.class);
        } finally {
            // Thread.interrupted() in the product clears the flag on the cancel path; clear
            // defensively so a case can never leak an interrupt into the next test.
            Thread.interrupted();
        }
    }

    /**
     * Legal-disposition guard over the ENUMERATED {@link #faultRows()} taxonomy — NOT a
     * reflection-backed exhaustiveness proof over the whole {@link SdkException} hierarchy (no
     * classpath scanner is on the test runtime, so the codomain cannot be checked for every
     * concrete subtype). For each enumerated family the classifier returns a disposition inside the
     * closed codomain {@link ThrottleException} | {@link RegionRedirectException} |
     * {@link ListingException} | {@link InterruptedException}, and NEVER lets a raw
     * {@link SdkException} escape. Because {@code ThrottleException} and {@code RegionRedirectException}
     * both extend {@code ListingException}, the legal set collapses to
     * {@code ListingException | InterruptedException}.
     *
     * <p>This guards two things concretely: (i) every enumerated family maps to a legal disposition,
     * and (ii) an unknown {@code SdkClientException}/{@code SdkServiceException} subtype (the explicit
     * "unknown ..." anchor rows) falls to the documented default fatal {@link ListingException} arm
     * ({@code S3FaultClassifier#classify}'s final {@code return}), never an unclassified escape.
     *
     * <p>RESIDUAL GAP (follow-up, NOT closed here): totality holds only over the hand-written rows.
     * A genuinely NEW SDK subtype that SHOULD be liveness-transient would still read as fatal
     * {@link ListingException} and this suite would stay green — reclassifying such a subtype as
     * transient is a classification-default design decision (tracked separately), not a test fix.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("faultRows")
    void everyEnumeratedFaultFamilyYieldsALegalDisposition(Row row) {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(row.fault()), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).as("%s must throw a classified disposition, never return unhandled", row.name())
                .isNotNull();
        assertThat(thrown).as("%s must not leak the raw SDK exception", row.name())
                .isNotInstanceOf(SdkException.class);
        assertThat(thrown instanceof ListingException || thrown instanceof InterruptedException)
                .as("%s disposition %s must be in {ListingException, InterruptedException}",
                        row.name(), thrown.getClass().getName())
                .isTrue();
    }

    /**
     * Redundant, explicit spotlight on the AbortedException-without-interrupt case so a regression
     * names itself in the failure report (not just as one parameterized row). An {@code AbortedException} with NO thread
     * interrupt is a transient {@link ThrottleException.Kind#ATTEMPT_TIMEOUT} throttle — never a
     * bare {@link InterruptedException}, never a plain fatal {@link ListingException}.
     */
    @Test
    void abortedExceptionWithoutInterruptIsTransientAttemptTimeout_not191BareInterrupt() {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(AbortedException.create("aborted")), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown)
                .isInstanceOf(ThrottleException.class)
                .isNotInstanceOf(InterruptedException.class);
        ThrottleException te = (ThrottleException) thrown;
        assertThat(te.kind()).isEqualTo(ThrottleException.Kind.ATTEMPT_TIMEOUT);
        assertThat(te.kind().votesAimdDown()).as("an abort is not S3 backpressure").isFalse();
    }

    /** Guard against the fixture list silently shrinking below the documented taxonomy. */
    @Test
    void taxonomyCoversEveryDocumentedFamily() {
        List<Row> rows = faultRows().toList();
        assertThat(rows).extracting(Row::name)
                .contains("AbortedException(no-interrupt)",
                        "ApiCallAttemptTimeoutException",
                        "ApiCallTimeoutException",
                        "S3Exception 503 SlowDown",
                        "S3Exception 500 InternalError",
                        "S3Exception 403 AccessDenied",
                        "connection-reset SdkClientException",
                        "credential/config SdkClientException",
                        "generic unknown SdkException",
                        "unknown SdkClientException(no-cause)",
                        "unknown SdkServiceException(non-5xx)");
    }
}
