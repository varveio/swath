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
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.RegionRedirectException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.ChecksumType;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.S3Object;

/** Unit checks for the S3 response mapping helpers: ETag normalization and timestamp conversion. */
class S3PageFetcherUnitTest {

    @Test
    void stripsSurroundingEtagQuotes() {
        assertThat(S3PageFetcher.stripEtagQuotes("\"d41d8cd98f00b204e9800998ecf8427e\""))
                .isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
    }

    @Test
    void keepsMultipartEtagFormVerbatim() {
        // Multipart ETags are hex-N; quotes stripped but the -N kept.
        assertThat(S3PageFetcher.stripEtagQuotes("\"9bb58f26192e4ba00f01e2e7b136bbd8-5\""))
                .isEqualTo("9bb58f26192e4ba00f01e2e7b136bbd8-5");
    }

    @Test
    void handlesNullAndUnquotedEtags() {
        assertThat(S3PageFetcher.stripEtagQuotes(null)).isNull();
        assertThat(S3PageFetcher.stripEtagQuotes("noquotes")).isEqualTo("noquotes");
    }

    @Test
    void convertsInstantToEpochMicros() {
        assertThat(S3PageFetcher.toEpochMicros(Instant.ofEpochSecond(1, 234_000))).isEqualTo(1_000_234L);
        assertThat(S3PageFetcher.toEpochMicros(null)).isEqualTo(0L);
    }

    @Test
    void s3CapabilitiesDoNotClaimVersionsUntilFetcherImplementsThem() {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.captureOnly(), "bucket");
        assertThat(fetcher.capabilities().supportsVersions()).isFalse();

        PageRequest versions = new PageRequest(ListingMode.VERSIONS, 1000,
                null, null, null, null, null, null, null, 0);
        assertThatThrownBy(() -> fetcher.fetchPage(versions))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * --fetch-owner: the flag must set {@code FetchOwner=true} on the ListObjectsV2
     * request — that is the only path by which S3 returns the Owner and {@code owner_id} becomes
     * reachable. A capturing fake client records the exact request the fetcher issues.
     */
    @Test
    void fetchOwnerFlagSetsFetchOwnerOnRequest() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        PageRequest req = PageRequest.objects(null, null, 1000);

        new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withFetchOwner(true)).fetchPage(req);
        assertThat(client.lastRequest().fetchOwner()).as("FetchOwner=true when --fetch-owner set").isTrue();

        new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT).fetchPage(req);
        assertThat(client.lastRequest().fetchOwner()).as("FetchOwner unset by default").isNull();
    }

    /**
     * --request-payer requester: the flag must set {@code x-amz-request-payer: requester}
     * (the SDK-modeled {@code RequestPayer.REQUESTER}) on the ListObjectsV2 request — that is the
     * only path by which a requester-pays bucket accepts the request instead of rejecting it. A
     * capturing fake client records the exact request the fetcher issues; absent the flag the
     * request-payer property is unset.
     */
    @Test
    void requestPayerFlagSetsRequestPayerOnRequest() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        PageRequest req = PageRequest.objects(null, null, 1000);

        new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withRequestPayer(true).withMetrics(new RunMetrics(new SimpleMeterRegistry())))
                .fetchPage(req);
        assertThat(client.lastRequest().requestPayerAsString())
                .as("x-amz-request-payer=requester when --request-payer requester set")
                .isEqualTo("requester");

        new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(new RunMetrics(new SimpleMeterRegistry())))
                .fetchPage(req);
        assertThat(client.lastRequest().requestPayer()).as("request-payer unset by default").isNull();
    }

    /**
     * {@link PageRequest#attemptTimeoutEscalationLevel()}, when non-zero, must be mapped onto the
     * ListObjectsV2 request's per-request {@code overrideConfiguration} — this is the ONLY wiring
     * that lets the escalation retry loops (TransientRetryFetcher / GaugedFetcher) actually lengthen
     * the SDK's per-attempt budget for a single retried attempt. At level 0 a scan-class call still
     * carries an {@code overrideConfiguration} for the direct-page carrier, but leaves {@code
     * apiCallAttemptTimeout} unset so the client-level base timeout applies.
     */
    @Test
    void escalationLevelIsMappedOntoTheRequestAsAnAttemptTimeout() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();

        new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));
        assertThat(client.lastRequest().overrideConfiguration())
                .as("the direct-page carrier does not change the default attempt timeout")
                .hasValueSatisfying(o -> assertThat(o.apiCallAttemptTimeout()).isEmpty());

        // Level 1 on a scan-class call (worker page, 10s base) -> 10s * 2^1 = 20s.
        PageRequest escalated = PageRequest.objects(null, null, 1000)
                .withAttemptTimeoutEscalationLevel(1);
        new S3PageFetcher(client, "bucket").fetchPage(escalated);
        assertThat(client.lastRequest().overrideConfiguration())
                .as("level 1 maps to 2x the scan-class base as a per-request override")
                .hasValueSatisfying(o -> assertThat(o.apiCallAttemptTimeout())
                        .hasValue(Duration.ofSeconds(20)));
    }

    /**
     * The Owner {@code displayName} and {@code checksumType} are siblings of the
     * already-captured {@code owner().id()} / {@code checksumAlgorithm} — no extra request
     * param, populated straight off the same {@code S3Object} when {@code --fetch-owner} is set.
     */
    @Test
    void mapsOwnerDisplayNameAndChecksumType() throws Exception {
        S3Object object = S3Object.builder()
                .key("k")
                .owner(Owner.builder().id("id1").displayName("Alice").build())
                .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                .checksumType(ChecksumType.FULL_OBJECT)
                .build();
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder().isTruncated(false).contents(object).build());

        ListPage page = new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withFetchOwner(true)).fetchPage(PageRequest.objects(null, null, 1000));

        assertThat(page.entries()).hasSize(1);
        ObjectEntry entry = (ObjectEntry) page.entries().getFirst();
        assertThat(entry.ownerId()).isEqualTo("id1");
        assertThat(entry.ownerDisplayName()).isEqualTo("Alice");
        assertThat(entry.checksumAlgorithm()).isEqualTo("SHA256");
        assertThat(entry.checksumType()).isEqualTo("FULL_OBJECT");
    }

    @Test
    void preservesAnUnknownChecksumAlgorithmAsTheRawSdkString() throws Exception {
        S3Object object = S3Object.builder()
                .key("k")
                .checksumAlgorithmWithStrings("FUTURE_CHECKSUM")
                .build();
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder().isTruncated(false).contents(object).build());

        ListPage page = new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));

        ObjectEntry entry = (ObjectEntry) page.entries().getFirst();
        assertThat(entry.checksumAlgorithm()).isEqualTo("FUTURE_CHECKSUM");
    }

    @Test
    void treatsAnExplicitlyEmptyChecksumAlgorithmListAsAbsent() throws Exception {
        S3Object object = S3Object.builder()
                .key("k")
                .checksumAlgorithmWithStrings(List.of())
                .build();
        S3Client client = FakeS3Client.respondingWith(
                ListObjectsV2Response.builder().isTruncated(false).contents(object).build());

        ListPage page = new S3PageFetcher(client, "bucket").fetchPage(PageRequest.objects(null, null, 1000));

        ObjectEntry entry = (ObjectEntry) page.entries().getFirst();
        assertThat(entry.checksumAlgorithm()).isNull();
    }

    // ---- throttle classification (algorithms.md §5; THR-1 wiring) -------------------

    @Test
    void classifiesSlowDownAndServiceUnavailableAsThrottle() {
        assertThat(S3FaultClassifier.isThrottle(s3Exception(503, "SlowDown"))).isTrue();
        assertThat(S3FaultClassifier.isThrottle(s3Exception(503, "ServiceUnavailable"))).isTrue();
        // 503 status alone is enough, even without a recognised error code.
        assertThat(S3FaultClassifier.isThrottle(s3Exception(503, "Whatever"))).isTrue();
        // Canonical throttle error codes are throttle even if the status is not 503.
        assertThat(S3FaultClassifier.isThrottle(s3Exception(400, "RequestThrottled"))).isTrue();
        assertThat(S3FaultClassifier.isThrottle(s3Exception(400, "Throttling"))).isTrue();
    }

    @Test
    void doesNotClassifyOrdinaryErrorsAsThrottle() {
        assertThat(S3FaultClassifier.isThrottle(s3Exception(404, "NoSuchKey"))).isFalse();
        assertThat(S3FaultClassifier.isThrottle(s3Exception(403, "AccessDenied"))).isFalse();
        assertThat(S3FaultClassifier.isThrottle(SdkException.builder().message("network blip").build())).isFalse();
    }

    /**
     * A 503 {@code SlowDown} surfaced by the SDK must reach the engine as a
     * {@link ThrottleException} (driven into AIMD + retried), NOT a fatal
     * {@link ListingException}; an ordinary error stays a fatal {@link ListingException}.
     */
    @Test
    void throttleSurfacesAsThrottleException_otherErrorsStayFatal() {
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(s3Exception(503, "SlowDown")), "b").fetchPage(req))
                .isInstanceOf(ThrottleException.class);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(s3Exception(403, "AccessDenied")), "b").fetchPage(req))
                .isInstanceOf(ListingException.class)
                .isNotInstanceOf(ThrottleException.class);
    }

    /**
     * Each throw site must carry the {@link ThrottleType}
     * matching its adjacent {@code recordStealReason(...)} tag exactly, so
     * {@code swath.throttle.events{type}} agrees with the {@code steal_reason} detail. (The
     * fetcher classifies via {@link ThrottleException.Kind}; {@link ThrottleException#type()}
     * derives the {@link ThrottleType} 1:1 from that kind.)
     */
    @Test
    void eachThrowSiteCarriesTheMatchingThrottleType() {
        PageRequest req = PageRequest.objects(null, null, 1000);
        SdkClientException connectionReset = SdkClientException.create(
                "Unable to execute HTTP request: Connection reset",
                new IOException("Connection reset"));

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(s3Exception(503, "SlowDown")), "b").fetchPage(req))
                .asInstanceOf(type(ThrottleException.class))
                .extracting(ThrottleException::type).isEqualTo(ThrottleType.SLOWDOWN);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(s3Exception(500, "InternalError")), "b").fetchPage(req))
                .asInstanceOf(type(ThrottleException.class))
                .extracting(ThrottleException::type).isEqualTo(ThrottleType.SERVER_5XX);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000)), "b")
                .fetchPage(req))
                .asInstanceOf(type(ThrottleException.class))
                .extracting(ThrottleException::type).isEqualTo(ThrottleType.ATTEMPT_TIMEOUT);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(connectionReset), "b").fetchPage(req))
                .asInstanceOf(type(ThrottleException.class))
                .extracting(ThrottleException::type).isEqualTo(ThrottleType.NETWORK);
    }

    // ---- API-call (attempt) timeout classification ----------------------------------

    /**
     * An {@link ApiCallAttemptTimeoutException} — a per-attempt SDK client timeout fired locally
     * when a LIST read stalls — is retryable-transient, so it still surfaces as a
     * {@link ThrottleException} (retried by the engine, NOT a fatal {@link ListingException} that
     * aborts the run). But it is NOT S3 backpressure: it carries
     * {@link ThrottleException.Kind#ATTEMPT_TIMEOUT}, whose {@code votesAimdDown()} is {@code false},
     * so the retry wrapper does NOT drive the AIMD concurrency target down. (Folding a hung local
     * read into the AIMD vote like a 503 SlowDown would strangle concurrency down to 1 under any
     * attempt-timeout burst — this is precisely the behavior this classification avoids.)
     */
    @Test
    void apiCallAttemptTimeoutSurfacesAsNonVotingThrottleException_notFatal() {
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000)), "b")
                .fetchPage(req))
                .isInstanceOf(ThrottleException.class)
                .satisfies(ex -> {
                    ThrottleException.Kind kind = ((ThrottleException) ex).kind();
                    assertThat(kind).isEqualTo(ThrottleException.Kind.ATTEMPT_TIMEOUT);
                    assertThat(kind.votesAimdDown()).as("attempt-timeout must NOT vote AIMD down").isFalse();
                });
    }

    /** Same non-voting classification for the whole-call variant, {@link ApiCallTimeoutException}. */
    @Test
    void apiCallTimeoutSurfacesAsNonVotingThrottleException_notFatal() {
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(ApiCallTimeoutException.create(5000)), "b")
                .fetchPage(req))
                .isInstanceOf(ThrottleException.class)
                .satisfies(ex -> {
                    ThrottleException.Kind kind = ((ThrottleException) ex).kind();
                    assertThat(kind).isEqualTo(ThrottleException.Kind.ATTEMPT_TIMEOUT);
                    assertThat(kind.votesAimdDown()).isFalse();
                });
    }

    /**
     * Instrumentation discipline: a retried attempt-timeout must stay observable but
     * NOT under the AIMD-driving {@code THROTTLE} steal_reason. It is recorded (a) as its own
     * {@code TRANSIENT.attempt_timeout} steal_reason, (b) on the unified typed
     * {@code swath.throttle.events{type=attempt_timeout}} counter — and (c) it is deliberately NOT
     * folded into {@code errors{type=throttle}}.
     */
    @Test
    void apiCallAttemptTimeoutIsTaggedTransientNotThrottleInMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000)), "b", S3PageFetcherConfig.DEFAULT.withMetrics(metrics)).fetchPage(req))
                .isInstanceOf(ThrottleException.class);

        // Its own TRANSIENT steal_reason, NOT the AIMD-driving THROTTLE outcome.
        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "TRANSIENT", "reason", "attempt_timeout").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("swath.steal_reason")
                .tags("outcome", "THROTTLE", "reason", "attempt_timeout").counter()).isNull();
        // Unified typed throttle-events counter, type=attempt_timeout (the single recording site).
        assertThat(registry.get("swath.throttle.events")
                .tags("type", "attempt_timeout").counter().count()).isEqualTo(1.0);
        // Not folded into errors{type=throttle}.
        assertThat(registry.find("swath.errors").tags("type", "throttle").counter()).isNull();
    }

    /**
     * An attempt-timeout aborts + destroys its connection (SDK-source-confirmed:
     * {@code abortable.abort()} always reaches {@code managedConn.shutdown()}, never the reusable-
     * release path), so the classification site must also bump the connection-churn counter — a
     * distinct series from {@code swath.throttle.events{type=attempt_timeout}}.
     */
    @Test
    void apiCallAttemptTimeoutIncrementsConnectionAbortedCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000)), "b", S3PageFetcherConfig.DEFAULT.withMetrics(metrics)).fetchPage(req))
                .isInstanceOf(ThrottleException.class);

        assertThat(registry.get("swath.s3.pool.connection_aborted").counter().count()).isEqualTo(1.0);
    }

    // ---- network-exhaustion / server-5xx classification ------------------------------

    /**
     * An exhausted network-class {@link SdkClientException} (connection reset, socket
     * read-timeout, DNS failure, TLS failure, ...) that survived the SDK's own
     * {@code RetryStrategy} retries must reach the engine as a {@link ThrottleException} (retried,
     * NOT a fatal {@link ListingException} that aborts the whole run). It is
     * decoupled from the AIMD vote for the same reason as an attempt-timeout: a client-side socket
     * fault is not S3 backpressure. So it carries {@link ThrottleException.Kind#NETWORK}, whose
     * {@code votesAimdDown()} is {@code false}.
     */
    @Test
    void exhaustedConnectionResetSurfacesAsNonVotingThrottleException_notFatal() {
        PageRequest req = PageRequest.objects(null, null, 1000);
        SdkClientException connectionReset = SdkClientException.create(
                "Unable to execute HTTP request: Connection reset",
                new IOException("Connection reset"));

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(connectionReset), "b").fetchPage(req))
                .isInstanceOf(ThrottleException.class)
                .satisfies(ex -> {
                    ThrottleException.Kind kind = ((ThrottleException) ex).kind();
                    assertThat(kind).isEqualTo(ThrottleException.Kind.NETWORK);
                    assertThat(kind.votesAimdDown()).as("network fault must NOT vote AIMD down").isFalse();
                });
    }

    /**
     * A 500 InternalError (an S3-side 5xx server error) reaching us after retry exhaustion is
     * transient — must surface as a {@link ThrottleException}, NOT a fatal {@link ListingException}.
     * Unlike a network fault, a 5xx IS a server-side backpressure signal, so it carries
     * {@link ThrottleException.Kind#SERVER_5XX} and DOES vote AIMD down.
     */
    @Test
    void internalErrorSurfacesAsVotingThrottleException_notFatal() {
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(s3Exception(500, "InternalError")), "b")
                .fetchPage(req))
                .isInstanceOf(ThrottleException.class)
                .satisfies(ex -> {
                    ThrottleException.Kind kind = ((ThrottleException) ex).kind();
                    assertThat(kind).isEqualTo(ThrottleException.Kind.SERVER_5XX);
                    assertThat(kind.votesAimdDown()).as("5xx server error must vote AIMD down").isTrue();
                });
    }

    /** A 403/404 client error stays fatal — retriability must not broaden to permanent errors. */
    @Test
    void clientErrorStaysFatalAfterNetworkAndServerErrorClassification() {
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(s3Exception(403, "AccessDenied")), "b")
                .fetchPage(req))
                .isInstanceOf(ListingException.class)
                .isNotInstanceOf(ThrottleException.class);
    }

    @Test
    void classifiesNetworkExhaustionAndServerErrorsButNotClientErrors() {
        // A bare SdkClientException with no IOException in its cause chain (e.g. a
        // credential/config/signing failure) is NOT network exhaustion — see
        // credentialFailureStaysFatal_notLivelockedAsThrottle below for the anti-livelock guard.
        assertThat(S3FaultClassifier.isNetworkExhaustion(SdkClientException.create("boom"))).isFalse();
        assertThat(S3FaultClassifier.isNetworkExhaustion(
                SdkClientException.create("boom", new IOException("reset")))).isTrue();
        assertThat(S3FaultClassifier.isNetworkExhaustion(s3Exception(500, "InternalError"))).isFalse();
        assertThat(S3FaultClassifier.isNetworkExhaustion(SdkException.builder().message("network blip").build())).isFalse();

        assertThat(S3FaultClassifier.isServerError5xx(s3Exception(500, "InternalError"))).isTrue();
        assertThat(S3FaultClassifier.isServerError5xx(s3Exception(502, "BadGateway"))).isTrue();
        assertThat(S3FaultClassifier.isServerError5xx(s3Exception(403, "AccessDenied"))).isFalse();
        assertThat(S3FaultClassifier.isServerError5xx(SdkClientException.create("boom"))).isFalse();
    }

    /**
     * Anti-livelock guard: a credential/config-style {@link SdkClientException} (e.g. "unable to
     * load credentials from any of the providers") has NO {@link IOException} in its
     * cause chain — it is a deterministic config/auth failure, not a transient network fault.
     * Because the engine retries a {@link ThrottleException} unboundedly (algorithms.md §5),
     * misclassifying this as network exhaustion would livelock a bad {@code --profile} /
     * missing-credentials run forever instead of failing fast. It must stay a fatal
     * {@link ListingException}.
     */
    @Test
    void credentialFailureStaysFatal_notLivelockedAsThrottle() {
        PageRequest req = PageRequest.objects(null, null, 1000);
        SdkClientException credentialFailure = SdkClientException.builder()
                .message("Unable to load credentials from any of the providers in the chain")
                .build();

        assertThat(S3FaultClassifier.isNetworkExhaustion(credentialFailure)).isFalse();
        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(credentialFailure), "b").fetchPage(req))
                .isInstanceOf(ListingException.class)
                .isNotInstanceOf(ThrottleException.class);
    }

    /**
     * The retry-reason attribution discipline extends to the two extra causes, but along the
     * same voting split: a network fault is a non-voting {@code TRANSIENT.network} (like an
     * attempt-timeout), while a 5xx server error is an AIMD-voting {@code THROTTLE.server5xx} (like a
     * 503 slowdown). Both also bump their unified {@code swath.throttle.events{type}} counter.
     */
    @Test
    void networkAndServer5xxAreTaggedAlongTheVotingSplitInMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        PageRequest req = PageRequest.objects(null, null, 1000);
        SdkClientException connectionReset = SdkClientException.create(
                "Unable to execute HTTP request: Connection reset",
                new IOException("Connection reset"));

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(connectionReset), "b", S3PageFetcherConfig.DEFAULT.withMetrics(metrics))
                .fetchPage(req))
                .isInstanceOf(ThrottleException.class);
        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "TRANSIENT", "reason", "network").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("swath.steal_reason")
                .tags("outcome", "THROTTLE", "reason", "network").counter()).isNull();
        assertThat(registry.get("swath.throttle.events")
                .tags("type", "network").counter().count()).isEqualTo(1.0);
        // A network fault also reaches this arm only after its connection has already been
        // abandoned/destroyed — the same connection-churn tally the attempt-timeout arm bumps.
        assertThat(registry.get("swath.s3.pool.connection_aborted").counter().count()).isEqualTo(1.0);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(s3Exception(500, "InternalError")), "b", S3PageFetcherConfig.DEFAULT.withMetrics(metrics)).fetchPage(req))
                .isInstanceOf(ThrottleException.class);
        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "THROTTLE", "reason", "server5xx").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.get("swath.throttle.events")
                .tags("type", "server5xx").counter().count()).isEqualTo(1.0);
        // A server5xx does NOT force-abort the connection (retryable within the same lease), so the
        // connection_aborted count from the network fault above must not have grown.
        assertThat(registry.get("swath.s3.pool.connection_aborted").counter().count()).isEqualTo(1.0);
    }

    // ---- 301 PermanentRedirect / region mismatch classification ---------------------

    @Test
    void classifiesPermanentRedirectByStatusOrErrorCode() {
        assertThat(S3FaultClassifier.isRegionRedirect(redirectException(301, "PermanentRedirect", "eu-west-1"))).isTrue();
        // Either signal alone is enough.
        assertThat(S3FaultClassifier.isRegionRedirect(redirectException(301, "Whatever", null))).isTrue();
        assertThat(S3FaultClassifier.isRegionRedirect(redirectException(400, "PermanentRedirect", null))).isTrue();
        assertThat(S3FaultClassifier.isRegionRedirect(s3Exception(404, "NoSuchKey"))).isFalse();
        assertThat(S3FaultClassifier.isRegionRedirect(s3Exception(403, "AccessDenied"))).isFalse();
    }

    @Test
    void extractsBucketRegionFromRedirectHeader() {
        assertThat(S3FaultClassifier.redirectRegion(redirectException(301, "PermanentRedirect", "ap-southeast-2")))
                .isEqualTo("ap-southeast-2");
        assertThat(S3FaultClassifier.redirectRegion(redirectException(301, "PermanentRedirect", null))).isNull();
    }

    /**
     * A 301 PermanentRedirect from a cross-region bucket must surface as a typed
     * {@link RegionRedirectException} naming the correct region — never as an untyped/opaque
     * crash, and never mis-classified as a retryable {@link ThrottleException} (a redirect will
     * never succeed against this client, so retrying it under AIMD would spin forever).
     */
    @Test
    void permanentRedirectSurfacesAsTypedRegionRedirectException_notCrashNotThrottle() {
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(
                FakeS3Client.throwing(redirectException(301, "PermanentRedirect", "eu-west-1")), "b").fetchPage(req))
                .isInstanceOf(RegionRedirectException.class)
                .isNotInstanceOf(ThrottleException.class)
                .satisfies(ex -> {
                    RegionRedirectException rre = (RegionRedirectException) ex;
                    assertThat(rre.bucket()).isEqualTo("b");
                    assertThat(rre.correctRegion()).isEqualTo("eu-west-1");
                    assertThat(rre.getMessage()).contains("eu-west-1");
                })
                .isInstanceOf(ListingException.class); // still exit-1 fatal via the sealed hierarchy
    }

    /** No {@code x-amz-bucket-region} header: still a typed exception, region simply unknown. */
    @Test
    void permanentRedirectWithoutRegionHeaderStillTyped() {
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(
                FakeS3Client.throwing(redirectException(301, "PermanentRedirect", null)), "b").fetchPage(req))
                .isInstanceOf(RegionRedirectException.class)
                .satisfies(ex -> assertThat(((RegionRedirectException) ex).correctRegion()).isNull());
    }

    /**
     * The instrumentation discipline requirement (§5): a region redirect must be
     * distinguishable from throttle/network/server5xx in the metrics alone via the
     * {@code swath.steal_reason{outcome=REDIRECT,reason=region}} engagement counter.
     */
    @Test
    void permanentRedirectIsTaggedDistinctlyInMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        PageRequest req = PageRequest.objects(null, null, 1000);

        assertThatThrownBy(() -> new S3PageFetcher(FakeS3Client.throwing(redirectException(301, "PermanentRedirect", "eu-west-1")), "b", S3PageFetcherConfig.DEFAULT.withMetrics(metrics))
                .fetchPage(req))
                .isInstanceOf(RegionRedirectException.class);

        assertThat(registry.get("swath.steal_reason")
                .tags("outcome", "REDIRECT", "reason", "region").counter().count())
                .isEqualTo(1.0);
        assertThat(registry.find("swath.steal_reason")
                .tags("outcome", "THROTTLE", "reason", "slowdown").counter()).isNull();
    }

}
