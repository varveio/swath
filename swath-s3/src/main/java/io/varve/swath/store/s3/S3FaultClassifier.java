/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.error.AccessDeniedException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.NoSuchBucketException;
import io.varve.swath.error.RegionRedirectException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.error.ThrottleType;
import io.varve.swath.error.UnauthorizedException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.SafeInput;
import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * The fault-classification tree {@link S3PageFetcher}'s {@code fetchPage} dispatches into for
 * every {@code catch (SdkException e)} / socket-closure {@code catch (RuntimeException e)}
 * disposition, once its own outcome-recording/interrupt-guard block has run. <b>Order-sensitive:
 * first match wins</b> — every arm below runs in the exact sequence listed; do not reorder.
 *
 * <p>Package-private: an internal collaborator of {@link S3PageFetcher}, not a public store-layer
 * surface. One instance per {@link S3PageFetcher} (constructed with that fetcher's bucket/metrics),
 * mirroring the fetcher's own per-run lifetime.
 */
final class S3FaultClassifier {

    /**
     * Deliberately {@link S3PageFetcher}'s logger, NOT this class's: the logger name is an
     * operational surface — it drives per-class levels/appenders in a deployment's logback config
     * and is rendered by the repo's {@code %logger} pattern — so every fault line here must stay
     * under the name operators already filter on.
     */
    private static final Logger log = LoggerFactory.getLogger(S3PageFetcher.class);

    private final String bucket;
    private final RunMetrics metrics;

    S3FaultClassifier(String bucket, RunMetrics metrics) {
        this.bucket = SafeInput.logText(bucket);
        this.metrics = metrics;
    }

    /**
     * Classifies an {@link SdkException} from {@code listObjectsV2} and returns the {@link
     * ListingException} {@code fetchPage} must throw, having already recorded the matching metrics
     * and emitted the matching log line. Never throws itself — the caller decides when to throw so
     * the top-of-classifier interrupt guard (in {@code fetchPage}) always runs first.
     *
     * <p>ONE total classifier for every SDK exception family: {@code AbortedException}, {@code
     * ApiCall(Attempt)TimeoutException}, {@code S3Exception}, and the generic client/service {@code
     * SdkException} families all extend {@code SdkException}, so a single dispatch method covers
     * all of them. Dispatch is most-specific-first: named exception types, then the {@code
     * S3Exception} sub-tree, then the generic (non-{@code S3Exception}) throttle/network/5xx/fatal
     * arms.
     */
    ListingException classify(SdkException e) {
        if (e instanceof AbortedException) {
            // An SDK-side request abort with NO thread interrupt (the top-of-classifier interrupt
            // guard in fetchPage already ran and did not fire) — transient/retryable, never a bare
            // InterruptedException, which TransientRetryFetcher won't retry and the seed catch would
            // mis-route to a fatal exit-1. Classified Kind.ATTEMPT_TIMEOUT (see ThrottleException.Kind
            // for why this must not vote AIMD down) with its own steal_reason (TRANSIENT/aborted) and
            // log line (s3_abort) so post-hoc analysis can tell an SDK abort apart from a genuine
            // attempt timeout; see docs/internals/metrics-internals.md §5a for the full registry.
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
            metrics.recordStealReason("TRANSIENT", "aborted");
            metrics.recordConnectionAborted();
            log.warn("s3_abort bucket={} type={}", bucket, e.getClass().getSimpleName());
            return ThrottleException.attemptTimeout("S3 listObjectsV2 aborted for bucket=" + bucket, e);
        }
        if (e instanceof ApiCallAttemptTimeoutException || e instanceof ApiCallTimeoutException) {
            // A per-attempt/per-call SDK client timeout, fired LOCALLY by the SDK (not a service
            // response) when a LIST read stalls past apiCallAttemptTimeout/apiCallTimeout. Surfaced
            // as Kind.ATTEMPT_TIMEOUT rather than a service throttle (see ThrottleException.Kind for
            // why it must not vote AIMD down); GaugedFetcher retries it (bounded, cancellation-aware).
            // See docs/internals/metrics-internals.md §5a for the full steal_reason/meter registry.
            metrics.recordThrottleEvent(ThrottleType.ATTEMPT_TIMEOUT);
            metrics.recordStealReason("TRANSIENT", "attempt_timeout");
            // ApiCallAttemptTimeoutException always resolves to a hard connection shutdown
            // (abortable.abort() -> ConnectionHolder.cancel() -> managedConn.shutdown()), never the
            // reusable-release path — a 1:1, SDK-source-confirmed connection-destroy tally.
            metrics.recordConnectionAborted();
            log.warn("s3_timeout bucket={} type={}", bucket, e.getClass().getSimpleName());
            return ThrottleException.attemptTimeout("S3 listObjectsV2 attempt timed out for bucket=" + bucket, e);
        }
        if (e instanceof S3Exception s3e) {
            return classifyS3Exception(s3e);
        }
        // Generic (non-S3Exception, non-timeout, non-abort) SdkException.
        if (isThrottle(e)) {
            metrics.recordS3Throttle();
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
            metrics.recordStealReason("THROTTLE", "slowdown");
            log.warn("s3_throttle bucket={} s3_code={}", bucket, e.getClass().getSimpleName());
            return ThrottleException.slowDown("S3 listObjectsV2 throttled (SlowDown) for bucket=" + bucket, e);
        }
        // A network-class SdkClientException (connection reset, socket read-timeout, DNS failure,
        // TLS handshake failure, ...) that survived the SDK's own retry budget reaches us here
        // (never as an S3Exception, since it never got a service response) — NOT fatal, retried by
        // GaugedFetcher (bounded, cancellation-aware) instead of aborting the run. A client-side
        // socket timeout shares its root cause with a hung read, so with apiCallAttemptTimeout=10s
        // the attempt-timeout arm above usually wins the race first. Surfaced as Kind.NETWORK (see
        // ThrottleException.Kind for why this must not vote AIMD down); see
        // docs/internals/metrics-internals.md §5a for the full steal_reason/meter registry.
        if (isNetworkExhaustion(e)) {
            metrics.recordThrottleEvent(ThrottleType.NETWORK);
            metrics.recordStealReason("TRANSIENT", "network");
            // Reaches us only after the connection was already abandoned/destroyed by the SDK (never
            // returned with a still-usable connection) — same connection-destroy tally as
            // ATTEMPT_TIMEOUT above, though less tightly source-confirmed (a broader exception
            // family here).
            metrics.recordConnectionAborted();
            log.warn("s3_network_error bucket={} type={}", bucket, e.getClass().getSimpleName());
            return ThrottleException.networkExhaustion(
                    "S3 listObjectsV2 network error (exhausted retries) for bucket=" + bucket, e);
        }
        // A generic (non-S3Exception) SdkServiceException 5xx — e.g. an opaque 500/502/504 from an
        // intermediary that the SDK could not unmarshal into the modeled S3Exception — is the same
        // transient/retryable shape as the S3Exception 5xx case above.
        if (isServerError5xx(e)) {
            metrics.recordS3Throttle();
            metrics.recordThrottleEvent(ThrottleType.SERVER_5XX);
            metrics.recordStealReason("THROTTLE", "server5xx");
            log.warn("s3_server_error bucket={} s3_code={}", bucket, e.getClass().getSimpleName());
            return ThrottleException.serverError("S3 listObjectsV2 server error (5xx) for bucket=" + bucket, e);
        }
        metrics.recordS3Error(e.getClass().getSimpleName());
        log.warn("s3_error bucket={} status={} s3_code={}",
                bucket, "unknown", e.getClass().getSimpleName());
        return new ListingException("S3 listObjectsV2 failed for bucket=" + bucket, e);
    }

    private ListingException classifyS3Exception(S3Exception s3e) {
        String code = s3ErrorCode(s3e);
        String requestId = requestId(s3e);
        // A 503 SlowDown / ServiceUnavailable (or any SDK throttling signal) reaches us only AFTER
        // the SDK's own RetryStrategy.standard() retries are exhausted (algorithms.md §5). NOT
        // fatal: surface it as a ThrottleException so the engine's gauge-aware wrapper drives AIMD
        // (reduce T, pause steals) and retries the same call. The broader non-retryable-4xx-on-probe
        // classification is deliberately out of scope here.
        if (isThrottle(s3e)) {
            metrics.recordS3Throttle();
            metrics.recordThrottleEvent(ThrottleType.SLOWDOWN);
            metrics.recordStealReason("THROTTLE", "slowdown");
            log.warn("s3_throttle bucket={} status={} s3_code={} request_id={}",
                    bucket, s3e.statusCode(), code, requestId);
            return ThrottleException.slowDown("S3 listObjectsV2 throttled (SlowDown) for bucket=" + bucket, s3e);
        }
        // A 5xx S3-side server error (500 InternalError and the 5xx class generally) reaching us
        // after the SDK's own RetryStrategy.standard() retries are exhausted is the same shape as the
        // 503/SlowDown case above: transient/retryable, not fatal. Route through ThrottleException so
        // GaugedFetcher retries the same call under AIMD; tagged "server5xx" (vs
        // "slowdown"/"attempt_timeout") so post-hoc metrics can tell the three apart. 4xx client
        // errors (403/404/etc.) are unaffected and stay fatal.
        if (isServerError5xx(s3e)) {
            metrics.recordS3Throttle();
            metrics.recordThrottleEvent(ThrottleType.SERVER_5XX);
            metrics.recordStealReason("THROTTLE", "server5xx");
            log.warn("s3_server_error bucket={} status={} s3_code={} request_id={}",
                    bucket, s3e.statusCode(), code, requestId);
            return ThrottleException.serverError("S3 listObjectsV2 server error (5xx) for bucket=" + bucket, s3e);
        }
        // A 301 PermanentRedirect. Unlike the throttle/5xx arms above, this is NOT retryable --
        // surface the typed RegionRedirectException; see its javadoc for why it's fatal, what it
        // carries, and why auto-retargeting the client is out of scope.
        if (isRegionRedirect(s3e)) {
            String correctRegion = redirectRegion(s3e);
            metrics.recordS3Error(code);
            metrics.recordStealReason("REDIRECT", "region");
            log.warn("s3_region_redirect bucket={} status={} correct_region={} request_id={}",
                    bucket, s3e.statusCode(), correctRegion, requestId);
            return new RegionRedirectException(bucket, correctRegion, s3e);
        }
        // Three more well-known PERMANENT S3 failures get a TYPED subtype here instead of falling
        // through to the generic ListingException below -- like the 301 redirect above, none of these
        // is retried or AIMD-fed. See AccessDeniedException / UnauthorizedException /
        // NoSuchBucketException for why each is fatal and typed. Matched narrowly by (status,
        // error-code) so a sibling code on the same status (e.g. a 404 NoSuchKey, which is not a
        // bucket-level failure) still takes the generic arm.
        if (isAccessDenied(s3e)) {
            metrics.recordS3Error(code);
            metrics.recordStealReason("FATAL", "access_denied");
            log.warn("s3_access_denied bucket={} status={} s3_code={} request_id={}",
                    bucket, s3e.statusCode(), code, requestId);
            return new AccessDeniedException(bucket, s3e);
        }
        if (isUnauthorized(s3e)) {
            metrics.recordS3Error(code);
            metrics.recordStealReason("FATAL", "unauthorized");
            log.warn("s3_unauthorized bucket={} status={} s3_code={} request_id={}",
                    bucket, s3e.statusCode(), code, requestId);
            return new UnauthorizedException(bucket, s3e);
        }
        if (isNoSuchBucket(s3e)) {
            metrics.recordS3Error(code);
            metrics.recordStealReason("FATAL", "no_such_bucket");
            log.warn("s3_no_such_bucket bucket={} status={} s3_code={} request_id={}",
                    bucket, s3e.statusCode(), code, requestId);
            return new NoSuchBucketException(bucket, s3e);
        }
        metrics.recordS3Error(code);
        log.warn("s3_error bucket={} status={} s3_code={} request_id={}",
                bucket, s3e.statusCode(), code, requestId);
        return new ListingException("S3 listObjectsV2 failed for bucket=" + bucket, s3e);
    }

    /**
     * Classifies a socket-closure {@link RuntimeException} — a plain (non-{@link SdkException})
     * fault whose cause chain the caller has ALREADY confirmed contains an {@link
     * IOException} (via {@link #hasIOExceptionCause}, gated before any metric/latency side
     * effect runs, per {@code fetchPage}'s own comment) — and returns the {@link ThrottleException}
     * to throw. Mirrors the generic {@link #isNetworkExhaustion} arm above (same failure shape,
     * same {@link ThrottleException.Kind#NETWORK}), plus its own recovery counter and log line so
     * post-hoc analysis can tell the wrapper-escape path apart from the SDK path.
     */
    ThrottleException classifySocketClosure(RuntimeException e) {
        metrics.recordThrottleEvent(ThrottleType.NETWORK);
        metrics.recordStealReason("TRANSIENT", "socket_closure");
        metrics.recordConnectionAborted();
        metrics.recordSocketClosureRecovered();
        log.warn("s3_socket_closure bucket={} type={} cause={}", bucket,
                e.getClass().getSimpleName(), ioCauseName(e));
        return ThrottleException.networkExhaustion(
                "S3 listObjectsV2 network error (socket closure) for bucket=" + bucket, e);
    }

    /**
     * Classify an {@link SdkException} as a transient throttle (503 {@code SlowDown} /
     * {@code ServiceUnavailable} / any SDK throttling signal) vs. a fatal error. Kept narrow on
     * purpose (algorithms.md §5 — the 503 trigger only): HTTP 503, the SDK's own {@code
     * isThrottlingException()} flag, and the canonical S3 throttle error codes.
     */
    static boolean isThrottle(SdkException e) {
        if (e instanceof SdkServiceException sse) {
            if (sse.isThrottlingException() || sse.statusCode() == 503) {
                return true;
            }
        }
        if (e instanceof AwsServiceException ase && ase.awsErrorDetails() != null) {
            String code = ase.awsErrorDetails().errorCode();
            return "SlowDown".equals(code) || "ServiceUnavailable".equals(code)
                    || "RequestThrottled".equals(code) || "Throttling".equals(code);
        }
        return false;
    }

    /**
     * Classify a non-throttle {@link SdkException} as a network-level failure that survived the
     * SDK's own retry budget: an {@link IOException} somewhere in the cause chain
     * (connection reset, read/connect timeout, DNS/TLS failure) — the request never reached S3, so
     * it can never be an {@link SdkServiceException}. {@link ApiCallAttemptTimeoutException} /
     * {@link ApiCallTimeoutException} are also {@link SdkClientException}s but are caught by their
     * own arm before this one runs.
     *
     * <p>Deliberately narrower than "any non-service {@code SdkClientException}": a
     * credential/config/signing failure is also a non-service {@code SdkClientException} but never
     * wraps an {@code IOException}, so it stays fatal instead of burning the whole retry budget on a
     * bad {@code --profile} before failing fast. See docs/internals/metrics-internals.md §5
     * "Retry-reason attribution" for the full THROTTLE-vs-TRANSIENT classification story.
     */
    static boolean isNetworkExhaustion(SdkException e) {
        if (!(e instanceof SdkClientException) || e instanceof SdkServiceException) {
            return false;
        }
        return firstIOExceptionCause(e) != null;
    }

    /**
     * Does {@code t}'s cause chain contain an {@link IOException}? The precise discriminator
     * for a client-local network fault that escaped the SDK call as a plain (non-{@link
     * SdkException}) RuntimeException — e.g. {@code UncheckedIOException(SocketException("Socket
     * closed"))} from a transient S3 500 burst, whose {@link java.net.SocketException} cause is an
     * {@link IOException}. Shares {@link #firstIOExceptionCause}'s walk with {@link
     * #isNetworkExhaustion} and {@link #ioCauseName} — all three walk the SAME chain the SAME way
     * (identity-set cycle guard, first match wins), differing only in what they report at the far
     * end. Any RuntimeException with no {@code IOException} in its cause chain (a genuine bug)
     * returns {@code false} and keeps propagating unchanged.
     */
    static boolean hasIOExceptionCause(Throwable t) {
        return firstIOExceptionCause(t) != null;
    }

    /** The simple class name of the first {@link IOException} in {@code t}'s cause chain (log context), or {@code "unknown"}. */
    private static String ioCauseName(Throwable t) {
        Throwable io = firstIOExceptionCause(t);
        return io != null ? io.getClass().getSimpleName() : "unknown";
    }

    /**
     * The single cause-chain walk shared by {@link #isNetworkExhaustion}, {@link
     * #hasIOExceptionCause}, and {@link #ioCauseName} — walks {@code t} and its causes (an
     * identity-set cycle guard against a self-referential chain) and returns the first {@link
     * IOException} found, or {@code null}. First match wins; never walks past a cycle.
     */
    private static Throwable firstIOExceptionCause(Throwable t) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        while (t != null && seen.add(t)) {
            if (t instanceof IOException) {
                return t;
            }
            t = t.getCause();
        }
        return null;
    }

    /**
     * Classify an {@link SdkException} as a retryable S3-side (or intermediary) server error: any
     * 5xx {@link SdkServiceException} status that {@link #isThrottle} did not already claim (503 is
     * handled there). 500 InternalError is the common case; other 5xx codes are treated the same way
     * since they are all server-side and transient by definition. 4xx client errors are never
     * matched here and stay fatal.
     */
    static boolean isServerError5xx(SdkException e) {
        if (e instanceof SdkServiceException sse) {
            int status = sse.statusCode();
            return status >= 500 && status < 600;
        }
        return false;
    }

    /**
     * Classify an {@link S3Exception} as a {@code 301 PermanentRedirect}: the bucket lives in a
     * region other than the one this client was built for. ListObjectsV2 returns this as HTTP 301
     * with error code {@code PermanentRedirect}; either signal alone is enough.
     */
    static boolean isRegionRedirect(S3Exception e) {
        if (e.statusCode() == 301) {
            return true;
        }
        return e.awsErrorDetails() != null && "PermanentRedirect".equals(e.awsErrorDetails().errorCode());
    }

    /**
     * Classify an {@link S3Exception} as a {@code 403 AccessDenied}: the caller is not authorized to
     * LIST the bucket (a permanent permissions/config failure). Matched on the {@code (403,
     * AccessDenied)} status+code PAIR — neither alone is sufficient — so a 403 with a different code,
     * or an {@code AccessDenied} code carried on a non-403 status, is left to the generic fatal arm.
     */
    static boolean isAccessDenied(S3Exception e) {
        return e.statusCode() == 403
                && e.awsErrorDetails() != null && "AccessDenied".equals(e.awsErrorDetails().errorCode());
    }

    /**
     * Classify an {@link S3Exception} as a {@code 401 Unauthorized}: missing/invalid/expired
     * credentials rejected at the auth layer (a permanent config failure). Matched on HTTP 401 — S3
     * uses assorted codes here ({@code InvalidAccessKeyId}, {@code InvalidToken}, a signature
     * mismatch), but they share the 401 status and the same fatal disposition.
     */
    static boolean isUnauthorized(S3Exception e) {
        return e.statusCode() == 401;
    }

    /**
     * Classify an {@link S3Exception} as a {@code 404 NoSuchBucket}: the named bucket does not exist
     * (a permanent failure). Matched on the {@code (404, NoSuchBucket)} status+code PAIR — neither
     * alone is sufficient — so a 404 {@code NoSuchKey} (not a bucket-level failure), or a {@code
     * NoSuchBucket} code carried on a non-404 status, still takes the generic fatal arm.
     */
    static boolean isNoSuchBucket(S3Exception e) {
        return e.statusCode() == 404
                && e.awsErrorDetails() != null && "NoSuchBucket".equals(e.awsErrorDetails().errorCode());
    }

    /**
     * The bucket's real region, from the {@code x-amz-bucket-region} response header S3 attaches to
     * a region-redirect response; {@code null} if absent (older/opaque intermediary responses may
     * not carry it).
     */
    static String redirectRegion(S3Exception e) {
        if (e.awsErrorDetails() == null || e.awsErrorDetails().sdkHttpResponse() == null) {
            return null;
        }
        return e.awsErrorDetails().sdkHttpResponse()
                .firstMatchingHeader("x-amz-bucket-region")
                .map(SafeInput::logText)
                .orElse(null);
    }

    static String s3ErrorCode(S3Exception e) {
        String code = e.awsErrorDetails() != null && e.awsErrorDetails().errorCode() != null
                ? e.awsErrorDetails().errorCode()
                : String.valueOf(e.statusCode());
        return SafeInput.logText(code);
    }

    static String requestId(S3Exception e) {
        return e.requestId() == null
                ? "unknown" : SafeInput.logText(e.requestId());
    }
}
