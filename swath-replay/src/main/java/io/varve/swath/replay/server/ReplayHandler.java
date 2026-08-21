/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.protocol.ListObjectsV2RequestParser;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.protocol.S3Error;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3Xml;
import io.varve.swath.sort.RowGroupOrderException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReplayHandler extends Handler.Abstract {

    private static final Logger log = LoggerFactory.getLogger(ReplayHandler.class);

    /**
     * Log every request whose own cost exceeds this many milliseconds, with the parameters that
     * produced it. Negative (the default) disables the log entirely.
     *
     * <p>{@code inject.overrun{shape}} says how many requests were slower than the profile they
     * were imitating, and {@code request.latency{shape}} says roughly how slow — but neither says
     * <b>which</b>, and a shape's cost is not uniform across a keyspace. A rollup over a prefix
     * holding ten objects and one over a prefix holding ten million are the same meter. When a
     * run's overruns concentrate in a handful of requests — which is the interesting case, because
     * it is the one a bigger machine does not fix — the only useful next question is what those
     * requests asked for, and the meters cannot answer it. This line can.
     *
     * <p>A system property rather than a CLI option, mirroring {@code swath.replay.prefetch.*}: it
     * is a diagnostic a run turns on when it has a tail to explain, not a serving parameter that a
     * receipt has to state. Off, it costs one {@code long} comparison against a {@code static
     * final} per request.
     */
    private static final String SLOW_REQUEST_LOG_PROPERTY = "swath.replay.slow-request-log-ms";
    private static final long SLOW_REQUEST_LOG_NANOS = slowRequestLogNanos();

    private static long slowRequestLogNanos() {
        long millis = Long.getLong(SLOW_REQUEST_LOG_PROPERTY, -1L);
        return millis < 0 ? -1L : millis * 1_000_000L;
    }

    private final String bucket;
    private final ListingFixture fixture;
    private final ReplayMetrics metrics;
    private final Semaphore readPermits;
    private final BiFunction<S3ListRequest, S3ListResult, Duration> latency;

    ReplayHandler(String bucket, ListingFixture fixture) {
        this(bucket, fixture, new ReplayMetrics(), 0);
    }

    /**
     * @param maxConcurrentReads if positive, request concurrency into the fixture is bounded to this
     *                           many in-flight reads (fairly). Zero or negative leaves request
     *                           admission unbounded; backing stores still enforce their own reader
     *                           limits, and page-read latency starts only after a reader is acquired.
     */
    ReplayHandler(String bucket, ListingFixture fixture, ReplayMetrics metrics, int maxConcurrentReads) {
        this(bucket, fixture, metrics, maxConcurrentReads, (req, result) -> Duration.ZERO);
    }

    /**
     * As the 4-arg constructor above, but with optional per-request latency injection (default
     * {@code Duration.ZERO} there). The hook sees the request AND the result it produced, so a
     * delay can be proportional to what the response carries — e.g. {@link ShapeLatency}'s
     * per-CommonPrefix structure-probe term. Used by the {@code serve} command's
     * {@code --inject-latency} and, test-side, via {@code ReplayLatencyAdapter} over swath-core's
     * canned {@code io.varve.swath.testkit.LatencyModel} profiles.
     */
    ReplayHandler(String bucket, ListingFixture fixture, ReplayMetrics metrics, int maxConcurrentReads,
                  BiFunction<S3ListRequest, S3ListResult, Duration> latency) {
        this.bucket = bucket;
        this.fixture = fixture;
        this.metrics = metrics;
        this.readPermits = maxConcurrentReads > 0 ? new Semaphore(maxConcurrentReads, true) : null;
        this.latency = latency;
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) {
        var sample = metrics.startTimer();
        int status = HttpStatus.OK_200;
        String body;
        try {
            body = handle(request);
        } catch (S3Error e) {
            status = e.status();
            body = S3Xml.error(e.code(), e.getMessage(), request.getHttpURI().getPath());
        } catch (RowGroupOrderException e) {
            // The fixture is internally disordered and no path can serve this request (see
            // docs/swath-replay.md, "--serving-mode"). The full report — including the
            // fixture's absolute path — goes to the server's own log; the response body carries the
            // typed reason, the file NAME and the row group, because a client is not entitled to the
            // server's filesystem layout and a sweep classifies from the reason, not from the text.
            log.error("replay refused a request over a disordered fixture: {}", e.getMessage(), e);
            status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            body = S3Xml.error("InternalError", e.redactedMessage(), request.getHttpURI().getPath());
        } catch (RuntimeException e) {
            status = HttpStatus.INTERNAL_SERVER_ERROR_500;
            body = S3Xml.error("InternalError", e.getMessage(), request.getHttpURI().getPath());
        }
        metrics.recordHttpRequest(sample, status);
        write(response, status, body, callback);
        return true;
    }

    private String handle(Request request) {
        if (!"GET".equals(request.getMethod())) {
            throw new S3Error(405, "MethodNotAllowed", "method not allowed");
        }
        String path = request.getHttpURI().getPath();
        String requestedBucket = parseBucket(path);
        if (!bucket.equals(requestedBucket)) {
            throw new S3Error(404, "NoSuchBucket", "The specified bucket does not exist");
        }
        S3ListRequest listRequest = ListObjectsV2RequestParser.parse(bucket, request.getHttpURI().getQuery());
        ServedListing served = boundedList(listRequest);
        String body = S3Xml.listBucket(served.result());
        sleepToDeadline(served.latency(), System.nanoTime() - served.startedNanos(), served.shape());
        return body;
    }

    private ServedListing boundedList(S3ListRequest listRequest) {
        S3ListResult result;
        // Started before the permit wait, unlike page.read.latency: what a client experiences from a
        // saturated server includes admission and backing-pool queueing, and this timer is the one asked
        // whether the server kept out of the way.
        long startNanos = System.nanoTime();
        var shaped = metrics.startTimer();
        if (readPermits == null) {
            result = fixture.list(listRequest);
        } else {
            try {
                readPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted acquiring a replay read permit", e);
            }
            try {
                result = fixture.list(listRequest);
            } finally {
                readPermits.release();
            }
        }
        long servedNanos = System.nanoTime() - startNanos;
        // Stopped before the injected sleep, so the meter reports the server's own cost and never
        // the delay it was configured to pretend to.
        ShapeLatency.Shape shape = ShapeLatency.classify(listRequest);
        metrics.recordShapedRequest(shaped, shape);
        logIfSlow(listRequest, result, servedNanos, shape);
        return new ServedListing(result, startNanos, shape, latency.apply(listRequest, result));
    }

    private record ServedListing(S3ListResult result, long startedNanos, ShapeLatency.Shape shape,
                                 Duration latency) { }

    /**
     * Name a request the meters can only count. See {@link #SLOW_REQUEST_LOG_PROPERTY}.
     *
     * <p>Logged after the request is served and before the injected sleep, so the number reported is
     * the server's own cost and never the delay it was told to pretend to — the same boundary
     * {@code request.latency} draws.
     */
    private static void logIfSlow(S3ListRequest request, S3ListResult result, long servedNanos,
                                  ShapeLatency.Shape shape) {
        if (SLOW_REQUEST_LOG_NANOS < 0 || servedNanos < SLOW_REQUEST_LOG_NANOS) {
            return;
        }
        log.warn("replay slow request shape={} ms={} prefix={} delimiter={} start_after={} "
                        + "continuation={} max_keys={} entries={} truncated={}",
                shape, servedNanos / 1_000_000.0, render(request.prefix()), render(request.delimiter()),
                render(request.startAfter()), request.continuationToken() == null ? "none" : "yes",
                request.maxKeys(), result.entries().size(), result.truncated());
    }

    /**
     * A request parameter as text for one log line. ISO-8859-1 because a key is bytes and this has
     * to be lossless per byte rather than valid UTF-8 — a replacement character would hide exactly
     * the odd key most likely to be interesting here.
     */
    private static String render(byte[] value) {
        return value == null || value.length == 0 ? "-" : new String(value, StandardCharsets.ISO_8859_1);
    }

    /**
     * Sleeps until the request has taken {@code target} in total, rather than sleeping {@code
     * target} on top of what it already took.
     *
     * <p><b>The distinction is the whole point of injection.</b> A profile says "this backend
     * answers a worker page in 223 ms". Sleeping the full delay after the fixture read makes the
     * client observe {@code server_cost + target}, so the server's own cost is charged to the
     * client on top of the cost it was told to simulate — and since that cost swings by two orders
     * of magnitude with the access pattern (a prefetch window hit versus a cold seek), every client
     * is charged a different surcharge for its own request shape. For a benchmark whose entire
     * purpose is to compare clients against one backend, that is not a rounding error; it is the
     * measurement inverting itself.
     *
     * <p>Waiting out the remainder instead makes the observed latency {@code max(server_cost,
     * target)}: identical for every client while the server is faster than the profile, and honest
     * about the excess when it is not — which {@code inject.overrun} then counts rather than hides.
     */
    private void sleepToDeadline(Duration target, long servedNanos, ShapeLatency.Shape shape) {
        if (target == null || target.isZero() || target.isNegative()) {
            return;
        }
        long remainingNanos = target.toNanos() - servedNanos;
        if (remainingNanos <= 0) {
            // The server was slower than the backend it is imitating, so the profile is no longer
            // what the client experiences. Recorded, never silently absorbed: an attempt whose
            // overruns are more than a rounding fraction of its requests was measured against a
            // different backend than one whose overruns are zero.
            metrics.recordInjectionOverrun(shape, -remainingNanos);
            return;
        }
        sleepFor(Duration.ofNanos(remainingNanos));
    }

    /** Real (interruptible) blocking, same shape as {@code MockPageFetcher}'s own sleep. */
    private static void sleepFor(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            return;
        }
        try {
            long millis = delay.toMillis();
            int nanos = delay.minusMillis(millis).getNano();
            Thread.sleep(millis, nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted injecting replay fixture latency", e);
        }
    }

    private static void write(Response response, int status, String body, Callback callback) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        response.setStatus(status);
        response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/xml");
        response.getHeaders().put(HttpHeader.CONTENT_LENGTH, Integer.toString(bytes.length));
        response.getHeaders().put("x-amz-request-id", "S3LISTINGREPLAY");
        response.getHeaders().put("x-amz-id-2", "S3LISTINGREPLAY");
        response.write(true, ByteBuffer.wrap(bytes), callback);
    }

    private static String parseBucket(String path) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            throw new S3Error(404, "NoSuchBucket", "The specified bucket does not exist");
        }
        String withoutSlash = path.charAt(0) == '/' ? path.substring(1) : path;
        int slash = withoutSlash.indexOf('/');
        return slash >= 0 ? withoutSlash.substring(0, slash) : withoutSlash;
    }
}
