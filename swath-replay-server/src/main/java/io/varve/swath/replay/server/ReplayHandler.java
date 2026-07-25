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

final class ReplayHandler extends Handler.Abstract {

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
     *                           many in-flight reads (fairly), so store-level reads never wait on a
     *                           connection pool — keeping pool contention out of page-read latency.
     *                           Zero or negative leaves concurrency unbounded.
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
        return S3Xml.listBucket(boundedList(listRequest));
    }

    private S3ListResult boundedList(S3ListRequest listRequest) {
        S3ListResult result;
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
        sleepFor(latency.apply(listRequest, result));
        return result;
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
