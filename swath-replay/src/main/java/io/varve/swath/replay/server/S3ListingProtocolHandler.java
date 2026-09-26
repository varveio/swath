/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.metrics.ListingObservation;
import io.varve.swath.replay.metrics.ObservationShape;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ListObjectsV2RequestParser;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.protocol.S3Error;
import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.protocol.S3Xml;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** S3's existing parser, pager and byte grammar behind the shared two-stage request contract. */
final class S3ListingProtocolHandler implements ListingProtocolHandler {
    private static final Logger log = LoggerFactory.getLogger(S3ListingProtocolHandler.class);
    private static final long SLOW_REQUEST_LOG_NANOS = slowRequestLogNanos();
    private static final Map<String, String> RESPONSE_HEADERS = Map.of(
            "x-amz-request-id", "S3LISTINGREPLAY", "x-amz-id-2", "S3LISTINGREPLAY");

    private final String bucket;
    private final ListingFixture fixture;
    private final ReplayMetrics metrics;
    private final BiFunction<S3ListRequest, S3ListResult, Duration> latency;

    S3ListingProtocolHandler(String bucket, ListingFixture fixture, ReplayMetrics metrics,
                             BiFunction<S3ListRequest, S3ListResult, Duration> latency) {
        this.bucket = bucket;
        this.fixture = fixture;
        this.metrics = metrics;
        this.latency = latency;
    }

    @Override
    public Protocol protocol() { return Protocol.S3; }

    @Override
    public ListingOperation parse(ListingHttpRequest request) {
        if (!"GET".equals(request.method())) {
            throw new S3Error(405, "MethodNotAllowed", "method not allowed");
        }
        if (!bucket.equals(parseBucket(request.path()))) {
            throw new S3Error(404, "NoSuchBucket", "The specified bucket does not exist");
        }
        S3ListRequest listRequest = ListObjectsV2RequestParser.parse(bucket, request.query());
        return new ListingOperation() {
            private io.micrometer.core.instrument.Timer.Sample shaped;
            private long startedNanos;

            @Override
            public int initialOutputBytes() {
                return Math.max(4096, 512 + listRequest.pageSize() * 320);
            }

            @Override
            public void beforePage() {
                startedNanos = System.nanoTime();
                shaped = metrics.startTimer();
            }

            @Override
            public PreparedPage page() {
                S3ListResult result = fixture.list(listRequest);
                io.varve.swath.replay.metrics.RequestShape shape = ShapeLatency.classify(listRequest);
                metrics.recordShapedRequest(shaped, shape);
                logIfSlow(listRequest, result, System.nanoTime() - startedNanos, shape);
                Duration delay = latency.apply(listRequest, result);
                long objects = 0;
                for (S3ResultEntry entry : result.entries()) {
                    if (entry instanceof S3ResultEntry.ObjectResult) objects++;
                }
                long prefixes = result.entries().size() - objects;
                ObservationShape neutralShape = switch (shape) {
                    case WORKER_PAGE -> ObservationShape.PAGE;
                    case PIVOT_PROBE -> ObservationShape.SEEK;
                    case STRUCTURE_PROBE -> ObservationShape.DELIMITER;
                };
                ListingObservation observation = new ListingObservation(neutralShape, objects, prefixes);
                return new PreparedPage() {
                    @Override
                    public int initialOutputBytesHint() {
                        return Math.max(4096, 512 + result.entries().size() * 320);
                    }

                    @Override
                    public ListingObservation observation() { return observation; }

                    @Override
                    public RenderedResponse render(BudgetedOutput output) {
                        OwnedBody body = S3Xml.listBucketBody(result, output);
                        return new RenderedResponse(200, "application/xml", RESPONSE_HEADERS, body);
                    }

                    @Override
                    public Duration injectedLatency() { return delay; }

                    @Override
                    public LongConsumer injectionOverrunRecorder() {
                        return nanos -> metrics.recordInjectionOverrun(shape, nanos);
                    }
                };
            }
        };
    }

    @Override
    public RenderedResponse error(ReplayFailure failure, ListingHttpRequest request) {
        String code = switch (failure.kind()) {
            case MALFORMED -> "InvalidArgument";
            case UNSUPPORTED -> "NotImplemented";
            case NOT_FOUND -> "NoSuchBucket";
            case WRONG_METHOD -> "MethodNotAllowed";
            case OVERLOAD -> "ServiceUnavailable";
            default -> "InternalError";
        };
        int status = switch (failure.kind()) {
            case MALFORMED -> 400;
            case UNSUPPORTED -> 501;
            case NOT_FOUND -> 404;
            case WRONG_METHOD -> 405;
            case OVERLOAD -> 503;
            default -> 500;
        };
        return error(status, code, failure.message(), request.path(), failure.reason());
    }

    RenderedResponse s3Error(S3Error error, String path) {
        return error(error.status(), error.code(), error.getMessage(), path, null);
    }

    private static RenderedResponse error(int status, String code, String message, String resource,
                                          String reason) {
        byte[] body = S3Xml.error(code, message == null ? "internal replay error" : message, resource)
                .getBytes(StandardCharsets.UTF_8);
        Map<String, String> headers = reason == null ? RESPONSE_HEADERS : Map.of(
                "x-amz-request-id", "S3LISTINGREPLAY", "x-amz-id-2", "S3LISTINGREPLAY",
                ReplayFailure.REASON_HEADER, reason);
        return new RenderedResponse(status, "application/xml", headers, ByteBuffer.wrap(body));
    }

    private static String parseBucket(String path) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            throw new S3Error(404, "NoSuchBucket", "The specified bucket does not exist");
        }
        String withoutSlash = path.charAt(0) == '/' ? path.substring(1) : path;
        int slash = withoutSlash.indexOf('/');
        return slash >= 0 ? withoutSlash.substring(0, slash) : withoutSlash;
    }

    private static long slowRequestLogNanos() {
        long millis = Long.getLong("swath.replay.slow-request-log-ms", -1L);
        return millis < 0 ? -1L : millis * 1_000_000L;
    }

    private static void logIfSlow(S3ListRequest request, S3ListResult result, long servedNanos,
                                  io.varve.swath.replay.metrics.RequestShape shape) {
        if (SLOW_REQUEST_LOG_NANOS < 0 || servedNanos < SLOW_REQUEST_LOG_NANOS) {
            return;
        }
        log.warn("replay slow request shape={} ms={} prefix={} delimiter={} start_after={} "
                        + "continuation={} max_keys={} entries={} truncated={}",
                shape, servedNanos / 1_000_000.0, render(request.prefix()), render(request.delimiter()),
                render(request.startAfter()), request.continuationToken() == null ? "none" : "yes",
                request.maxKeys(), result.entries().size(), result.truncated());
    }

    private static String render(byte[] value) {
        return value == null || value.length == 0 ? "-" : new String(value, StandardCharsets.ISO_8859_1);
    }
}
