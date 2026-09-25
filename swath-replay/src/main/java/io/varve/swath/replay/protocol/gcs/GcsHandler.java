/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import io.varve.swath.replay.metrics.ListingObservation;
import io.varve.swath.replay.metrics.ObservationShape;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.PaginationTestProfile;
import io.varve.swath.replay.server.ListingHttpRequest;
import io.varve.swath.replay.server.ListingOperation;
import io.varve.swath.replay.server.ListingProtocolHandler;
import io.varve.swath.replay.server.PreparedPage;
import io.varve.swath.replay.server.Protocol;
import io.varve.swath.replay.server.RenderedResponse;
import io.varve.swath.replay.server.ReplayFailure;
import io.varve.swath.replay.server.ReplayRequestException;
import io.varve.swath.replay.store.ListingStore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Native GCS JSON v1 list route and response envelope. */
public final class GcsHandler implements ListingProtocolHandler {
    private static final String CONTENT_TYPE = "application/json; charset=utf-8";
    private final String bucket;
    private final GcsPager pager;
    private final ReplayMetrics metrics;

    public GcsHandler(String bucket, ListingStore store, String fixtureIdentity) {
        this(bucket, store, fixtureIdentity, null);
    }

    public GcsHandler(String bucket, ListingStore store, String fixtureIdentity, ReplayMetrics metrics) {
        this.bucket = bucket;
        this.pager = new GcsPager(store, fixtureIdentity, PaginationTestProfile.NONE, metrics);
        this.metrics = metrics;
    }

    @Override
    public Protocol protocol() {
        return Protocol.GCS;
    }

    @Override
    public ListingOperation parse(ListingHttpRequest request) {
        if (!"GET".equals(request.method())) {
            record("request_refused", "wrong_method");
            throw failure(ReplayFailure.Kind.WRONG_METHOD, "wrong_method");
        }
        if (!request.path().equals("/storage/v1/b/" + bucket + "/o")) {
            record("request_refused", "bucket_not_found");
            throw failure(ReplayFailure.Kind.NOT_FOUND, "bucket_not_found");
        }
        GcsListRequest parsed;
        try {
            parsed = GcsQuery.parse(bucket, request.query());
        } catch (GcsQuery.QueryFailure e) {
            if (metrics != null) metrics.recordProviderPath("gcs", "query_refused", e.reason());
            throw failure(e.unsupported() ? ReplayFailure.Kind.UNSUPPORTED : ReplayFailure.Kind.MALFORMED,
                    e.reason());
        }
        try {
            pager.validateToken(parsed);
        } catch (IllegalArgumentException e) {
            if (metrics != null) metrics.recordProviderPath("gcs", "token_refused", "invalid_or_mismatched");
            throw failure(ReplayFailure.Kind.MALFORMED, "invalid_page_token");
        }
        return new ListingOperation() {
            @Override
            public int initialOutputBytes() {
                // Synthetic-v1's fixed metadata is roughly 220 bytes per item before names.
                // The runner caps this reservation at configured maxResponseBytes.
                return 1024 + parsed.pageSize() * 384;
            }

            @Override
            public PreparedPage page() {
                GcsPage page = pager.list(parsed);
                ObservationShape shape = parsed.delimiter() != null ? ObservationShape.DELIMITER
                        : parsed.pageSize() == 1 ? ObservationShape.SEEK : ObservationShape.PAGE;
                ListingObservation observation = new ListingObservation(shape,
                        page.objects().size(), page.prefixes().size());
                return new PreparedPage() {
                    @Override
                    public ListingObservation observation() { return observation; }

                    @Override
                    public int initialOutputBytesHint() {
                        return Math.max(4096, 1024 + (page.objects().size() + page.prefixes().size()) * 384);
                    }

                    @Override
                    public RenderedResponse render(io.varve.swath.replay.server.BudgetedOutput output) {
                        try {
                            GcsJson.write(parsed, page, output, metrics);
                        } catch (GcsJson.FixtureProblem e) {
                            record("fixture_rejected", e.reason());
                            throw failure(ReplayFailure.Kind.FIXTURE_INCOMPATIBLE, e.reason());
                        }
                        return new RenderedResponse(200, CONTENT_TYPE, Map.of(), output.body());
                    }
                };
            }
        };
    }

    private static ReplayRequestException failure(ReplayFailure.Kind kind, String reason) {
        return new ReplayRequestException(new ReplayFailure(kind, reason, reason));
    }

    private void record(String path, String reason) {
        if (metrics != null) metrics.recordProviderPath("gcs", path, reason);
    }

    @Override
    public RenderedResponse error(ReplayFailure failure, ListingHttpRequest request) {
        int status = switch (failure.kind()) {
            case MALFORMED, UNSUPPORTED -> 400;
            case NOT_FOUND -> 404;
            case WRONG_METHOD -> 405;
            case OVERLOAD -> 503;
            case FIXTURE_INCOMPATIBLE, FIXTURE_DISORDERED, RESPONSE_TOO_LARGE, INTERNAL -> 500;
        };
        String reason = switch (failure.kind()) {
            case MALFORMED, UNSUPPORTED -> "invalid";
            case NOT_FOUND -> "notFound";
            case WRONG_METHOD -> "methodNotAllowed";
            case OVERLOAD -> "backendError";
            case FIXTURE_INCOMPATIBLE, FIXTURE_DISORDERED, RESPONSE_TOO_LARGE, INTERNAL -> "internalError";
        };
        String message = switch (failure.kind()) {
            case MALFORMED -> "Invalid GCS listing request";
            case UNSUPPORTED -> "Unsupported replay feature";
            case NOT_FOUND -> "Bucket not found";
            case WRONG_METHOD -> "Method not allowed";
            case OVERLOAD -> "Replay server busy";
            case FIXTURE_INCOMPATIBLE, FIXTURE_DISORDERED -> "Fixture incompatible with GCS listing";
            case RESPONSE_TOO_LARGE -> "Replay response too large";
            case INTERNAL -> "Internal replay error";
        };
        String body = "{\"error\":{\"code\":" + status + ",\"message\":\"" + message
                + "\",\"errors\":[{\"message\":\"" + message
                + "\",\"domain\":\"global\",\"reason\":\"" + reason + "\"}]}}";
        ByteBuffer bytes = request.head() ? ByteBuffer.allocate(0)
                : ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8));
        Map<String, String> headers = new HashMap<>();
        if (failure.reason() != null && !failure.reason().isBlank()) {
            headers.put(ReplayFailure.REASON_HEADER, failure.reason());
        }
        if (failure.kind() == ReplayFailure.Kind.WRONG_METHOD) {
            headers.put("allow", "GET");
        }
        return new RenderedResponse(status, CONTENT_TYPE, headers, bytes);
    }
}
