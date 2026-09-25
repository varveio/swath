/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Native Azure flat List Blobs XML route, profile and errors. */
public final class AzureHandler implements ListingProtocolHandler {
    private static final String CONTENT_TYPE = "application/xml";
    private static final DateTimeFormatter HTTP_TIME = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", java.util.Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final AtomicLong REQUEST_IDS = new AtomicLong();
    private final String account;
    private final String container;
    private final Supplier<String> endpointBase;
    private final AzureListPager pager;
    private final ReplayMetrics metrics;

    public AzureHandler(String account, String container, Supplier<String> endpointBase,
                        ListingStore store, String fixtureIdentity, ReplayMetrics metrics) {
        this.account = account;
        this.container = container;
        this.endpointBase = endpointBase;
        this.pager = new AzureListPager(store, fixtureIdentity, PaginationTestProfile.NONE, metrics);
        this.metrics = metrics;
    }

    @Override
    public Protocol protocol() { return Protocol.AZURE; }

    @Override
    public ListingOperation parse(ListingHttpRequest request) {
        if (!request.path().equals("/" + account + "/" + container)) {
            record("request_refused", "container_not_found");
            throw failure(ReplayFailure.Kind.NOT_FOUND, "container_not_found");
        }
        if (request.head()) {
            record("request_refused", "container_head_unsupported");
            throw failure(ReplayFailure.Kind.UNSUPPORTED, "container_head_unsupported");
        }
        if (!"GET".equals(request.method())) {
            record("request_refused", "wrong_method");
            throw failure(ReplayFailure.Kind.WRONG_METHOD, "wrong_method");
        }
        AzureListRequest parsed;
        try {
            parsed = AzureQuery.parse(account, container, request);
        } catch (ReplayRequestException e) {
            record("request_refused", e.failure().reason());
            throw e;
        }
        try {
            pager.validateMarker(parsed);
        } catch (IllegalArgumentException e) {
            record("marker_refused", "invalid_or_mismatched");
            throw failure(ReplayFailure.Kind.MALFORMED, "invalid_marker");
        }
        return new ListingOperation() {
            @Override
            public int initialOutputBytes() {
                // Synthetic-v1 XML fields average under this bound for ordinary names; the runner
                // clamps reservation to maxResponseBytes and charges any growth through its budget.
                return 2048 + parsed.pageSize() * 512;
            }

            @Override
            public PreparedPage page() {
                AzureListResult result = pager.list(parsed);
                return output -> {
                    try {
                        AzureXml.write(result, endpointBase.get(), output, metrics);
                    } catch (AzureXml.FixtureProblem e) {
                        if (metrics != null) metrics.recordProviderPath("azure", "fixture_rejected", e.reason());
                        throw failure(ReplayFailure.Kind.FIXTURE_INCOMPATIBLE, e.reason());
                    }
                    return new RenderedResponse(200, CONTENT_TYPE, headers(parsed.version(),
                            parsed.clientRequestId(), null), output.buffer());
                };
            }
        };
    }

    @Override
    public RenderedResponse error(ReplayFailure failure, ListingHttpRequest request) {
        String reason = failure.reason() == null ? "" : failure.reason();
        int status = switch (failure.kind()) {
            case MALFORMED, UNSUPPORTED -> 400;
            case NOT_FOUND -> 404;
            case WRONG_METHOD -> 405;
            case OVERLOAD -> 503;
            case FIXTURE_INCOMPATIBLE, FIXTURE_DISORDERED, RESPONSE_TOO_LARGE, INTERNAL -> 500;
        };
        String code = switch (failure.kind()) {
            case MALFORMED -> switch (reason) {
                case "maxresults_out_of_range" -> "OutOfRangeQueryParameterValue";
                case "invalid_client_request_id", "duplicate_header" -> "InvalidHeaderValue";
                default -> "InvalidQueryParameterValue";
            };
            case UNSUPPORTED -> switch (reason) {
                case "invalid_header_value", "unsupported_accept" -> "InvalidHeaderValue";
                default -> "UnsupportedQueryParameter";
            };
            case NOT_FOUND -> "ContainerNotFound";
            case WRONG_METHOD -> "UnsupportedHttpVerb";
            case OVERLOAD -> "ServerBusy";
            case FIXTURE_INCOMPATIBLE, FIXTURE_DISORDERED, RESPONSE_TOO_LARGE, INTERNAL -> "InternalError";
        };
        String body = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Error><Code>" + code
                + "</Code><Message>" + code + "</Message></Error>";
        ByteBuffer bytes = request.head() ? ByteBuffer.allocate(0)
                : ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8));
        Map<String, String> headers = new HashMap<>(headers(
                request.hasDuplicateHeader("x-ms-version") ? null : request.header("x-ms-version"),
                request.hasDuplicateHeader("x-ms-client-request-id") ? null
                        : request.header("x-ms-client-request-id"), code));
        if (failure.reason() != null && !failure.reason().isBlank()) {
            headers.put(ReplayFailure.REASON_HEADER, failure.reason());
        }
        if (failure.kind() == ReplayFailure.Kind.WRONG_METHOD) {
            headers.put("allow", "GET");
        }
        return new RenderedResponse(status, CONTENT_TYPE, headers, bytes);
    }

    private static Map<String, String> headers(String version, String clientRequestId, String errorCode) {
        Map<String, String> headers = new HashMap<>();
        headers.put("x-ms-request-id", "replay-" + Long.toUnsignedString(REQUEST_IDS.incrementAndGet(), 16));
        headers.put("date", httpDate(Instant.now()));
        if (version != null && (version.equals("2026-06-06") || version.equals("2026-10-06"))) {
            headers.put("x-ms-version", version);
        }
        if (clientRequestId != null && clientRequestId.getBytes(StandardCharsets.UTF_8).length <= 1024
                && clientRequestId.indexOf('\r') < 0 && clientRequestId.indexOf('\n') < 0) {
            headers.put("x-ms-client-request-id", clientRequestId);
        }
        if (errorCode != null) headers.put("x-ms-error-code", errorCode);
        return headers;
    }

    private static ReplayRequestException failure(ReplayFailure.Kind kind, String reason) {
        return new ReplayRequestException(new ReplayFailure(kind, reason, reason));
    }

    static String httpDate(Instant now) {
        return HTTP_TIME.format(now);
    }

    private void record(String path, String reason) {
        if (metrics != null) metrics.recordProviderPath("azure", path, reason);
    }
}
