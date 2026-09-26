/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Small replay-owned error envelope for ambiguous and unclaimed routes. */
final class ReplayEnvelopeHandler implements ListingProtocolHandler {
    static final ReplayEnvelopeHandler AMBIGUOUS = new ReplayEnvelopeHandler(
            new ReplayFailure(ReplayFailure.Kind.MALFORMED, "replay_ambiguous_request", "ambiguous request"));
    static final ReplayEnvelopeHandler NOT_FOUND = new ReplayEnvelopeHandler(
            new ReplayFailure(ReplayFailure.Kind.NOT_FOUND, "replay_not_found", "route not found"));

    private final ReplayFailure classification;

    private ReplayEnvelopeHandler(ReplayFailure classification) {
        this.classification = classification;
    }

    @Override public Protocol protocol() { return Protocol.REPLAY; }

    @Override public ListingOperation parse(ListingHttpRequest request) {
        throw new ReplayRequestException(classification);
    }

    @Override public RenderedResponse error(ReplayFailure failure, ListingHttpRequest request) {
        int status = switch (failure.kind()) {
            case MALFORMED -> 400;
            case NOT_FOUND -> 404;
            case OVERLOAD -> 503;
            default -> 500;
        };
        byte[] body = request.head() ? new byte[0]
                : ("{\"error\":\"" + failure.reason() + "\"}").getBytes(StandardCharsets.UTF_8);
        return new RenderedResponse(status, "application/json",
                failure.kind() == ReplayFailure.Kind.OVERLOAD
                        ? Map.of(ReplayFailure.REASON_HEADER, failure.reason()) : Map.of(),
                ByteBuffer.wrap(body));
    }
}
