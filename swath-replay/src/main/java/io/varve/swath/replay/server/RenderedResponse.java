/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.nio.ByteBuffer;
import java.util.Map;

/** Provider response metadata and encoded bytes owned through the write callback. */
public record RenderedResponse(int status, String contentType, Map<String, String> headers,
                               OwnedBody body) {
    public RenderedResponse {
        headers = Map.copyOf(headers);
    }

    /** Small unbudgeted error envelopes retain the existing ByteBuffer call sites. */
    public RenderedResponse(int status, String contentType, Map<String, String> headers,
                            ByteBuffer body) {
        this(status, contentType, headers, OwnedBody.unbudgeted(body));
    }
}
