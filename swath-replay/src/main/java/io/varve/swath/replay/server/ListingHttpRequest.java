/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.util.Map;
import java.util.Set;

/** Immutable HTTP fields needed by native listing parsers. */
public record ListingHttpRequest(String method, String path, String query, Map<String, String> headers,
                                 Set<String> duplicateHeaders) {
    public ListingHttpRequest(String method, String path, String query, Map<String, String> headers) {
        this(method, path, query, headers, Set.of());
    }

    public ListingHttpRequest {
        headers = Map.copyOf(headers);
        duplicateHeaders = Set.copyOf(duplicateHeaders);
    }

    public boolean head() {
        return "HEAD".equals(method);
    }

    public String header(String name) {
        return headers.get(name.toLowerCase(java.util.Locale.ROOT));
    }

    public boolean hasDuplicateHeader(String name) {
        return duplicateHeaders.contains(name.toLowerCase(java.util.Locale.ROOT));
    }
}
