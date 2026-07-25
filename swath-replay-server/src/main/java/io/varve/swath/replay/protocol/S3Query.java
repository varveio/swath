/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** A parsed S3 query string preserving raw byte values (replay fidelity depends on byte-level keys). */
final class S3Query {

    private final Map<String, byte[]> values;

    private S3Query(Map<String, byte[]> values) {
        this.values = values;
    }

    byte[] bytes(String name) {
        return values.get(name);
    }

    String value(String name) {
        byte[] value = bytes(name);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    static S3Query parse(String query) {
        Map<String, byte[]> values = new LinkedHashMap<>();
        if (query == null || query.isEmpty()) {
            return new S3Query(values);
        }
        for (String part : query.split("&", -1)) {
            int eq = part.indexOf('=');
            String name = eq >= 0 ? part.substring(0, eq) : part;
            String value = eq >= 0 ? part.substring(eq + 1) : "";
            values.put(new String(ByteKeys.percentDecode(name), StandardCharsets.UTF_8),
                    ByteKeys.percentDecode(value));
        }
        return new S3Query(values);
    }
}
