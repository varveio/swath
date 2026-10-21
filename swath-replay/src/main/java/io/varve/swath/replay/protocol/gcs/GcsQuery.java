/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Strict native query parser. Raw plus currently remains plus, pending the live probe. */
public final class GcsQuery {
    private static final Set<String> ALLOWED = Set.of("prefix", "delimiter", "startOffset", "endOffset",
            "maxResults", "pageToken", "projection", "versions", "softDeleted", "includeFoldersAsPrefixes",
            "includeTrailingDelimiter", "matchGlob", "filter", "fields", "prettyPrint", "alt");

    private GcsQuery() {
    }

    public static GcsListRequest parse(String bucket, String rawQuery) {
        Map<String, String> params = new HashMap<>();
        if (rawQuery != null && !rawQuery.isEmpty()) {
            for (String part : rawQuery.split("&", -1)) {
                int eq = part.indexOf('=');
                String name = decode(eq < 0 ? part : part.substring(0, eq));
                String value = decode(eq < 0 ? "" : part.substring(eq + 1));
                if (!ALLOWED.contains(name) || params.putIfAbsent(name, value) != null) {
                    throw malformed("unknown_or_duplicate_parameter");
                }
            }
        }
        for (String name : Set.of("versions", "softDeleted", "includeFoldersAsPrefixes",
                "includeTrailingDelimiter")) {
            String value = params.get(name);
            if (value != null && !"false".equals(value)) {
                throw unsupported("feature_" + name);
            }
        }
        for (String name : Set.of("matchGlob", "filter", "fields")) {
            if (params.containsKey(name) && !params.get(name).isEmpty()) {
                throw unsupported("feature_" + name);
            }
        }
        String alt = params.get("alt");
        if (alt != null && !"json".equals(alt)) {
            throw unsupported("alt_format");
        }
        String pretty = params.get("prettyPrint");
        if (pretty != null && !"true".equals(pretty) && !"false".equals(pretty)) {
            throw malformed("invalid_pretty_print");
        }
        String projection = params.getOrDefault("projection", "noAcl");
        if (!"noAcl".equals(projection) && !"full".equals(projection)) {
            throw malformed("invalid_projection");
        }
        int pageSize = 1000;
        boolean pageSizeClamped = false;
        String rawSize = params.get("maxResults");
        if (rawSize != null) {
            if (!rawSize.matches("0|[1-9][0-9]*")) {
                throw malformed("invalid_max_results");
            }
            try {
                long requested = Long.parseLong(rawSize);
                if (requested <= 0) {
                    throw new NumberFormatException();
                }
                pageSize = (int) Math.min(1000L, requested);
                pageSizeClamped = requested > 1000;
            } catch (NumberFormatException e) {
                throw malformed("invalid_max_results");
            }
        }
        return new GcsListRequest(bucket, value(params, "prefix"), value(params, "delimiter"),
                value(params, "startOffset"), value(params, "endOffset"), pageSize,
                value(params, "pageToken"), "full".equals(projection), pageSizeClamped);
    }

    private static String value(Map<String, String> params, String key) {
        String value = params.get(key);
        if (value == null || value.isEmpty()) {
            return null;
        }
        int limit = "pageToken".equals(key) ? 2048 : 1024;
        if (value.getBytes(StandardCharsets.UTF_8).length > limit) {
            throw unsupported("field_too_long_" + key);
        }
        return value;
    }

    private static String decode(String raw) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(raw.length());
        for (int i = 0; i < raw.length();) {
            char c = raw.charAt(i);
            if (c == '%') {
                if (i + 2 >= raw.length()) {
                    throw malformed("invalid_percent_escape");
                }
                int hi = asciiHex(raw.charAt(i + 1));
                int lo = asciiHex(raw.charAt(i + 2));
                if (hi < 0 || lo < 0) {
                    throw malformed("invalid_percent_escape");
                }
                bytes.write((hi << 4) | lo);
                i += 3;
            } else {
                int cp = raw.codePointAt(i);
                if (cp >= Character.MIN_SURROGATE && cp <= Character.MAX_SURROGATE) {
                    throw malformed("invalid_unicode");
                }
                bytes.writeBytes(new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8));
                i += Character.charCount(cp);
            }
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes.toByteArray())).toString();
        } catch (CharacterCodingException e) {
            throw malformed("invalid_utf8");
        }
    }

    private static int asciiHex(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }

    private static QueryFailure malformed(String reason) {
        return new QueryFailure(false, reason);
    }

    private static QueryFailure unsupported(String reason) {
        return new QueryFailure(true, reason);
    }

    public static final class QueryFailure extends IllegalArgumentException {
        private final boolean unsupported;
        private final String reason;

        private QueryFailure(boolean unsupported, String reason) {
            super(reason);
            this.unsupported = unsupported;
            this.reason = reason;
        }

        public boolean unsupported() { return unsupported; }
        public String reason() { return reason; }
    }
}
