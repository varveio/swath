/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import io.varve.swath.replay.server.ListingHttpRequest;
import io.varve.swath.replay.server.ReplayFailure;
import io.varve.swath.replay.server.ReplayRequestException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Strict Azure List Blobs query/headers, separate from the S3 and GCS parsers. */
public final class AzureQuery {
    private static final Set<String> ALLOWED = Set.of("restype", "comp", "prefix", "delimiter",
            "marker", "maxresults", "startFrom", "timeout", "include");

    private AzureQuery() {
    }

    public static AzureListRequest parse(String account, String container, ListingHttpRequest request) {
        if (request.hasDuplicateHeader("x-ms-version") || request.hasDuplicateHeader("x-ms-client-request-id")) {
            throw failure(ReplayFailure.Kind.MALFORMED, "duplicate_header");
        }
        String version = request.header("x-ms-version");
        if (!"2026-06-06".equals(version) && !"2026-10-06".equals(version)) {
            throw failure(ReplayFailure.Kind.UNSUPPORTED, "invalid_header_value");
        }
        String accept = request.header("accept");
        if (!acceptsXmlOnly(accept)) {
            throw failure(ReplayFailure.Kind.UNSUPPORTED, "unsupported_accept");
        }
        String clientRequestId = request.header("x-ms-client-request-id");
        if (clientRequestId != null && (clientRequestId.getBytes(StandardCharsets.UTF_8).length > 1024
                || clientRequestId.indexOf('\r') >= 0 || clientRequestId.indexOf('\n') >= 0)) {
            throw failure(ReplayFailure.Kind.MALFORMED, "invalid_client_request_id");
        }
        Map<String, String> params = new HashMap<>();
        if (request.query() != null && !request.query().isEmpty()) {
            for (String part : request.query().split("&", -1)) {
                int eq = part.indexOf('=');
                String key = decode(eq < 0 ? part : part.substring(0, eq));
                String value = decode(eq < 0 ? "" : part.substring(eq + 1));
                if (!ALLOWED.contains(key)) {
                    throw failure(ReplayFailure.Kind.UNSUPPORTED, "unsupported_query_parameter");
                }
                if (params.putIfAbsent(key, value) != null) {
                    throw failure(ReplayFailure.Kind.MALFORMED, "duplicate_query_parameter");
                }
            }
        }
        if (!"container".equals(params.get("restype")) || !"list".equals(params.get("comp"))) {
            throw failure(ReplayFailure.Kind.MALFORMED, "invalid_selector");
        }
        if (params.containsKey("include") && !params.get("include").isEmpty()) {
            throw failure(ReplayFailure.Kind.UNSUPPORTED, "unsupported_include");
        }
        if (params.containsKey("timeout")) {
            int timeout = decimal(params.get("timeout"), Integer.MAX_VALUE, "invalid_timeout");
            if (timeout <= 0 || timeout > 3600) {
                throw failure(ReplayFailure.Kind.MALFORMED, "invalid_timeout");
            }
        }
        String prefix = bounded(params.get("prefix"), 4096);
        String delimiter = bounded(params.get("delimiter"), 4096);
        String startFrom = bounded(params.get("startFrom"), 4096);
        String marker = bounded(params.get("marker"), 8192);
        if (invalidXmlEcho(prefix) || invalidXmlEcho(delimiter)) {
            throw failure(ReplayFailure.Kind.UNSUPPORTED, "xml_query_echo_unmeasured");
        }
        if (startFrom != null && delimiter != null) {
            throw failure(ReplayFailure.Kind.UNSUPPORTED, "startfrom_delimiter_unmeasured");
        }
        int pageSize = 5000;
        String rawSize = params.get("maxresults");
        if (rawSize != null) {
            if (rawSize.matches("-[0-9]+")) {
                throw failure(ReplayFailure.Kind.MALFORMED, "maxresults_out_of_range");
            }
            int requested = decimal(rawSize, Integer.MAX_VALUE, "invalid_maxresults");
            if (requested < 1) {
                throw failure(ReplayFailure.Kind.MALFORMED, "maxresults_out_of_range");
            }
            pageSize = Math.min(5000, requested);
        }
        return new AzureListRequest(account, container, version, prefix, delimiter, startFrom, marker,
                pageSize, rawSize, params.containsKey("prefix"), params.containsKey("delimiter"),
                params.containsKey("marker"), params.containsKey("maxresults"), clientRequestId);
    }

    private static int decimal(String value, int max, String reason) {
        if (value == null || !value.matches("[0-9]+") || value.length() > 10) {
            throw failure(ReplayFailure.Kind.MALFORMED, reason);
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed > max) {
                throw failure(ReplayFailure.Kind.MALFORMED, reason);
            }
            return (int) parsed;
        } catch (NumberFormatException e) {
            throw failure(ReplayFailure.Kind.MALFORMED, reason);
        }
    }

    private static String bounded(String value, int maxBytes) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw failure(ReplayFailure.Kind.UNSUPPORTED, "query_field_too_long");
        }
        return value;
    }

    private static boolean invalidXmlEcho(String value) {
        if (value == null) return false;
        for (int i = 0; i < value.length();) {
            int cp = value.codePointAt(i);
            if (cp < 0x20 && cp != '\t' && cp != '\n'
                    || cp == 0xfffe || cp == 0xffff) return true;
            i += Character.charCount(cp);
        }
        return false;
    }

    private static boolean acceptsXmlOnly(String accept) {
        if (accept == null || accept.isBlank()) return true;
        boolean xmlAccepted = false;
        for (String part : accept.split(",")) {
            String[] pieces = part.trim().toLowerCase(Locale.ROOT).split(";", -1);
            String media = pieces[0].trim();
            if (media.equals("application/vnd.apache.arrow.stream")) return false;
            double quality = 1.0;
            for (int i = 1; i < pieces.length; i++) {
                String parameter = pieces[i].trim();
                if (parameter.startsWith("q=")) {
                    try { quality = Double.parseDouble(parameter.substring(2)); }
                    catch (NumberFormatException e) { return false; }
                }
            }
            if (quality < 0 || quality > 1) return false;
            if ((media.equals("application/xml") || media.equals("*/*")) && quality > 0) {
                xmlAccepted = true;
            }
        }
        return xmlAccepted;
    }

    private static String decode(String raw) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(raw.length());
        for (int i = 0; i < raw.length();) {
            char c = raw.charAt(i);
            if (c == '%') {
                if (i + 2 >= raw.length()) {
                    throw failure(ReplayFailure.Kind.MALFORMED, "invalid_percent_escape");
                }
                int hi = hex(raw.charAt(i + 1));
                int lo = hex(raw.charAt(i + 2));
                if (hi < 0 || lo < 0) {
                    throw failure(ReplayFailure.Kind.MALFORMED, "invalid_percent_escape");
                }
                bytes.write((hi << 4) | lo);
                i += 3;
            } else {
                int cp = raw.codePointAt(i);
                if (cp >= Character.MIN_SURROGATE && cp <= Character.MAX_SURROGATE) {
                    throw failure(ReplayFailure.Kind.MALFORMED, "invalid_unicode");
                }
                bytes.writeBytes(new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8));
                i += Character.charCount(cp);
            }
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes.toByteArray())).toString();
        } catch (CharacterCodingException e) {
            throw failure(ReplayFailure.Kind.MALFORMED, "invalid_utf8");
        }
    }

    private static int hex(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }

    private static ReplayRequestException failure(ReplayFailure.Kind kind, String reason) {
        return new ReplayRequestException(new ReplayFailure(kind, reason, reason));
    }
}
