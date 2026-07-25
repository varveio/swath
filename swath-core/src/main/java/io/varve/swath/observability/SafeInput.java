/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import io.varve.swath.error.InvalidArgsException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Central policy for untrusted CLI values that may reach diagnostics or durable artifacts. */
public final class SafeInput {

    public static final String REDACTED_ENDPOINT = "<redacted endpoint>";

    private static final Set<String> ENDPOINT_OPTIONS = Set.of("--endpoint-url", "--metrics-endpoint");

    private SafeInput() {
    }

    /**
     * Parse an absolute HTTP(S) endpoint without ever echoing the supplied value in an exception.
     * Relative, opaque, and hostless URIs are rejected before their path/scheme-specific material
     * can bypass the component checks below. Userinfo, queries, and fragments are forbidden: each
     * can carry credentials, none can be reproduced safely from a redacted checkpoint during
     * resume, and endpoint credentials are not used by swath's AWS/OTLP authentication mechanisms.
     */
    public static URI endpoint(String option, String raw) throws InvalidArgsException {
        if (raw == null) {
            return null;
        }
        final URI uri;
        try {
            uri = URI.create(raw);
        } catch (IllegalArgumentException ignored) {
            throw invalidEndpoint(option, "must be a valid URI");
        }
        String scheme = uri.getScheme();
        if (!uri.isAbsolute() || uri.isOpaque()
                || scheme == null
                || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
                || uri.getHost() == null) {
            throw invalidEndpoint(option, "must be an absolute HTTP(S) URI with a host");
        }
        if (uri.getRawUserInfo() != null) {
            throw invalidEndpoint(option,
                    "must not contain userinfo; configure credentials outside the endpoint URI");
        }
        if (uri.getRawQuery() != null) {
            throw invalidEndpoint(option,
                    "must not contain a query; token, signature, and credential query parameters are unsafe");
        }
        if (uri.getRawFragment() != null) {
            throw invalidEndpoint(option, "must not contain a fragment");
        }
        return uri;
    }

    private static InvalidArgsException invalidEndpoint(String option, String reason) {
        return new InvalidArgsException("invalid " + option + ": " + reason);
    }

    /**
     * Safe persisted argv: endpoint values are represented by a fixed marker and all remaining
     * arguments have log/terminal control characters escaped. Handles both {@code --opt value}
     * and {@code --opt=value} forms.
     */
    public static List<String> argv(List<String> original) {
        if (original == null || original.isEmpty()) {
            return List.of();
        }
        List<String> safe = new ArrayList<>(original.size());
        boolean redactNext = false;
        for (String arg : original) {
            if (redactNext) {
                safe.add(REDACTED_ENDPOINT);
                redactNext = false;
                continue;
            }
            String value = arg == null ? "" : arg;
            if (ENDPOINT_OPTIONS.contains(value)) {
                safe.add(value);
                redactNext = true;
                continue;
            }
            int equals = value.indexOf('=');
            if (equals > 0 && ENDPOINT_OPTIONS.contains(value.substring(0, equals))) {
                safe.add(value.substring(0, equals + 1) + REDACTED_ENDPOINT);
                continue;
            }
            safe.add(logText(value));
        }
        return List.copyOf(safe);
    }

    /** Escape ISO control characters so one untrusted value cannot forge another log line. */
    public static String logText(String value) {
        if (value == null) {
            return "<null>";
        }
        int first = -1;
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                first = i;
                break;
            }
        }
        if (first < 0) {
            return value;
        }
        StringBuilder safe = new StringBuilder(value.length() + 8);
        safe.append(value, 0, first);
        for (int i = first; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!Character.isISOControl(c)) {
                safe.append(c);
            } else if (c <= 0xff) {
                safe.append("\\x").append(hex(c >>> 4)).append(hex(c));
            } else {
                safe.append("\\u")
                        .append(hex(c >>> 12)).append(hex(c >>> 8))
                        .append(hex(c >>> 4)).append(hex(c));
            }
        }
        return safe.toString();
    }

    private static char hex(int value) {
        return "0123456789abcdef".charAt(value & 0xf);
    }
}
