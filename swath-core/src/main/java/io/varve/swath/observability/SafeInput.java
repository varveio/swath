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

    /**
     * Marker for an option whose value can BE a credential rather than merely identify one.
     * Distinct from {@link #REDACTED_ENDPOINT} so a summary reader can tell "an endpoint was given"
     * from "a secret was given" without either value being recoverable.
     */
    public static final String REDACTED_SECRET = "<redacted secret>";

    private static final Set<String> ENDPOINT_OPTIONS = Set.of("--endpoint-url", "--metrics-endpoint");

    /**
     * Options whose value must never reach a durable artifact. {@code --bearer-token-command} is
     * nominally a command, but nothing obliges it to MINT a token — {@code --bearer-token-command
     * 'echo eyJhbGci…'} is a plausible shortcut for someone holding one already, which would put a
     * live credential in the run summary's {@code argv}. Redacting the option wholesale is the only
     * safe reading, since the difference between a minting command and an embedded secret is not
     * something this layer can determine.
     */
    private static final Set<String> SECRET_OPTIONS = Set.of("--bearer-token-command");

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
     * Safe persisted argv: endpoint values and secret-bearing values are each represented by a
     * fixed marker, and all remaining arguments have log/terminal control characters escaped.
     * Handles both {@code --opt value} and {@code --opt=value} forms.
     */
    public static List<String> argv(List<String> original) {
        if (original == null || original.isEmpty()) {
            return List.of();
        }
        List<String> safe = new ArrayList<>(original.size());
        String pendingMarker = null;
        for (String arg : original) {
            if (pendingMarker != null) {
                safe.add(pendingMarker);
                pendingMarker = null;
                continue;
            }
            String value = arg == null ? "" : arg;
            String marker = redactionMarkerFor(value);
            if (marker != null) {
                safe.add(value);
                pendingMarker = marker;
                continue;
            }
            int equals = value.indexOf('=');
            if (equals > 0) {
                String inlineMarker = redactionMarkerFor(value.substring(0, equals));
                if (inlineMarker != null) {
                    safe.add(value.substring(0, equals + 1) + inlineMarker);
                    continue;
                }
            }
            safe.add(logText(value));
        }
        return List.copyOf(safe);
    }

    /** The marker an option's VALUE must be replaced by, or {@code null} to keep it (escaped). */
    private static String redactionMarkerFor(String option) {
        if (ENDPOINT_OPTIONS.contains(option)) {
            return REDACTED_ENDPOINT;
        }
        return SECRET_OPTIONS.contains(option) ? REDACTED_SECRET : null;
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
