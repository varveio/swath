/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Replaces private identifiers before anything is recorded. Each byte of an identifier matches
 * either itself or its percent escape, ignoring ASCII case, so literal, fully encoded and mixed
 * spellings (hosts, escapes and provider echoes vary) are all replaced. Placeholders come from
 * the caller.
 */
public final class Redactor {
    /** JSON object {@code {"private value": "placeholder"}}; absent means no redaction. */
    public static final String ENV = "SWATH_CAPTURE_REDACT";
    public static final Redactor NONE = new Redactor(List.of());

    private record Rule(byte[] pattern, byte[] replacement) { }

    private final List<Rule> rules;

    private Redactor(List<Rule> rules) {
        this.rules = List.copyOf(rules);
    }

    public static Redactor of(Map<String, String> replacements) {
        List<Rule> rules = new ArrayList<>();
        replacements.forEach((secret, placeholder) -> {
            if (secret == null || secret.isEmpty() || placeholder == null) {
                throw new IllegalArgumentException("redaction entries need a nonempty value and a placeholder");
            }
            rules.add(new Rule(secret.getBytes(StandardCharsets.UTF_8), placeholder.getBytes(StandardCharsets.UTF_8)));
        });
        // Longest identifiers first, so one containing another is replaced whole.
        rules.sort((a, b) -> Integer.compare(b.pattern().length, a.pattern().length));
        return new Redactor(rules);
    }

    /** Reads {@value #ENV}; the values are never echoed in errors. */
    public static Redactor fromEnvironment(String json) {
        if (json == null || json.isBlank()) {
            return NONE;
        }
        try {
            JsonNode node = new ObjectMapper().readTree(json);
            if (!node.isObject()) {
                throw new IllegalArgumentException(ENV + " must be a JSON object");
            }
            java.util.LinkedHashMap<String, String> map = new java.util.LinkedHashMap<>();
            for (var field : node.properties()) {
                if (!field.getValue().isTextual()) {
                    throw new IllegalArgumentException(ENV + " placeholders must be strings");
                }
                map.put(field.getKey(), field.getValue().textValue());
            }
            return of(map);
        } catch (IOException e) {
            throw new IllegalArgumentException(ENV + " is not valid JSON");
        }
    }

    public String apply(String text) {
        if (text == null || rules.isEmpty()) {
            return text;
        }
        return new String(apply(text.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    public byte[] apply(byte[] bytes) {
        if (bytes == null || rules.isEmpty()) {
            return bytes;
        }
        byte[] current = bytes;
        for (Rule rule : rules) {
            current = replace(current, rule);
        }
        return current;
    }

    private static byte[] replace(byte[] input, Rule rule) {
        ByteArrayOutputStream out = null;
        int copied = 0;
        for (int i = 0; i < input.length; ) {
            int end = matchEnd(input, i, rule.pattern());
            if (end < 0) {
                i++;
                continue;
            }
            if (out == null) out = new ByteArrayOutputStream(input.length);
            out.write(input, copied, i - copied);
            out.write(rule.replacement(), 0, rule.replacement().length);
            i = end;
            copied = end;
        }
        if (out == null) return input;
        out.write(input, copied, input.length - copied);
        return out.toByteArray();
    }

    /** End of a match of {@code pattern} at {@code offset}, or -1. */
    private static int matchEnd(byte[] input, int offset, byte[] pattern) {
        int k = offset;
        for (byte expected : pattern) {
            if (k >= input.length) return -1;
            if (lower(input[k]) == lower(expected)) {
                k++;
            } else if (input[k] == '%' && k + 2 < input.length
                    && hex(input[k + 1]) >= 0 && hex(input[k + 2]) >= 0
                    && lower((byte) (hex(input[k + 1]) << 4 | hex(input[k + 2]))) == lower(expected)) {
                k += 3;
            } else {
                return -1;
            }
        }
        return k;
    }

    private static int hex(byte b) {
        if (b >= '0' && b <= '9') return b - '0';
        if (b >= 'a' && b <= 'f') return b - 'a' + 10;
        if (b >= 'A' && b <= 'F') return b - 'A' + 10;
        return -1;
    }

    private static byte lower(byte b) {
        return b >= 'A' && b <= 'Z' ? (byte) (b + 32) : b;
    }
}
