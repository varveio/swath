/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** GCS JSON v1 inventory projection; full metadata comparison is a separate evidence gate. */
public final class GcsEvidence {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private GcsEvidence() {
    }

    public static Page parse(byte[] body) throws IOException {
        JsonNode root = GcsProfileComparator.readPage(body, false);
        if (!root.isObject() || !"storage#objects".equals(requiredText(root, "kind"))) {
            throw new IllegalArgumentException("expected GCS storage#objects response");
        }
        List<ObjectRow> objects = new ArrayList<>();
        JsonNode items = root.path("items");
        if (!items.isMissingNode()) {
            if (!items.isArray()) {
                throw new IllegalArgumentException("GCS items must be an array");
            }
            for (JsonNode item : items) {
                objects.add(new ObjectRow(requiredText(item, "name"), requiredDecimal(item, "size")));
            }
        }
        List<String> prefixes = new ArrayList<>();
        Set<String> seenPrefixes = new HashSet<>();
        JsonNode prefixNode = root.path("prefixes");
        if (!prefixNode.isMissingNode()) {
            if (!prefixNode.isArray()) {
                throw new IllegalArgumentException("GCS prefixes must be an array");
            }
            for (JsonNode prefix : prefixNode) {
                if (!prefix.isTextual()) {
                    throw new IllegalArgumentException("GCS prefix must be text");
                }
                if (!seenPrefixes.add(prefix.textValue())) {
                    throw new IllegalArgumentException("duplicate GCS prefix");
                }
                prefixes.add(prefix.textValue());
            }
        }
        JsonNode next = root.path("nextPageToken");
        if (!next.isMissingNode() && !next.isTextual()) {
            throw new IllegalArgumentException("GCS nextPageToken must be text");
        }
        return new Page(List.copyOf(objects), List.copyOf(prefixes), next.isMissingNode() ? null : next.textValue());
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual()) {
            throw new IllegalArgumentException("GCS " + field + " must be text");
        }
        return value.textValue();
    }

    private static long requiredDecimal(JsonNode node, String field) {
        String value = requiredText(node, field);
        if (!value.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException("GCS " + field + " must be a canonical decimal string");
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("GCS " + field + " must be a nonnegative decimal string", e);
        }
    }

    public record ObjectRow(String name, long size) {
    }

    public record Page(List<ObjectRow> objects, List<String> prefixes, String nextToken) {
    }
}
