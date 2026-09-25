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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Field-policy comparator for a deterministic GCS JSON v1 page, independent of replay code. */
public final class GcsProfileComparator {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Set<String> PAGE_FIELDS = Set.of("kind", "items", "prefixes", "nextPageToken");
    private static final Set<String> REPLAY_ITEM_FIELDS = Set.of("kind", "bucket", "name", "size", "updated",
            "etag", "generation", "metageneration", "storageClass", "contentType", "acl");
    private static final Set<String> NATIVE_ONLY_FIELDS = Set.of("id", "selfLink", "mediaLink", "timeCreated",
            "md5Hash", "crc32c", "owner", "metadata", "timeStorageClassUpdated", "componentCount",
            "cacheControl", "contentEncoding", "contentDisposition", "contentLanguage", "kmsKeyName",
            "customerEncryption", "temporaryHold", "eventBasedHold", "retentionExpirationTime",
            "customTime", "softDeleteTime", "hardDeleteTime");

    private GcsProfileComparator() {
    }

    static JsonNode readPage(byte[] body, boolean replay) throws IOException {
        JsonNode page = JSON.readTree(body);
        checkPage(page, replay);
        items(page);
        prefixes(page);
        token(page);
        return page;
    }

    /** Compare complete walks while following each endpoint's own opaque token chain. */
    public static void assertWalk(List<CapturedPage> nativePages, List<CapturedPage> replayPages,
                                  String nativeBucket, String replayBucket, boolean fullProjection,
                                  int maxPages, int maxEntries) throws IOException {
        JsonNode nativeInventory = aggregate(nativePages, false, maxPages, maxEntries);
        JsonNode replayInventory = aggregate(replayPages, true, maxPages, maxEntries);
        assertPage(JSON.writeValueAsBytes(nativeInventory), JSON.writeValueAsBytes(replayInventory),
                nativeBucket, replayBucket, fullProjection);
    }

    private static JsonNode aggregate(List<CapturedPage> pages, boolean replay, int maxPages,
                                      int maxEntries) throws IOException {
        if (pages.isEmpty() || pages.size() > maxPages || maxEntries < 0) {
            fail("GCS walk page bound violated");
        }
        ObjectNode merged = JSON.createObjectNode().put("kind", "storage#objects");
        ArrayNode mergedItems = JSON.createArrayNode();
        ArrayNode mergedPrefixes = JSON.createArrayNode();
        String expectedToken = null;
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < pages.size(); i++) {
            CapturedPage capture = pages.get(i);
            equal("GCS request token at page " + i, expectedToken, capture.requestToken());
            JsonNode page = readPage(capture.body(), replay);
            for (JsonNode item : items(page)) mergedItems.add(item);
            for (String prefix : prefixes(page)) mergedPrefixes.add(prefix);
            if (mergedItems.size() + mergedPrefixes.size() > maxEntries) {
                fail("GCS walk entry bound violated");
            }
            expectedToken = token(page);
            if (i + 1 < pages.size() && (expectedToken == null || !seen.add(expectedToken))) {
                fail("GCS walk stopped early or repeated token");
            }
        }
        if (expectedToken != null) fail("GCS walk ended with nonterminal token");
        if (!mergedItems.isEmpty()) merged.set("items", mergedItems);
        if (!mergedPrefixes.isEmpty()) merged.set("prefixes", mergedPrefixes);
        return merged;
    }

    public record CapturedPage(String requestToken, byte[] body) {
        public CapturedPage {
            body = body.clone();
        }

        @Override
        public byte[] body() { return body.clone(); }
    }

    public static void assertPage(byte[] nativeBody, byte[] replayBody, String nativeBucket,
                                  String replayBucket, boolean fullProjection) throws IOException {
        JsonNode nativePage = readPage(nativeBody, false);
        JsonNode replayPage = readPage(replayBody, true);
        equal("page kind", text(nativePage, "kind"), text(replayPage, "kind"));
        List<JsonNode> nativeItems = items(nativePage);
        List<JsonNode> replayItems = items(replayPage);
        equal("item count", nativeItems.size(), replayItems.size());
        for (int i = 0; i < nativeItems.size(); i++) {
            JsonNode nativeItem = nativeItems.get(i);
            JsonNode replayItem = replayItems.get(i);
            fields(nativeItem, union(REPLAY_ITEM_FIELDS, NATIVE_ONLY_FIELDS), "native item");
            fields(replayItem, REPLAY_ITEM_FIELDS, "replay item");
            equal("item kind " + i, "storage#object", text(nativeItem, "kind"));
            equal("replay item kind " + i, "storage#object", text(replayItem, "kind"));
            equal("native bucket " + i, nativeBucket, text(nativeItem, "bucket"));
            equal("replay bucket " + i, replayBucket, text(replayItem, "bucket"));
            equal("name " + i, text(nativeItem, "name"), text(replayItem, "name"));
            equal("size " + i, decimal(nativeItem, "size"), decimal(replayItem, "size"));
            if (!text(replayItem, "updated").matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T"
                    + "[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z")) {
                fail("replay updated must use six UTC fractional digits");
            }
            equal("updated " + i, instant(nativeItem, "updated"), instant(replayItem, "updated"));
            requiredText(replayItem, "etag");
            equal("generation " + i, "1", decimal(replayItem, "generation"));
            equal("metageneration " + i, "1", decimal(replayItem, "metageneration"));
            equal("storageClass " + i, "STANDARD", text(replayItem, "storageClass"));
            equal("contentType " + i, "application/octet-stream", text(replayItem, "contentType"));
            requiredText(nativeItem, "etag");
            decimal(nativeItem, "generation");
            decimal(nativeItem, "metageneration");
            optionalText(nativeItem, "storageClass");
            optionalText(nativeItem, "contentType");
            if (fullProjection) {
                JsonNode acl = replayItem.path("acl");
                if (!acl.isArray() || !acl.isEmpty()) fail("replay full ACL must be an empty array");
            } else if (replayItem.has("acl")) {
                fail("replay noAcl projection must omit acl");
            }
            if (replayItem.has("timeCreated") || replayItem.has("owner") || replayItem.has("crc32c")) {
                fail("replay contains profile-omitted native metadata");
            }
        }
        equal("prefixes", prefixes(nativePage), prefixes(replayPage));
        boolean nativeTerminal = token(nativePage) == null;
        boolean replayTerminal = token(replayPage) == null;
        equal("terminal pagination", nativeTerminal, replayTerminal);
    }

    private static void checkPage(JsonNode page, boolean replay) {
        if (!page.isObject()) fail("GCS page must be an object");
        fields(page, PAGE_FIELDS, replay ? "replay page" : "native page");
        equal("page kind", "storage#objects", text(page, "kind"));
        if (replay && page.has("items") && page.path("items").isEmpty()) {
            fail("replay must omit empty items array");
        }
        if (replay && page.has("prefixes") && page.path("prefixes").isEmpty()) {
            fail("replay must omit empty prefixes array");
        }
    }

    private static List<JsonNode> items(JsonNode page) {
        JsonNode array = page.path("items");
        if (array.isMissingNode()) return List.of();
        if (!array.isArray()) fail("GCS items must be an array");
        List<JsonNode> result = new ArrayList<>();
        array.forEach(result::add);
        return result;
    }

    private static List<String> prefixes(JsonNode page) {
        JsonNode array = page.path("prefixes");
        if (array.isMissingNode()) return List.of();
        if (!array.isArray()) fail("GCS prefixes must be an array");
        List<String> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (JsonNode value : array) {
            if (!value.isTextual() || !seen.add(value.textValue())) fail("invalid or duplicate GCS prefix");
            result.add(value.textValue());
        }
        return result;
    }

    private static String token(JsonNode page) {
        JsonNode token = page.path("nextPageToken");
        if (token.isMissingNode()) return null;
        if (!token.isTextual() || token.textValue().isEmpty()) fail("invalid GCS nextPageToken");
        return token.textValue();
    }

    private static void fields(JsonNode node, Set<String> allowed, String label) {
        if (!node.isObject()) fail(label + " must be an object");
        node.fieldNames().forEachRemaining(field -> {
            if (!allowed.contains(field)) fail(label + " has unknown field " + field);
        });
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        Set<String> out = new HashSet<>(a);
        out.addAll(b);
        return out;
    }

    private static String text(JsonNode node, String name) {
        JsonNode value = node.path(name);
        if (!value.isTextual()) fail(name + " must be text");
        return value.textValue();
    }

    private static void requiredText(JsonNode node, String name) {
        if (text(node, name).isEmpty()) fail(name + " must be nonempty");
    }

    private static void optionalText(JsonNode node, String name) {
        if (node.has(name)) text(node, name);
    }

    private static String decimal(JsonNode node, String name) {
        String value = text(node, name);
        if (!value.matches("0|[1-9][0-9]*")) fail(name + " must be a decimal string");
        return value;
    }

    private static void optionalDecimal(JsonNode node, String name) {
        if (node.has(name)) decimal(node, name);
    }

    private static Instant instant(JsonNode node, String name) {
        try {
            return Instant.parse(text(node, name));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(name + " must be RFC3339 timestamp", e);
        }
    }

    private static void equal(String label, Object expected, Object actual) {
        if (!java.util.Objects.equals(expected, actual)) {
            fail(label + " differs: expected=" + expected + " actual=" + actual);
        }
    }

    private static void fail(String message) { throw new IllegalArgumentException(message); }
}
