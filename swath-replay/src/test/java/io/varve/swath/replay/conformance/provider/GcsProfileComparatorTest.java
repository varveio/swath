/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.gcs.GcsJson;
import io.varve.swath.replay.protocol.gcs.GcsListRequest;
import io.varve.swath.replay.protocol.gcs.GcsPage;
import io.varve.swath.replay.server.BudgetedOutput;
import io.varve.swath.replay.testkit.OwnedBodyBytes;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class GcsProfileComparatorTest {
    private static final String NATIVE = """
            {"kind":"storage#objects","items":[{"kind":"storage#object","bucket":"live",
             "name":"a/1","size":"12","updated":"2026-01-01T00:00:00.123Z","etag":"native-etag",
             "generation":"42","metageneration":"3","storageClass":"STANDARD","contentType":"text/plain",
             "id":"live/a/1/42","timeCreated":"2025-12-01T00:00:00Z"}],"prefixes":["b/"]}
            """;
    private static final String REPLAY = """
            {"kind":"storage#objects","items":[{"kind":"storage#object","bucket":"replay",
             "name":"a/1","size":"12","updated":"2026-01-01T00:00:00.123000Z",
             "etag":"synthetic","generation":"1","metageneration":"1","storageClass":"STANDARD",
             "contentType":"application/octet-stream"}],"prefixes":["b/"]}
            """;

    @Test
    void mapsNamespaceAndComparesExactFieldsWhileCheckingSyntheticTypes() {
        assertThatCode(() -> compare(NATIVE, REPLAY)).doesNotThrowAnyException();
    }

    @Test
    void strictReplayPolicyAcceptsActualGcsRendererOutput() throws Exception {
        long micros = Instant.parse("2026-01-01T00:00:00.123Z").toEpochMilli() * 1000;
        ListedObject object = new ListedObject(bytes("a/1"), 12, micros,
                null, null, null, null, null, null);
        try (BudgetedOutput output = BudgetedOutput.standalone(4096)) {
            GcsJson.write(new GcsListRequest("replay", null, null, null, null, 100,
                            null, false),
                    new GcsPage(List.of(object), List.of(bytes("b/")), null), output);
            GcsProfileComparator.assertPage(bytes(NATIVE), OwnedBodyBytes.copy(output.body()),
                    "live", "replay", false);
        }
    }

    @Test
    void refusesMissingObjectsWrongTypesAndMetadataLeaks() {
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"size\":\"12\"", "\"size\":\"13\"")))
                .hasMessageContaining("size");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"size\":\"12\"", "\"size\":12")))
                .hasMessageContaining("size must be text");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"name\":\"a/1\"", "\"name\":\"a/2\"")))
                .hasMessageContaining("name");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"etag\":\"synthetic\"",
                "\"etag\":\"synthetic\",\"owner\":\"x\"")))
                .hasMessageContaining("owner");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"contentType\":\"application/octet-stream\"",
                "\"contentType\":\"application/octet-stream\",\"futureField\":true")))
                .hasMessageContaining("unknown field");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"generation\":\"1\"",
                "\"generation\":\"2\""))).hasMessageContaining("generation");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"bucket\":\"replay\"",
                "\"bucket\":\"wrong\""))).hasMessageContaining("replay bucket");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("00.123000Z", "00.124000Z")))
                .hasMessageContaining("updated");
        assertThatThrownBy(() -> compare(NATIVE.replace("\"name\":\"a/1\"",
                "\"name\":\"a/1\",\"surprise\":1"), REPLAY))
                .hasMessageContaining("unknown field");
    }

    @Test
    void refusesDuplicatePrefixesTokenStateAndJsonDuplicateKeys() {
        assertThatThrownBy(() -> compare(NATIVE,
                REPLAY.replace("\"prefixes\":[\"b/\"]", "\"prefixes\":[\"b/\",\"b/\"]")))
                .hasMessageContaining("duplicate GCS prefix");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"prefixes\":[\"b/\"]",
                "\"prefixes\":[\"b/\"],\"nextPageToken\":\"opaque\"")))
                .hasMessageContaining("terminal pagination");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("\"name\":\"a/1\"",
                "\"name\":\"a/1\",\"name\":\"a/1\"")))
                .isInstanceOf(Exception.class);
    }

    @Test
    void fullProjectionRequiresEmptyAclAndNoAclForbidsIt() {
        String withAcl = REPLAY.replace("\"contentType\":\"application/octet-stream\"",
                "\"contentType\":\"application/octet-stream\",\"acl\":[]");
        assertThatCode(() -> GcsProfileComparator.assertPage(bytes(NATIVE), bytes(withAcl),
                "live", "replay", true)).doesNotThrowAnyException();
        assertThatThrownBy(() -> compare(NATIVE, withAcl)).hasMessageContaining("omit acl");
        assertThatThrownBy(() -> GcsProfileComparator.assertPage(bytes(NATIVE), bytes(REPLAY),
                "live", "replay", true)).hasMessageContaining("empty array");
    }

    @Test
    void completedWalkComparesInventoriesAcrossDifferentPageBoundaries() throws Exception {
        String nativeA = page(item("live", "a/1", false), "native-1");
        String nativeB = page(item("live", "a/2", false), null);
        String replayTwo = page(item("replay", "a/1", true) + "," + item("replay", "a/2", true), null);
        GcsProfileComparator.assertWalk(List.of(
                new GcsProfileComparator.CapturedPage(null, bytes(nativeA)),
                new GcsProfileComparator.CapturedPage("native-1", bytes(nativeB))),
                List.of(new GcsProfileComparator.CapturedPage(null, bytes(replayTwo))),
                "live", "replay", false, 3, 10);
        assertThatThrownBy(() -> GcsProfileComparator.assertWalk(List.of(
                new GcsProfileComparator.CapturedPage(null, bytes(nativeA)),
                new GcsProfileComparator.CapturedPage("wrong", bytes(nativeB))),
                List.of(new GcsProfileComparator.CapturedPage(null, bytes(replayTwo))),
                "live", "replay", false, 3, 10)).hasMessageContaining("request token");
    }

    @Test
    void crossPagePrefixRepeatIsRecordedAsUnsupportedRatherThanDeduplicated() {
        String first = "{\"kind\":\"storage#objects\",\"prefixes\":[\"a/\"],"
                + "\"nextPageToken\":\"native-1\"}";
        String second = "{\"kind\":\"storage#objects\",\"prefixes\":[\"a/\"]}";
        assertThatThrownBy(() -> GcsProfileComparator.assertWalk(List.of(
                new GcsProfileComparator.CapturedPage(null, bytes(first)),
                new GcsProfileComparator.CapturedPage("native-1", bytes(second))),
                List.of(new GcsProfileComparator.CapturedPage(null,
                        bytes("{\"kind\":\"storage#objects\",\"prefixes\":[\"a/\"]}"))),
                "live", "replay", false, 3, 10))
                .hasMessageContaining("cross-page repeated GCS prefix");
    }

    private static String page(String items, String token) {
        return "{\"kind\":\"storage#objects\",\"items\":[" + items + "]"
                + (token == null ? "}" : ",\"nextPageToken\":\"" + token + "\"}");
    }

    private static String item(String bucket, String name, boolean replay) {
        return "{\"kind\":\"storage#object\",\"bucket\":\"" + bucket + "\",\"name\":\"" + name
                + "\",\"size\":\"12\",\"updated\":\"2026-01-01T00:00:00.123000Z\","
                + "\"etag\":\"x\",\"generation\":\"" + (replay ? "1" : "2")
                + "\",\"metageneration\":\"1\",\"storageClass\":\"STANDARD\","
                + "\"contentType\":\"application/octet-stream\"}";
    }

    private static void compare(String nativeJson, String replayJson) throws Exception {
        GcsProfileComparator.assertPage(bytes(nativeJson), bytes(replayJson), "live", "replay", false);
    }

    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
}
