/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.BudgetedOutput;
import io.varve.swath.replay.server.ListingHttpRequest;
import io.varve.swath.replay.server.ReplayFailure;
import io.varve.swath.replay.server.ReplayRequestException;
import io.varve.swath.replay.testkit.FakeListingStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsJsonTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void rendersProfileTypesAndKeepsOutputLeaseOpen() throws Exception {
        GcsListRequest request = new GcsListRequest("bucket", null, null, null, null, 1000, null, true);
        ListedObject object = new ListedObject("é/\"".getBytes(StandardCharsets.UTF_8), 12, -1,
                "source-etag", "GLACIER", null, null, null, null);
        try (BudgetedOutput output = BudgetedOutput.standalone(64)) {
            GcsJson.write(request, new GcsPage(List.of(object), List.of(), null), output);
            assertThat(output.size()).isPositive();
            var root = JSON.readTree(output.buffer().array());
            var item = root.path("items").get(0);
            assertThat(item.path("name").textValue()).isEqualTo("é/\"");
            assertThat(item.path("size").isTextual()).isTrue();
            assertThat(item.path("size").textValue()).isEqualTo("12");
            assertThat(item.path("updated").textValue()).isEqualTo("1969-12-31T23:59:59.999999Z");
            assertThat(item.path("generation").textValue()).isEqualTo("1");
            assertThat(item.path("acl").isArray()).isTrue();
            assertThat(item.has("owner")).isFalse();
        }
    }

    @Test
    void refusesInvalidUtf8FixtureName() {
        ListedObject object = new ListedObject(new byte[] {(byte) 0xff}, 1, 0,
                null, null, null, null, null, null);
        try (BudgetedOutput output = BudgetedOutput.standalone(64)) {
            assertThatThrownBy(() -> GcsJson.write(
                    new GcsListRequest("bucket", null, null, null, null, 1, null, false),
                    new GcsPage(List.of(object), List.of(), null), output))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rawJsonEscapesControlCharactersAndOmitsEmptyArrays() {
        ListedObject object = new ListedObject("a\\\"\u2028\uD83D\uDE00".getBytes(StandardCharsets.UTF_8),
                0, 0, null, null, null, null, null, null);
        try (BudgetedOutput output = BudgetedOutput.standalone(128)) {
            GcsJson.write(new GcsListRequest("bucket", null, null, null, null, 1, null, false),
                    new GcsPage(List.of(object), List.of(), null), output);
            String wire = new String(output.buffer().array(), 0, output.size(), StandardCharsets.UTF_8);
            assertThat(wire).contains("\"name\":\"a\\\\\\\"\u2028\uD83D\uDE00\"");
            assertThat(wire).contains("\"updated\":\"1970-01-01T00:00:00.000000Z\"");
            assertThat(wire).doesNotContain("\"prefixes\"").doesNotContain("\"acl\"");
        }
    }

    @Test
    void refusesTooLongAndReservedNamesAndNegativeSize() {
        for (String name : List.of(".", "..", ".well-known/acme-challenge/x", "x".repeat(1025))) {
            assertThatThrownBy(() -> renderName(name.getBytes(StandardCharsets.UTF_8), 1))
                    .as(name.length() > 40 ? "long name" : name)
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> renderName("ok".getBytes(StandardCharsets.UTF_8), -1))
                .isInstanceOf(IllegalArgumentException.class);
        renderName("x".repeat(1024).getBytes(StandardCharsets.UTF_8), 1);
    }

    @Test
    void handlerClassifiesFixtureFailureAndRecordsReason() {
        ReplayMetrics metrics = new ReplayMetrics();
        ListedObject invalid = new ListedObject(new byte[] {(byte) 0xff}, 1, 0,
                null, null, null, null, null, null);
        try (FakeListingStore store = new FakeListingStore(List.of(invalid));
             BudgetedOutput output = BudgetedOutput.standalone(128)) {
            var handler = new GcsHandler("bucket", store, "fixture", metrics);
            var operation = handler.parse(new ListingHttpRequest("GET", "/storage/v1/b/bucket/o", "",
                    Map.of()));
            var page = operation.page();
            assertThatThrownBy(() -> page.render(output)).isInstanceOf(ReplayRequestException.class)
                    .satisfies(error -> {
                        ReplayFailure failure = ((ReplayRequestException) error).failure();
                        assertThat(failure.kind()).isEqualTo(ReplayFailure.Kind.FIXTURE_INCOMPATIBLE);
                        assertThat(failure.reason()).isEqualTo("invalid_utf8_name");
                    });
            assertThat(metrics.registry().get("swath.replay.provider.path")
                    .tags("protocol", "gcs", "path", "fixture_rejected", "reason", "invalid_utf8_name")
                    .counter().count()).isEqualTo(1.0);
        } finally {
            metrics.registry().close();
        }
    }

    private static void renderName(byte[] name, long size) {
        ListedObject object = new ListedObject(name, size, 0, null, null, null, null, null, null);
        try (BudgetedOutput output = BudgetedOutput.standalone(128)) {
            GcsJson.write(new GcsListRequest("bucket", null, null, null, null, 1, null, false),
                    new GcsPage(List.of(object), List.of(), null), output);
        }
    }
}
