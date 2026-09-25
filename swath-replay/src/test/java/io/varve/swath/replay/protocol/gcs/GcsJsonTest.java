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
import io.varve.swath.replay.testkit.OwnedBodyBytes;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class GcsJsonTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter ORACLE = new DateTimeFormatterBuilder().appendInstant(6).toFormatter();

    @Test
    void timestampWireMatchesIndependentInstantFormatterAcrossEpochAndDayBoundaries() throws Exception {
        long min = LocalDate.of(0, 1, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
        long maxExclusive = LocalDate.of(10_000, 1, 1).atStartOfDay().toEpochSecond(ZoneOffset.UTC)
                * 1_000_000L;
        List<Long> micros = new java.util.ArrayList<>(List.of(-1L, 0L, 1L, -1_000_001L,
                951_782_399_999_999L, 951_782_400_000_000L, min, maxExclusive - 1));
        Random random = new Random(0x220223);
        for (int i = 0; i < 500; i++) micros.add(random.nextLong(min, maxExclusive));
        GcsListRequest request = new GcsListRequest("bucket", null, null, null, null, 1, null, false);
        for (long value : micros) {
            var object = new ListedObject("x".getBytes(StandardCharsets.UTF_8), 1, value,
                    null, null, null, null, null, null);
            try (BudgetedOutput output = BudgetedOutput.standalone(128)) {
                GcsJson.write(request, new GcsPage(List.of(object), List.of(), null), output);
                String actual = JSON.readTree(OwnedBodyBytes.copy(output.body())).path("items")
                        .get(0).path("updated").asText();
                Instant instant = Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000L),
                        Math.floorMod(value, 1_000_000L) * 1000L);
                assertThat(actual).as("micros=%s", value).isEqualTo(ORACLE.format(instant));
            }
        }
        List<Long> transitions = List.of(-1L, 0L, 1L, 951_782_399_999_999L,
                951_782_400_000_000L, 951_782_400_000_001L);
        List<ListedObject> rows = new java.util.ArrayList<>();
        for (int i = 0; i < transitions.size(); i++) {
            rows.add(new ListedObject(("k" + i).getBytes(StandardCharsets.UTF_8), 1,
                    transitions.get(i), null, null, null, null, null, null));
        }
        try (BudgetedOutput output = BudgetedOutput.standalone(128)) {
            GcsJson.write(request, new GcsPage(rows, List.of(), null), output);
            var items = JSON.readTree(OwnedBodyBytes.copy(output.body())).path("items");
            for (int i = 0; i < transitions.size(); i++) {
                long value = transitions.get(i);
                Instant instant = Instant.ofEpochSecond(Math.floorDiv(value, 1_000_000L),
                        Math.floorMod(value, 1_000_000L) * 1000L);
                assertThat(items.get(i).path("updated").asText()).isEqualTo(ORACLE.format(instant));
            }
        }
        for (long invalid : List.of(Long.MIN_VALUE, Long.MAX_VALUE)) {
            assertThatThrownBy(() -> {
                var object = new ListedObject("x".getBytes(StandardCharsets.UTF_8), 1, invalid,
                        null, null, null, null, null, null);
                try (BudgetedOutput output = BudgetedOutput.standalone(128)) {
                    GcsJson.write(request, new GcsPage(List.of(object), List.of(), null), output);
                }
            }).isInstanceOf(GcsJson.FixtureProblem.class).hasMessage("timestamp_out_of_range");
        }
    }

    @Test
    void rendersProfileTypesAndKeepsOutputLeaseOpen() throws Exception {
        GcsListRequest request = new GcsListRequest("bucket", null, null, null, null, 1000, null, true);
        ListedObject object = new ListedObject("é/\"".getBytes(StandardCharsets.UTF_8), 12, -1,
                "source-etag", "GLACIER", null, null, null, null);
        try (BudgetedOutput output = BudgetedOutput.standalone(64)) {
            GcsJson.write(request, new GcsPage(List.of(object), List.of(), null), output);
            assertThat(output.size()).isPositive();
            var root = JSON.readTree(OwnedBodyBytes.copy(output.body()));
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
            String wire = new String(OwnedBodyBytes.copy(output.body()), StandardCharsets.UTF_8);
            assertThat(wire).contains("\"name\":\"a\\\\\\\"\u2028\uD83D\uDE00\"");
            assertThat(wire).contains("\"updated\":\"1970-01-01T00:00:00.000000Z\"");
            assertThat(wire).doesNotContain("\"prefixes\"").doesNotContain("\"acl\"");
        }
    }

    @Test
    void completeWireGoldenPreservesFieldOrderAndCachedMicroseconds() {
        GcsListRequest request = new GcsListRequest("bucket", null, null, null, null, 2,
                null, true);
        var first = new ListedObject("a".getBytes(StandardCharsets.UTF_8), 12,
                1_767_225_600_000_000L, null, null, null, null, null, null);
        var second = new ListedObject("b".getBytes(StandardCharsets.UTF_8), 34,
                1_767_225_600_123_456L, null, null, null, null, null, null);
        try (BudgetedOutput output = BudgetedOutput.standalone(128)) {
            GcsJson.write(request, new GcsPage(List.of(first, second),
                    List.of("c/".getBytes(StandardCharsets.UTF_8)), "opaque"), output);
            String wire = new String(OwnedBodyBytes.copy(output.body()), StandardCharsets.UTF_8);
            String expected = "{\"kind\":\"storage#objects\",\"items\":["
                    + "{\"kind\":\"storage#object\",\"bucket\":\"bucket\",\"name\":\"a\","
                    + "\"size\":\"12\",\"updated\":\"2026-01-01T00:00:00.000000Z\","
                    + "\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\","
                    + "\"metageneration\":\"1\",\"storageClass\":\"STANDARD\","
                    + "\"contentType\":\"application/octet-stream\",\"acl\":[]},"
                    + "{\"kind\":\"storage#object\",\"bucket\":\"bucket\",\"name\":\"b\","
                    + "\"size\":\"34\",\"updated\":\"2026-01-01T00:00:00.123456Z\","
                    + "\"etag\":\"swath-gcs-synthetic-v1\",\"generation\":\"1\","
                    + "\"metageneration\":\"1\",\"storageClass\":\"STANDARD\","
                    + "\"contentType\":\"application/octet-stream\",\"acl\":[]}],"
                    + "\"prefixes\":[\"c/\"],\"nextPageToken\":\"opaque\"}";
            assertThat(wire).isEqualTo(expected);
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
