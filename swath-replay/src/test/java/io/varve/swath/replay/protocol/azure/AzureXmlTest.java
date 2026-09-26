/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.conformance.provider.AzureEvidence;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.BudgetedOutput;
import io.varve.swath.replay.testkit.OwnedBodyBytes;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class AzureXmlTest {
    @Test
    void preservesInterleavedElementOrderEndpointAndExactNameText() throws Exception {
        AzureListRequest request = request(true);
        AzureListResult page = new AzureListResult(request, List.of(
                new AzureListResult.Entry.BlobPrefix("a/".getBytes(StandardCharsets.UTF_8)),
                new AzureListResult.Entry.Blob(object(" leading & trailing ", 12, -1)),
                new AzureListResult.Entry.BlobPrefix("c/".getBytes(StandardCharsets.UTF_8))), "opaque");
        try (BudgetedOutput out = BudgetedOutput.standalone(128)) {
            AzureXml.write(page, "http://127.0.0.1:1234/replay/", out);
            byte[] bytes = OwnedBodyBytes.copy(out.body());
            String xml = new String(bytes, StandardCharsets.UTF_8);
            assertThat(xml).startsWith("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
            assertThat(xml).contains("<Last-Modified>Wed, 31 Dec 1969 23:59:59 GMT</Last-Modified>");
            assertThat(xml).contains("<Content-Length>12</Content-Length>");
            assertThat(xml).contains("<Marker>opaque-request</Marker>");
            AzureEvidence.Page parsed = AzureEvidence.parse(bytes);
            assertThat(parsed.entries()).containsExactly(
                    new AzureEvidence.Entry("BlobPrefix", "a/", null),
                    new AzureEvidence.Entry("Blob", " leading & trailing ", null),
                    new AzureEvidence.Entry("BlobPrefix", "c/", null));
            assertThat(parsed.nextMarker()).isEqualTo("opaque");
            assertThat(parsed.serviceEndpoint()).isEqualTo("http://127.0.0.1:1234/replay/");
        }
    }

    @Test
    void percentEncodesXmlForbiddenNameAndRejectsControls() throws Exception {
        AzureListRequest request = request(false);
        try (BudgetedOutput out = BudgetedOutput.standalone(128)) {
            AzureXml.write(new AzureListResult(request, List.of(
                    new AzureListResult.Entry.Blob(object("x\uFFFE/y", 1, 0))), null),
                    "http://127.0.0.1/replay/", out);
            String xml = new String(OwnedBodyBytes.copy(out.body()), StandardCharsets.UTF_8);
            assertThat(xml).contains("<Name Encoded=\"true\">x%EF%BF%BE%2Fy</Name>");
        }
        try (BudgetedOutput out = BudgetedOutput.standalone(128)) {
            assertThatThrownBy(() -> AzureXml.write(new AzureListResult(request, List.of(
                    new AzureListResult.Entry.Blob(object("bad\u0001", 1, 0))), null),
                    "http://127.0.0.1/replay/", out)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsUnrenderableYearWithoutChangingValidNegativeEpoch() throws Exception {
        long first = LocalDateTime.of(1, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC);
        long afterLast = LocalDateTime.of(10000, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC);
        for (long micros : List.of((first - 1) * 1_000_000L, afterLast * 1_000_000L)) {
            try (BudgetedOutput out = BudgetedOutput.standalone(128)) {
                assertThatThrownBy(() -> AzureXml.write(new AzureListResult(request(false), List.of(
                        new AzureListResult.Entry.Blob(object("a", 1, micros))), null),
                        "http://127.0.0.1/replay/", out))
                        .isInstanceOf(AzureXml.FixtureProblem.class)
                        .hasMessage("timestamp_year_out_of_range");
            }
        }
        for (long micros : List.of(first * 1_000_000L, afterLast * 1_000_000L - 1,
                -1L)) {
            try (BudgetedOutput out = BudgetedOutput.standalone(128)) {
                AzureXml.write(new AzureListResult(request(false), List.of(
                        new AzureListResult.Entry.Blob(object("a", 1, micros))), null),
                        "http://127.0.0.1/replay/", out);
                assertThat(new String(OwnedBodyBytes.copy(out.body()), StandardCharsets.UTF_8))
                        .contains("<Last-Modified>");
            }
        }
    }

    private static AzureListRequest request(boolean echoes) {
        return new AzureListRequest("replay", "bucket", "2026-06-06", echoes ? "a" : null,
                echoes ? "/" : null, null, echoes ? "opaque-request" : null, 2,
                echoes ? "2" : null, echoes, echoes, echoes, echoes, null);
    }

    private static ListedObject object(String name, long size, long micros) {
        return new ListedObject(name.getBytes(StandardCharsets.UTF_8), size, micros,
                "source-etag", "source-class", null, null, null, null);
    }
}
