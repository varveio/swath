/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.BudgetedOutput;
import io.varve.swath.replay.testkit.OwnedBodyBytes;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class AzureXmlGoldenTest {
    @Test
    void exactWireBytesForAsciiEscapesDerivedCrUnicodeAndEncodedName() {
        AzureListRequest request = request();
        AzureListResult result = new AzureListResult(request, List.of(
                new AzureListResult.Entry.Blob(object("A&B<C>D\"E'F", 7)),
                new AzureListResult.Entry.BlobPrefix(raw("p/\t\n\r")),
                new AzureListResult.Entry.Blob(object("é😀", 8)),
                new AzureListResult.Entry.Blob(object("x\uFFFE/y", 9))), null);
        String expected = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
                + "<EnumerationResults ServiceEndpoint=\"http://127.0.0.1/replay/\" ContainerName=\"bucket\">"
                + "<Prefix>a</Prefix><Marker>opaque</Marker><MaxResults>4</MaxResults><Delimiter>/</Delimiter>"
                + "<Blobs>"
                + "<Blob><Name>A&amp;B&lt;C&gt;D&quot;E&apos;F</Name>" + properties(7) + "</Blob>"
                + "<BlobPrefix><Name>p/\t\n&#13;</Name></BlobPrefix>"
                + "<Blob><Name>é😀</Name>" + properties(8) + "</Blob>"
                + "<Blob><Name Encoded=\"true\">x%EF%BF%BE%2Fy</Name>" + properties(9) + "</Blob>"
                + "</Blobs><NextMarker></NextMarker></EnumerationResults>";
        assertThat(render(result)).isEqualTo(expected);
    }

    @Test
    void utf16UnitsAndSegmentsStayAtProfileBoundaries() {
        assertThat(render(one("x".repeat(1024)))).contains("<Name>" + "x".repeat(1024) + "</Name>");
        assertThatThrownBy(() -> render(one("x".repeat(1025))))
                .isInstanceOf(AzureXml.FixtureProblem.class).hasMessage("name_too_long");
        assertThat(render(one("😀".repeat(512)))).contains("<Name>" + "😀".repeat(512) + "</Name>");
        assertThatThrownBy(() -> render(one("😀".repeat(513))))
                .isInstanceOf(AzureXml.FixtureProblem.class).hasMessage("name_too_long");
        assertThat(render(one("x/".repeat(253) + "x"))).contains("<Name>");
        assertThatThrownBy(() -> render(one("x/".repeat(254) + "x")))
                .isInstanceOf(AzureXml.FixtureProblem.class).hasMessage("too_many_segments");
    }

    @Test
    void malformedUtf8AndObjectControlsRemainTypedFixtureErrors() {
        AzureListResult invalid = new AzureListResult(request(), List.of(
                new AzureListResult.Entry.Blob(new ListedObject(new byte[] {(byte) 0xff}, 1, 0,
                        null, null, null, null, null, null))), null);
        assertThatThrownBy(() -> render(invalid)).isInstanceOf(AzureXml.FixtureProblem.class)
                .hasMessage("invalid_utf8_name");
        assertThatThrownBy(() -> render(one("bad\rname"))).isInstanceOf(AzureXml.FixtureProblem.class)
                .hasMessage("control_character");
    }

    private static String properties(long size) {
        return "<Properties><Last-Modified>Thu, 01 Jan 2026 00:00:00 GMT</Last-Modified>"
                + "<Etag>0x000000000000001</Etag><Content-Length>" + size + "</Content-Length>"
                + "<Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>"
                + "<AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>"
                + "<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties>";
    }

    private static AzureListResult one(String name) {
        return new AzureListResult(request(), List.of(new AzureListResult.Entry.Blob(object(name, 1))), null);
    }

    private static ListedObject object(String name, long size) {
        return new ListedObject(raw(name), size, 1_767_225_600_000_000L,
                null, null, null, null, null, null);
    }

    private static AzureListRequest request() {
        return new AzureListRequest("replay", "bucket", "2026-06-06", "a", "/", null,
                "opaque", 4, "4", true, true, true, true, null);
    }

    private static byte[] raw(String name) { return name.getBytes(StandardCharsets.UTF_8); }

    private static String render(AzureListResult result) {
        try (BudgetedOutput out = BudgetedOutput.standalone(128)) {
            AzureXml.write(result, "http://127.0.0.1/replay/", out);
            return new String(OwnedBodyBytes.copy(out.body()), StandardCharsets.UTF_8);
        }
    }
}
