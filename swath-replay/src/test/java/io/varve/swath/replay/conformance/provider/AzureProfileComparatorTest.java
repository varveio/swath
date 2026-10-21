/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.azure.AzureListRequest;
import io.varve.swath.replay.protocol.azure.AzureListResult;
import io.varve.swath.replay.protocol.azure.AzureXml;
import io.varve.swath.replay.server.BudgetedOutput;
import io.varve.swath.replay.testkit.OwnedBodyBytes;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class AzureProfileComparatorTest {
    private static final String NATIVE = """
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults ServiceEndpoint="https://live.blob.core.windows.net/" ContainerName="live">
              <Prefix>a/</Prefix><Blobs>
                <BlobPrefix><Name Encoded="true">a%EF%BF%BE/</Name></BlobPrefix>
                <Blob><Name> leading &amp; trailing </Name><Properties>
                  <Creation-Time>Thu, 01 Jan 2026 00:00:00 GMT</Creation-Time>
                  <Last-Modified>Thu, 01 Jan 2026 00:00:00 GMT</Last-Modified>
                  <Etag>native</Etag><Content-Length>12</Content-Length><Content-Type>text/plain</Content-Type>
                  <BlobType>BlockBlob</BlobType><AccessTier>Cool</AccessTier><LeaseStatus>unlocked</LeaseStatus>
                </Properties></Blob>
              </Blobs><NextMarker></NextMarker>
            </EnumerationResults>
            """;
    private static final String REPLAY = """
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults ServiceEndpoint="http://127.0.0.1:1234/replay/" ContainerName="bucket">
              <Prefix>a/</Prefix><Blobs>
                <BlobPrefix><Name Encoded="true">a%EF%BF%BE/</Name></BlobPrefix>
                <Blob><Name> leading &amp; trailing </Name><Properties>
                  <Last-Modified>Thu, 01 Jan 2026 00:00:00 GMT</Last-Modified>
                  <Etag>0x000000000000001</Etag><Content-Length>12</Content-Length>
                  <Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>
                  <AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>
                  <LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState>
                </Properties></Blob>
              </Blobs><NextMarker></NextMarker>
            </EnumerationResults>
            """;

    @Test
    void comparesExactMembershipTextOrderAndTypedSyntheticFields() {
        assertThatCode(() -> compare(NATIVE, REPLAY)).doesNotThrowAnyException();
    }

    @Test
    void strictReplayPolicyAcceptsActualAzureRendererOutput() throws Exception {
        long micros = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli() * 1000;
        AzureListRequest request = new AzureListRequest("replay", "bucket", "2026-06-06",
                "a/", null, null, null, 2, null, true, false, false, false, null);
        AzureListResult page = new AzureListResult(request, List.of(
                new AzureListResult.Entry.BlobPrefix(bytes("a\uFFFE/")),
                new AzureListResult.Entry.Blob(new ListedObject(bytes(" leading & trailing "),
                        12, micros, null, null, null, null, null, null))), null);
        try (BudgetedOutput output = BudgetedOutput.standalone(4096)) {
            AzureXml.write(page, "http://127.0.0.1:1234/replay/", output);
            AzureProfileComparator.assertPage(bytes(NATIVE.replace("a%EF%BF%BE/", "a%EF%BF%BE%2F")),
                    OwnedBodyBytes.copy(output.body()),
                    "https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/",
                    "live", "bucket", null, null);
        }
    }

    @Test
    void refusesSizeTextAttributeAndElementOrderChanges() {
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("<Content-Length>12</Content-Length>",
                "<Content-Length>13</Content-Length>"))).hasMessageContaining("Content-Length");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace(" leading &amp; trailing ",
                "leading &amp; trailing"))).hasMessageContaining("Name text");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("Encoded=\"true\"",
                "Encoded=\"true\" Extra=\"x\""))).hasMessageContaining("attribute");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("<BlobPrefix>", "<Blob>")))
                .isInstanceOf(Exception.class);
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("<Etag>0x000000000000001</Etag>",
                "<Etag>0x000000000000001</Etag><Future>bad</Future>")))
                .hasMessageContaining("unknown Azure Properties field");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("<AccessTier>Hot</AccessTier>",
                "<AccessTier>Cool</AccessTier>"))).hasMessageContaining("AccessTier");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("00:00:00 GMT", "00:00:01 GMT")))
                .hasMessageContaining("Last-Modified");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("ContainerName=\"bucket\"",
                "ContainerName=\"wrong\""))).hasMessageContaining("replay container");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("/replay/\"", "/replay\"")))
                .hasMessageContaining("trailing slash");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("<Prefix>a/</Prefix>",
                "<Prefix>wrong/</Prefix>"))).hasMessageContaining("Prefix echo");
        assertThatThrownBy(() -> compare(NATIVE, REPLAY.replace("<Last-Modified>",
                "<Creation-Time>Thu, 01 Jan 2026 00:00:00 GMT</Creation-Time><Last-Modified>")))
                .hasMessageContaining("unknown Azure Properties field");
        String prefix = "<BlobPrefix><Name Encoded=\"true\">a%EF%BF%BE/</Name></BlobPrefix>";
        int start = REPLAY.indexOf(prefix);
        int blobEnd = REPLAY.indexOf("</Blob>", start) + "</Blob>".length();
        String blob = REPLAY.substring(start + prefix.length(), blobEnd);
        String swapped = REPLAY.substring(0, start) + blob + prefix + REPLAY.substring(blobEnd);
        assertThatThrownBy(() -> compare(NATIVE, swapped)).hasMessageContaining("entry kind");
        assertThatThrownBy(() -> compare(NATIVE.replace("<BlobType>BlockBlob</BlobType>",
                "<BlobType>Unknown</BlobType>"), REPLAY)).hasMessageContaining("native BlobType");
    }

    @Test
    void completedWalkAllowsDifferentPageBoundariesButChecksOwnMarkers() throws Exception {
        String nativeFirst = NATIVE.replaceAll("(?s)<Blob><Name>.*?</Blob>", "")
                .replace("<NextMarker></NextMarker>", "<NextMarker>native-1</NextMarker>");
        String nativeSecond = NATIVE.replaceAll("(?s)<BlobPrefix>.*?</BlobPrefix>", "")
                .replace("<Prefix>a/</Prefix>", "<Prefix>a/</Prefix><Marker>native-1</Marker>");
        var nativePages = List.of(new AzureProfileComparator.CapturedPage(null, "a/", null, null,
                        bytes(nativeFirst)),
                new AzureProfileComparator.CapturedPage("native-1", "a/", null, null,
                        bytes(nativeSecond)));
        var replayPages = List.of(new AzureProfileComparator.CapturedPage(null, "a/", null, null,
                bytes(REPLAY)));
        AzureProfileComparator.assertWalk(nativePages, replayPages,
                "https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/",
                "live", "bucket", 3, 10);
        assertThatThrownBy(() -> AzureProfileComparator.assertWalk(List.of(
                new AzureProfileComparator.CapturedPage(null, "a/", null, null, bytes(nativeFirst)),
                new AzureProfileComparator.CapturedPage("wrong", "a/", null, null,
                        bytes(nativeSecond))), replayPages,
                "https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/",
                "live", "bucket", 3, 10)).hasMessageContaining("request marker");
        assertThatThrownBy(() -> AzureProfileComparator.assertWalk(List.of(
                new AzureProfileComparator.CapturedPage(null, "wrong/", null, null, bytes(nativeFirst))),
                replayPages, "https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/",
                "live", "bucket", 3, 10)).hasMessageContaining("Prefix echo");
        String wrongEcho = nativeSecond.replace("<Marker>native-1</Marker>", "<Marker>wrong</Marker>");
        assertThatThrownBy(() -> AzureProfileComparator.assertWalk(List.of(
                new AzureProfileComparator.CapturedPage(null, "a/", null, null, bytes(nativeFirst)),
                new AzureProfileComparator.CapturedPage("native-1", "a/", null, null,
                        bytes(wrongEcho))), replayPages,
                "https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/",
                "live", "bucket", 3, 10)).hasMessageContaining("Marker echo");
    }

    @Test
    void crossPageBlobPrefixRepeatIsExplicitlyUnsupported() {
        String first = "<?xml version=\"1.0\"?><EnumerationResults "
                + "ServiceEndpoint=\"https://live.blob.core.windows.net/\" ContainerName=\"live\">"
                + "<Blobs><BlobPrefix><Name>a/</Name></BlobPrefix></Blobs>"
                + "<NextMarker>native-1</NextMarker></EnumerationResults>";
        String second = first.replace("<Blobs>", "<Marker>native-1</Marker><Blobs>")
                .replace("<NextMarker>native-1</NextMarker>", "<NextMarker></NextMarker>");
        String replay = first.replace("https://live.blob.core.windows.net/", "http://127.0.0.1/replay/")
                .replace("ContainerName=\"live\"", "ContainerName=\"bucket\"")
                .replace("<NextMarker>native-1</NextMarker>", "<NextMarker></NextMarker>");
        assertThatThrownBy(() -> AzureProfileComparator.assertWalk(List.of(
                new AzureProfileComparator.CapturedPage(null, bytes(first)),
                new AzureProfileComparator.CapturedPage("native-1", bytes(second))),
                List.of(new AzureProfileComparator.CapturedPage(null, bytes(replay))),
                "https://live.blob.core.windows.net/", "http://127.0.0.1/replay/",
                "live", "bucket", 3, 10)).hasMessageContaining("cross-page repeated Azure BlobPrefix");
    }

    private static byte[] bytes(String xml) { return xml.getBytes(StandardCharsets.UTF_8); }

    private static void compare(String nativeXml, String replayXml) throws Exception {
        AzureProfileComparator.assertPage(nativeXml.getBytes(StandardCharsets.UTF_8),
                replayXml.getBytes(StandardCharsets.UTF_8), "https://live.blob.core.windows.net/",
                "http://127.0.0.1:1234/replay/", "live", "bucket", null, null);
    }
}
