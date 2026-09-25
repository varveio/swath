/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
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
        var nativePages = List.of(new AzureProfileComparator.CapturedPage(null, bytes(nativeFirst)),
                new AzureProfileComparator.CapturedPage("native-1", bytes(nativeSecond)));
        var replayPages = List.of(new AzureProfileComparator.CapturedPage(null, bytes(REPLAY)));
        AzureProfileComparator.assertWalk(nativePages, replayPages,
                "https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/",
                "live", "bucket", 3, 10);
        assertThatThrownBy(() -> AzureProfileComparator.assertWalk(List.of(
                new AzureProfileComparator.CapturedPage(null, bytes(nativeFirst)),
                new AzureProfileComparator.CapturedPage("wrong", bytes(nativeSecond))), replayPages,
                "https://live.blob.core.windows.net/", "http://127.0.0.1:1234/replay/",
                "live", "bucket", 3, 10)).hasMessageContaining("request marker");
    }

    private static byte[] bytes(String xml) { return xml.getBytes(StandardCharsets.UTF_8); }

    private static void compare(String nativeXml, String replayXml) throws Exception {
        AzureProfileComparator.assertPage(nativeXml.getBytes(StandardCharsets.UTF_8),
                replayXml.getBytes(StandardCharsets.UTF_8), "https://live.blob.core.windows.net/",
                "http://127.0.0.1:1234/replay/", "live", "bucket", null, null);
    }
}
