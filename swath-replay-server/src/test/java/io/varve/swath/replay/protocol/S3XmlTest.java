/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class S3XmlTest {

    @Test
    void rendersListBucketXmlWithEncodingKeyCountAndEtagQuotes() {
        byte[] key = bytes("a/has space.txt");
        S3ListRequest request = new S3ListRequest(
                "bucket", bytes("a/"), bytes("/"), bytes("a/0"), null, 1000, true, true);
        S3ListResult result = new S3ListResult(request, List.of(
                new S3ResultEntry.CommonPrefixResult(bytes("a/dir/")),
                new S3ResultEntry.ObjectResult(new ListedObject(key, 10, 1_767_225_600_123_456L,
                        "abc&def", "STANDARD", "owner-1", "Alice & Bob", "CRC32", "FULL_OBJECT"))), true,
                ContinuationToken.encode(bytes("a/dir/")));

        String xml = S3Xml.listBucket(result);

        assertThat(xml).contains("<Name>bucket</Name>");
        assertThat(xml).contains("<Prefix>a/</Prefix>");
        assertThat(xml).contains("<Delimiter>/</Delimiter>");
        assertThat(xml).contains("<StartAfter>a/0</StartAfter>");
        assertThat(xml).contains("<EncodingType>url</EncodingType>");
        assertThat(xml).contains("<KeyCount>2</KeyCount>");
        assertThat(xml.indexOf("<StartAfter>")).isLessThan(xml.indexOf("<KeyCount>"));
        assertThat(xml.indexOf("<KeyCount>")).isLessThan(xml.indexOf("<MaxKeys>"));
        assertThat(xml.indexOf("<MaxKeys>")).isLessThan(xml.indexOf("<Delimiter>"));
        assertThat(xml.indexOf("<Delimiter>")).isLessThan(xml.indexOf("<EncodingType>"));
        assertThat(xml).contains("<IsTruncated>true</IsTruncated>");
        assertThat(xml).contains("<Key>a/has+space.txt</Key>");
        assertThat(xml).contains("<ETag>&quot;abc&amp;def&quot;</ETag>");
        assertThat(xml).contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>");
        assertThat(xml).contains("<ChecksumType>FULL_OBJECT</ChecksumType>");
        assertThat(xml).contains("<Owner><ID>owner-1</ID><DisplayName>Alice &amp; Bob</DisplayName></Owner>");
        assertThat(xml).contains("<Prefix>a/dir/</Prefix>");
        assertThat(xml).contains("<NextContinuationToken>v2:");
        assertThat(xml.indexOf("<Contents>")).isLessThan(xml.indexOf("<CommonPrefixes>"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
