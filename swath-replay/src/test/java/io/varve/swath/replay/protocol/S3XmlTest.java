/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
        assertThat(xml).contains("<Key>a/has%20space.txt</Key>");
        assertThat(xml).contains("<LastModified>2026-01-01T00:00:00.123Z</LastModified>");
        assertThat(xml).contains("<ETag>&quot;abc&amp;def&quot;</ETag>");
        assertThat(xml).contains("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>");
        assertThat(xml).contains("<ChecksumType>FULL_OBJECT</ChecksumType>");
        assertThat(xml).contains("<Owner><ID>owner-1</ID><DisplayName>Alice &amp; Bob</DisplayName></Owner>");
        assertThat(xml).contains("<Prefix>a/dir/</Prefix>");
        assertThat(xml).contains("<NextContinuationToken>v2:");
        assertThat(xml.indexOf("<Contents>")).isLessThan(xml.indexOf("<CommonPrefixes>"));
    }

    @Test
    void rendersAdversarialUnencodedValuesAsExactUtf8Bytes() {
        byte[] malformed = new byte[] {'a', '&', '<', '>', '"', '\'', (byte) 0xff};
        S3ListRequest request = new S3ListRequest(
                "b&<>\"'\ud800", malformed, null, null, null, Integer.MIN_VALUE, false, true);
        ListedObject object = new ListedObject(bytes("k/😀&<>'\""), Long.MIN_VALUE, -1,
                "e&<>\"'😀", "ST😀&", "id<", "display'", "CRC&", "TYPE>");
        S3ListResult result = new S3ListResult(request,
                List.of(new S3ResultEntry.ObjectResult(object)), false, null);

        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                + "<Name>b&amp;&lt;&gt;&quot;&apos;?</Name>"
                + "<Prefix>a&amp;&lt;&gt;&quot;&apos;�</Prefix>"
                + "<KeyCount>1</KeyCount><MaxKeys>-2147483648</MaxKeys>"
                + "<IsTruncated>false</IsTruncated><Contents>"
                + "<Key>k/😀&amp;&lt;&gt;&apos;&quot;</Key>"
                + "<LastModified>1969-12-31T23:59:59.999Z</LastModified>"
                + "<ETag>&quot;e&amp;&lt;&gt;&quot;&apos;😀&quot;</ETag>"
                + "<ChecksumAlgorithm>CRC&amp;</ChecksumAlgorithm><ChecksumType>TYPE&gt;</ChecksumType>"
                + "<Size>-9223372036854775808</Size><StorageClass>ST😀&amp;</StorageClass>"
                + "<Owner><ID>id&lt;</ID><DisplayName>display&apos;</DisplayName></Owner>"
                + "</Contents></ListBucketResult>";

        assertThat(S3Xml.listBucketBytes(result)).isEqualTo(expected.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void percentEncodesEveryUnsafeByteWithoutUtf8Decoding() {
        byte[] raw = new byte[] {'/', ' ', (byte) 0xff, '&', '<', '>'};
        S3ListRequest request = new S3ListRequest("bucket", raw, bytes("/"), raw, null,
                1, true, false);
        S3ListResult result = new S3ListResult(request, List.of(new S3ResultEntry.CommonPrefixResult(raw)),
                false, null);

        String xml = S3Xml.listBucket(result);
        assertThat(xml).contains("<Prefix>/%20%FF%26%3C%3E</Prefix>")
                .contains("<StartAfter>/%20%FF%26%3C%3E</StartAfter>")
                .contains("<Delimiter>/</Delimiter>")
                .contains("<CommonPrefixes><Prefix>/%20%FF%26%3C%3E</Prefix></CommonPrefixes>");
    }

    /** The response reports the boundary the pager actually resumed at, so the ignored one is omitted. */
    @Test
    void continuationTokenSuppressesIgnoredStartAfterInResponse() {
        S3ListRequest request = new S3ListRequest(
                "bucket", null, null, bytes("conflicting-boundary"), "opaque-token", 1000, true, false);
        S3ListResult result = new S3ListResult(request, List.of(), false, null);

        assertThat(S3Xml.listBucket(result))
                .contains("<ContinuationToken>opaque-token</ContinuationToken>")
                .doesNotContain("<StartAfter>");
    }

    /**
     * A blank {@code continuation-token=} is not a resume point, so it neither suppresses the
     * start-after the pager honored nor gets echoed back as an empty element the client never sent.
     */
    @Test
    void blankContinuationTokenNeitherSuppressesNorEchoes() {
        for (String blank : new String[]{"", "   "}) {
            S3ListRequest request = new S3ListRequest(
                    "bucket", null, null, bytes("a/2"), blank, 1000, true, false);
            S3ListResult result = new S3ListResult(request, List.of(), false, null);

            assertThat(S3Xml.listBucket(result))
                    .as("blank token %s", blank.isEmpty() ? "<empty>" : "<whitespace>")
                    .contains("<StartAfter>a/2</StartAfter>")
                    .doesNotContain("<ContinuationToken>");
        }
    }

    @Test
    void boundedBufferGrowthPreservesEveryResponseByte() {
        List<S3ResultEntry> entries = new ArrayList<>();
        String longKey = "x".repeat(1_024);
        for (int i = 0; i < 20; i++) {
            entries.add(new S3ResultEntry.ObjectResult(new ListedObject(
                    bytes(longKey + i), i, 0, null, "STANDARD", null, null, null, null)));
        }
        S3ListRequest request = new S3ListRequest("bucket", null, null, null, null, 20, true, false);
        S3ListResult result = new S3ListResult(request, entries, false, null);

        byte[] exact = S3Xml.listBucketBytes(result);
        var buffer = S3Xml.listBucketBuffer(result);
        byte[] bounded = new byte[buffer.remaining()];
        buffer.get(bounded);

        assertThat(bounded).isEqualTo(exact);
        assertThat(new String(exact, StandardCharsets.UTF_8))
                .contains("<Key>" + longKey + "19</Key>")
                .endsWith("</ListBucketResult>");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
