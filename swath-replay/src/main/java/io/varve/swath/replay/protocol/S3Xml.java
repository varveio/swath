/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import io.varve.swath.replay.server.BudgetedOutput;
import io.varve.swath.replay.server.OwnedBody;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Renders {@link S3ListResult} and {@link S3Error} as byte-faithful S3 ListObjectsV2 XML. */
public final class S3Xml {

    private static final int RESPONSE_BASE_CAPACITY = 512;
    private static final int ESTIMATED_BYTES_PER_ENTRY = 320;

    private static final DateTimeFormatter S3_LAST_MODIFIED_PREFIX =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.").withZone(ZoneOffset.UTC);

    private S3Xml() {
    }

    /**
     * Renders directly into the bytes Jetty writes. The replay hot path must not first construct a
     * response-sized UTF-16 {@link String} and immediately encode the whole response back to UTF-8.
     */
    public static ByteBuffer listBucketBuffer(S3ListResult result) {
        try (BudgetedOutput xml = BudgetedOutput.standalone(Math.max(4096,
                RESPONSE_BASE_CAPACITY + result.entries().size() * ESTIMATED_BYTES_PER_ENTRY))) {
            writeBucket(result, xml);
            // The compatibility form is not used by the serving path; flattening is confined here.
            return xml.buffer();
        }
    }

    /** Render into request-owned chunks while retaining the S3 byte grammar. */
    public static OwnedBody listBucketBody(S3ListResult result, BudgetedOutput xml) {
        writeBucket(result, xml);
        return xml.body();
    }

    private static void writeBucket(S3ListResult result, BudgetedOutput xml) {
        S3ListRequest request = result.request();
        xml.appendAscii("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.appendAscii("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        element(xml, "Name", request.bucket());
        byteElement(xml, "Prefix", request.prefix(), request.encodingTypeUrl());
        // A token and a start-after can both arrive; the pager resumes at the token, so the response
        // echoes the token and omits the start-after S3 ignored.
        if (request.hasContinuationToken()) {
            element(xml, "ContinuationToken", request.continuationToken());
        } else if (request.startAfter() != null) {
            byteElement(xml, "StartAfter", request.startAfter(), request.encodingTypeUrl());
        }
        if (result.nextContinuationToken() != null) {
            element(xml, "NextContinuationToken", result.nextContinuationToken());
        }
        numericElement(xml, "KeyCount", result.keyCount());
        numericElement(xml, "MaxKeys", request.maxKeys());
        if (request.delimiter() != null && request.delimiter().length > 0) {
            byteElement(xml, "Delimiter", request.delimiter(), request.encodingTypeUrl());
        }
        if (request.encodingTypeUrl()) {
            element(xml, "EncodingType", "url");
        }
        fixedElement(xml, "IsTruncated", result.truncated() ? "true" : "false");
        TimestampPrefixCache timestamps = new TimestampPrefixCache();
        for (S3ResultEntry entry : result.entries()) {
            if (entry instanceof S3ResultEntry.ObjectResult object) {
                contents(xml, object.object(), request.encodingTypeUrl(), request.fetchOwner(), timestamps);
            }
        }
        for (S3ResultEntry entry : result.entries()) {
            if (entry instanceof S3ResultEntry.CommonPrefixResult prefix) {
                commonPrefix(xml, prefix.key(), request.encodingTypeUrl());
            }
        }
        xml.appendAscii("</ListBucketResult>");
    }

    /** Exact-sized compatibility form for callers that specifically need an owning byte array. */
    public static byte[] listBucketBytes(S3ListResult result) {
        ByteBuffer rendered = listBucketBuffer(result);
        byte[] exact = new byte[rendered.remaining()];
        rendered.get(exact);
        return exact;
    }

    /** Compatibility form for callers and tests that consume XML as text. */
    public static String listBucket(S3ListResult result) {
        return new String(listBucketBytes(result), StandardCharsets.UTF_8);
    }

    public static String error(String code, String message, String resource) {
        StringBuilder xml = new StringBuilder(512);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.append("<Error>");
        stringElement(xml, "Code", code);
        stringElement(xml, "Message", message);
        if (resource != null) {
            stringElement(xml, "Resource", resource);
        }
        stringElement(xml, "RequestId", "S3LISTINGREPLAY");
        xml.append("</Error>");
        return xml.toString();
    }

    private static void contents(BudgetedOutput xml, ListedObject object, boolean encoded,
                                 boolean fetchOwner, TimestampPrefixCache timestamps) {
        xml.appendAscii("<Contents>");
        byteElement(xml, "Key", object.key(), encoded);
        xml.appendAscii("<LastModified>");
        timestamps.append(xml, object.lastModifiedEpochMicros());
        xml.appendAscii("</LastModified>");
        if (object.etag() != null) {
            xml.appendAscii("<ETag>&quot;");
            xml.appendEscaped(object.etag());
            xml.appendAscii("&quot;</ETag>");
        }
        if (object.checksumAlgorithm() != null) {
            element(xml, "ChecksumAlgorithm", object.checksumAlgorithm());
        }
        if (object.checksumType() != null) {
            element(xml, "ChecksumType", object.checksumType());
        }
        numericElement(xml, "Size", object.size());
        if (object.storageClass() != null) {
            element(xml, "StorageClass", object.storageClass());
        }
        if (fetchOwner && (object.ownerId() != null || object.ownerDisplayName() != null)) {
            xml.appendAscii("<Owner>");
            if (object.ownerId() != null) {
                element(xml, "ID", object.ownerId());
            }
            if (object.ownerDisplayName() != null) {
                element(xml, "DisplayName", object.ownerDisplayName());
            }
            xml.appendAscii("</Owner>");
        }
        xml.appendAscii("</Contents>");
    }

    private static void commonPrefix(BudgetedOutput xml, byte[] prefix, boolean encoded) {
        xml.appendAscii("<CommonPrefixes>");
        byteElement(xml, "Prefix", prefix, encoded);
        xml.appendAscii("</CommonPrefixes>");
    }

    /**
     * Last-modified values in captures commonly arrive in runs from the same second. Formatting the
     * calendar prefix is far more expensive than appending its three millisecond digits, so cache
     * the last second within one response. The cache is request-local and therefore needs no lock.
     */
    private static final class TimestampPrefixCache {
        private long second = Long.MIN_VALUE;
        private String prefix;

        private void append(BudgetedOutput xml, long epochMicros) {
            long currentSecond = Math.floorDiv(epochMicros, 1_000_000L);
            if (prefix == null || currentSecond != second) {
                second = currentSecond;
                prefix = S3_LAST_MODIFIED_PREFIX.format(Instant.ofEpochSecond(currentSecond));
            }
            int millis = (int) (Math.floorMod(epochMicros, 1_000_000L) / 1_000L);
            xml.appendAscii(prefix);
            xml.appendThreeDigits(millis);
            xml.appendByte('Z');
        }
    }

    private static void element(BudgetedOutput xml, String name, String value) {
        xml.appendByte('<');
        xml.appendAscii(name);
        xml.appendByte('>');
        xml.appendEscaped(value);
        xml.appendAscii("</");
        xml.appendAscii(name);
        xml.appendByte('>');
    }

    private static void fixedElement(BudgetedOutput xml, String name, String asciiValue) {
        xml.appendByte('<');
        xml.appendAscii(name);
        xml.appendByte('>');
        xml.appendAscii(asciiValue);
        xml.appendAscii("</");
        xml.appendAscii(name);
        xml.appendByte('>');
    }

    private static void numericElement(BudgetedOutput xml, String name, long value) {
        xml.appendByte('<');
        xml.appendAscii(name);
        xml.appendByte('>');
        xml.appendLong(value);
        xml.appendAscii("</");
        xml.appendAscii(name);
        xml.appendByte('>');
    }

    private static void byteElement(BudgetedOutput xml, String name, byte[] value, boolean encoded) {
        xml.appendByte('<');
        xml.appendAscii(name);
        xml.appendByte('>');
        if (value != null) {
            if (encoded) {
                xml.appendPercentEncoded(value);
            } else {
                // Preserve the previous decoder semantics for malformed UTF-8 before XML escaping.
                xml.appendEscaped(new String(value, StandardCharsets.UTF_8));
            }
        }
        xml.appendAscii("</");
        xml.appendAscii(name);
        xml.appendByte('>');
    }

    private static void stringElement(StringBuilder xml, String name, String value) {
        xml.append('<').append(name).append('>');
        appendEscaped(xml, value);
        xml.append("</").append(name).append('>');
    }

    private static void appendEscaped(StringBuilder xml, String value) {
        for (int i = 0; i < value.length(); i++) {
            switch (value.charAt(i)) {
                case '&' -> xml.append("&amp;");
                case '<' -> xml.append("&lt;");
                case '>' -> xml.append("&gt;");
                case '"' -> xml.append("&quot;");
                case '\'' -> xml.append("&apos;");
                default -> xml.append(value.charAt(i));
            }
        }
    }

}
