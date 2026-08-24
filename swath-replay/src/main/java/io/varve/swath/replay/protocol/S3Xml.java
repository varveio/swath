/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

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
        S3ListRequest request = result.request();
        Utf8XmlBuilder xml = new Utf8XmlBuilder(Math.max(4096,
                RESPONSE_BASE_CAPACITY + result.entries().size() * ESTIMATED_BYTES_PER_ENTRY));
        xml.appendAscii("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.appendAscii("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        element(xml, "Name", request.bucket());
        byteElement(xml, "Prefix", request.prefix(), request.encodingTypeUrl());
        if (request.continuationToken() != null) {
            element(xml, "ContinuationToken", request.continuationToken());
        }
        if (request.startAfter() != null) {
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
        return xml.toByteBuffer();
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

    private static void contents(Utf8XmlBuilder xml, ListedObject object, boolean encoded,
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

    private static void commonPrefix(Utf8XmlBuilder xml, byte[] prefix, boolean encoded) {
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

        private void append(Utf8XmlBuilder xml, long epochMicros) {
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

    private static void element(Utf8XmlBuilder xml, String name, String value) {
        xml.appendByte('<');
        xml.appendAscii(name);
        xml.appendByte('>');
        xml.appendEscaped(value);
        xml.appendAscii("</");
        xml.appendAscii(name);
        xml.appendByte('>');
    }

    private static void fixedElement(Utf8XmlBuilder xml, String name, String asciiValue) {
        xml.appendByte('<');
        xml.appendAscii(name);
        xml.appendByte('>');
        xml.appendAscii(asciiValue);
        xml.appendAscii("</");
        xml.appendAscii(name);
        xml.appendByte('>');
    }

    private static void numericElement(Utf8XmlBuilder xml, String name, long value) {
        xml.appendByte('<');
        xml.appendAscii(name);
        xml.appendByte('>');
        xml.appendLong(value);
        xml.appendAscii("</");
        xml.appendAscii(name);
        xml.appendByte('>');
    }

    private static void byteElement(Utf8XmlBuilder xml, String name, byte[] value, boolean encoded) {
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

    /** Minimal growable UTF-8 sink specialized for the replay response grammar. */
    private static final class Utf8XmlBuilder {
        private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
        private static final boolean[] URL_SAFE = urlSafeTable();

        private byte[] bytes;
        private int length;

        private Utf8XmlBuilder(int capacity) {
            bytes = new byte[capacity];
        }

        private void appendByte(int value) {
            ensure(1);
            bytes[length++] = (byte) value;
        }

        @SuppressWarnings("deprecation")
        private void appendAscii(String value) {
            ensure(value.length());
            // This overload copies the low byte of each UTF-16 code unit straight into the target
            // array. Every caller is intentionally ASCII (tags, fixed values, or the proven-safe
            // fast path in appendEscaped), so truncation is exactly the desired encoding and avoids
            // both a temporary byte[] and a Java-level character-copy loop.
            value.getBytes(0, value.length(), bytes, length);
            length += value.length();
        }

        private void appendPercentEncoded(byte[] value) {
            ensure(value.length * 3);
            for (byte b : value) {
                int v = b & 0xff;
                if (URL_SAFE[v]) {
                    bytes[length++] = b;
                } else {
                    bytes[length++] = '%';
                    bytes[length++] = HEX[v >>> 4];
                    bytes[length++] = HEX[v & 0x0f];
                }
            }
        }

        private void appendEscaped(String value) {
            int safeLength = 0;
            while (safeLength < value.length()) {
                char c = value.charAt(safeLength);
                if (c > 0x7f || c == '&' || c == '<' || c == '>' || c == '"' || c == '\'') {
                    break;
                }
                safeLength++;
            }
            if (safeLength == value.length()) {
                appendAscii(value);
                return;
            }
            // One byte per code unit is enough for the overwhelmingly common ASCII case. Safe ASCII
            // is copied directly below; only metacharacters and non-ASCII take the general encoder.
            ensure(value.length());
            for (int offset = 0; offset < value.length();) {
                char c = value.charAt(offset);
                switch (c) {
                    case '&' -> appendAscii("&amp;");
                    case '<' -> appendAscii("&lt;");
                    case '>' -> appendAscii("&gt;");
                    case '"' -> appendAscii("&quot;");
                    case '\'' -> appendAscii("&apos;");
                    default -> {
                        if (c <= 0x7f) {
                            bytes[length++] = (byte) c;
                            offset++;
                            continue;
                        }
                        int codePoint;
                        if (Character.isHighSurrogate(c) && offset + 1 < value.length()
                                && Character.isLowSurrogate(value.charAt(offset + 1))) {
                            codePoint = Character.toCodePoint(c, value.charAt(offset + 1));
                            offset++;
                        } else if (Character.isSurrogate(c)) {
                            // String.getBytes(UTF_8), used by the old renderer, replaces malformed
                            // UTF-16 with the encoder's one-byte default replacement, '?'.
                            codePoint = '?';
                        } else {
                            codePoint = c;
                        }
                        appendCodePoint(codePoint);
                    }
                }
                offset++;
            }
        }

        private void appendLong(long value) {
            if (value == Long.MIN_VALUE) {
                appendAscii("-9223372036854775808");
                return;
            }
            boolean negative = value < 0;
            long magnitude = negative ? -value : value;
            int digits = 1;
            for (long remaining = magnitude; remaining >= 10; remaining /= 10) {
                digits++;
            }
            int width = digits + (negative ? 1 : 0);
            ensure(width);
            int end = length + width;
            int cursor = end;
            do {
                bytes[--cursor] = (byte) ('0' + magnitude % 10);
                magnitude /= 10;
            } while (magnitude != 0);
            if (negative) {
                bytes[length] = '-';
            }
            length = end;
        }

        private void appendThreeDigits(int value) {
            ensure(3);
            bytes[length++] = (byte) ('0' + value / 100);
            bytes[length++] = (byte) ('0' + value / 10 % 10);
            bytes[length++] = (byte) ('0' + value % 10);
        }

        private void appendCodePoint(int codePoint) {
            if (codePoint <= 0x7f) {
                appendByte(codePoint);
            } else if (codePoint <= 0x7ff) {
                ensure(2);
                bytes[length++] = (byte) (0xc0 | codePoint >>> 6);
                bytes[length++] = (byte) (0x80 | codePoint & 0x3f);
            } else if (codePoint <= 0xffff) {
                ensure(3);
                bytes[length++] = (byte) (0xe0 | codePoint >>> 12);
                bytes[length++] = (byte) (0x80 | codePoint >>> 6 & 0x3f);
                bytes[length++] = (byte) (0x80 | codePoint & 0x3f);
            } else {
                ensure(4);
                bytes[length++] = (byte) (0xf0 | codePoint >>> 18);
                bytes[length++] = (byte) (0x80 | codePoint >>> 12 & 0x3f);
                bytes[length++] = (byte) (0x80 | codePoint >>> 6 & 0x3f);
                bytes[length++] = (byte) (0x80 | codePoint & 0x3f);
            }
        }

        private void ensure(int additional) {
            int needed = length + additional;
            if (needed > bytes.length) {
                bytes = Arrays.copyOf(bytes, Math.max(needed, bytes.length + (bytes.length >>> 1)));
            }
        }

        private ByteBuffer toByteBuffer() {
            // Jetty accepts a bounded ByteBuffer, so the hot server path can write the populated
            // prefix directly instead of copying every ~300-KiB page into an exact-sized byte[].
            return ByteBuffer.wrap(bytes, 0, length);
        }

        private static boolean[] urlSafeTable() {
            boolean[] safe = new boolean[256];
            for (int value = 'A'; value <= 'Z'; value++) {
                safe[value] = true;
            }
            for (int value = 'a'; value <= 'z'; value++) {
                safe[value] = true;
            }
            for (int value = '0'; value <= '9'; value++) {
                safe[value] = true;
            }
            safe['-'] = true;
            safe['_'] = true;
            safe['.'] = true;
            safe['/'] = true;
            return safe;
        }
    }
}
