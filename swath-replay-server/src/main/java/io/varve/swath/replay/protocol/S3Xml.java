/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Renders {@link S3ListResult} and {@link S3Error} as byte-faithful S3 ListObjectsV2 XML. */
public final class S3Xml {

    private static final DateTimeFormatter S3_LAST_MODIFIED =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    private S3Xml() {
    }

    public static String listBucket(S3ListResult result) {
        S3ListRequest request = result.request();
        StringBuilder xml = new StringBuilder(4096);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        element(xml, "Name", request.bucket());
        element(xml, "Prefix", text(request.prefix(), request.encodingTypeUrl()));
        if (request.continuationToken() != null) {
            element(xml, "ContinuationToken", request.continuationToken());
        }
        if (request.startAfter() != null) {
            element(xml, "StartAfter", text(request.startAfter(), request.encodingTypeUrl()));
        }
        if (result.nextContinuationToken() != null) {
            element(xml, "NextContinuationToken", result.nextContinuationToken());
        }
        element(xml, "KeyCount", Integer.toString(result.keyCount()));
        element(xml, "MaxKeys", Integer.toString(request.maxKeys()));
        if (request.delimiter() != null && request.delimiter().length > 0) {
            element(xml, "Delimiter", text(request.delimiter(), request.encodingTypeUrl()));
        }
        if (request.encodingTypeUrl()) {
            element(xml, "EncodingType", "url");
        }
        element(xml, "IsTruncated", Boolean.toString(result.truncated()));
        for (S3ResultEntry entry : result.entries()) {
            if (entry instanceof S3ResultEntry.ObjectResult object) {
                contents(xml, object.object(), request.encodingTypeUrl(), request.fetchOwner());
            }
        }
        for (S3ResultEntry entry : result.entries()) {
            if (entry instanceof S3ResultEntry.CommonPrefixResult prefix) {
                commonPrefix(xml, prefix.key(), request.encodingTypeUrl());
            }
        }
        xml.append("</ListBucketResult>");
        return xml.toString();
    }

    public static String error(String code, String message, String resource) {
        StringBuilder xml = new StringBuilder(512);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.append("<Error>");
        element(xml, "Code", code);
        element(xml, "Message", message);
        if (resource != null) {
            element(xml, "Resource", resource);
        }
        element(xml, "RequestId", "S3LISTINGREPLAY");
        xml.append("</Error>");
        return xml.toString();
    }

    private static void contents(StringBuilder xml, ListedObject object, boolean encoded, boolean fetchOwner) {
        xml.append("<Contents>");
        element(xml, "Key", text(object.key(), encoded));
        element(xml, "LastModified", S3_LAST_MODIFIED.format(instant(object.lastModifiedEpochMicros())));
        if (object.etag() != null) {
            element(xml, "ETag", "\"" + object.etag() + "\"");
        }
        if (object.checksumAlgorithm() != null) {
            element(xml, "ChecksumAlgorithm", object.checksumAlgorithm());
        }
        if (object.checksumType() != null) {
            element(xml, "ChecksumType", object.checksumType());
        }
        element(xml, "Size", Long.toString(object.size()));
        if (object.storageClass() != null) {
            element(xml, "StorageClass", object.storageClass());
        }
        if (fetchOwner && (object.ownerId() != null || object.ownerDisplayName() != null)) {
            xml.append("<Owner>");
            if (object.ownerId() != null) {
                element(xml, "ID", object.ownerId());
            }
            if (object.ownerDisplayName() != null) {
                element(xml, "DisplayName", object.ownerDisplayName());
            }
            xml.append("</Owner>");
        }
        xml.append("</Contents>");
    }

    private static void commonPrefix(StringBuilder xml, byte[] prefix, boolean encoded) {
        xml.append("<CommonPrefixes>");
        element(xml, "Prefix", text(prefix, encoded));
        xml.append("</CommonPrefixes>");
    }

    private static String text(byte[] raw, boolean encoded) {
        if (raw == null) {
            return "";
        }
        return encoded ? ByteKeys.percentEncode(raw) : ByteKeys.utf8(raw);
    }

    private static Instant instant(long epochMicros) {
        long seconds = Math.floorDiv(epochMicros, 1_000_000L);
        long micros = Math.floorMod(epochMicros, 1_000_000L);
        return Instant.ofEpochSecond(seconds, micros * 1000L);
    }

    private static void element(StringBuilder xml, String name, String value) {
        xml.append('<').append(name).append('>');
        escape(xml, value);
        xml.append("</").append(name).append('>');
    }

    private static void escape(StringBuilder xml, String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '&' -> xml.append("&amp;");
                case '<' -> xml.append("&lt;");
                case '>' -> xml.append("&gt;");
                case '"' -> xml.append("&quot;");
                case '\'' -> xml.append("&apos;");
                default -> xml.append(c);
            }
        }
    }
}
