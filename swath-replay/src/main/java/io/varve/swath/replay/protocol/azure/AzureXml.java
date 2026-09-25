/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.azure;

import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.BudgetedOutput;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Synthetic-v1 Azure flat List Blobs XML encoder. */
public final class AzureXml {
    private static final DateTimeFormatter HTTP_TIME = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    private AzureXml() {
    }

    public static void write(AzureListResult result, String endpoint, BudgetedOutput out) {
        write(result, endpoint, out, null);
    }

    public static void write(AzureListResult result, String endpoint, BudgetedOutput out,
                             ReplayMetrics metrics) {
        AzureListRequest request = result.request();
        out.appendAscii("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
        out.appendAscii("<EnumerationResults ServiceEndpoint=\"");
        out.appendEscaped(endpoint);
        out.appendAscii("\" ContainerName=\"");
        out.appendEscaped(request.container());
        out.appendAscii("\">");
        if (request.prefixSupplied()) element(out, "Prefix", request.prefix() == null ? "" : request.prefix());
        if (request.markerSupplied()) element(out, "Marker", request.marker() == null ? "" : request.marker());
        if (request.maxResultsSupplied()) element(out, "MaxResults", request.requestedMaxResults());
        if (request.delimiterSupplied()) element(out, "Delimiter",
                request.delimiter() == null ? "" : request.delimiter());
        out.appendAscii("<Blobs>");
        long cachedSecond = Long.MIN_VALUE;
        String cachedTime = null;
        for (AzureListResult.Entry entry : result.entries()) {
            if (entry instanceof AzureListResult.Entry.Blob blob) {
                ListedObject object = blob.object();
                if (object.size() < 0) {
                    throw new FixtureProblem("negative_size");
                }
                out.appendAscii("<Blob>");
                name(out, object.key(), false, metrics);
                out.appendAscii("<Properties>");
                long second = Math.floorDiv(object.lastModifiedEpochMicros(), 1_000_000L);
                if (cachedSecond != second) {
                    cachedTime = HTTP_TIME.format(Instant.ofEpochSecond(second));
                    cachedSecond = second;
                }
                element(out, "Last-Modified", cachedTime);
                element(out, "Etag", "0x000000000000001");
                out.appendAscii("<Content-Length>");
                out.appendLong(object.size());
                out.appendAscii("</Content-Length>");
                element(out, "Content-Type", "application/octet-stream");
                element(out, "BlobType", "BlockBlob");
                element(out, "AccessTier", "Hot");
                element(out, "AccessTierInferred", "true");
                element(out, "LeaseStatus", "unlocked");
                element(out, "LeaseState", "available");
                out.appendAscii("</Properties></Blob>");
            } else if (entry instanceof AzureListResult.Entry.BlobPrefix prefix) {
                out.appendAscii("<BlobPrefix>");
                name(out, prefix.name(), true, metrics);
                out.appendAscii("</BlobPrefix>");
            }
        }
        out.appendAscii("</Blobs><NextMarker>");
        if (result.nextMarker() != null) out.appendEscaped(result.nextMarker());
        out.appendAscii("</NextMarker></EnumerationResults>");
    }

    private static void name(BudgetedOutput out, byte[] raw, boolean derivedPrefix, ReplayMetrics metrics) {
        String decoded = decodeName(raw, derivedPrefix);
        boolean encoded = decoded.indexOf('\ufffe') >= 0 || decoded.indexOf('\uffff') >= 0;
        if (encoded && metrics != null) {
            metrics.recordProviderPath("azure", "encoded_name", derivedPrefix ? "prefix_xml_forbidden"
                    : "blob_xml_forbidden");
        }
        out.appendAscii(encoded ? "<Name Encoded=\"true\">" : "<Name>");
        if (encoded) {
            for (byte b : raw) {
                int value = b & 0xff;
                if (value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
                        || value >= '0' && value <= '9' || value == '-' || value == '_' || value == '.'
                        || value == '~') {
                    out.appendByte(value);
                } else {
                    out.appendByte('%');
                    out.appendByte(HEX[value >>> 4]);
                    out.appendByte(HEX[value & 15]);
                }
            }
        } else {
            appendXmlText(out, decoded);
        }
        out.appendAscii("</Name>");
    }

    private static String decodeName(byte[] raw, boolean derivedPrefix) {
        if (raw.length == 0) throw new FixtureProblem("empty_name");
        String name;
        try {
            name = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
        } catch (CharacterCodingException e) {
            throw new FixtureProblem("invalid_utf8_name");
        }
        if (name.length() > 1024) {
            throw new FixtureProblem("name_too_long");
        }
        int segments = 1;
        for (int i = 0; i < name.length();) {
            int cp = name.codePointAt(i);
            if (cp < 0x20 && (!derivedPrefix || cp != '\t' && cp != '\n' && cp != '\r')) {
                throw new FixtureProblem("control_character");
            }
            if (cp == '/') segments++;
            i += Character.charCount(cp);
        }
        if (!derivedPrefix && segments > 254) {
            throw new FixtureProblem("too_many_segments");
        }
        return name;
    }

    private static void element(BudgetedOutput out, String tag, String value) {
        out.appendAscii("<" + tag + ">");
        out.appendEscaped(value);
        out.appendAscii("</" + tag + ">");
    }

    private static void appendXmlText(BudgetedOutput out, String value) {
        int from = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == '\r') {
                if (i > from) out.appendEscaped(value.substring(from, i));
                out.appendAscii("&#13;");
                from = i + 1;
            }
        }
        if (from < value.length()) out.appendEscaped(value.substring(from));
    }

    /** Typed fixture validation keeps incompatible data distinct from bad client requests. */
    public static final class FixtureProblem extends IllegalArgumentException {
        private final String reason;

        FixtureProblem(String reason) {
            super(reason);
            this.reason = reason;
        }

        public String reason() { return reason; }
    }
}
