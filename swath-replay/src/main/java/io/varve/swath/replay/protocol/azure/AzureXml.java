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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Synthetic-v1 Azure flat List Blobs XML encoder. */
public final class AzureXml {
    private static final DateTimeFormatter HTTP_TIME = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ROOT).withZone(ZoneOffset.UTC);
    private static final long FIRST_RENDERABLE_SECOND = LocalDateTime.of(1, 1, 1, 0, 0)
            .toEpochSecond(ZoneOffset.UTC);
    private static final long LAST_RENDERABLE_SECOND = LocalDateTime.of(9999, 12, 31, 23, 59, 59)
            .toEpochSecond(ZoneOffset.UTC);
    private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
    private static final String PROLOG_AND_ROOT = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
            + "<EnumerationResults ServiceEndpoint=\"";
    private static final String BLOB_PROPERTIES_BEFORE_SIZE = "<Properties><Last-Modified>";
    private static final String BLOB_PROPERTIES_AFTER_TIME = "</Last-Modified><Etag>0x000000000000001</Etag>"
            + "<Content-Length>";
    private static final String BLOB_PROPERTIES_AFTER_SIZE = "</Content-Length>"
            + "<Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>"
            + "<AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>"
            + "<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties></Blob>";

    private AzureXml() {
    }

    public static void write(AzureListResult result, String endpoint, BudgetedOutput out) {
        write(result, endpoint, out, null);
    }

    public static void write(AzureListResult result, String endpoint, BudgetedOutput out,
                             ReplayMetrics metrics) {
        AzureListRequest request = result.request();
        out.appendAscii(PROLOG_AND_ROOT);
        out.appendEscaped(endpoint);
        out.appendAscii("\" ContainerName=\"");
        out.appendEscaped(request.container());
        out.appendAscii("\">");
        if (request.prefixSupplied()) echo(out, "<Prefix>", "</Prefix>",
                request.prefix() == null ? "" : request.prefix());
        if (request.markerSupplied()) echo(out, "<Marker>", "</Marker>",
                request.marker() == null ? "" : request.marker());
        if (request.maxResultsSupplied()) echo(out, "<MaxResults>", "</MaxResults>",
                request.requestedMaxResults());
        if (request.delimiterSupplied()) echo(out, "<Delimiter>", "</Delimiter>",
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
                out.appendAscii(BLOB_PROPERTIES_BEFORE_SIZE);
                long second = Math.floorDiv(object.lastModifiedEpochMicros(), 1_000_000L);
                if (second < FIRST_RENDERABLE_SECOND || second > LAST_RENDERABLE_SECOND) {
                    throw new FixtureProblem("timestamp_year_out_of_range");
                }
                if (cachedSecond != second) {
                    cachedTime = HTTP_TIME.format(Instant.ofEpochSecond(second));
                    cachedSecond = second;
                }
                out.appendAscii(cachedTime);
                out.appendAscii(BLOB_PROPERTIES_AFTER_TIME);
                out.appendLong(object.size());
                out.appendAscii(BLOB_PROPERTIES_AFTER_SIZE);
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
        if (raw.length == 0) throw new FixtureProblem("empty_name");
        boolean ascii = true;
        int segments = 1;
        for (byte value : raw) {
            int b = value & 0xff;
            if (b >= 0x80) ascii = false;
            else {
                if (b < 0x20 && (!derivedPrefix || b != '\t' && b != '\n' && b != '\r')) {
                    throw new FixtureProblem("control_character");
                }
                if (b == '/') segments++;
            }
        }
        if (ascii) {
            if (raw.length > 1024) throw new FixtureProblem("name_too_long");
            if (!derivedPrefix && segments > 254) throw new FixtureProblem("too_many_segments");
            out.appendAscii("<Name>");
            appendAsciiName(out, raw);
            out.appendAscii("</Name>");
            return;
        }
        String decoded = decodeNonAsciiName(raw, derivedPrefix);
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

    private static String decodeNonAsciiName(byte[] raw, boolean derivedPrefix) {
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

    private static void echo(BudgetedOutput out, String open, String close, String value) {
        out.appendAscii(open);
        out.appendEscaped(value);
        out.appendAscii(close);
    }

    private static void appendAsciiName(BudgetedOutput out, byte[] raw) {
        int from = 0;
        for (int i = 0; i < raw.length; i++) {
            String replacement = switch (raw[i]) {
                case '&' -> "&amp;";
                case '<' -> "&lt;";
                case '>' -> "&gt;";
                case '"' -> "&quot;";
                case '\'' -> "&apos;";
                case '\r' -> "&#13;";
                default -> null;
            };
            if (replacement != null) {
                if (i > from) out.write(raw, from, i - from);
                out.appendAscii(replacement);
                from = i + 1;
            }
        }
        if (from < raw.length) out.write(raw, from, raw.length - from);
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
