/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.BudgetedOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/** Synthetic-v1 GCS JSON encoder over the request-owned byte budget. */
public final class GcsJson {
    private static final JsonFactory JSON = JsonFactory.builder()
            .recyclerPool(JsonRecyclerPools.nonRecyclingPool()).build();
    private static final DateTimeFormatter MICROS = new DateTimeFormatterBuilder().appendInstant(6).toFormatter();
    private static final String ETAG = "swath-gcs-synthetic-v1";
    private static final byte[] ACME_PREFIX = ".well-known/acme-challenge/".getBytes(StandardCharsets.US_ASCII);

    private GcsJson() {
    }

    public static void write(GcsListRequest request, GcsPage page, BudgetedOutput output) {
        try (JsonGenerator json = JSON.createGenerator(output, JsonEncoding.UTF8)) {
            json.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
            json.writeStartObject();
            json.writeStringField("kind", "storage#objects");
            if (!page.objects().isEmpty()) {
                json.writeArrayFieldStart("items");
                for (ListedObject object : page.objects()) {
                    validateObjectName(object.key());
                    if (object.size() < 0) {
                        throw new FixtureProblem("negative_size");
                    }
                    Instant updated = instant(object.lastModifiedEpochMicros());
                    int year = updated.atOffset(ZoneOffset.UTC).getYear();
                    if (year < 0 || year > 9999) {
                        throw new FixtureProblem("timestamp_out_of_range");
                    }
                    json.writeStartObject();
                    json.writeStringField("kind", "storage#object");
                    json.writeStringField("bucket", request.bucket());
                    json.writeFieldName("name");
                    json.writeUTF8String(object.key(), 0, object.key().length);
                    json.writeStringField("size", Long.toString(object.size()));
                    json.writeStringField("updated", MICROS.format(updated));
                    json.writeStringField("etag", ETAG);
                    json.writeStringField("generation", "1");
                    json.writeStringField("metageneration", "1");
                    json.writeStringField("storageClass", "STANDARD");
                    json.writeStringField("contentType", "application/octet-stream");
                    if (request.fullProjection()) {
                        json.writeArrayFieldStart("acl");
                        json.writeEndArray();
                    }
                    json.writeEndObject();
                }
                json.writeEndArray();
            }
            if (!page.prefixes().isEmpty()) {
                json.writeArrayFieldStart("prefixes");
                for (byte[] prefix : page.prefixes()) {
                    validateUtf8(prefix, 1024);
                    json.writeUTF8String(prefix, 0, prefix.length);
                }
                json.writeEndArray();
            }
            if (page.nextPageToken() != null) {
                json.writeStringField("nextPageToken", page.nextPageToken());
            }
            json.writeEndObject();
        } catch (IOException e) {
            throw new IllegalStateException("GCS JSON encoding failed", e);
        }
    }

    private static Instant instant(long micros) {
        return Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
                Math.floorMod(micros, 1_000_000L) * 1000L);
    }

    private static void validateObjectName(byte[] bytes) {
        validateUtf8(bytes, 1024);
        if (bytes.length == 1 && bytes[0] == '.'
                || bytes.length == 2 && bytes[0] == '.' && bytes[1] == '.'
                || startsWith(bytes, ACME_PREFIX)) {
            throw new FixtureProblem("reserved_name");
        }
    }

    private static boolean startsWith(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static void validateUtf8(byte[] bytes, int maxLength) {
        if (bytes.length == 0 || bytes.length > maxLength) {
            throw new FixtureProblem("name_length");
        }
        for (int i = 0; i < bytes.length;) {
            int first = bytes[i] & 0xff;
            if (first < 0x80) {
                if (first == '\r' || first == '\n') {
                    throw new FixtureProblem("name_newline");
                }
                i++;
                continue;
            }
            int width = first >= 0xc2 && first <= 0xdf ? 2
                    : first >= 0xe0 && first <= 0xef ? 3
                    : first >= 0xf0 && first <= 0xf4 ? 4 : 0;
            if (width == 0 || i + width > bytes.length) {
                throw new FixtureProblem("invalid_utf8_name");
            }
            int second = bytes[i + 1] & 0xff;
            if ((second & 0xc0) != 0x80
                    || first == 0xe0 && second < 0xa0
                    || first == 0xed && second >= 0xa0
                    || first == 0xf0 && second < 0x90
                    || first == 0xf4 && second >= 0x90) {
                throw new FixtureProblem("invalid_utf8_name");
            }
            for (int j = 2; j < width; j++) {
                if ((bytes[i + j] & 0xc0) != 0x80) {
                    throw new FixtureProblem("invalid_utf8_name");
                }
            }
            i += width;
        }
    }

    public static final class FixtureProblem extends IllegalArgumentException {
        private final String reason;

        private FixtureProblem(String reason) {
            super(reason);
            this.reason = reason;
        }

        public String reason() { return reason; }
    }
}
