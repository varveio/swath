/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol.gcs;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;
import io.varve.swath.replay.metrics.ReplayMetrics;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.BudgetedOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

/** Synthetic-v1 GCS JSON encoder over the request-owned byte budget. */
public final class GcsJson {
    private static final JsonFactory JSON = JsonFactory.builder()
            .recyclerPool(JsonRecyclerPools.nonRecyclingPool()).build();
    private static final byte[] TIMESTAMP = "0000-00-00T00:00:00.000000Z"
            .getBytes(StandardCharsets.US_ASCII);
    private static final String ETAG = "swath-gcs-synthetic-v1";
    private static final byte[] ACME_PREFIX = ".well-known/acme-challenge/".getBytes(StandardCharsets.US_ASCII);
    private static final SerializedString KIND = new SerializedString("kind");
    private static final SerializedString ITEMS = new SerializedString("items");
    private static final SerializedString PREFIXES = new SerializedString("prefixes");
    private static final SerializedString NEXT_PAGE_TOKEN = new SerializedString("nextPageToken");
    private static final SerializedString BUCKET = new SerializedString("bucket");
    private static final SerializedString NAME = new SerializedString("name");
    private static final SerializedString SIZE = new SerializedString("size");
    private static final SerializedString UPDATED = new SerializedString("updated");
    private static final SerializedString ETAG_FIELD = new SerializedString("etag");
    private static final SerializedString GENERATION = new SerializedString("generation");
    private static final SerializedString METAGENERATION = new SerializedString("metageneration");
    private static final SerializedString STORAGE_CLASS = new SerializedString("storageClass");
    private static final SerializedString CONTENT_TYPE = new SerializedString("contentType");
    private static final SerializedString ACL = new SerializedString("acl");

    private GcsJson() {
    }

    public static void write(GcsListRequest request, GcsPage page, BudgetedOutput output) {
        write(request, page, output, null);
    }

    public static void write(GcsListRequest request, GcsPage page, BudgetedOutput output,
                             ReplayMetrics metrics) {
        int dayChanges = 0;
        try (JsonGenerator json = JSON.createGenerator(output, JsonEncoding.UTF8)) {
            json.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
            json.writeStartObject();
            stringField(json, KIND, "storage#objects");
            if (!page.objects().isEmpty()) {
                json.writeFieldName(ITEMS);
                json.writeStartArray();
                long cachedDay = Long.MIN_VALUE;
                byte[] updatedBytes = TIMESTAMP.clone();
                byte[] decimal = new byte[20];
                for (ListedObject object : page.objects()) {
                    validateObjectName(object.key());
                    if (object.size() < 0) {
                        throw new FixtureProblem("negative_size");
                    }
                    long second = Math.floorDiv(object.lastModifiedEpochMicros(), 1_000_000L);
                    int fraction = (int) Math.floorMod(object.lastModifiedEpochMicros(), 1_000_000L);
                    long day = Math.floorDiv(second, 86_400L);
                    if (cachedDay != day) {
                        LocalDate date = LocalDate.ofEpochDay(day);
                        int year = date.getYear();
                        if (year < 0 || year > 9999) {
                            throw new FixtureProblem("timestamp_out_of_range");
                        }
                        fourDigits(updatedBytes, 0, year);
                        twoDigits(updatedBytes, 5, date.getMonthValue());
                        twoDigits(updatedBytes, 8, date.getDayOfMonth());
                        cachedDay = day;
                        dayChanges++;
                    }
                    int time = (int) Math.floorMod(second, 86_400L);
                    twoDigits(updatedBytes, 11, time / 3600);
                    twoDigits(updatedBytes, 14, time / 60 % 60);
                    twoDigits(updatedBytes, 17, time % 60);
                    for (int index = 25; index >= 20; index--) {
                        updatedBytes[index] = (byte) ('0' + fraction % 10);
                        fraction /= 10;
                    }
                    json.writeStartObject();
                    stringField(json, KIND, "storage#object");
                    stringField(json, BUCKET, request.bucket());
                    json.writeFieldName(NAME);
                    json.writeUTF8String(object.key(), 0, object.key().length);
                    json.writeFieldName(SIZE);
                    writeDecimalString(json, object.size(), decimal);
                    json.writeFieldName(UPDATED);
                    json.writeUTF8String(updatedBytes, 0, updatedBytes.length);
                    stringField(json, ETAG_FIELD, ETAG);
                    stringField(json, GENERATION, "1");
                    stringField(json, METAGENERATION, "1");
                    stringField(json, STORAGE_CLASS, "STANDARD");
                    stringField(json, CONTENT_TYPE, "application/octet-stream");
                    if (request.fullProjection()) {
                        json.writeFieldName(ACL);
                        json.writeStartArray();
                        json.writeEndArray();
                    }
                    json.writeEndObject();
                }
                json.writeEndArray();
            }
            if (!page.prefixes().isEmpty()) {
                json.writeFieldName(PREFIXES);
                json.writeStartArray();
                for (byte[] prefix : page.prefixes()) {
                    validateUtf8(prefix, 1024);
                    json.writeUTF8String(prefix, 0, prefix.length);
                }
                json.writeEndArray();
            }
            if (page.nextPageToken() != null) {
                stringField(json, NEXT_PAGE_TOKEN, page.nextPageToken());
            }
            json.writeEndObject();
        } catch (IOException e) {
            throw new IllegalStateException("GCS JSON encoding failed", e);
        }
        if (metrics != null) {
            metrics.recordProviderPath("gcs", "timestamp_render", dayChanges == 0 ? "no_objects"
                    : dayChanges == 1 ? "single_utc_day" : "multiple_utc_days");
        }
    }

    private static void twoDigits(byte[] target, int offset, int value) {
        target[offset] = (byte) ('0' + value / 10);
        target[offset + 1] = (byte) ('0' + value % 10);
    }

    private static void fourDigits(byte[] target, int offset, int value) {
        target[offset] = (byte) ('0' + value / 1000);
        target[offset + 1] = (byte) ('0' + value / 100 % 10);
        target[offset + 2] = (byte) ('0' + value / 10 % 10);
        target[offset + 3] = (byte) ('0' + value % 10);
    }

    private static void stringField(JsonGenerator json, SerializedString name, String value) throws IOException {
        json.writeFieldName(name);
        json.writeString(value);
    }

    private static void writeDecimalString(JsonGenerator json, long value, byte[] scratch) throws IOException {
        int start = scratch.length;
        do {
            scratch[--start] = (byte) ('0' + value % 10);
            value /= 10;
        } while (value != 0);
        json.writeUTF8String(scratch, start, scratch.length - start);
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
