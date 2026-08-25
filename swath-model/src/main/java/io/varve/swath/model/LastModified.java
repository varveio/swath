/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalQuery;
import java.util.List;
import java.util.Objects;

/** Conversions for consumers that explicitly need typed last-modified semantics. */
public final class LastModified {

    private static final DateTimeFormatter ALTERNATE_ISO_8601 = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .toFormatter()
            .withZone(ZoneOffset.UTC);
    private static final List<DateTimeFormatter> ISO_8601_FORMATTERS = List.of(
            DateTimeFormatter.ISO_INSTANT,
            ALTERNATE_ISO_8601,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    private LastModified() {
    }

    /** Epoch micros to canonical wire text; empty for the existing zero/missing sentinel. */
    public static String textFromEpochMicros(long epochMicros) {
        if (epochMicros == 0L) {
            return "";
        }
        long seconds = Math.floorDiv(epochMicros, 1_000_000L);
        long micros = Math.floorMod(epochMicros, 1_000_000L);
        return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(seconds, micros * 1_000L));
    }

    /** Parse only for a consumer that explicitly requires timestamp semantics. */
    public static long epochMicrosFromText(String wireText) {
        if (wireText == null || wireText.isEmpty()) {
            return 0L;
        }
        Instant instant = parseWireInstant(wireText);
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    /**
     * Matches the AWS SDK ISO-8601 parser's accepted grammar without making the model depend on
     * the SDK: {@code +0000} normalization followed by its three formatter fallbacks.
     */
    static Instant parseWireInstant(String input) {
        String value = input.endsWith("+0000")
                ? input.substring(0, input.length() - 5) + "Z"
                : input;
        DateTimeParseException failure = null;
        TemporalQuery<Instant> query = Instant::from;
        for (DateTimeFormatter formatter : ISO_8601_FORMATTERS) {
            try {
                DateTimeFormatter parser = formatter == DateTimeFormatter.ISO_OFFSET_DATE_TIME
                        ? formatter
                        : formatter.withZone(ZoneOffset.UTC);
                return parser.parse(value, query);
            } catch (DateTimeParseException e) {
                failure = e;
            }
        }
        throw Objects.requireNonNull(failure);
    }

}
