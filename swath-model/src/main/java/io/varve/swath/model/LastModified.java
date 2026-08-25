/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalQuery;
import java.util.List;
import java.util.Objects;

/** Conversions for consumers that explicitly need typed last-modified semantics. */
public final class LastModified {

    private static final long NOT_CANONICAL = Long.MIN_VALUE;

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
        int micros = (int) Math.floorMod(epochMicros, 1_000_000L);
        LocalDate date = LocalDate.ofEpochDay(Math.floorDiv(seconds, 86_400L));
        int year = date.getYear();
        if (year < 0 || year > 9_999) {
            return formatGeneralInstant(seconds, micros);
        }

        int fractionDigits = micros == 0 ? 0 : micros % 1_000 == 0 ? 3 : 6;
        byte[] text = new byte[20 + (fractionDigits == 0 ? 0 : fractionDigits + 1)];
        putDigits(text, 0, year, 4);
        text[4] = '-';
        putDigits(text, 5, date.getMonthValue(), 2);
        text[7] = '-';
        putDigits(text, 8, date.getDayOfMonth(), 2);
        text[10] = 'T';
        int secondOfDay = (int) Math.floorMod(seconds, 86_400L);
        putDigits(text, 11, secondOfDay / 3_600, 2);
        text[13] = ':';
        putDigits(text, 14, secondOfDay / 60 % 60, 2);
        text[16] = ':';
        putDigits(text, 17, secondOfDay % 60, 2);
        int z = 19;
        if (fractionDigits != 0) {
            text[19] = '.';
            putDigits(text, 20, fractionDigits == 3 ? micros / 1_000 : micros, fractionDigits);
            z += fractionDigits + 1;
        }
        text[z] = 'Z';
        return new String(text, StandardCharsets.US_ASCII);
    }

    /** Parse only for a consumer that explicitly requires timestamp semantics. */
    public static long epochMicrosFromText(String wireText) {
        if (wireText == null || wireText.isEmpty()) {
            return 0L;
        }
        long canonical = canonicalEpochMicros(wireText);
        if (canonical != NOT_CANONICAL) {
            return canonical;
        }
        Instant instant = parseWireInstant(wireText);
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    /**
     * Parses the narrow UTC form emitted by S3 and by {@link #textFromEpochMicros(long)} without
     * constructing the general formatter's parse graph. Anything outside that grammar, including
     * its unusual compatibility cases, falls through to {@link #parseWireInstant(String)}.
     */
    private static long canonicalEpochMicros(String value) {
        int length = value.length();
        int fractionDigits;
        if (length == 20 && value.charAt(19) == 'Z') {
            fractionDigits = 0;
        } else if (length >= 22 && length <= 30 && value.charAt(19) == '.'
                && value.charAt(length - 1) == 'Z') {
            fractionDigits = length - 21;
        } else {
            return NOT_CANONICAL;
        }
        if (value.charAt(4) != '-'
                || value.charAt(7) != '-'
                || value.charAt(10) != 'T'
                || value.charAt(13) != ':'
                || value.charAt(16) != ':') {
            return NOT_CANONICAL;
        }

        int year = parseDigits(value, 0, 4);
        int month = parseDigits(value, 5, 2);
        int day = parseDigits(value, 8, 2);
        int hour = parseDigits(value, 11, 2);
        int minute = parseDigits(value, 14, 2);
        int second = parseDigits(value, 17, 2);
        if ((year | month | day | hour | minute | second) < 0
                || hour > 23 || minute > 59 || second > 59) {
            return NOT_CANONICAL;
        }

        int fraction = fractionDigits == 0 ? 0 : parseDigits(value, 20, fractionDigits);
        if (fraction < 0) {
            return NOT_CANONICAL;
        }
        int micros = fraction;
        if (fractionDigits < 6) {
            for (int i = fractionDigits; i < 6; i++) {
                micros *= 10;
            }
        } else if (fractionDigits > 6) {
            for (int i = fractionDigits; i > 6; i--) {
                micros /= 10;
            }
        }

        try {
            long epochDay = LocalDate.of(year, month, day).toEpochDay();
            long epochSecond = epochDay * 86_400L + hour * 3_600L + minute * 60L + second;
            return epochSecond * 1_000_000L + micros;
        } catch (DateTimeException ignored) {
            return NOT_CANONICAL;
        }
    }

    private static int parseDigits(String value, int offset, int count) {
        int parsed = 0;
        for (int i = offset, end = offset + count; i < end; i++) {
            int digit = value.charAt(i) - '0';
            if (digit < 0 || digit > 9) {
                return -1;
            }
            parsed = parsed * 10 + digit;
        }
        return parsed;
    }

    private static void putDigits(byte[] target, int offset, int value, int count) {
        for (int i = offset + count - 1; i >= offset; i--) {
            target[i] = (byte) ('0' + value % 10);
            value /= 10;
        }
    }

    private static String formatGeneralInstant(long seconds, int micros) {
        return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(seconds, micros * 1_000L));
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
