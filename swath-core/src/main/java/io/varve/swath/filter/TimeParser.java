/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.error.InvalidArgsException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

/**
 * Parses a date/time for the mtime filter into epoch micros (UTC). Accepts an
 * ISO instant ({@code 2026-01-01T12:00:00Z}), a local date-time (assumed UTC),
 * or a bare date ({@code 2026-01-01}, start of day UTC).
 */
public final class TimeParser {

    private TimeParser() {
    }

    public static long parseToMicros(String text) throws InvalidArgsException {
        String s = text.trim();
        Instant instant = tryInstant(s);
        if (instant == null) {
            instant = tryLocalDateTime(s);
        }
        if (instant == null) {
            instant = tryDate(s);
        }
        if (instant == null) {
            throw new InvalidArgsException("invalid date/time: " + text
                    + " (use 2026-01-01, 2026-01-01T12:00:00, or 2026-01-01T12:00:00Z)");
        }
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    private static Instant tryInstant(String s) {
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static Instant tryLocalDateTime(String s) {
        try {
            return LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static Instant tryDate(String s) {
        try {
            return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
