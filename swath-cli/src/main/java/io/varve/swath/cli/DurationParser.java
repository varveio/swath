/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidConfigException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * The shared duration grammar behind every {@code DURATION}-typed {@code swath list} flag
 * ({@code --progress-interval}, the internal metrics interval, {@code
 * --part-rotation-interval}, the
 * liveness windows): a suffix form ({@code ms}/{@code s}/{@code m}/{@code h}) or ISO-8601 via
 * {@link Duration#parse}, differing only in whether {@code 0}/{@code none}/{@code off} is an
 * explicit "disabled" and whether zero itself is then rejected.
 */
final class DurationParser {

    private DurationParser() {
    }

    static Duration progressInterval(String raw) throws InvalidConfigException {
        return parse(raw, "progress-interval", false);
    }

    static Duration metricsInterval(String raw) throws InvalidConfigException {
        return parse(raw, "metrics-interval", false);
    }

    /**
     * Parse a duration flag.
     *
     * @param zeroDisables {@code true} for options where zero means "disabled" (rotation interval:
     *                     negative is rejected, zero is not); {@code false} for options that must be
     *                     strictly positive (progress interval: zero or negative is rejected)
     */
    static Duration parse(String raw, String optionName, boolean zeroDisables)
            throws InvalidConfigException {
        String value = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
        if (value.isEmpty()) {
            throw new InvalidConfigException("--" + optionName + " must not be empty");
        }
        if (zeroDisables && (value.equals("0") || value.equals("none") || value.equals("off"))) {
            return Duration.ZERO;   // disabled
        }
        try {
            Duration parsed = switch (suffix(value)) {
                case "ms" -> Duration.ofMillis(digits(value, 2));
                case "s" -> Duration.ofSeconds(digits(value, 1));
                case "m" -> Duration.ofMinutes(digits(value, 1));
                case "h" -> Duration.ofHours(digits(value, 1));
                default -> Duration.parse(raw);
            };
            if (zeroDisables ? parsed.isNegative() : (parsed.isZero() || parsed.isNegative())) {
                throw new InvalidConfigException(zeroDisables
                        ? "--" + optionName + " must not be negative (got " + raw + ")"
                        : "--" + optionName + " must be positive (got " + raw + ")");
            }
            return parsed;
        } catch (NumberFormatException | DateTimeParseException e) {
            throw new InvalidConfigException(zeroDisables
                    ? "invalid --" + optionName + ": " + raw + " (expected e.g. 30s, 500ms, PT30S, or 0/none to disable)"
                    : "invalid --" + optionName + ": " + raw + " (expected e.g. 2s, 500ms, or PT2S)", e);
        }
    }

    private static String suffix(String value) {
        if (value.endsWith("ms") && isUnsignedDigits(value.substring(0, value.length() - 2))) {
            return "ms";
        }
        if (value.endsWith("s") && isUnsignedDigits(value.substring(0, value.length() - 1))) {
            return "s";
        }
        if (value.endsWith("m") && isUnsignedDigits(value.substring(0, value.length() - 1))) {
            return "m";
        }
        if (value.endsWith("h") && isUnsignedDigits(value.substring(0, value.length() - 1))) {
            return "h";
        }
        return "";
    }

    private static boolean isUnsignedDigits(String value) {
        return !value.isEmpty() && value.chars().allMatch(Character::isDigit);
    }

    private static long digits(String value, int suffixLength) {
        return Long.parseLong(value.substring(0, value.length() - suffixLength));
    }
}
