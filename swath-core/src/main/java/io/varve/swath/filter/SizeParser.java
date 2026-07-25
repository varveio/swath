/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.error.InvalidArgsException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;

/**
 * Parses a size like {@code 1024}, {@code 64k}, {@code 256mb}, {@code 1.5g} into
 * bytes. Binary multipliers (1024-based): {@code k/kb}, {@code m/mb}, {@code
 * g/gb}, {@code t/tb}. A bare number is bytes.
 */
public final class SizeParser {

    private SizeParser() {
    }

    public static long parse(String text) throws InvalidArgsException {
        String s = text.trim().toLowerCase(Locale.ROOT);
        if (s.isEmpty()) {
            throw new InvalidArgsException("empty size");
        }
        int i = 0;
        while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) {
            i++;
        }
        String numberPart = s.substring(0, i);
        String unit = s.substring(i).trim();
        if (numberPart.isEmpty()) {
            throw new InvalidArgsException("invalid size: " + text);
        }
        BigDecimal value;
        try {
            value = new BigDecimal(numberPart);
        } catch (NumberFormatException e) {
            throw new InvalidArgsException("invalid size number: " + text, e);
        }
        if (value.signum() < 0) {
            throw new InvalidArgsException("size must be non-negative: " + text);
        }
        long multiplier = switch (unit) {
            case "", "b" -> 1L;
            case "k", "kb", "kib" -> 1024L;
            case "m", "mb", "mib" -> 1024L * 1024;
            case "g", "gb", "gib" -> 1024L * 1024 * 1024;
            case "t", "tb", "tib" -> 1024L * 1024 * 1024 * 1024;
            default -> throw new InvalidArgsException("unknown size unit '" + unit + "' in: " + text);
        };
        BigDecimal bytes = value.multiply(BigDecimal.valueOf(multiplier));
        try {
            return bytes.setScale(0, RoundingMode.UNNECESSARY).longValueExact();
        } catch (ArithmeticException e) {
            throw new InvalidArgsException("size is out of range or not an exact byte count: " + text, e);
        }
    }
}
