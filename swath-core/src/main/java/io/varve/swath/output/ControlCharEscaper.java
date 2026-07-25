/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

/**
 * Escapes terminal-dangerous bytes in text-sink output: C0
 * controls {@code \x00..\x1f} (incl. newline {@code \x0a}, tab {@code \x09}, and
 * ANSI ESC {@code \x1b}) and DEL {@code \x7f}, rendered as {@code \xHH}.
 * On by default for text formatters; {@code --raw-output} bypasses it.
 *
 * <p><b>Fast path:</b> a clean string is returned by identity — no allocation.
 */
public final class ControlCharEscaper {

    private ControlCharEscaper() {
    }

    public static boolean needsEscaping(char c) {
        return c < 0x20 || c == 0x7f;
    }

    /** Escape control/DEL characters; returns the same instance when nothing needs escaping. */
    public static String escape(String s) {
        int n = s.length();
        int firstDirty = -1;
        for (int i = 0; i < n; i++) {
            if (needsEscaping(s.charAt(i))) {
                firstDirty = i;
                break;
            }
        }
        if (firstDirty < 0) {
            return s;   // no-alloc fast path
        }
        StringBuilder out = new StringBuilder(n + 8);
        out.append(s, 0, firstDirty);
        for (int i = firstDirty; i < n; i++) {
            char c = s.charAt(i);
            if (needsEscaping(c)) {
                out.append('\\').append('x').append(Hex.digit(c >> 4)).append(Hex.digit(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
