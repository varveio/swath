/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

/** Minimal JSON string escaping (RFC 8259). */
final class Json {

    private Json() {
    }

    /** Append {@code s} as a quoted JSON string. */
    static void quote(StringBuilder out, String s) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) {
                        out.append("\\u00").append(Hex.digit(c >> 4)).append(Hex.digit(c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
