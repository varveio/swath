/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The fast path is allocation-free (identity) on clean keys; control bytes,
 * DEL, newline, tab and ANSI ESC are escaped.
 */
class ControlCharEscaperTest {

    private static String wrap(int b) {
        return "a" + (char) b + "b";
    }

    @Test
    void cleanKeyReturnsSameInstanceNoAlloc() {
        String clean = "crawl=01/pid=ab/part-00001.parquet";
        assertThat(ControlCharEscaper.escape(clean)).isSameAs(clean);
    }

    @Test
    void escapesC0ControlsDelNewlineTabEsc() {
        assertThat(ControlCharEscaper.escape(wrap(0x00))).isEqualTo("a\\x00b");
        assertThat(ControlCharEscaper.escape(wrap(0x0a))).isEqualTo("a\\x0ab");   // newline
        assertThat(ControlCharEscaper.escape(wrap(0x09))).isEqualTo("a\\x09b");   // tab
        assertThat(ControlCharEscaper.escape(wrap(0x1b))).isEqualTo("a\\x1bb");   // ANSI ESC
        assertThat(ControlCharEscaper.escape(wrap(0x7f))).isEqualTo("a\\x7fb");   // DEL
    }

    @Test
    void escapesRunsOfControls() {
        String s = "" + (char) 0x01 + (char) 0x02 + (char) 0x1f;
        assertThat(ControlCharEscaper.escape(s)).isEqualTo("\\x01\\x02\\x1f");
    }

    @Test
    void highBytesAndNormalCharsArePreserved() {
        // Multibyte (non-control) chars are not escaped and must pass through untouched.
        String s = "café/€/数据";
        assertThat(ControlCharEscaper.escape(s)).isSameAs(s);
    }
}
