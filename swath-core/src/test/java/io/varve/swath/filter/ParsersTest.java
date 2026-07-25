/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ParsersTest {

    @Test
    void sizeParserHandlesBytesAndBinarySuffixes() throws Exception {
        assertThat(SizeParser.parse("1024")).isEqualTo(1024);
        assertThat(SizeParser.parse("1k")).isEqualTo(1024);
        assertThat(SizeParser.parse("256mb")).isEqualTo(256L * 1024 * 1024);
        assertThat(SizeParser.parse("1.5g")).isEqualTo((long) (1.5 * 1024 * 1024 * 1024));
        assertThat(SizeParser.parse("2T")).isEqualTo(2L * 1024 * 1024 * 1024 * 1024);
    }

    @Test
    void sizeParserRejectsGarbage() {
        assertThatThrownBy(() -> SizeParser.parse("abc")).isInstanceOf(InvalidArgsException.class);
        assertThatThrownBy(() -> SizeParser.parse("10pb")).isInstanceOf(InvalidArgsException.class);
    }

    @Test
    void sizeParserRejectsOverflowAndFractionalBytes() {
        assertThatThrownBy(() -> SizeParser.parse("999999999999999999999999999999999999999t"))
                .isInstanceOf(InvalidArgsException.class);
        assertThatThrownBy(() -> SizeParser.parse("1.5"))
                .isInstanceOf(InvalidArgsException.class);
    }

    @Test
    void timeParserAcceptsDateDatetimeAndInstant() throws Exception {
        long date = TimeParser.parseToMicros("2026-01-01");
        assertThat(date).isEqualTo(Instant.parse("2026-01-01T00:00:00Z").getEpochSecond() * 1_000_000L);
        long instant = TimeParser.parseToMicros("2026-01-01T12:00:00Z");
        assertThat(instant).isEqualTo(Instant.parse("2026-01-01T12:00:00Z").getEpochSecond() * 1_000_000L);
        // local date-time assumed UTC
        assertThat(TimeParser.parseToMicros("2026-01-01T12:00:00")).isEqualTo(instant);
    }

    @Test
    void timeParserRejectsGarbage() {
        assertThatThrownBy(() -> TimeParser.parseToMicros("not-a-date")).isInstanceOf(InvalidArgsException.class);
    }
}
