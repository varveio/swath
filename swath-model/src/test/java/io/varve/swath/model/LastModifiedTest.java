/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class LastModifiedTest {

    @Test
    void wireTextIsPreservedWithoutCanonicalization() {
        String value = "2026-08-24T12:34:56+0000";

        assertThat(value).isEqualTo("2026-08-24T12:34:56+0000");
        assertThat(LastModified.epochMicrosFromText(value)).isEqualTo(1_787_574_896_000_000L);
    }

    @Test
    void typedCompatibilityValueRendersTheExistingCanonicalText() {
        String value = LastModified.textFromEpochMicros(1_700_000_000_123_456L);

        assertThat(value).isEqualTo("2023-11-14T22:13:20.123456Z");
        assertThat(LastModified.epochMicrosFromText(value)).isEqualTo(1_700_000_000_123_456L);
    }

    @Test
    void typedParsingRetainsTheAwsSdkAcceptedFallbackGrammar() {
        assertThat(LastModified.epochMicrosFromText("2026-02-29T00:00:00Z"))
                .isEqualTo(1_772_236_800_000_000L);
        assertThat(LastModified.epochMicrosFromText("2026-08-24T14:34:56+02:00"))
                .isEqualTo(1_787_574_896_000_000L);
    }

    @Test
    void missingAndCanonicalValuesHaveStableText() {
        assertThat(LastModified.epochMicrosFromText(null)).isZero();
        assertThat(LastModified.epochMicrosFromText("")).isZero();
        assertThat(LastModified.textFromEpochMicros(1_700_000_000_123_456L))
                .isEqualTo("2023-11-14T22:13:20.123456Z");
    }

    @Test
    void objectEntryMapsUnsupportedWireTextToAnExplicitParseFailure() {
        ObjectEntry entry = new ObjectEntry(KeyBytes.ofUtf8("bad-time"), 1L, "not-a-timestamp",
                "etag", "STANDARD", null, true, null, null, null, null);

        assertThatThrownBy(entry::lastModifiedEpochMicros)
                .isInstanceOf(LastModifiedParseException.class)
                .hasMessage("invalid last-modified timestamp in object-store response")
                .hasCauseInstanceOf(java.time.format.DateTimeParseException.class)
                .satisfies(error -> {
                    LastModifiedParseException parseFailure = (LastModifiedParseException) error;
                    assertThat(parseFailure.key()).isEqualTo(entry.key());
                    assertThat(parseFailure.lastModifiedText()).isEqualTo("not-a-timestamp");
                });
    }
}
