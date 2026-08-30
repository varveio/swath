/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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

    @ParameterizedTest
    @MethodSource("representativeEpochMicros")
    void canonicalFormattingRemainsByteExact(long epochMicros) {
        long seconds = Math.floorDiv(epochMicros, 1_000_000L);
        long micros = Math.floorMod(epochMicros, 1_000_000L);
        String expected = DateTimeFormatter.ISO_INSTANT.format(
                Instant.ofEpochSecond(seconds, micros * 1_000L));

        assertThat(LastModified.textFromEpochMicros(epochMicros)).isEqualTo(expected);
        assertThat(LastModified.epochMicrosFromText(expected)).isEqualTo(epochMicros);
    }

    private static LongStream representativeEpochMicros() {
        return LongStream.of(
                Long.MIN_VALUE,
                -9_223_372_036_854_775_807L,
                -62_167_219_200_000_000L,
                -1_000_001L,
                -1L,
                1L,
                999L,
                1_000L,
                123_000L,
                123_400L,
                999_999L,
                1_700_000_000_123_456L,
                253_402_300_799_999_999L,
                Long.MAX_VALUE);
    }

    @Test
    void canonicalParserMatchesInstantForEverySupportedFractionWidth() {
        for (int digits = 0; digits <= 9; digits++) {
            String fraction = digits == 0 ? "" : ".123456789".substring(0, digits + 1);
            String value = "2024-02-29T23:59:58" + fraction + "Z";
            Instant expected = Instant.parse(value);
            long expectedMicros = expected.getEpochSecond() * 1_000_000L
                    + expected.getNano() / 1_000L;

            assertThat(LastModified.epochMicrosFromText(value)).as(value).isEqualTo(expectedMicros);
        }
    }

    @Test
    void typedParsingRetainsTheAwsSdkAcceptedFallbackGrammar() {
        assertThat(LastModified.epochMicrosFromText("2026-02-29T00:00:00Z"))
                .isEqualTo(1_772_236_800_000_000L);
        assertThat(LastModified.epochMicrosFromText("2026-08-24T14:34:56+02:00"))
                .isEqualTo(1_787_574_896_000_000L);
        Instant leapSecond = Instant.parse("2016-12-31T23:59:60Z");
        assertThat(LastModified.epochMicrosFromText("2016-12-31T23:59:60Z"))
                .isEqualTo(leapSecond.getEpochSecond() * 1_000_000L
                        + leapSecond.getNano() / 1_000L);
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

    @Test
    void objectEntryRetainsRecordValueSemanticsAcrossTimestampConstructors() {
        long epochMicros = 1_700_000_000_123_456L;
        String text = LastModified.textFromEpochMicros(epochMicros);
        ObjectEntry fromText = objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "STANDARD",
                "version", true, "owner", "display", "SHA256", "FULL_OBJECT");
        ObjectEntry fromTyped = new ObjectEntry(KeyBytes.ofUtf8("key"), 7L, epochMicros, "etag", "STANDARD",
                "version", true, "owner", "display", "SHA256", "FULL_OBJECT");

        assertThat(fromText).isEqualTo(fromTyped).hasSameHashCodeAs(fromTyped);
        assertThat(fromText.toString()).isEqualTo("ObjectEntry[key=key, size=7, lastModifiedText=" + text
                + ", etag=etag, storageClass=STANDARD, versionId=version, isLatest=true, ownerId=owner"
                + ", ownerDisplayName=display, checksumAlgorithm=SHA256, checksumType=FULL_OBJECT]");

        assertThat(List.of(
                objectEntry(KeyBytes.ofUtf8("other"), 7L, text, "etag", "STANDARD",
                        "version", true, "owner", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 8L, text, "etag", "STANDARD",
                        "version", true, "owner", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, "2023-11-14T22:13:21Z", "etag", "STANDARD",
                        "version", true, "owner", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "other", "STANDARD",
                        "version", true, "owner", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "GLACIER",
                        "version", true, "owner", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "STANDARD",
                        "other", true, "owner", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "STANDARD",
                        "version", false, "owner", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "STANDARD",
                        "version", true, "other", "display", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "STANDARD",
                        "version", true, "owner", "other", "SHA256", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "STANDARD",
                        "version", true, "owner", "display", "CRC32", "FULL_OBJECT"),
                objectEntry(KeyBytes.ofUtf8("key"), 7L, text, "etag", "STANDARD",
                        "version", true, "owner", "display", "SHA256", "COMPOSITE")))
                .doesNotContain(fromText);
    }

    @Test
    void typedObjectEntryCachesTheFullEpochMicrosDomainWithoutASentinelCollision() {
        ObjectEntry entry = new ObjectEntry(KeyBytes.ofUtf8("minimum-time"), 1L, Long.MIN_VALUE,
                "etag", "STANDARD", null, true, null, null, null, null);

        assertThat(entry.lastModifiedEpochMicros()).isEqualTo(Long.MIN_VALUE);
    }

    private static ObjectEntry objectEntry(
            KeyBytes key,
            long size,
            String lastModifiedText,
            String etag,
            String storageClass,
            String versionId,
            boolean isLatest,
            String ownerId,
            String ownerDisplayName,
            String checksumAlgorithm,
            String checksumType
    ) {
        return new ObjectEntry(key, size, lastModifiedText, etag, storageClass, versionId,
                isLatest, ownerId, ownerDisplayName, checksumAlgorithm, checksumType);
    }
}
