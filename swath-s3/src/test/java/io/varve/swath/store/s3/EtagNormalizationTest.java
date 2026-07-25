/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Pins the ETag normalization rule: quotes stripped; multipart form {@code hex-N}
 * kept verbatim as string; never panic-parse. S3 wraps ETags in literal double
 * quotes in the API response; swath stores the unquoted value, and a multipart
 * ETag's {@code -N} suffix is part of the string, never parsed.
 */
class EtagNormalizationTest {

    @Test
    void stripsTheSurroundingQuotes() {
        assertThat(S3PageFetcher.stripEtagQuotes("\"d41d8cd98f00b204e9800998ecf8427e\""))
                .isEqualTo("d41d8cd98f00b204e9800998ecf8427e");
    }

    @Test
    void keepsMultipartHexNSuffixVerbatim() {
        assertThat(S3PageFetcher.stripEtagQuotes("\"9bb58f26192e4ba00f01e2e7b136bbd8-7\""))
                .isEqualTo("9bb58f26192e4ba00f01e2e7b136bbd8-7");
    }

    @Test
    void passesThroughAlreadyUnquotedAndEdgeCases() {
        assertThat(S3PageFetcher.stripEtagQuotes("abc")).isEqualTo("abc");   // no quotes → unchanged
        assertThat(S3PageFetcher.stripEtagQuotes(null)).isNull();
        assertThat(S3PageFetcher.stripEtagQuotes("\"\"")).isEmpty();          // empty quoted
        assertThat(S3PageFetcher.stripEtagQuotes("\"")).isEqualTo("\"");      // lone quote, not a pair
    }
}
