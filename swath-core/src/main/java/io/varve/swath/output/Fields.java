/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

/** Shared field rendering for the text formatters. */
final class Fields {

    private Fields() {
    }

    /** Epoch micros → ISO-8601 UTC instant; empty for the sentinel 0 / missing. */
    static String isoMicros(long epochMicros) {
        if (epochMicros == 0L) {
            return "";
        }
        long secs = Math.floorDiv(epochMicros, 1_000_000L);
        long micros = Math.floorMod(epochMicros, 1_000_000L);
        Instant instant = Instant.ofEpochSecond(secs, micros * 1_000L);
        return DateTimeFormatter.ISO_INSTANT.format(instant);
    }

    static String orEmpty(String s) {
        return s == null ? "" : s;
    }
}
