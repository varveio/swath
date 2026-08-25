/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.LastModifiedParseException;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;

/**
 * Keeps rows last-modified in {@code [afterMicros, beforeMicros]} (inclusive,
 * epoch micros UTC). Rows without a timestamp (common prefixes) always pass.
 */
public record MtimeFilter(long afterMicros, long beforeMicros) implements Filter {

    public static MtimeFilter after(long micros) {
        return new MtimeFilter(micros, Long.MAX_VALUE);
    }

    public static MtimeFilter before(long micros) {
        return new MtimeFilter(Long.MIN_VALUE, micros);
    }

    @Override
    public boolean matches(ListEntry e) throws ListingException {
        Long mtime;
        try {
            mtime = switch (e) {
                case ObjectEntry o -> o.lastModifiedEpochMicros();
                case DeleteMarkerEntry d -> d.lastModifiedEpochMicros();
                default -> null;
            };
        } catch (LastModifiedParseException parseFailure) {
            throw new ListingException(parseFailure.getMessage(), parseFailure);
        }
        if (mtime == null) {
            return true;
        }
        return mtime >= afterMicros && mtime <= beforeMicros;
    }

    @Override
    public int cost() {
        return 0;
    }
}
