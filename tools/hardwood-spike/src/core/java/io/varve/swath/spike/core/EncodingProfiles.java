/* SPDX-License-Identifier: Apache-2.0 */
package io.varve.swath.spike.core;

import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.WriterConfig;

/** Explicit, reusable release-API policies. AUTO in Beta2 only chooses plain or dictionary. */
public final class EncodingProfiles {
    private EncodingProfiles() {}

    public static WriterConfig.Builder apply(WriterConfig.Builder builder, String profile) {
        return switch (profile) {
            case "auto" -> builder;
            case "delta" -> builder
                    .encoding("key", ColumnEncoding.DELTA_BYTE_ARRAY)
                    .encoding("etag", ColumnEncoding.DELTA_BYTE_ARRAY)
                    .encoding("version_id", ColumnEncoding.DELTA_BYTE_ARRAY)
                    .encoding("size", ColumnEncoding.DELTA_BINARY_PACKED)
                    .encoding("last_modified", ColumnEncoding.DELTA_BINARY_PACKED);
            default -> throw new IllegalArgumentException("Unknown encoding profile: " + profile);
        };
    }
}
