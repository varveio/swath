/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.parts;
import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * INT-6 contract: the Parquet output is exactly the
 * canonical schema and carries byte-exact / correctly-null'd values — not merely
 * the right column names. Strengthens the prior count/union and name-containment
 * checks.
 *
 * <p>Covered here: (1) read-back schema {@code ==} {@link ParquetSchema#canonical}
 * (physical + logical types, nullability, order); (2) raw key bytes preserved
 * byte-exact for multibyte/4-byte keys (not UTF-8-coerced); (3) per-row
 * nullability — {@code size}/{@code last_modified} <b>null</b> for a
 * {@code COMMON_PREFIX} row, present for an object; (4) ETag stored verbatim
 * (quotes already stripped upstream; multipart {@code hex-N} kept). The
 * normalization step itself is pinned in {@code EtagNormalizationTest}.
 */
class ParquetCanonicalContractTest {

    private static final byte[] MULTIBYTE_KEY = "café/data/🚀.bin".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PREFIX_KEY = "中文/".getBytes(StandardCharsets.UTF_8);

    @Test
    void schemaIsExactAndValuesAreByteExactAndCorrectlyNulled(@TempDir Path dir) throws Exception {
        ObjectEntry object = new ObjectEntry(KeyBytes.of(MULTIBYTE_KEY), 123L, 1_700_000_000_000_000L,
                "d41d8cd98f00b204e9800998ecf8427e-3",   // multipart etag, already quote-stripped
                "GLACIER", null, true, "owner1", "Alice", "SHA256", "FULL_OBJECT");
        CommonPrefixEntry prefix = new CommonPrefixEntry(KeyBytes.of(PREFIX_KEY));

        try (ParquetWriterPool pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "h",
                1, Long.MAX_VALUE, 8)) {
            // One node → one sticky lane → one part; both rows land together, in order.
            pool.submit(new PageBatch(0, 0, List.of(object, prefix)));
        }

        List<Path> parts = parts(dir);
        assertThat(parts).hasSize(1);
        Path part = parts.getFirst();

        // (1) exact canonical schema — logical types + nullability + order, not just names.
        assertThat(ParquetReads.schema(part)).isEqualTo(ParquetSchema.canonical());

        List<Group> rows = ParquetReads.readAll(part);
        assertThat(rows).hasSize(2);
        Group objRow = rowOfType(rows, "OBJECT");
        Group cpRow = rowOfType(rows, "COMMON_PREFIX");

        // (2) raw key bytes preserved byte-exact (no UTF-8 coercion).
        assertThat(objRow.getBinary("key", 0).getBytes()).isEqualTo(MULTIBYTE_KEY);
        assertThat(cpRow.getBinary("key", 0).getBytes()).isEqualTo(PREFIX_KEY);

        // (3a) object row: size + last_modified present and exact; is_delete_marker false.
        assertThat(objRow.getFieldRepetitionCount("size")).isEqualTo(1);
        assertThat(objRow.getLong("size", 0)).isEqualTo(123L);
        assertThat(objRow.getFieldRepetitionCount("last_modified")).isEqualTo(1);
        assertThat(objRow.getLong("last_modified", 0)).isEqualTo(1_700_000_000_000_000L);
        assertThat(objRow.getBoolean("is_delete_marker", 0)).isFalse();

        // (3b) common-prefix row: size + last_modified NULL (absent); required flag still present.
        assertThat(cpRow.getFieldRepetitionCount("size")).isZero();
        assertThat(cpRow.getFieldRepetitionCount("last_modified")).isZero();
        assertThat(cpRow.getFieldRepetitionCount("etag")).isZero();
        assertThat(cpRow.getBoolean("is_delete_marker", 0)).isFalse();

        // (4) ETag stored verbatim — multipart hex-N kept, no quotes re-added.
        assertThat(objRow.getString("etag", 0)).isEqualTo("d41d8cd98f00b204e9800998ecf8427e-3");

        // (5) owner_display_name / checksum_type carried through byte-exact.
        assertThat(objRow.getString("owner_id", 0)).isEqualTo("owner1");
        assertThat(objRow.getString("owner_display_name", 0)).isEqualTo("Alice");
        assertThat(objRow.getString("checksum_algorithm", 0)).isEqualTo("SHA256");
        assertThat(objRow.getString("checksum_type", 0)).isEqualTo("FULL_OBJECT");
    }

    private static Group rowOfType(List<Group> rows, String rowType) {
        List<Group> matched = new ArrayList<>();
        for (Group g : rows) {
            if (g.getString("row_type", 0).equals(rowType)) {
                matched.add(g);
            }
        }
        assertThat(matched).as("exactly one %s row", rowType).hasSize(1);
        return matched.getFirst();
    }
}
