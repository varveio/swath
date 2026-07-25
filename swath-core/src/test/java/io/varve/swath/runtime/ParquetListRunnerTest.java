/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.filter.FilterChain;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * INT-6 logic over the deterministic MockPageFetcher: a list run
 * to Parquet produces parts whose union equals the bucket and whose schema is the
 * canonical schema; the manifest records every finalized part. (The LocalStack
 * variant is {@code ParquetIT}.)
 */
class ParquetListRunnerTest {

    private static List<Path> parts(Path dir) throws Exception {
        return DatasetLayout.of(dir).dataParts();
    }

    @Test
    void listToParquetUnionEqualsBucketAndSchemaMatches(@TempDir Path dir) throws Exception {
        int n = 12_345;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();

        // Small part target forces several parts so "union of parts" is meaningful.
        var spec = new ListRunner.ParquetSpec(new byte[0], 16, 1000, FilterChain.EMPTY,
                3, 128 * 1024, 16, "argshash123", null, null, 0L, 0L, "");
        var stats = new ListRunner().runToParquet(RunContext.create(), fetcher, dir, spec);

        assertThat(stats.objects()).isEqualTo(n);

        List<Path> parts = parts(dir);
        assertThat(parts).isNotEmpty();

        // Schema is *exactly* the canonical MessageType — physical + logical types,
        // nullability and field order all pinned (not mere column-name containment).
        assertThat(ParquetReads.schema(parts.getFirst()))
                .isEqualTo(ParquetSchema.canonical());

        // Union of all parts == the bucket, exactly once.
        TreeSet<String> union = new TreeSet<>();
        long rowCount = 0;
        for (Path part : parts) {
            var keys = ParquetReads.keys(part);
            rowCount += keys.size();
            union.addAll(keys);
        }
        assertThat(rowCount).isEqualTo(n);          // no duplicates across parts
        assertThat(union).hasSize(n);

        // Consumer manifest lists the finalized parts with data/-prefixed keys + MD5;
        // the args_hash lives in the internal state file, never the consumer manifest.
        DatasetLayout layout = DatasetLayout.of(dir);
        String manifest = Files.readString(layout.manifest());
        assertThat(manifest).contains("\"key\": \"data/part-");
        assertThat(manifest).contains("\"MD5checksum\":");
        assertThat(manifest).doesNotContain("args_hash");
        String state = Files.readString(layout.state());
        assertThat(state).contains("\"args_hash\": \"argshash123\"");
    }
}
