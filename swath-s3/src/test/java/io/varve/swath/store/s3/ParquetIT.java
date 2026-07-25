/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.filter.FilterChain;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.ParquetReads;
import java.nio.file.Path;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * INT-6 end-to-end against LocalStack: a real list run to Parquet produces parts
 * whose union equals the bucket, read back with the Parquet reader.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class ParquetIT {

    @Container
    static final LocalStackContainer LOCALSTACK = LocalStackSupport.s3Container();

    private static S3Client s3;

    @BeforeAll
    static void setUp() {
        s3 = LocalStackSupport.client(LOCALSTACK);
    }

    @AfterAll
    static void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    @Test
    void int6_parquetPartsUnionEqualsBucket(@TempDir Path dir) throws Exception {
        String bucket = "int6-parquet";
        LocalStackSupport.createBucket(s3, bucket);
        int n = 4500;
        LocalStackSupport.putObjects(s3, bucket, Keyspaces.exactly(n), 96);

        S3PageFetcher fetcher = new S3PageFetcher(s3, bucket);
        var spec = new ListRunner.ParquetSpec(new byte[0], 16, 1000, FilterChain.EMPTY,
                3, 64 * 1024, 16, "int6hash", null, null, 0L, 0L, "");   // small target → multiple parts
        var stats = new ListRunner().runToParquet(RunContext.create(), fetcher, dir, spec);

        assertThat(stats.objects()).isEqualTo(n);
        assertThat(DatasetLayout.of(dir).manifest()).exists();

        TreeSet<String> union = new TreeSet<>();
        long rows = 0;
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            var keys = ParquetReads.keys(part);
            rows += keys.size();
            union.addAll(keys);
        }
        assertThat(rows).isEqualTo(n);          // exactly-once across parts
        assertThat(union).hasSize(n);
    }
}
