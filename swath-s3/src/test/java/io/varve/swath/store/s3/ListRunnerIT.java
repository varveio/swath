/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.filter.FilterChain;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.Keyspaces;
import java.io.StringWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * INT-2: {@code list --format jsonl} emits one JSON line per object, so
 * {@code jq -s length} equals the bucket size — verified end-to-end through
 * {@link ListRunner} against a real LocalStack bucket.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class ListRunnerIT {

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
    void int2_jsonlLineCountEqualsBucketSize() throws Exception {
        String bucket = "int2-jsonl";
        LocalStackSupport.createBucket(s3, bucket);
        int n = 3210;
        LocalStackSupport.putObjects(s3, bucket, Keyspaces.exactly(n), 96);

        S3PageFetcher fetcher = new S3PageFetcher(s3, bucket);
        StringWriter out = new StringWriter();
        var stats = new ListRunner().run(RunContext.create(), fetcher, out,
                new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 16, 1000, FilterChain.EMPTY, null, null));

        long lines = out.toString().lines().count();
        assertThat(lines).isEqualTo(n);
        assertThat(stats.objects()).isEqualTo(n);
        assertThat(out.toString().lines().allMatch(l -> l.startsWith("{") && l.contains("\"row_type\":\"OBJECT\"")))
                .isTrue();
    }
}
