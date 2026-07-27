/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The {@code sdk_unmarshal} latency phase is fed entirely by the AWS SDK's own per-attempt stamps,
 * bridged in by {@link S3CallClassLatencyPublisher} ({@link S3CallClassLatencyPublisherTest} and
 * {@link S3PageFetcherCallClassTest} prove the derivation and the per-call-class attribution against
 * a HAND-BUILT {@code MetricCollection} — never against a real SDK attempt). Only a real attempt can
 * answer whether the SDK reports what the derivation needs at all, and <b>this IT is what caught
 * that it does not report the obvious thing</b>: {@code CoreMetric.UNMARSHALLING_DURATION} — the
 * exact unmarshal boundary, and the phase's first implementation — is never published for S3 {@code
 * ListObjectsV2} in this SDK version, so the phase was silently absent from a real run while every
 * unit test stayed green. The derivation was moved onto {@code TIME_TO_LAST_BYTE} minus {@code
 * TIME_TO_FIRST_BYTE} (stamped in the SDK's {@code HandleResponseStage} after the response handler
 * returns, on the sync path) because of what this test observed.
 *
 * <p>So this IT is load-bearing in both directions: it guards against the SDK stopping to report
 * either stamp (the phase would go permanently absent from {@code probe_latency[]}, and the SDK's
 * response handling would go back to being invisible inside the {@code total}-minus-{@code ttfb}
 * residual — the gap this phase exists to close), and against the derivation ever silently
 * degenerating (a window that is not strictly inside the call's own wall-clock total).
 *
 * <p>Builds the client via the PRODUCTION wiring ({@link S3ClientFactory#create(S3Config,
 * RunMetrics)}) and fetches one real multi-key page through {@link S3PageFetcher}.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class S3SdkUnmarshalPhaseLocalStackIT {

    private static final String BUCKET = "s3-sdk-unmarshal-phase-it";
    private static final int KEYS = 50;

    @Container
    static final LocalStackContainer LOCALSTACK = LocalStackSupport.s3Container();

    private S3Client s3;

    @AfterEach
    void tearDown() {
        if (s3 != null) {
            s3.close();
        }
    }

    @Test
    @Timeout(120)
    void aRealListObjectsV2PopulatesTheSdkUnmarshalPhaseFromTheSyncApacheHttpClient() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3Config config = LocalStackSupport.config(LOCALSTACK, 8);
        s3 = S3ClientFactory.create(config, metrics);

        LocalStackSupport.createBucket(s3, BUCKET);
        List<byte[]> keys = new ArrayList<>(KEYS);
        for (int i = 0; i < KEYS; i++) {
            keys.add(("k/%04d".formatted(i)).getBytes(StandardCharsets.UTF_8));
        }
        LocalStackSupport.putObjects(s3, BUCKET, keys, 8);

        S3PageFetcher fetcher = new S3PageFetcher(s3, BUCKET, S3PageFetcherConfig.DEFAULT.withMetrics(metrics));
        assertThat(fetcher.fetchPage(PageRequest.objects(null, null, 1000)).entries()).hasSize(KEYS);

        List<RunSummary.CallClassLatencySummary> rows =
                metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0).callClassLatency();
        double unmarshalMs = phaseMaxMs(rows, RunMetrics.LATENCY_PHASE_SDK_UNMARSHAL);
        assertThat(unmarshalMs)
                .as("the sdk_unmarshal window was derivable from a genuine sync ApacheHttpClient "
                        + "ListObjectsV2 attempt's own stamps and attributed to the fetch's call class -- a "
                        + "stamp the SDK stopped reporting would leave this row absent, not zero")
                .isPositive();
        assertThat(unmarshalMs)
                .as("the SDK's response-handling window lies strictly inside the call's own wall-clock total")
                .isLessThanOrEqualTo(phaseMaxMs(rows, RunMetrics.LATENCY_PHASE_TOTAL));
    }

    /** The {@code worker_page} row's {@code max_ms} for {@code phase}; fails the test if absent. */
    private static double phaseMaxMs(List<RunSummary.CallClassLatencySummary> rows, String phase) {
        return rows.stream()
                .filter(r -> r.callClass().equals(RunMetrics.CALL_CLASS_WORKER_PAGE) && r.phase().equals(phase))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no worker_page row for phase=" + phase + " in " + rows))
                .maxMs();
    }
}
