/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Probe call classes get their own (short) per-attempt timeout.
 *
 * <p>Each probe call class ({@code structure_probe} = {@code delimiter=/}, {@code pivot_probe} =
 * {@code max_keys<=1} — see {@link S3PageFetcher#callClass}) gets its own configurable, shorter
 * per-attempt budget (default {@code 3s}) threaded onto the request via the SDK's per-request
 * {@code overrideConfiguration}, so a stuck probe is abandoned in ~3&nbsp;s and retried (probes
 * already cap their transient retries at {@code PROBE_TRANSIENT_RETRY_CAP=1}) rather than holding
 * the shared {@link S3Config#DEFAULT_ATTEMPT_TIMEOUT} (10&nbsp;s) client-level budget for a full
 * attempt into unwarmed keyspace; a WORKER page keeps that 10&nbsp;s client-level budget unchanged.
 *
 * <p><b>Placement.</b> The knob lives store-side ({@code swath-s3}): the timeout is
 * selected by {@link S3PageFetcher#callClass}, which the core engine / {@code MockPageFetcher} never
 * sees (the mock models attempt timeouts through a test-scripted interceptor, not an S3Config knob),
 * so the differentiation can only actually bite here — a fetcher-level assertion on the per-call-class
 * per-attempt timeout written onto the outgoing {@code ListObjectsV2Request}. A capturing fake client
 * records the exact request each fetch issues (same technique as {@code S3PageFetcherUnitTest}).
 *
 * <p>This test binds the probe default to {@code 3s} ({@code S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT}).
 * A default-constructed {@link S3PageFetcher} applies it to probe-class requests; the assertions
 * below read the effective per-request {@code apiCallAttemptTimeout} straight off the request.
 */
class S3PageFetcherProbeAttemptTimeoutTest {

    /** Default probe per-attempt budget (worker pages keep the 10 s client-level default). */
    private static final Duration PROBE_ATTEMPT_TIMEOUT = Duration.ofSeconds(3);

    @Test
    void pivotProbeGetsTheShortProbeAttemptTimeout() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        // pivot probe: max_keys<=1, no delimiter (Thief#probeNonEmpty) -> CALL_CLASS_PIVOT_PROBE.
        PageRequest pivotProbe = PageRequest.objects(null, null, 1);

        new S3PageFetcher(client, "bucket").fetchPage(pivotProbe);

        assertThat(S3PageFetcher.callClass(pivotProbe)).isEqualTo(RunMetrics.CALL_CLASS_PIVOT_PROBE);
        assertThat(client.lastRequest().overrideConfiguration())
                .as("a pivot probe carries its own short per-attempt timeout override")
                .hasValueSatisfying(o -> assertThat(o.apiCallAttemptTimeout())
                        .as("pivot-probe attempt timeout == the short probe default (3 s), not 10 s")
                        .hasValue(PROBE_ATTEMPT_TIMEOUT));
    }

    @Test
    void structureProbeGetsTheShortProbeAttemptTimeout() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        // structure probe: delimiter=/ (SeedStep / Thief#structurePivot) -> CALL_CLASS_STRUCTURE_PROBE.
        PageRequest structureProbe = PageRequest.objectsDelimited(
                null, "/".getBytes(StandardCharsets.UTF_8), null, 1000);

        new S3PageFetcher(client, "bucket").fetchPage(structureProbe);

        assertThat(S3PageFetcher.callClass(structureProbe)).isEqualTo(RunMetrics.CALL_CLASS_STRUCTURE_PROBE);
        assertThat(client.lastRequest().overrideConfiguration())
                .as("a delimiter=/ structure probe carries the short per-attempt timeout override")
                .hasValueSatisfying(o -> assertThat(o.apiCallAttemptTimeout())
                        .as("structure-probe attempt timeout == the short probe default (3 s), not 10 s")
                        .hasValue(PROBE_ATTEMPT_TIMEOUT));
    }

    @Test
    void workerPageKeepsTheDefaultClientLevelTimeout_noPerRequestOverride() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        // worker page: configured page size, no delimiter -> CALL_CLASS_WORKER_PAGE.
        PageRequest workerPage = PageRequest.objects(null, null, 1000);

        new S3PageFetcher(client, "bucket").fetchPage(workerPage);

        assertThat(S3PageFetcher.callClass(workerPage)).isEqualTo(RunMetrics.CALL_CLASS_WORKER_PAGE);
        assertThat(client.lastRequest().overrideConfiguration())
                .as("a worker page keeps the 10 s client-level budget -- no per-request override "
                        + "(unchanged from today; the probe knob must not touch worker pages)")
                .isEmpty();
    }

    @Test
    void explicitAttemptTimeoutOverrideStillWinsOverTheProbeDefault() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        // A probe whose logical fetch already escalated its per-attempt budget must keep the
        // escalated value -- the probe default must not clobber the retry loop's explicit override.
        PageRequest escalatedProbe = PageRequest.objects(null, null, 1)
                .withApiCallAttemptTimeoutOverride(Duration.ofSeconds(20));

        new S3PageFetcher(client, "bucket").fetchPage(escalatedProbe);

        assertThat(client.lastRequest().overrideConfiguration())
                .as("an explicit escalation override wins over the probe attempt-timeout default")
                .hasValueSatisfying(o -> assertThat(o.apiCallAttemptTimeout())
                        .hasValue(Duration.ofSeconds(20)));
    }

}
