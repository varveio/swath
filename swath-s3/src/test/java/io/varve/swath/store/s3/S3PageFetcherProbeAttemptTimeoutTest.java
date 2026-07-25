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
 * Per-attempt timeout budgets are assigned by CALL-CLASS COST SHAPE, not by "is it a probe".
 *
 * <p>The split is point-lookup vs scan. The POINT-probe class ({@code pivot_probe} =
 * {@code max_keys<=1}, no delimiter — see {@link S3PageFetcher#callClass}) is answered from the
 * first key at/after the cursor, so it is cheap and near-constant; it gets a configurable, shorter
 * per-attempt budget (default {@code 3s}) threaded onto the request via the SDK's per-request
 * {@code overrideConfiguration}, so a stuck one is abandoned in ~3&nbsp;s and retried (probes
 * already cap their transient retries at {@code PROBE_TRANSIENT_RETRY_CAP=1}).
 *
 * <p>The SCAN classes — a WORKER page and, despite also being a probe, a {@code delimiter=/}
 * {@code structure_probe} — both keep the {@link S3Config#DEFAULT_ATTEMPT_TIMEOUT} (10&nbsp;s)
 * client-level budget with no per-request override. A structure probe makes S3 scan forward rolling
 * keys up into {@code CommonPrefixes}, so its cost tracks the keyspace crossed rather than being
 * constant; it previously shared the 3&nbsp;s point-probe fuse, which is the defect
 * {@link #structureProbeKeepsTheScanClassTimeout_noShortProbeFuse} guards.
 *
 * <p><b>Placement.</b> The knob lives store-side ({@code swath-s3}): the timeout is
 * selected by {@link S3PageFetcher#callClass}, which the core engine / {@code MockPageFetcher} never
 * sees (the mock models attempt timeouts through a test-scripted interceptor, not an S3Config knob),
 * so the differentiation can only actually bite here — a fetcher-level assertion on the per-call-class
 * per-attempt timeout written onto the outgoing {@code ListObjectsV2Request}. A capturing fake client
 * records the exact request each fetch issues (same technique as {@code S3PageFetcherUnitTest}).
 *
 * <p>This test binds the point-probe default to {@code 3s}
 * ({@code S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT}). A default-constructed {@link S3PageFetcher}
 * applies it to pivot-probe requests; the assertions below read the effective per-request
 * {@code apiCallAttemptTimeout} straight off the request.
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

    /**
     * REGRESSION (probe-timeout storm): a {@code delimiter=/} structure probe is a SCAN-class call,
     * not a point probe, and must keep the client-level {@link S3Config#DEFAULT_ATTEMPT_TIMEOUT}
     * budget rather than the short point-probe fuse.
     *
     * <p>Against the old behavior (structure probes sharing the 3 s pivot budget) this test fails on
     * the {@code isEmpty()} assertion. That 3 s fuse is what produced the genomeark storm: structure
     * probes measured p50 1.15 s standalone and 5.4 s at the run's own 64-way concurrency, so ~half of
     * all structure-probe attempts (1308 of 2612) timed out, starving the thief of pivots. A pivot
     * probe over the same run never timed out once under the same 3 s budget — see
     * {@code docs/internals/probe-budgets.md}.
     */
    @Test
    void structureProbeKeepsTheScanClassTimeout_noShortProbeFuse() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        // structure probe: delimiter=/ (SeedStep / Thief#structurePivot) -> CALL_CLASS_STRUCTURE_PROBE.
        PageRequest structureProbe = PageRequest.objectsDelimited(
                null, "/".getBytes(StandardCharsets.UTF_8), null, 1000);

        new S3PageFetcher(client, "bucket").fetchPage(structureProbe);

        assertThat(S3PageFetcher.callClass(structureProbe)).isEqualTo(RunMetrics.CALL_CLASS_STRUCTURE_PROBE);
        assertThat(client.lastRequest().overrideConfiguration())
                .as("a delimiter=/ structure probe is scan-class: it keeps the 10 s client-level budget "
                        + "with NO per-request override, exactly like a worker page")
                .isEmpty();
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
