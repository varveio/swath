/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

/**
 * {@link S3PageFetcher#callClass} classifies every fetch purely from the {@link PageRequest}
 * shape (worker page fetch vs the thief's 1-key pivot probe vs its {@code delimiter=/} structure
 * probe), and the classification feeds both the per-call-class latency-phase decomposition
 * ({@code swath.fetch.latency.phase{call_class,phase}}, read back as {@code RunSummary
 * #callClassLatency()}) and the slow-probe exemplar log's call-class filter/rate-limiter.
 */
class S3PageFetcherCallClassTest {

    private static final byte[] DELIM = "/".getBytes(StandardCharsets.UTF_8);

    @Test
    void classifiesAWorkerPageFetchByPlainConfiguredPageSize() {
        assertThat(S3PageFetcher.callClass(PageRequest.objects(null, null, 1000)))
                .isEqualTo(RunMetrics.CALL_CLASS_WORKER_PAGE);
    }

    @Test
    void classifiesA1KeyNoDelimiterRequestAsThePivotProbe() {
        assertThat(S3PageFetcher.callClass(PageRequest.objects(null, null, 1)))
                .isEqualTo(RunMetrics.CALL_CLASS_PIVOT_PROBE);
    }

    @Test
    void classifiesADelimitedRequestAsTheStructureProbeRegardlessOfMaxKeys() {
        assertThat(S3PageFetcher.callClass(PageRequest.objectsDelimited(null, DELIM, null, 1000)))
                .isEqualTo(RunMetrics.CALL_CLASS_STRUCTURE_PROBE);
        // Structure probes are capped low (STRUCTURE_PROBE_MAX_KEYS in Thief), but the classifier's
        // discriminator is the delimiter's presence, not max_keys -- a delimited maxKeys=1 request
        // (never actually issued today) would still be a structure probe, not a pivot probe.
        assertThat(S3PageFetcher.callClass(PageRequest.objectsDelimited(null, DELIM, null, 1)))
                .isEqualTo(RunMetrics.CALL_CLASS_STRUCTURE_PROBE);
    }

    @Test
    void successfulFetchesRecordTotalLatencyPerCallClass() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        fetcher.fetchPage(PageRequest.objects(null, null, 1000));   // worker_page
        fetcher.fetchPage(PageRequest.objects(null, null, 1));      // pivot_probe
        fetcher.fetchPage(PageRequest.objectsDelimited(null, DELIM, null, 1000));   // structure_probe

        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0);
        List<RunSummary.CallClassLatencySummary> totalRows = summary.callClassLatency().stream()
                .filter(r -> r.phase().equals(RunMetrics.LATENCY_PHASE_TOTAL))
                .toList();

        assertThat(totalRows)
                .as("total-phase row present for each of the 3 call classes")
                .anySatisfy(r -> assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_WORKER_PAGE))
                .anySatisfy(r -> assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_PIVOT_PROBE))
                .anySatisfy(r -> assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_STRUCTURE_PROBE));
        assertThat(totalRows).allSatisfy(r -> assertThat(r.count()).isEqualTo(1L));
    }

    /**
     * The client-side response-PARSE span: one {@code response_parse} observation per successful
     * fetch, on the SAME {@code call_class} the wall-clock total was attributed to, so a
     * client-service-cost analysis can separate a worker page's parse cost from a probe's. Recorded
     * from the fetcher's own conversion of the response into swath's page model — the only parse
     * work swath owns (the SDK unmarshals inside the call).
     */
    @Test
    void successfulFetchesRecordTheResponseParseSpanPerCallClass() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.captureOnly(), "bucket",
                S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        fetcher.fetchPage(PageRequest.objects(null, null, 1000));   // worker_page
        fetcher.fetchPage(PageRequest.objects(null, null, 1));      // pivot_probe
        fetcher.fetchPage(PageRequest.objectsDelimited(null, DELIM, null, 1000));   // structure_probe

        List<RunSummary.CallClassLatencySummary> parseRows =
                metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0).callClassLatency().stream()
                        .filter(r -> r.phase().equals(RunMetrics.LATENCY_PHASE_RESPONSE_PARSE))
                        .toList();

        assertThat(parseRows).extracting(RunSummary.CallClassLatencySummary::callClass)
                .containsExactlyInAnyOrder(RunMetrics.CALL_CLASS_WORKER_PAGE,
                        RunMetrics.CALL_CLASS_PIVOT_PROBE, RunMetrics.CALL_CLASS_STRUCTURE_PROBE);
        assertThat(parseRows).allSatisfy(r -> {
            assertThat(r.count()).isEqualTo(1L);
            assertThat(r.p50Ms()).isNotNull();
        });
    }

    /** A fetch that never returned a response never fabricates a parse span. */
    @Test
    void aFailedFetchRecordsNoResponseParseSpan() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000)),
                "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1000)))
                .isInstanceOf(ThrottleException.class);

        assertThat(metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0).callClassLatency())
                .noneMatch(r -> r.phase().equals(RunMetrics.LATENCY_PHASE_RESPONSE_PARSE));
    }

    /**
     * A bare fake {@link S3Client} (no real SDK HTTP pipeline, so no {@code MetricPublisher} ever
     * fires) never populates connect-acquire/TTFB — {@link RunMetrics#recordCallClassLatency} must
     * silently skip the unobserved {@code -1} sentinel rather than fabricate a 0-nanosecond sample.
     */
    @Test
    void unobservedPhasesAreSkippedNotFabricated() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        new S3PageFetcher(FakeS3Client.captureOnly(), "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics))
                .fetchPage(PageRequest.objects(null, null, 1000));

        List<RunSummary.CallClassLatencySummary> rows =
                metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0).callClassLatency();
        assertThat(rows).noneMatch(r -> r.phase().equals(RunMetrics.LATENCY_PHASE_CONNECT_ACQUIRE));
        assertThat(rows).noneMatch(r -> r.phase().equals(RunMetrics.LATENCY_PHASE_TTFB));
    }

    /**
     * The slow-probe exemplar rate-limiter's candidate counter engages for a PROBE (pivot or
     * structure) that reaches the exception path, on EVERY exhausted attempt (the exception path
     * always logs, unlike the success path's elapsed-threshold gate) -- but never for a worker-page
     * fetch, which {@link S3PageFetcher#maybeLogSlowProbeExemplar} excludes outright -- the exemplar
     * log is specifically about probe pressure.
     */
    @Test
    void exemplarCounterEngagesOnlyForProbeCallClasses() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3Client throwing = FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000));
        S3PageFetcher fetcher = new S3PageFetcher(throwing, "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1000)))
                .isInstanceOf(ThrottleException.class);
        assertThat(fetcher.slowProbeExemplarCountForTest())
                .as("worker-page fetch never engages the probe exemplar counter")
                .isZero();

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1)))
                .isInstanceOf(ThrottleException.class);
        assertThat(fetcher.slowProbeExemplarCountForTest())
                .as("a pivot-probe fetch that reached the exception path engages the counter")
                .isEqualTo(1L);

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objectsDelimited(null, DELIM, null, 1000)))
                .isInstanceOf(ThrottleException.class);
        assertThat(fetcher.slowProbeExemplarCountForTest())
                .as("a structure-probe fetch that reached the exception path also engages the counter")
                .isEqualTo(2L);
    }

    /**
     * A FAILED probe fetch (never reaching the success path) must still contribute a {@code total}-phase
     * sample — recording success-path-only would be survivorship bias, excluding a timeout storm's
     * failed probes from {@code probe_latency[]}'s percentiles and making a mid-storm run look
     * artificially healthy. Asserted for BOTH probe classes (pivot and structure); a worker-page
     * failure also records — this is call-class-agnostic at the recording site — asserted too for
     * completeness.
     */
    @Test
    void timedOutFetchesStillContributeToTheTotalLatencyPercentileStream() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3Client throwing = FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000));
        S3PageFetcher fetcher = new S3PageFetcher(throwing, "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1000)))
                .isInstanceOf(ThrottleException.class);
        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1)))
                .isInstanceOf(ThrottleException.class);
        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objectsDelimited(null, DELIM, null, 1000)))
                .isInstanceOf(ThrottleException.class);

        List<RunSummary.CallClassLatencySummary> rows =
                metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0).callClassLatency();
        assertThat(rows)
                .as("every call class recorded a total-phase sample from its FAILED (exception-path) attempt")
                .anySatisfy(r -> {
                    assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_WORKER_PAGE);
                    assertThat(r.phase()).isEqualTo(RunMetrics.LATENCY_PHASE_TOTAL);
                    assertThat(r.count()).isEqualTo(1L);
                })
                .anySatisfy(r -> {
                    assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_PIVOT_PROBE);
                    assertThat(r.phase()).isEqualTo(RunMetrics.LATENCY_PHASE_TOTAL);
                    assertThat(r.count()).isEqualTo(1L);
                })
                .anySatisfy(r -> {
                    assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_STRUCTURE_PROBE);
                    assertThat(r.phase()).isEqualTo(RunMetrics.LATENCY_PHASE_TOTAL);
                    assertThat(r.count()).isEqualTo(1L);
                });
        // No connect_acquire/ttfb rows -- this bare fake S3Client never fires a MetricPublisher, so
        // both phases stay the -1 unobserved sentinel and are correctly skipped, not fabricated.
        assertThat(rows).noneMatch(r -> r.phase().equals(RunMetrics.LATENCY_PHASE_CONNECT_ACQUIRE));
        assertThat(rows).noneMatch(r -> r.phase().equals(RunMetrics.LATENCY_PHASE_TTFB));
    }

    /**
     * The slow-probe engagement counter says THAT a probe was slow and for which call class — it
     * deliberately does NOT say which phase dominated. {@code connect_acquire} and {@code ttfb} are
     * independent best-effort SDK measurements that may partially overlap (the SDK never states
     * whether time-to-first-byte starts at request-issue or after the connection is leased), so a
     * dominance test between them is not a valid share comparison. This is the case that makes the
     * point: 900 ms of pool-checkout wait reported alongside a 1000 ms time-to-first-byte —
     * pool-dominated if TTFB is inclusive, store-dominated if the two are disjoint, and nothing in
     * the SDK's contract says which. Exactly one {@code PROBE} counter fires; the two raw timings
     * stay readable as their own {@code swath.fetch.latency.phase} series.
     */
    @Test
    void aSlowProbeCountsEngagementByCallClassAndNeverClassifiesADominantPhase() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        S3Client client = FakeS3Client.respondingWith(request -> {
            publishPhaseTimings(Duration.ofMillis(900), Duration.ofMillis(1000));
            throw ApiCallAttemptTimeoutException.create(5000);
        });
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1)))
                .isInstanceOf(ThrottleException.class);

        assertThat(registry.find("swath.steal_reason").tag("outcome", "PROBE").counters())
                .as("the call-class engagement counter is the only PROBE counter a slow probe fires")
                .extracting(c -> c.getId().getTag("reason"))
                .containsExactly("slow_pivot_probe");
        List<RunSummary.CallClassLatencySummary> rows =
                metrics.summary(Duration.ofSeconds(1), "work_stealing", 0, 0).callClassLatency();
        assertThat(rows)
                .as("both raw phase timings survive as independent per-call-class series")
                .anySatisfy(r -> {
                    assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_PIVOT_PROBE);
                    assertThat(r.phase()).isEqualTo(RunMetrics.LATENCY_PHASE_CONNECT_ACQUIRE);
                    assertThat(r.p50Ms()).isCloseTo(900.0, within(50.0));
                })
                .anySatisfy(r -> {
                    assertThat(r.callClass()).isEqualTo(RunMetrics.CALL_CLASS_PIVOT_PROBE);
                    assertThat(r.phase()).isEqualTo(RunMetrics.LATENCY_PHASE_TTFB);
                    assertThat(r.p50Ms()).isCloseTo(1000.0, within(50.0));
                });
    }

    /**
     * The same single-counter shape when the SDK published no phase timings at all (an early
     * failure that never reached the wire) — a missing measurement produces no extra counter, so
     * post-analysis never sees a phase-shaped signal it cannot trust.
     */
    @Test
    void aSlowProbeWithNoSdkPhaseTimingsFiresTheSameSingleCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        S3Client throwing = FakeS3Client.throwing(ApiCallAttemptTimeoutException.create(5000));
        S3PageFetcher fetcher = new S3PageFetcher(throwing, "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objectsDelimited(null, DELIM, null, 1000)))
                .isInstanceOf(ThrottleException.class);

        assertThat(registry.find("swath.steal_reason").tag("outcome", "PROBE").counters())
                .extracting(c -> c.getId().getTag("reason"))
                .containsExactly("slow_structure_probe");
    }

    /**
     * Reports both SDK phase metrics into the capture the in-flight {@code fetchPage} started on
     * this thread — the same synchronous {@code publish()} an {@code ApacheHttpClient} call makes
     * before returning to the caller (see {@link S3CallClassLatencyPublisher}'s javadoc).
     */
    private static void publishPhaseTimings(Duration connectAcquire, Duration timeToFirstByte) {
        MetricCollector root = MetricCollector.create("ApiCall");
        MetricCollector attempt = root.createChild("ApiCallAttempt");
        attempt.reportMetric(HttpMetric.CONCURRENCY_ACQUIRE_DURATION, connectAcquire);
        attempt.reportMetric(CoreMetric.TIME_TO_FIRST_BYTE, timeToFirstByte);
        new S3CallClassLatencyPublisher().publish(root.collect());
    }

    /**
     * A non-{@code SdkException} {@code RuntimeException} escaping the SDK call (e.g. a client-side
     * interceptor bug) bypasses the typed-exception catch arm entirely --
     * {@link S3CallClassLatencyPublisher#end()} must still fire (via the OUTER {@code finally} in
     * {@code S3PageFetcher#fetchPage}), or the thread-local capture would leak into the NEXT fetch on
     * this thread and silently attribute stale/wrong phase data to it.
     */
    @Test
    void aNonSdkExceptionStillClearsTheThreadLocalCapture() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        S3Client client = FakeS3Client.builder()
                .thenThrow(new IllegalStateException("simulated non-SdkException interceptor bug"))
                .then(ListObjectsV2Response.builder().isTruncated(false).build())
                .build();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1000)))
                .isInstanceOf(IllegalStateException.class);

        // The thread-local capture from the failed call must be cleared -- proven by a fresh capture
        // being observed cleanly on THIS thread's very next fetch (no leaked/stale PhaseCapture from
        // the IllegalStateException attempt corrupting it).
        S3CallClassLatencyPublisher.PhaseCapture leaked = S3CallClassLatencyPublisher.begin();
        try {
            assertThat(leaked.connectAcquireNanos())
                    .as("a fresh begin() after the non-SdkException throw sees an unobserved capture, "
                            + "not a stale one from the failed attempt")
                    .isEqualTo(-1L);
        } finally {
            S3CallClassLatencyPublisher.end();
        }

        assertThatCode(() -> fetcher.fetchPage(PageRequest.objects(null, null, 1000)))
                .as("the fetcher itself is still usable for a subsequent call on this thread")
                .doesNotThrowAnyException();
    }

}
