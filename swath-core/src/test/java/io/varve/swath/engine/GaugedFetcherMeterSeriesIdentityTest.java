/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Characterization: the FROZEN meter-series identity {@link GaugedFetcher} owns.
 *
 * <p><b>Why this exists.</b> {@code GaugedFetcher} is the worker/thief retry-throttle loop. Every
 * distinct signal it emits about a fault path flows through the shared {@code swath.steal_reason}
 * counter under the {@code outcome=TRANSIENT} category, with the {@code reason} tag value chosen
 * from a STRING LITERAL in {@code GaugedFetcher} itself (e.g. {@code "attempt_timeout_worker"},
 * {@code "storm_ride_out"}). Those literals are the class's own public metric surface: Varve
 * consumes them by name+tag, so a series that is added, dropped, renamed or re-tagged is a breaking
 * change to that consumer. This snapshot pins the COMPLETE set so a later refactor — in particular
 * lifting {@code GaugedFetcher} out to a top-level type in this package — cannot silently rename,
 * drop or re-tag one. The {@code TRANSIENT} series are NOT covered by
 * {@code RunMetricsSimpleRegistrySeriesIdentityTest} (its frozen list pins the other
 * {@code steal_reason} categories but reaches no {@code TRANSIENT} reason), so without this guard
 * they are unpinned.
 *
 * <p><b>Scope — what this pins.</b> Exactly the series whose identity is DETERMINED by
 * {@code GaugedFetcher}'s own code: the {@code COUNTER swath.steal_reason{outcome=TRANSIENT,reason}}
 * family, exhaustively (see {@link #OWNED_TRANSIENT_SERIES}). The assertion is
 * containment-exact over that category, so a dropped series fails just as loudly as a renamed one.
 *
 * <p><b>Out of snapshot scope — series {@code GaugedFetcher} merely WRITES to, owned elsewhere.</b>
 * The fetcher also drives, but does not own the identity of:
 * <ul>
 *   <li>{@code TIMER swath.rate_limit.wait} and {@code GAUGE swath.in_flight.avg} — registered by
 *       {@code RunMetrics}' constructor (the slot-wait timer / in-flight sampler seams); their
 *       name+tags are {@code RunMetrics} literals a {@code GaugedFetcher} move cannot touch, and are
 *       pinned by {@code RunMetricsSimpleRegistrySeriesIdentityTest};</li>
 *   <li>the AIMD {@code swath.aimd.*} counters/gauges and the {@code swath.steal_reason} entries
 *       under {@code outcome=AIMD/SHED/GROWTH/FREEZE} — driven here via {@code gauge.reportStatus} /
 *       {@code gauge.onTransientTimeout} / {@code gauge.onAttemptLatency}, but owned by
 *       {@code ConcurrencyGauge} and guarded by the {@code ConcurrencyGauge*} suite;</li>
 *   <li>{@code COUNTER swath.throttle.events{type}} — classified and recorded by the store fetcher
 *       ({@code S3PageFetcher}), never by {@code GaugedFetcher}.</li>
 * </ul>
 * A move of {@code GaugedFetcher} cannot rename any of these, so pinning them here would guard the
 * wrong file. The disjoint {@code outcome} tag is exactly why the assertion below filters to
 * {@code TRANSIENT} rather than snapshotting every {@code swath.steal_reason} series in the registry.
 *
 * <p><b>No logger to pin.</b> {@code GaugedFetcher} instantiates no {@link org.slf4j.Logger} and
 * emits no log lines (the enclosing {@code WorkStealingScan.log} is a separate, unmoved field), so
 * its class name — the only thing a {@code LoggerFactory.getLogger(GaugedFetcher.class)} call would
 * expose — is not observable through logging, and a package/name change on the move cannot alter a
 * logger name that does not exist.
 *
 * <p>Every reason below is deterministically reachable by driving the fetcher directly with an
 * injected no-op {@link TransientRetryFetcher.Sleeper}; nothing is left out of scope, and there are
 * no wall-clock sleeps or timing races.
 */
final class GaugedFetcherMeterSeriesIdentityTest {

    /**
     * The COMPLETE set of {@code swath.steal_reason} series {@code GaugedFetcher} owns, as
     * {@code TYPE|name|{sorted tag=value pairs}}. FROZEN: updated only DELIBERATELY, in the same
     * commit as the intended metric change and its Varve-facing change note — never silently
     * re-captured to turn a red test green.
     */
    private static final List<String> OWNED_TRANSIENT_SERIES = List.of(
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=attempt_timeout_worker}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=attempt_timeout_probe}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=attempt_timeout_escalated_1}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=attempt_timeout_escalated_2}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=page_completed_at_1}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=page_completed_at_2}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=probe_retry_cap_failfast}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=retry_cap_stuck}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=storm_ride_out}",
            "COUNTER|swath.steal_reason|{outcome=TRANSIENT,reason=storm_ride_out_worker}");

    private static final TransientRetryFetcher.Sleeper NO_SLEEP = ms -> { };

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @Timeout(30)
    void transientMeterSeriesIdentityIsFrozen() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        // Drive each fault path the loop can take. Sharing one RunMetrics/registry lets the
        // lazily-registered counters accumulate into a single snapshot of the owned surface.
        driveWorkerPageCompletesAtEscalationLevel(metrics, 1);
        driveProbeFailsFast(metrics);
        driveWorkerBoundedCapStuck(metrics);
        driveWorkerStormRideOutThenHeals(metrics);                                    // level 2 + ride-out

        assertThat(ownedTransientSeries(metrics))
                .as("the complete set of swath.steal_reason{outcome=TRANSIENT} series GaugedFetcher owns")
                .containsExactlyInAnyOrderElementsOf(OWNED_TRANSIENT_SERIES);
    }

    /**
     * A worker fetch of a page that genuinely needs {@code needs} completes only after the loop
     * escalates the per-attempt timeout — materializing {@code attempt_timeout_worker},
     * {@code attempt_timeout_escalated_1} and {@code page_completed_at_1} for a 20&nbsp;s page.
     */
    private static void driveWorkerPageCompletesAtEscalationLevel(RunMetrics metrics, int needsLevel)
            throws Exception {
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    if (req.attemptTimeoutEscalationLevel() < needsLevel) {
                        throw ThrottleException.attemptTimeout("page needs escalation level " + needsLevel);
                    }
                    return page;
                })
                .build();
        worker(delegate, metrics, RetryPolicy.RIDE_OUT)
                .fetchPage(PageRequest.objects(new byte[0], null, 1000));
    }

    /**
     * A permanently-wedged {@code slotGated=false} probe fails fast on its own small cap —
     * materializing {@code attempt_timeout_probe} and {@code probe_retry_cap_failfast}, and along the
     * way the {@code attempt_timeout_escalated_1}/{@code _2} counters (never {@code retry_cap_stuck}:
     * that disposition is worker-only).
     */
    private static void driveProbeFailsFast(RunMetrics metrics) {
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("permanently wedged probe");
                })
                .build();
        assertThatThrownBy(() -> probe(delegate, metrics)
                .fetchPage(PageRequest.objects(new byte[0], null, 1)))
                .isInstanceOf(ThrottleException.class);
    }

    /**
     * A worker under {@link RetryPolicy#BOUNDED} whose page never heals exhausts the transient-retry
     * cap and resumably STUCKs the run — materializing {@code retry_cap_stuck}.
     */
    private static void driveWorkerBoundedCapStuck(RunMetrics metrics) {
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.attemptTimeout("permanently wedged worker");
                })
                .build();
        assertThatThrownBy(() -> worker(delegate, metrics, RetryPolicy.BOUNDED)
                .fetchPage(PageRequest.objects(new byte[0], null, 1000)))
                .isInstanceOf(InterruptedException.class);
    }

    /**
     * A worker under {@link RetryPolicy#RIDE_OUT} that stays wedged past
     * {@code MAX_TRANSIENT_RETRIES} and then heals — materializing {@code storm_ride_out},
     * {@code storm_ride_out_worker}, {@code attempt_timeout_escalated_2} and (on the heal at the
     * escalation cap) {@code page_completed_at_2}, without ever cancelling the run.
     */
    private static void driveWorkerStormRideOutThenHeals(RunMetrics metrics) throws Exception {
        int healAfter = TransientRetryFetcher.MAX_TRANSIENT_RETRIES + 3;   // rides out, then completes
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher delegate = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    if (calls.getAndIncrement() < healAfter) {
                        throw ThrottleException.attemptTimeout("wedged then heals");
                    }
                    return page;
                })
                .build();
        worker(delegate, metrics, RetryPolicy.RIDE_OUT)
                .fetchPage(PageRequest.objects(new byte[0], null, 1000));
    }

    private static GaugedFetcher worker(
            MockPageFetcher delegate, RunMetrics metrics, RetryPolicy policy) {
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);
        return new GaugedFetcher(delegate, gauge, true, true, metrics,
                () -> new CancellationToken(), NO_SLEEP, policy);
    }

    private static GaugedFetcher probe(MockPageFetcher delegate, RunMetrics metrics) {
        ConcurrencyGauge gauge = new ConcurrencyGauge(4, metrics);
        return new GaugedFetcher(delegate, gauge, false, false, metrics,
                () -> new CancellationToken(), NO_SLEEP, RetryPolicy.RIDE_OUT);
    }

    /**
     * {@code TYPE|name|{tags}} for every registered {@code swath.steal_reason} meter under the
     * {@code outcome=TRANSIENT} category — the series {@code GaugedFetcher} owns. The
     * {@code AIMD/SHED/GROWTH/FREEZE} entries {@code ConcurrencyGauge} writes to the same counter
     * (owned there) are excluded by the outcome filter.
     */
    private static List<String> ownedTransientSeries(RunMetrics metrics) {
        return metrics.registry().getMeters().stream()
                .filter(meter -> meter.getId().getName().equals("swath.steal_reason"))
                .filter(meter -> "TRANSIENT".equals(meter.getId().getTag("outcome")))
                .map(meter -> meter.getId().getType() + "|" + meter.getId().getName()
                        + "|{" + tags(meter) + "}")
                .sorted()
                .toList();
    }

    private static String tags(Meter meter) {
        return meter.getId().getTags().stream()
                .sorted(Comparator.comparing(Tag::getKey).thenComparing(Tag::getValue))
                .map(tag -> tag.getKey() + "=" + tag.getValue())
                .collect(Collectors.joining(","));
    }
}
