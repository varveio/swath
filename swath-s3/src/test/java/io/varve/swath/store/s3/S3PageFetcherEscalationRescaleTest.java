/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The engine's attempt-timeout escalation ladder is re-expressed against each call class's OWN base
 * budget ({@link S3PageFetcher#escalatedAttemptTimeoutFor}).
 *
 * <p>{@code TransientRetryFetcher.ATTEMPT_TIMEOUT_ESCALATION_LEVELS} is {@code {20s, 40s}} —
 * absolute durations authored against the SCAN base of 10 s, i.e. really "2x base, then 4x base". The
 * engine only knows the escalation level; only this store layer knows what each call class's base
 * budget is. Applied unclamped to a 3 s point probe, level 1 is a 6.7x jump straight to 20 s, and
 * with {@code PROBE_TRANSIENT_RETRY_CAP=1} a failing pivot probe burns 3 s + 20 s to return nothing.
 *
 * <p>REGRESSION: against the un-rescaled behavior
 * {@link #pivotProbeEscalationIsRescaledToItsOwn3sBase} fails (it sees 20 s / 40 s rather than
 * 6 s / 12 s). {@link #scanClassEscalationIsTheLadderExactlyAsAuthored} is the companion guard that
 * the fix does NOT disturb the worker/structure path the ladder was written for.
 *
 * <p>See {@code docs/internals/probe-budgets.md} §3.
 */
class S3PageFetcherEscalationRescaleTest {

    /** The engine ladder, as authored in {@code TransientRetryFetcher}. */
    private static final Duration LADDER_LEVEL_1 = Duration.ofSeconds(20);
    private static final Duration LADDER_LEVEL_2 = Duration.ofSeconds(40);

    private static Duration effectiveAttemptTimeout(FakeS3Client client) {
        return client.lastRequest().overrideConfiguration()
                .flatMap(o -> o.apiCallAttemptTimeout())
                .orElseThrow(() -> new AssertionError("expected a per-request attempt-timeout override"));
    }

    /** A pivot probe: {@code max_keys<=1}, no delimiter — the 3 s POINT base. */
    private static PageRequest pivotProbeEscalatedTo(Duration ladderLevel) {
        return PageRequest.objects(null, null, 1).withApiCallAttemptTimeoutOverride(ladderLevel);
    }

    /** A structure probe: {@code delimiter=/} — a SCAN class, on the 10 s base. */
    private static PageRequest structureProbeEscalatedTo(Duration ladderLevel) {
        return PageRequest.objectsDelimited(null, "/".getBytes(StandardCharsets.UTF_8), null, 1000)
                .withApiCallAttemptTimeoutOverride(ladderLevel);
    }

    @Test
    void pivotProbeEscalationIsRescaledToItsOwn3sBase() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        fetcher.fetchPage(pivotProbeEscalatedTo(LADDER_LEVEL_1));
        assertThat(effectiveAttemptTimeout(client))
                .as("ladder level 1 is 2x the scan base, so a 3s point probe escalates to 6s -- not 20s")
                .isEqualTo(Duration.ofSeconds(6));

        fetcher.fetchPage(pivotProbeEscalatedTo(LADDER_LEVEL_2));
        assertThat(effectiveAttemptTimeout(client))
                .as("ladder level 2 is 4x the scan base, so a 3s point probe escalates to 12s -- not 40s")
                .isEqualTo(Duration.ofSeconds(12));
    }

    @Test
    void scanClassEscalationIsTheLadderExactlyAsAuthored() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        fetcher.fetchPage(structureProbeEscalatedTo(LADDER_LEVEL_1));
        assertThat(effectiveAttemptTimeout(client))
                .as("a scan-class call IS the ladder's own base: level 1 passes through untouched")
                .isEqualTo(LADDER_LEVEL_1);

        fetcher.fetchPage(structureProbeEscalatedTo(LADDER_LEVEL_2));
        assertThat(effectiveAttemptTimeout(client))
                .as("a scan-class call IS the ladder's own base: level 2 passes through untouched")
                .isEqualTo(LADDER_LEVEL_2);
    }

    /**
     * The rescale keeps the ladder single-sourced in the engine: it derives the multiple from the two
     * base durations rather than hardcoding 2x/4x, so a re-tuned ladder follows automatically.
     */
    @Test
    void rescaleTracksTheLadderRatherThanHardcodingItsMultiples() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        // A hypothetical re-tuned ladder level of 35s = 3.5x the 10s scan base.
        fetcher.fetchPage(pivotProbeEscalatedTo(Duration.ofSeconds(35)));

        assertThat(effectiveAttemptTimeout(client))
                .as("3.5x the scan base re-expressed on the 3s point base = 10.5s, derived not hardcoded")
                .isEqualTo(Duration.ofMillis(10_500));
    }

    /** Escalation only ever buys room — a rescale must never hand back less than the class's base. */
    @Test
    void rescaleNeverReturnsLessThanTheCallClassBase() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        // A degenerate "escalation" below the scan base would rescale to 0.3s on the point base.
        fetcher.fetchPage(pivotProbeEscalatedTo(Duration.ofSeconds(1)));

        assertThat(effectiveAttemptTimeout(client))
                .as("floored at the point-probe base (3s), never below it")
                .isEqualTo(Duration.ofSeconds(3));
    }
}
