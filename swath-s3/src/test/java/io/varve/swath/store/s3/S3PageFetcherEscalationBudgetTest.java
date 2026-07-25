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
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * An escalation LEVEL becomes wall-clock here and nowhere else:
 * {@code base(callClass) * 2^level} ({@link S3PageFetcher#attemptTimeoutForLevel}).
 *
 * <p>The engine publishes only a level on {@link PageRequest#attemptTimeoutEscalationLevel()} —
 * retry policy is its business, duration is the store's, because only the store knows each call
 * class's base budget. So the two classes climb their own ladders from bases that reflect what the
 * call actually costs:
 *
 * <ul>
 *   <li>scan (worker page, {@code delimiter=/} structure probe), 10 s base: 10 → 20 → 40 s
 *   <li>point ({@code pivot_probe}), 3 s base: 3 → 6 → 12 s
 * </ul>
 *
 * <p>Doubling from the class's own base is monotone by construction, which is the property the
 * previous design lacked: it had the engine author absolute durations against one assumed base and
 * divide them back out here, so a larger configured base let an "escalation" SHRINK the budget and
 * needed an explicit floor to stop it. {@link #escalationNeverShrinksTheBudgetAtAnyConfiguredBase}
 * pins that the bug class is now unrepresentable. See {@code docs/internals/probe-budgets.md} §3.
 */
class S3PageFetcherEscalationBudgetTest {

    private static Optional<Duration> effectiveAttemptTimeout(FakeS3Client client) {
        return client.lastRequest().overrideConfiguration().flatMap(o -> o.apiCallAttemptTimeout());
    }

    /** A scan-class request: {@code delimiter=/} — 10 s base. */
    private static PageRequest structureProbeAtLevel(int level) {
        return PageRequest.objectsDelimited(null, "/".getBytes(StandardCharsets.UTF_8), null, 32)
                .withAttemptTimeoutEscalationLevel(level);
    }

    /** A point-class request: {@code max_keys<=1}, no delimiter — 3 s base. */
    private static PageRequest pivotProbeAtLevel(int level) {
        return PageRequest.objects(null, null, 1).withAttemptTimeoutEscalationLevel(level);
    }

    @Test
    void scanClassClimbsItsOwn10sLadder() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        fetcher.fetchPage(structureProbeAtLevel(0));
        assertThat(effectiveAttemptTimeout(client))
                .as("level 0 is the client-level base itself -- no per-request override at all")
                .isEmpty();

        fetcher.fetchPage(structureProbeAtLevel(1));
        assertThat(effectiveAttemptTimeout(client)).contains(Duration.ofSeconds(20));

        fetcher.fetchPage(structureProbeAtLevel(2));
        assertThat(effectiveAttemptTimeout(client)).contains(Duration.ofSeconds(40));
    }

    @Test
    void pointClassClimbsItsOwn3sLadder() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        fetcher.fetchPage(pivotProbeAtLevel(0));
        assertThat(effectiveAttemptTimeout(client))
                .as("level 0 for a point probe is its own 3s base, applied as an override")
                .contains(Duration.ofSeconds(3));

        fetcher.fetchPage(pivotProbeAtLevel(1));
        assertThat(effectiveAttemptTimeout(client))
                .as("a point probe escalates on ITS base (3s x 2), never onto the scan ladder's 20s")
                .contains(Duration.ofSeconds(6));

        fetcher.fetchPage(pivotProbeAtLevel(2));
        assertThat(effectiveAttemptTimeout(client)).contains(Duration.ofSeconds(12));
    }

    /**
     * REGRESSION (structural): escalation must never hand back less than the class's own base, at any
     * configured base. Under the previous divide-out-the-multiple design a 30 s scan base took a
     * pass-through branch and returned the engine's absolute 20 s — an escalation that shrank the
     * budget. Deriving from the class's own base makes that unrepresentable rather than guarded.
     */
    @Test
    void escalationNeverShrinksTheBudgetAtAnyConfiguredBase() throws Exception {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket",
                S3PageFetcherConfig.DEFAULT.withScanApiCallAttemptTimeout(Duration.ofSeconds(30)));

        fetcher.fetchPage(structureProbeAtLevel(1));

        assertThat(effectiveAttemptTimeout(client))
                .as("30s base at level 1 doubles to 60s -- strictly more room, never less")
                .contains(Duration.ofSeconds(60));
    }

    /** A negative/garbage level is clamped to the base rather than producing a nonsense budget. */
    @Test
    void negativeLevelClampsToTheBase() {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        assertThat(fetcher.attemptTimeoutForLevel(io.varve.swath.observability.RunMetrics.CALL_CLASS_PIVOT_PROBE, -3))
                .isEqualTo(Duration.ofSeconds(3));
    }

    /**
     * A level far beyond anything the engine ever asks for must not overflow or wrap {@code 1L <<
     * level} back to a smaller budget -- {@code level=64} in particular wraps a raw shift to 0,
     * which would silently hand the request the UNESCALATED base instead of more room.
     */
    @Test
    void runawayLevelIsClampedRatherThanOverflowingOrWrappingTheShift() {
        FakeS3Client client = FakeS3Client.captureOnly();
        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        Duration atCap = fetcher.attemptTimeoutForLevel(
                io.varve.swath.observability.RunMetrics.CALL_CLASS_PIVOT_PROBE, 64);

        assertThat(atCap)
                .as("a runaway level must still buy MORE room than the base, never wrap back to it")
                .isGreaterThan(Duration.ofSeconds(3));
        assertThat(fetcher.attemptTimeoutForLevel(
                        io.varve.swath.observability.RunMetrics.CALL_CLASS_PIVOT_PROBE, Integer.MAX_VALUE))
                .as("clamping saturates rather than throwing on extreme input")
                .isEqualTo(atCap);
    }

    /**
     * The no-config convenience constructor must derive its scan-class base from the REAL client's
     * own {@code apiCallAttemptTimeout} when the client can report it, not silently assume {@link
     * S3Config#DEFAULT_ATTEMPT_TIMEOUT} -- otherwise a caller pairing a custom-timeout client with
     * this overload would escalate scan-class calls against the wrong base (§ the convenience
     * constructor's own javadoc).
     */
    @Test
    void theConvenienceConstructorReadsTheScanBaseBackFromANonDefaultClient() {
        try (software.amazon.awssdk.services.s3.S3Client client = software.amazon.awssdk.services.s3.S3Client
                .builder()
                .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                .credentialsProvider(
                        software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider.create())
                .overrideConfiguration(o -> o.apiCallAttemptTimeout(Duration.ofSeconds(45)))
                .build()) {
            S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

            assertThat(fetcher.attemptTimeoutForLevel(
                            io.varve.swath.observability.RunMetrics.CALL_CLASS_WORKER_PAGE, 0))
                    .as("the client's own 45s override, not the swath-internal 10s default")
                    .isEqualTo(Duration.ofSeconds(45));
        }
    }

    /**
     * A hand-rolled {@code S3Client} test double (every other test in this suite) does not support
     * {@code serviceClientConfiguration()} -- the convenience constructor must fall back to the
     * default rather than propagating that {@link UnsupportedOperationException}.
     */
    @Test
    void theConvenienceConstructorFallsBackToTheDefaultWhenTheClientCannotReportItsConfiguration() {
        FakeS3Client client = FakeS3Client.captureOnly();

        S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

        assertThat(fetcher.attemptTimeoutForLevel(
                        io.varve.swath.observability.RunMetrics.CALL_CLASS_WORKER_PAGE, 0))
                .isEqualTo(Duration.ofSeconds(10));
    }
}
