/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.engine.TransientRetryFetcher;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.MockPageFetcher;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A burst that NEVER heals is ended by the {@code LivenessWatchdog}, NOT by the per-fetch retry cap.
 *
 * <p><b>The contract.</b> A from-{@code t=0} attempt-timeout burst does not self-terminate at
 * the retry cap — the seed's {@code TransientRetryFetcher} rides it out INDEFINITELY. When the store
 * never heals, the run is bounded by the {@code LivenessWatchdog} alone: it cancels {@code STUCK} →
 * the ride-out unwinds cooperatively → a RESUMABLE exit 75 (never a fatal exit 1, never
 * resume-poisoning).
 *
 * <p><b>WHICH watchdog rung ends it.</b> The {@code LivenessWatchdog} has two independent tripwires:
 * the {@code --idle-timeout} TOTAL-FREEZE rung (fires when {@code progressSignal()} — which folds in
 * throttle/retry activity — stops advancing) and the {@code --no-progress-timeout} ZERO-REAL-PROGRESS
 * backstop (fires when {@code realProgressSignal()} — committed work only — stays flat while the run
 * is otherwise active). A {@code MockPageFetcher} burst records NO {@code swath.throttle.events} (only
 * {@code S3PageFetcher} does, at its classification point), so end-to-end here {@code
 * progressSignal()} does NOT climb — meaning the total-freeze rung would ALSO fire if armed, making
 * the two rungs indistinguishable. Do not arm both windows: this test DISABLES the stall tripwire
 * ({@code --idle-timeout 0}) and arms ONLY the {@code --no-progress-timeout} backstop, so with
 * {@code stallWindowNanos == 0} the total-freeze arm of {@code LivenessWatchdog.tick()} can never
 * fire, and the run's STUCK disposition is UNAMBIGUOUSLY owned by the zero-real-progress backstop.
 * (The active-burst variant where {@code progressSignal()} keeps CLIMBING while
 * {@code realProgressSignal()} stays flat — a property this mock-driven test cannot reproduce
 * end-to-end — is guarded per-commit at the unit level by
 * {@code LivenessWatchdogTest.zeroRealProgressBackstopTripsWhileProgressSignalKeepsAdvancing} and
 * {@code NoProgressBackstopContractTest}.)
 *
 * <p><b>Why the retry count is the discriminator.</b> A run whose per-fetch retry cap ends the burst
 * aborts at EXACTLY attempt 9 — the fetch is served exactly 9 times, and the run never reaches the
 * watchdog at all. This test asserts the fetch is served {@code >> 9} times before the backstop ends
 * it, which a cap-bound abort can never satisfy. Both dispositions exit 75 STUCK, so the retry
 * COUNT — not the exit code — is what pins death to the watchdog rather than the per-fetch cap.
 *
 * <p><b>error_class note.</b> The terminal disposition's {@code error_class=stuck_api_timeouts} token is
 * classified from {@code swath.throttle.events{attempt_timeout}}, which only {@code S3PageFetcher}
 * records — a {@code MockPageFetcher} cannot populate it, so end-to-end here the token reads
 * {@code stuck_unknown} by construction (not a defect). The classifier→{@code stuck_api_timeouts}
 * contract itself is guarded at the unit level by {@code RunMetricsContractTest}.
 *
 * <p><b>Coordination.</b> A package-private {@code ListCommand.backoffSleeperOverride} field of type
 * {@code TransientRetryFetcher.Sleeper} (null in production), plumbed into the seed's
 * {@code TransientRetryFetcher}. Here it caps each backoff at ~1 ms so the ride-out accrues
 * thousands of retries within the tight liveness window instead of sleeping for minutes. The
 * {@code TransientRetryFetcher.Sleeper} nested type must stay {@code public} so this
 * {@code cli}-package test can bind the field across packages.
 */
final class SeedRideOutWatchdogStopContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final String PREFIX = "data/";
    private static final List<byte[]> KEYS = List.of(b("data/a/1"), b("data/a/2"), b("data/b/1"));

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** ~1 ms backoff sleeper: the ride-out accrues many retries per second without the real burst sleeps. */
    private static final TransientRetryFetcher.Sleeper FAST_SLEEPER = millis -> Thread.sleep(1);

    private ListCommand baseCommand(Path db, PageFetcher fetcher) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.connection.region = REGION;
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = db.toString();
        cmd.fetcherOverride = fetcher;
        cmd.backoffSleeperOverride = FAST_SLEEPER;
        // Arm ONLY the zero-real-progress backstop (see the class doc for why arming both rungs would
        // make them indistinguishable here): a permanent attempt-timeout burst commits zero real pages,
        // so realProgressSignal stays flat and the backstop fires in ~1 s.
        cmd.liveness.noProgressTimeout = "1s";
        cmd.liveness.stallTimeout = "0";
        return cmd;
    }

    @Test
    @Timeout(60)
    void neverHealingStorm_ridesOutPastTheCap_thenDiesResumableStuckViaWatchdog(@TempDir Path dir)
            throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("out");
        Path summary = dir.resolve("summary.json");

        AtomicInteger thrown = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(KEYS)
                .interceptor((req, idx, page) -> {
                    thrown.incrementAndGet();
                    throw ThrottleException.attemptTimeout("permanent t=0 attempt-timeout storm");
                })
                .build();

        ListCommand cmd = baseCommand(db, fetcher);
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = out.toString();
        cmd.output.summaryJson = summary.toString();

        int exit = cmd.call();

        assertThat(exit)
                .as("a never-healing storm is a RESUMABLE tempfail (exit 75 STUCK), not a fatal exit 1")
                .isEqualTo(ExitCodes.STUCK);
        assertThat(thrown.get())
                .as("the storm rode out FAR past the legacy 9-attempt cap before the watchdog killed it")
                .isGreaterThan(15);

        // Resumable disposition (mirrors SeedAbortResumableContractTest): zero nodes, RUNNING, not fatal.
        assertThat(CheckpointDbProbe.nodeCount(db)).as("I2: a STUCK seed commits ZERO nodes").isZero();
        assertThat(CheckpointDbProbe.runStatus(db))
                .as("a watchdog-killed storm leaves the run RUNNING (resumable), never fatal-FAILED")
                .isEqualTo("RUNNING");
        assertThat(CheckpointDbProbe.fatalError(db))
                .as("never the fatal_error flag that would poison swath resume")
                .isFalse();
        assertThat(DatasetLayout.of(out).dataParts())
                .as("no listing happened, so the dataset has no output parts")
                .isEmpty();
        assertThat(Files.exists(DatasetLayout.of(out).success()))
                .as("a STUCK run never publishes the dataset")
                .isFalse();

        JsonNode root = MAPPER.readTree(summary.toFile());
        assertThat(root.get("completed").asBoolean()).as("a STUCK run is never completed").isFalse();
        assertThat(root.get("stop_reason").asText())
                .as("the terminal disposition is STUCK").isEqualTo("stuck");
    }
}
