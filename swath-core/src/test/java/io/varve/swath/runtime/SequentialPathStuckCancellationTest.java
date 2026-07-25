/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.Counter;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.RetryConfig;
import io.varve.swath.engine.RetryPolicy;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.StopReason;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end guard: a cancellation-driven {@link InterruptedException} on
 * the SEQUENTIAL/seed fetch path (the {@code TransientRetryFetcher} tripping the run's cancellation
 * with {@link StopReason#STUCK} on transient-retry cap exhaustion) must surface to the caller as the
 * contractual {@link CancelledException} — NOT a bare {@link InterruptedException} the CLI would fail
 * to recognize (exit 1). {@code Pipeline#run} performs that conversion, mirroring the engine's own
 * {@code throwCancellationOrReceiverGone}; the token still carries the winning {@code stop_reason}, so
 * {@code ListCommand#timeboxExitOrRethrow} maps it to exit 75.
 *
 * <p>The isolated {@code TransientRetryFetcherTest} confirms cap exhaustion trips STUCK +
 * InterruptedException at the fetch layer; these tests confirm the FULL sequential pipeline
 * ({@link ListRunner#runCheckpointed} → {@code Pipeline} → {@code CheckpointedScanProducer} →
 * {@code observedSequentialFetcher(TransientRetryFetcher)}) converts it, leaves the checkpoint
 * resumable (RUNNING, {@code fatal_error} unset), and never reclassifies a genuine, non-cancel
 * interrupt or a genuine fatal listing error.
 */
final class SequentialPathStuckCancellationTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "h1",
                "SEQUENTIAL", ListingMode.OBJECTS, "", "jsonl");
    }

    private static ListRunner.Spec jsonl() {
        return new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 1000, 1000, FilterChain.EMPTY, null, null);
    }

    private static Node openRootNode(SqliteCheckpointStore store) throws Exception {
        RunMeta run = store.openRun(runKey(), false, false);
        store.insertNode(NodeSpec.rootRange(run.id()));
        return store.loadResumable(run.id(), false).getFirst();
    }

    /**
     * A STUCK cancellation that fires DURING a sequential listing fetch (the watchdog or a
     * cap-exhausting retry tripping {@code cancel(STUCK)}) surfaces as {@link CancelledException},
     * carrying the token's {@code stop_reason=stuck} — so the CLI maps it to exit 75, not a bare
     * InterruptedException (exit 1). FAST: the interceptor cancels then throws a single throttle, so
     * the retrier aborts on its cancellation check without any real backoff sleep.
     */
    @Test
    @Timeout(30)
    void stuckCancellationDuringSequentialListingSurfacesCancelledException(@TempDir Path dir) throws Exception {
        CancellationToken token = new CancellationToken();
        RunContext ctx = new RunContext(token);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("data/a"), b("data/b")))
                .interceptor((req, idx, page) -> {
                    // Model the watchdog / cap-exhaustion tripping the run's cancellation with STUCK
                    // mid-fetch: TransientRetryFetcher then aborts via InterruptedException, which the
                    // pipeline must convert to CancelledException.
                    token.cancel(StopReason.STUCK);
                    throw ThrottleException.attemptTimeout("wedged");
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            Node node = openRootNode(store);
            long runId = node.id();

            assertThatThrownBy(() -> new ListRunner().runCheckpointed(
                    ctx, fetcher, new StringWriter(), jsonl(), store, runId, node))
                    .as("a STUCK-cancelled sequential listing surfaces the contractual CancelledException, "
                            + "not a bare InterruptedException the CLI would map to exit 1")
                    .isInstanceOf(CancelledException.class);

            assertThat(token.stopReason())
                    .as("the token still carries stop_reason=stuck (exit 75), unaltered by the conversion")
                    .isEqualTo(StopReason.STUCK);

            RunMeta reloaded = store.openRun(runKey(), true, false);
            assertThat(reloaded.status())
                    .as("a STUCK abort leaves the run RUNNING (resumable), never terminal-FAILED")
                    .isEqualTo(RunStatus.RUNNING);
            assertThat(reloaded.fatalError())
                    .as("resume must NOT be poisoned: fatal_error stays unset")
                    .isFalse();
        }
    }

    /**
     * Storm ride-out end-to-end: an over-cap
     * ({@code ATTEMPT_TIMEOUT}) storm on the leading fetch drives the real {@link
     * io.varve.swath.engine.TransientRetryFetcher} PAST {@code MAX_TRANSIENT_RETRIES}, but with a token
     * wired it is NO LONGER cancelled — a never-healing storm's death is owned by the watchdog. Crossing
     * the cap only engages ride-out (a raised backoff ceiling); this bounded storm then clears and the
     * sequential listing COMPLETES normally, never cancelled. Deep: rides the real backoff past the cap
     * (the seam that would speed this up is package-private to the engine, not reachable here).
     */
    @Tag("deep")
    @Test
    @Timeout(60)
    void overCapStormEndToEnd_ridesOut_completesNotCancelled(@TempDir Path dir) throws Exception {
        CancellationToken token = new CancellationToken();
        RunContext ctx = new RunContext(token);
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    // Throw 10 times (> the cap of 8, crossing into ride-out), then serve the page.
                    if (calls.getAndIncrement() < 10) {
                        throw ThrottleException.attemptTimeout("storm");
                    }
                    return page;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            Node node = openRootNode(store);
            long runId = node.id();

            // RetryConfig.DEFAULT is now BOUNDED (never an owner-less infinite ride-out), so ride-out
            // is threaded EXPLICITLY here where it is the behavior under test. A null sleeper coalesces to
            // the real backoff sleeper (RetryConfig ctor), preserving the implicit default's semantics.
            new ListRunner().runCheckpointed(ctx, fetcher, new StringWriter(), jsonl(), store, runId, node,
                    new RetryConfig(RetryPolicy.RIDE_OUT, null));

            assertThat(token.isCancelled())
                    .as("over-cap ride-out never cancels the sequential run (the watchdog owns storm death)")
                    .isFalse();
        }
    }

    /**
     * The never-heals guard on the SEQUENTIAL path under {@link
     * RetryPolicy#BOUNDED}: when no watchdog is armed (both windows disabled, {@code --checkpoint none
     * --stall-timeout 0 --no-progress-timeout 0} in the real CLI), nothing else could ever end an
     * unbounded ride-out — so {@link ListRunner#runCheckpointed} threaded with a {@code BOUNDED}
     * {@link RetryConfig} must still cancel resumably {@code STUCK} on cap exhaustion, attributing
     * {@link CancelSource#TRANSIENT_RETRY_CAP} and recording {@code retry_cap_stuck} — never
     * an infinite ride-out. This is the sequential-path sibling of the engine/seed BOUNDED guards
     * ({@code TransientTimeoutRetryEngineContractTest}/{@code TransientRetryFetcherTest}). FAST via the
     * injected no-op sleeper (no real backoff sleeps).
     */
    @Test
    @Timeout(30)
    void overCapStormEndToEnd_boundedPolicy_cancelsResumableStuck_recordsRetryCapStuck(@TempDir Path dir)
            throws Exception {
        CancellationToken token = new CancellationToken();
        RunContext ctx = new RunContext(token);
        AtomicInteger calls = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    calls.incrementAndGet();
                    throw ThrottleException.attemptTimeout("never heals");
                })
                .build();
        RetryConfig bounded = new RetryConfig(RetryPolicy.BOUNDED, millis -> { });

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("bounded.sqlite"))) {
            Node node = openRootNode(store);
            long runId = node.id();

            assertThatThrownBy(() -> new ListRunner().runCheckpointed(
                    ctx, fetcher, new StringWriter(), jsonl(), store, runId, node, bounded))
                    .as("BOUNDED cap exhaustion surfaces the resumable CancelledException end-to-end, "
                            + "never an infinite ride-out")
                    .isInstanceOf(CancelledException.class);

            assertThat(token.stopReason()).isEqualTo(StopReason.STUCK);
            assertThat(token.source())
                    .as("the retry cap is named as the cancel source")
                    .isEqualTo(CancelSource.TRANSIENT_RETRY_CAP);
            assertThat(calls.get())
                    // MAX_TRANSIENT_RETRIES (engine-package-private) is 8; bounded at exactly +1 attempts.
                    .as("bounded at exactly MAX_TRANSIENT_RETRIES+1 attempts — no indefinite ride-out")
                    .isEqualTo(9);

            RunMetrics metrics = ctx.metrics();
            Counter counter = metrics.registry().find("swath.steal_reason")
                    .tag("outcome", "TRANSIENT").tag("reason", "retry_cap_stuck").counter();
            assertThat(counter)
                    .as("the BOUNDED cap-stuck path records its engagement counter")
                    .isNotNull();

            RunMeta reloaded = store.openRun(runKey(), true, false);
            assertThat(reloaded.status())
                    .as("a BOUNDED stuck cap-exhaustion leaves the run RUNNING (resumable), never FAILED")
                    .isEqualTo(RunStatus.RUNNING);
            assertThat(reloaded.fatalError())
                    .as("never the fatal_error flag that would poison --resume")
                    .isFalse();
        }
    }

    /**
     * The conversion must NOT swallow a genuine fatal listing error: a non-throttle
     * {@link ListingException} (token never cancelled) escapes as-is — a SwathException the CLI maps
     * to exit 1 — never rewritten to a resumable {@link CancelledException}.
     */
    @Test
    @Timeout(30)
    void genuineFatalListingErrorIsNotConvertedToCancelled(@TempDir Path dir) throws Exception {
        CancellationToken token = new CancellationToken();
        RunContext ctx = new RunContext(token);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw new ListingException("fatal listing error");
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            Node node = openRootNode(store);
            long runId = node.id();

            assertThatThrownBy(() -> new ListRunner().runCheckpointed(
                    ctx, fetcher, new StringWriter(), jsonl(), store, runId, node))
                    .as("a genuine fatal listing error stays a ListingException (exit 1), never converted")
                    .isInstanceOf(ListingException.class)
                    .isNotInstanceOf(CancelledException.class);

            assertThat(token.isCancelled())
                    .as("a fatal listing error does not trip the run's cancellation")
                    .isFalse();
        }
    }

    /**
     * Guards the {@code isCancelled()} condition: a genuine {@link InterruptedException} with NO
     * run-cancel attributed is preserved as-is, never masqueraded as a run cancellation.
     */
    @Test
    @Timeout(30)
    void genuineInterruptWithoutRunCancelIsPreserved(@TempDir Path dir) throws Exception {
        CancellationToken token = new CancellationToken();
        RunContext ctx = new RunContext(token);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("data/a")))
                .interceptor((req, idx, page) -> {
                    throw new InterruptedException("genuine interrupt, no run cancel");
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            Node node = openRootNode(store);
            long runId = node.id();

            assertThatThrownBy(() -> new ListRunner().runCheckpointed(
                    ctx, fetcher, new StringWriter(), jsonl(), store, runId, node))
                    .as("an interrupt with no run-cancel attributed is NOT reclassified as CancelledException")
                    .isInstanceOf(InterruptedException.class)
                    .isNotInstanceOf(CancelledException.class);
        }
    }
}
