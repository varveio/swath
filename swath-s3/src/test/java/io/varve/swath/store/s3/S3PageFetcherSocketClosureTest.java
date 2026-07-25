/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageRequest;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A transient {@code java.net.SocketException("Socket closed")} wrapped in a
 * {@link java.io.UncheckedIOException} must not escape {@link S3PageFetcher}'s fetch classifier
 * <b>unclassified</b> — an unclassified escape surfaces at the top-level CLI handler as
 * {@code swath: unexpected error: java.io.UncheckedIOException: java.net.SocketException: Socket
 * closed (error_class=unknown)} — a non-classified, non-resumable crash disposition
 * ({@code stop_reason=crash}, {@code completed=false}, no {@code error_class}, no resumable
 * {@code stuck} marker).
 *
 * <p>{@link S3PageFetcher#fetchPage} classifies SDK faults in a {@code catch (SdkException e)}
 * arm; a {@link UncheckedIOException} is a plain {@link RuntimeException}, NOT an
 * {@link SdkException}, so a socket-closure fault surfacing in that wrapper from
 * {@code s3.listObjectsV2(...)} is not caught there. A second {@code catch (RuntimeException e)}
 * arm classifies it instead: when the cause chain contains an {@code IOException} the fault
 * becomes a retryable {@link ThrottleException} of kind {@code NETWORK}, recovered the same way
 * the SDK-side arm's own {@link ThrottleException} families are; any other {@code RuntimeException}
 * is a genuine bug and is rethrown unchanged, before any metric/latency side effect.
 *
 * <p>A socket closure is a client-local network fault (the underlying cause is a
 * {@link SocketException}, an {@link java.io.IOException}) — exactly the shape the modeled
 * {@code connection-reset SdkClientException} row of {@link S3PageFetcherFaultTaxonomyTest}
 * classifies as {@link ThrottleException.Kind#NETWORK} and recovers; the ONLY difference here is
 * the wrapper type ({@code UncheckedIOException} instead of {@code SdkClientException}).
 *
 * <p><b>Disposition contract asserted (not a specific fix).</b> The run must NOT end at
 * {@code error_class=unknown} / a bare crash. It must be either recovered/retried OR end in a
 * resumable, classified disposition. At THIS store-classifier seam the fetcher's only
 * recover-or-resumable-enabling option is to surface the fault as a retryable
 * {@link ThrottleException} (which the engine's wrappers then ride out — the "recovered" branch);
 * the resumable-{@code stuck} branch is only reachable end-to-end via the cancellation/watchdog
 * path, not from a fetcher throw. So this test asserts the transient-classification direction here
 * and documents that a resumable classified terminal is also contract-acceptable elsewhere. What
 * is NEVER acceptable — and is what this test pins RED — is the raw {@code UncheckedIOException}
 * escape (an unclassified {@code error_class=unknown} crash), and (weaker) a plain fatal
 * {@link ListingException} would still be a non-resumable exit-1 crash, also failing the contract.
 */
class S3PageFetcherSocketClosureTest {

    private static final PageRequest REQ = PageRequest.objects(null, null, 1000);

    /** The exact escaping throwable this test models: SocketException("Socket closed") -> UncheckedIOException. */
    private static UncheckedIOException socketClosedUncheckedIOException() {
        return new UncheckedIOException(new SocketException("Socket closed"));
    }

    /**
     * A {@code UncheckedIOException(SocketException("Socket closed"))} raised on a
     * page fetch during listing must be CLASSIFIED as a retryable transient throttle (so the
     * engine's retry/ride-out machinery recovers it) — never escape raw as the unclassified
     * {@code error_class=unknown} crash the top-level handler reports.
     */
    @Test
    void socketClosedUncheckedIOException_isClassifiedTransient_notUnknownCrash() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(socketClosedUncheckedIOException()), "b", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).as("a socket-closure fetch fault must throw").isNotNull();
        // The raw wrapper must not escape unclassified. This is the exact
        // shape ExitCodes.forThrowable maps to exit-1 / error_class=unknown.
        assertThat(thrown)
                .as("socket-closure must NOT escape as a raw UncheckedIOException (error_class=unknown crash-through)")
                .isNotInstanceOf(UncheckedIOException.class);
        // Recover-or-resumable: at the store-classifier seam only a retryable ThrottleException enables
        // recovery. A socket closure is a client-local network fault (SocketException == IOException),
        // so its natural classification is Kind.NETWORK (retried, non-AIMD-voting) — mirroring the
        // modeled connection-reset row. A plain fatal ListingException would still be a non-resumable
        // exit-1 crash and is therefore NOT contract-acceptable here.
        assertThat(thrown)
                .as("socket-closure must be a retryable transient ThrottleException (recovered branch)")
                .isInstanceOf(ThrottleException.class)
                .isInstanceOf(ListingException.class);
        ThrottleException te = (ThrottleException) thrown;
        // Codomain lockdown (1): the EXACT kind, not just "some throttle" — a socket closure is a
        // client-local network fault, never a store-backpressure SLOWDOWN/SERVER_5XX.
        assertThat(te.kind()).as("socket-closure must classify as exactly Kind.NETWORK")
                .isEqualTo(ThrottleException.Kind.NETWORK);
        assertThat(te.kind().votesAimdDown())
                .as("a client-local socket closure is not S3 backpressure — it must not vote AIMD down")
                .isFalse();
        // Codomain lockdown (4): the recovery counter/rollup DOES increment on this path — the
        // observability half of the contract (recovered_errors.socket_closure in the JSON summary is
        // sourced 1:1 from this SAME meter name, JsonRunSummaryWriter#counterCount("swath.s3.socket_closure_recovered")).
        assertThat(registry.get("swath.s3.socket_closure_recovered").counter().count())
                .as("swath.s3.socket_closure_recovered must increment on the recovered socket-closure path")
                .isEqualTo(1.0);
    }

    /**
     * Codomain lockdown (2): a genuine bug — a plain {@link RuntimeException} with NO
     * {@link java.io.IOException} anywhere in its cause chain — must propagate UNCHANGED (same
     * instance), never reclassified as a transient {@link ThrottleException}. This is the core
     * codomain guarantee this classifier must not over-reach: only an IO-caused escape is a network
     * fault, everything else keeps its original (fatal, unclassified-by-design) disposition.
     */
    @Test
    void nonIoRuntimeException_isRethrownUnchanged_notReclassified() {
        IllegalStateException bug = new IllegalStateException("genuine bug, no IO cause");
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(bug), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).as("must be exactly the SAME injected instance, not wrapped/reclassified")
                .isSameAs(bug);
        assertThat(thrown).as("must never become a ThrottleException").isNotInstanceOf(ThrottleException.class);
    }

    /**
     * Codomain lockdown (3): no side effect on the non-IO rethrow path — the recovery counter
     * must be untouched, proving the cause-chain check and the rethrow happen BEFORE any metric is
     * recorded (mirrors the production comment "rethrown here BEFORE any metric/latency side effect").
     */
    @Test
    void nonIoRuntimeException_doesNotIncrementSocketClosureRecoveredCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(new IllegalStateException("genuine bug, no IO cause")), "b", S3PageFetcherConfig.DEFAULT.withMetrics(metrics));

        catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(registry.find("swath.s3.socket_closure_recovered").counter())
                .as("swath.s3.socket_closure_recovered must not be registered/incremented on a genuine-bug rethrow")
                .satisfiesAnyOf(
                        counter -> assertThat(counter).isNull(),
                        counter -> assertThat(counter.count()).isEqualTo(0.0));
    }

    /**
     * Codomain lockdown (5): cause-chain walk safety. A self-referential (cyclic) cause chain with
     * NO {@link java.io.IOException} anywhere in it must not hang/StackOverflow the classifier — the
     * cycle-guarded ({@code IdentityHashMap}-backed {@code seen} set) walk in
     * {@code S3FaultClassifier#hasIOExceptionCause} must terminate and the fault must still be
     * rethrown (unchanged, per lockdown (2), since no IOException is present).
     */
    @Test
    void cyclicCauseChainWithNoIoException_terminatesAndRethrowsUnchanged() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);   // a -> b -> a: a genuine cycle, no IOException anywhere in it

        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(a), "b");
        Throwable thrown = assertTimeoutPreventsHang(() -> catchThrowable(() -> fetcher.fetchPage(REQ)));

        assertThat(thrown).as("cyclic non-IO cause chain must still be rethrown, same instance").isSameAs(a);
        assertThat(thrown).as("must never become a ThrottleException").isNotInstanceOf(ThrottleException.class);
    }

    /** Bounds the cycle-safety test itself so a regression fails fast instead of hanging the suite. */
    private static Throwable assertTimeoutPreventsHang(Supplier<Throwable> op) {
        return Assertions.assertTimeout(Duration.ofSeconds(5), op::get);
    }

    /**
     * Codomain guard (mirrors {@link S3PageFetcherFaultTaxonomyTest}'s legal-disposition property):
     * whatever the eventual classification, the socket-closure fault must land in the legal
     * disposition codomain {@code {ListingException | InterruptedException}} and must NEVER leak a
     * raw {@link RuntimeException} / {@link UncheckedIOException}. This is the seam-agnostic form of
     * the contract that stays valid across fix directions.
     */
    @Test
    void socketClosureNeverEscapesAsUnclassifiedRuntimeException() {
        S3PageFetcher fetcher = new S3PageFetcher(FakeS3Client.throwing(socketClosedUncheckedIOException()), "b");
        Throwable thrown = catchThrowable(() -> fetcher.fetchPage(REQ));

        assertThat(thrown).as("must throw a classified disposition, never leak the raw wrapper").isNotNull();
        assertThat(thrown)
                .as("must not leak a raw UncheckedIOException/RuntimeException (error_class=unknown crash)")
                .isNotInstanceOf(UncheckedIOException.class);
        assertThat(thrown instanceof ListingException || thrown instanceof InterruptedException)
                .as("disposition %s must be in {ListingException, InterruptedException}",
                        thrown.getClass().getName())
                .isTrue();
    }

}
