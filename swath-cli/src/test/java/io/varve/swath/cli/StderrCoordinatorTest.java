/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one writer of stderr: whether a progress frame, a log event, the summary block and an error
 * line can splice each other, and whether anything at all escapes after progress is closed.
 */
final class StderrCoordinatorTest {

    @Test
    @Timeout(30)
    void aLogEventArrivingMidFrameWaitsInsteadOfSplicingIt() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        CountDownLatch insideFrame = new CountDownLatch(1);
        CountDownLatch releaseFrame = new CountDownLatch(1);
        AtomicBoolean firstWrite = new AtomicBoolean(true);
        // The supplier is consulted under the coordinator's lock, so parking here parks a writer
        // that is midway through emitting its frame -- the exact moment a log event must not
        // interleave at.
        StderrCoordinator coordinator = new StderrCoordinator(() -> {
            if (firstWrite.compareAndSet(true, false)) {
                insideFrame.countDown();
                await(releaseFrame);
            }
            return stream;
        });
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress();

        Thread frame = new Thread(() -> channel.frame("  listing · 10 objects"), "frame");
        frame.start();
        assertThat(insideFrame.await(10, TimeUnit.SECONDS)).isTrue();

        CountDownLatch logWritten = new CountDownLatch(1);
        Thread logEvent = new Thread(() -> {
            coordinator.record(err -> err.println("12:00:00.000 WARN  swath - transient retried"));
            logWritten.countDown();
        }, "log");
        logEvent.start();

        assertThat(logWritten.await(200, TimeUnit.MILLISECONDS))
                .as("a log event must wait for the frame to finish, not write into the middle of it")
                .isFalse();
        releaseFrame.countDown();
        frame.join();
        logEvent.join();

        assertThat(captured.toString(StandardCharsets.UTF_8).lines())
                .containsExactly("  listing · 10 objects",
                        "12:00:00.000 WARN  swath - transient retried");
    }

    @Test
    void logbackWritesThroughTheCoordinatorOnceItIsAttached() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        StderrCoordinator coordinator = new StderrCoordinator(() -> stream);
        Logger log = LoggerFactory.getLogger(StderrCoordinatorTest.class);
        try {
            CliLogging.serializeThrough(coordinator);
            log.warn("coordinated_log_event");
        } finally {
            // Hand the console appender back to the process's own stderr coordinator.
            CliLogging.serializeThrough(StderrCoordinator.shared());
        }

        assertThat(captured.toString(StandardCharsets.UTF_8))
                .as("the console appender writes through the coordinator, so its events take the "
                        + "same lock a progress frame does")
                .contains("coordinated_log_event");
    }

    @Test
    void everyFrameIsFlushedWithoutWaitingForABuffer() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream unflushed = new PrintStream(captured, false, StandardCharsets.UTF_8);
        StderrCoordinator coordinator = new StderrCoordinator(() -> unflushed);

        assertThat(coordinator.openProgress().frame("  seeding · 1/64 probes (2%)")).isTrue();

        assertThat(captured.toString(StandardCharsets.UTF_8))
                .as("a frame an operator cannot see yet is not progress")
                .isEqualTo("  seeding · 1/64 probes (2%)" + System.lineSeparator());
    }

    @Test
    void aClosedChannelNeverWritesAgainAndClosingIsIdempotent() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        StderrCoordinator coordinator = coordinator(captured);
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress();
        channel.frame("  listing · 1 objects");

        channel.close();
        channel.close();

        assertThat(channel.frame("  listing · 2 objects")).isFalse();
        assertThat(channel.isActive()).isFalse();
        assertThat(captured.toString(StandardCharsets.UTF_8).lines())
                .containsExactly("  listing · 1 objects");
    }

    @Test
    void aFrameStillInFlightWhenProgressFinishesIsDropped() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        StderrCoordinator coordinator = coordinator(captured);
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress();

        // The summary/error path's permanent finish, taken while a tick is still formatting.
        coordinator.finishProgress();
        coordinator.record(err -> err.println("  1,204,993 objects in 4m12s"));

        assertThat(channel.frame("  listing · late")).isFalse();
        assertThat(captured.toString(StandardCharsets.UTF_8).lines())
                .as("no output after close, ever")
                .containsExactly("  1,204,993 objects in 4m12s");
    }

    @Test
    void aNewGenerationRetiresThePreviousRunsChannel() {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        StderrCoordinator coordinator = coordinator(captured);
        StderrCoordinator.ProgressChannel first = coordinator.openProgress();

        StderrCoordinator.ProgressChannel second = coordinator.openProgress();

        assertThat(first.frame("  stale")).isFalse();
        assertThat(second.frame("  fresh")).isTrue();
        assertThat(captured.toString(StandardCharsets.UTF_8).lines()).containsExactly("  fresh");
    }

    @Test
    void aBrokenStderrDisablesProgressAndStaysDisabled() {
        // EPIPE: PrintStream records a write failure as an error flag instead of throwing, so
        // catching RuntimeException would never see it.
        PrintStream broken = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("Broken pipe");
            }
        }, true, StandardCharsets.UTF_8);
        StderrCoordinator coordinator = new StderrCoordinator(() -> broken);
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress();

        assertThat(channel.frame("  listing · 1 objects")).isFalse();
        assertThat(channel.isActive()).isFalse();
        assertThat(channel.frame("  listing · 2 objects"))
                .as("the stream this generation writes to failed once; it will not start working again")
                .isFalse();
    }

    @Test
    @Timeout(30)
    void anErrorRecordOnPicocliOwnWriterStillTakesTheLock() throws Exception {
        // The terminal error path writes through picocli's writer (an embedding application may have
        // redirected it), so it cannot be handed this coordinator's stream -- but a multi-line stack
        // trace must still be written whole, with no log event spliced into it.
        StringWriter picocli = new StringWriter();
        StderrCoordinator coordinator = new StderrCoordinator(() -> new PrintStream(
                OutputStream.nullOutputStream(), true, StandardCharsets.UTF_8));
        CountDownLatch insideError = new CountDownLatch(1);
        CountDownLatch releaseError = new CountDownLatch(1);

        Thread error = new Thread(() -> coordinator.record(() -> {
            PrintWriter err = new PrintWriter(picocli);
            err.println("swath: unexpected error: java.lang.IllegalStateException");
            insideError.countDown();
            await(releaseError);
            err.println("\tat io.varve.swath.Whatever.method(Whatever.java:1)");
            err.flush();
        }), "error");
        error.start();
        assertThat(insideError.await(10, TimeUnit.SECONDS)).isTrue();

        CountDownLatch logWritten = new CountDownLatch(1);
        Thread logEvent = new Thread(() -> {
            coordinator.record(err -> err.println("12:00:00.000 WARN  swath - late event"));
            logWritten.countDown();
        }, "log");
        logEvent.start();

        assertThat(logWritten.await(200, TimeUnit.MILLISECONDS))
                .as("a log event must wait for the whole error record, stack trace included")
                .isFalse();
        releaseError.countDown();
        error.join();
        logEvent.join();
        assertThat(picocli.toString().lines()).hasSize(2);
    }

    @Test
    void brokennessBelongsToTheStreamThatBrokeNotToTheProcess() {
        // The shared coordinator outlives a single invocation and resolves System.err per write, so
        // one run's broken capture must not disable progress for every later run in this JVM.
        PrintStream broken = new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("Broken pipe");
            }
        }, true, StandardCharsets.UTF_8);
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream healthy = new PrintStream(captured, true, StandardCharsets.UTF_8);
        AtomicBoolean useBroken = new AtomicBoolean(true);
        StderrCoordinator coordinator = new StderrCoordinator(() -> useBroken.get() ? broken : healthy);

        assertThat(coordinator.openProgress().frame("  listing · lost")).isFalse();
        useBroken.set(false);

        assertThat(coordinator.openProgress().frame("  listing · 2 objects"))
                .as("a later invocation with a healthy stderr gets its progress back")
                .isTrue();
        assertThat(captured.toString(StandardCharsets.UTF_8).lines())
                .containsExactly("  listing · 2 objects");
    }

    private static StderrCoordinator coordinator(ByteArrayOutputStream captured) {
        PrintStream stream = new PrintStream(captured, true, StandardCharsets.UTF_8);
        return new StderrCoordinator(() -> stream);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("latch never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
