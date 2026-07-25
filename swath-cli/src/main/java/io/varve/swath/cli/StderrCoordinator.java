/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The one writer of stderr. Four independent producers share that single file descriptor — the live
 * {@link ProgressDisplay}, Logback's console appender, the {@link SummaryRenderer} block and
 * picocli's {@code swath: …} error lines — and nothing but a shared lock can stop them from
 * splicing each other's records. Every one of them goes through here, so a record is written whole
 * or not at all.
 *
 * <p><b>How Logback participates.</b> Its console appender is re-pointed at {@link #logStream()}
 * ({@link CliLogging#serializeThrough}), so a log event takes the same lock a progress frame does
 * rather than racing it. The alternative — suppressing the display whenever logging is on — was
 * rejected because the default level is WARN: a run can emit a warning at any moment, so
 * "logging is active" is not a state the CLI can wait out.
 *
 * <p><b>Progress is a generation, not a flag.</b> {@link #openProgress()} hands out a {@link
 * ProgressChannel} that owns the channel until it is closed; a frame from any older channel is
 * dropped. That is what makes teardown safe: {@code DaemonSchedulers.stop()} can return while a
 * tick is still being formatted, and the summary or an error path can close the channel while that
 * frame is in mid-flight, so the close decision must be re-checked under the lock at the moment of
 * writing. Closing is idempotent and permanent for that channel — after it, no output, ever.
 *
 * <p><b>Broken stderr.</b> {@link PrintStream} swallows an EPIPE and records it as an error flag
 * instead of throwing, so every frame is flushed and then checked with {@link
 * PrintStream#checkError()}. A broken channel disables further progress and nothing else: the run's
 * disposition and exit status are not stderr's to decide.
 */
final class StderrCoordinator {

    /**
     * The process's stderr coordinator. {@link System#err} is resolved per write rather than
     * captured, exactly as Logback's own {@code ConsoleTarget} does it, so a caller that swaps the
     * stream (a test, an embedding application) is honored instead of bypassed.
     */
    private static final StderrCoordinator SHARED = new StderrCoordinator(() -> System.err);

    static StderrCoordinator shared() {
        return SHARED;
    }

    private final Supplier<PrintStream> target;
    private final Object lock = new Object();

    /** The channel currently allowed to emit frames; {@code null} once progress has finished. */
    private ProgressChannel current;

    /** Sticky: a stderr that failed a write once is not going to start working again. */
    private boolean channelBroken;

    StderrCoordinator(Supplier<PrintStream> target) {
        this.target = target;
    }

    /** Open a progress generation, superseding any previous one. */
    ProgressChannel openProgress() {
        synchronized (lock) {
            current = new ProgressChannel();
            return current;
        }
    }

    /**
     * Permanently end live progress before writing something that must be the last word on this
     * stderr — the summary block, a {@code swath: …} error line. Idempotent.
     */
    void finishProgress() {
        synchronized (lock) {
            current = null;
        }
    }

    /**
     * Write one complete non-progress record: the summary block, an error line, a log event. Never
     * suppressed — only live progress is.
     */
    void record(Consumer<PrintStream> body) {
        synchronized (lock) {
            PrintStream err = target.get();
            body.accept(err);
            err.flush();
        }
    }

    /**
     * The stream Logback's console appender is re-pointed at, so its events are serialized with
     * everything else on this fd. Writes pass straight through to the current stderr under the
     * lock, so an appender's write/flush pair cannot be reordered around a frame.
     */
    OutputStream logStream() {
        return new OutputStream() {
            @Override
            public void write(int b) {
                synchronized (lock) {
                    target.get().write(b);
                }
            }

            @Override
            public void write(byte[] b, int off, int len) {
                synchronized (lock) {
                    target.get().write(b, off, len);
                }
            }

            @Override
            public void flush() {
                synchronized (lock) {
                    target.get().flush();
                }
            }
        };
    }

    /** One live-progress generation: it may write frames until it — or the summary — closes it. */
    final class ProgressChannel implements AutoCloseable {

        /**
         * Write one complete frame, flushed. Returns whether it was written: {@code false} once
         * this channel has been closed or stderr has broken, which is also what {@link #isActive()}
         * reports so a tick can skip building an event at all.
         */
        boolean frame(String line) {
            synchronized (lock) {
                if (current != this || channelBroken) {
                    return false;
                }
                PrintStream err = target.get();
                err.println(line);
                err.flush();
                if (err.checkError()) {
                    channelBroken = true;
                    return false;
                }
                return true;
            }
        }

        boolean isActive() {
            synchronized (lock) {
                return current == this && !channelBroken;
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                if (current == this) {
                    current = null;
                }
            }
        }
    }
}
