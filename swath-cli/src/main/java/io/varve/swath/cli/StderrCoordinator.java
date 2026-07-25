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
 * <p><b>Progress is a generation, not a flag.</b> {@link #openProgress(boolean)} hands out a {@link
 * ProgressChannel} that owns the channel until it is closed; a frame from any older channel is
 * dropped. That is what makes teardown safe: {@code DaemonSchedulers.stop()} can return while a
 * tick is still being formatted, and the summary or an error path can close the channel while that
 * frame is in mid-flight, so the close decision must be re-checked under the lock at the moment of
 * writing. Closing is idempotent and permanent for that channel — after it, no output, ever.
 *
 * <p><b>A redrawing frame makes the lock load-bearing twice over.</b> On a terminal a frame
 * overwrites its predecessor in place and leaves the cursor on the line it owns, so that line is
 * unfinished output that any other producer would otherwise write straight into. Every non-progress
 * path therefore erases the frame before writing and puts it back afterwards, all inside the one
 * lock — which is why a log event, a summary block and a frame can never be seen interleaved even
 * though the frame is, by construction, never terminated.
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

    /**
     * Return to column one, and erase from the cursor to the end of the line. The two sequences a
     * redrawing frame needs, and the only control codes swath emits that are not colour.
     *
     * <p>They live here rather than in {@link AnsiPalette} because they are not governed by {@code
     * --color}: redraw and colour are independently resolved, and a run with {@code --color=never}
     * on a terminal still redraws. What they share is the gate — both are written only where {@link
     * TerminalCapabilities} has already said fd 2 is a terminal.
     */
    private static final String ERASE_LINE = "\r\u001B[K";

    private final Supplier<PrintStream> target;
    private final Object lock = new Object();

    /** The channel currently allowed to emit frames; {@code null} once progress has finished. */
    private ProgressChannel current;

    /**
     * The most recent redrawing frame's text, kept so an interposed record can put it back. Null
     * for a plain-record channel, which leaves nothing to repaint.
     */
    private String lastFrame;

    /** Whether {@link #lastFrame} is on screen right now, and so must be erased before any write. */
    private boolean framePainted;

    StderrCoordinator(Supplier<PrintStream> target) {
        this.target = target;
    }

    /**
     * Open a progress generation, superseding any previous one.
     *
     * @param redraw whether frames overwrite one another in place ({@code \r} + clear-to-end-of-line,
     *         no newline) rather than each taking a line of their own. Only ever true for a
     *         terminal: a redirected stderr must receive no control sequences at all, and a file of
     *         carriage returns is not a log
     */
    ProgressChannel openProgress(boolean redraw) {
        synchronized (lock) {
            eraseIfPainted();
            lastFrame = null;
            current = new ProgressChannel(redraw);
            return current;
        }
    }

    /**
     * Permanently end live progress before writing something that must be the last word on this
     * stderr — the summary block, a {@code swath: …} error line. Idempotent.
     *
     * <p>A live frame is erased rather than newline-terminated: what follows is the record the
     * operator actually wants, and a half-finished counter frozen above it reads like part of the
     * result.
     */
    void finishProgress() {
        synchronized (lock) {
            eraseIfPainted();
            lastFrame = null;
            current = null;
        }
    }

    /**
     * Write one complete non-progress record: the summary block, an error line, a log event. Never
     * suppressed — only live progress is.
     *
     * <p>A redrawing frame is erased first and repainted after, so the record lands on a clean line
     * and the operator does not lose sight of a run still in flight. Both happen under the one lock
     * that already makes the record atomic.
     */
    void record(Consumer<PrintStream> body) {
        synchronized (lock) {
            PrintStream err = target.get();
            eraseFrame(err);
            body.accept(err);
            repaintFrame(err);
            err.flush();
        }
    }

    /**
     * Erase the live frame, for a caller that has a stream in hand because it is about to write
     * anyway. Returns whether anything was written.
     */
    private boolean eraseFrame(PrintStream err) {
        if (!framePainted) {
            return false;
        }
        framePainted = false;
        err.print(ERASE_LINE);
        return true;
    }

    /**
     * Erase the live frame, for a caller whose only reason to touch stderr is that a frame might be
     * on it. The stream is resolved only when there is genuinely something to erase — a run with no
     * live display must not so much as ask for stderr on its way through here.
     */
    private void eraseIfPainted() {
        if (framePainted) {
            PrintStream err = target.get();
            eraseFrame(err);
            err.flush();
        }
    }

    /** Put the last redrawing frame back, if its channel is still the live one. */
    private void repaintFrame(PrintStream err) {
        if (lastFrame != null && current != null && current.canWrite()) {
            err.print("\r" + lastFrame + "\u001B[K");
            framePainted = true;
        }
    }

    /**
     * Write one complete record through a writer this coordinator does not own — picocli's {@code
     * cmd.getErr()}, which the CLI's tests and an embedding application may have redirected, so it
     * cannot simply be replaced by the stream above. The body still runs under the one lock, which
     * is the part that matters: a multi-line stack trace is written whole, with no log event spliced
     * into it and none after it.
     */
    void record(Runnable body) {
        synchronized (lock) {
            // Erased on the stream this coordinator owns, which is the one the frame was painted
            // on; the body's own writer may be elsewhere, and that is the caller's business. Not
            // repainted: this overload's callers are terminal, and every one of them has already
            // called finishProgress().
            eraseIfPainted();
            body.run();
        }
    }

    /**
     * The stream Logback's console appender is re-pointed at, so its events are serialized with
     * everything else on this fd. Writes pass straight through to the current stderr under the
     * lock, so an appender's write/flush pair cannot be reordered around a frame.
     */
    OutputStream logStream() {
        return new LogStream();
    }

    /** Whether {@code stream} is already one of this coordinator's — see {@link #logStream()}. */
    boolean ownsLogStream(Object stream) {
        return stream instanceof LogStream logStream && logStream.owner() == this;
    }

    /**
     * The {@link #logStream()} type, named so an already-routed appender is recognisable.
     *
     * <p>A log event arrives as a sequence of writes followed by a flush, so the frame is erased on
     * the first write that finds one painted and repainted at the flush that ends the event —
     * rather than once per byte, or once per event with the event's own bytes landing on top of a
     * frame that is still on screen.
     */
    private final class LogStream extends OutputStream {

        @Override
        public void write(int b) {
            synchronized (lock) {
                PrintStream err = target.get();
                eraseFrame(err);
                err.write(b);
            }
        }

        @Override
        public void write(byte[] b, int off, int len) {
            synchronized (lock) {
                PrintStream err = target.get();
                eraseFrame(err);
                err.write(b, off, len);
            }
        }

        @Override
        public void flush() {
            synchronized (lock) {
                PrintStream err = target.get();
                repaintFrame(err);
                err.flush();
            }
        }

        private StderrCoordinator owner() {
            return StderrCoordinator.this;
        }
    }

    /** One live-progress generation: it may write frames until it — or the summary — closes it. */
    final class ProgressChannel implements AutoCloseable {

        /** Whether this generation's frames overwrite in place — see {@link #openProgress}. */
        private final boolean redraw;

        /**
         * Sticky for THIS generation only. A write failure says the stream this generation has been
         * writing to is gone, not that the process's stderr is gone forever: the target is resolved
         * per write, so a later in-process invocation — after {@link System#setErr} handed it a
         * healthy stream — opens a new generation and starts clean.
         */
        private boolean broken;

        private ProgressChannel(boolean redraw) {
            this.redraw = redraw;
        }

        /**
         * Write one complete frame, flushed. Returns whether it was written: {@code false} once
         * this channel has been closed or its stderr has broken, which is also what {@link
         * #isActive()} reports so a tick can skip building an event at all.
         *
         * <p>A redrawing frame replaces the one before it and leaves the cursor parked at its own
         * end, with no newline — so the record is complete on screen but the line is still this
         * channel's to overwrite. A plain frame is a line of its own, exactly as before.
         */
        boolean frame(String line) {
            synchronized (lock) {
                if (!canWrite()) {
                    return false;
                }
                PrintStream err = target.get();
                if (redraw) {
                    eraseFrame(err);
                    err.print("\r" + line + "\u001B[K");
                    lastFrame = line;
                    framePainted = true;
                } else {
                    err.println(line);
                }
                err.flush();
                if (err.checkError()) {
                    broken = true;
                    // Nothing more will be written to this stream, so nothing may be left owed to
                    // it either: a frame recorded as painted would have the summary path try to
                    // erase it on a stream already known to be gone.
                    framePainted = false;
                    lastFrame = null;
                    return false;
                }
                return true;
            }
        }

        boolean isActive() {
            synchronized (lock) {
                return canWrite();
            }
        }

        /** Whether this channel is still the live one. Callers hold {@link #lock}. */
        private boolean canWrite() {
            return current == this && !broken;
        }

        @Override
        public void close() {
            synchronized (lock) {
                if (current == this) {
                    eraseIfPainted();
                    lastFrame = null;
                    current = null;
                }
            }
        }
    }
}
