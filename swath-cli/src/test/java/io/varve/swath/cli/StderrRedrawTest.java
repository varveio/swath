/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * The in-place redraw, which is the one progress form that leaves an unterminated line on screen.
 * Every case here is about that: what must be erased before someone else writes, what must be put
 * back afterwards, and — the regression these tests exist for — what must never reach a stream that
 * is not a terminal.
 *
 * <p>The plain form's own contract stays in {@link StderrCoordinatorTest}; these are the additions.
 */
class StderrRedrawTest {

    private static final String ERASE = "\r\u001B[K";

    private final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    private final PrintStream stream = new PrintStream(captured, true, StandardCharsets.UTF_8);
    private final StderrCoordinator coordinator = new StderrCoordinator(() -> stream);

    private String written() {
        return captured.toString(StandardCharsets.UTF_8);
    }

    @Test
    void aRedrawnFrameCarriesNoNewlineSoTheNextOneReplacesIt() {
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress(true);

        channel.frame("  listing · 10 objects");
        channel.frame("  listing · 20 objects");

        // Frame one paints; frame two erases it and paints in its place. No newline anywhere: one
        // run of listing progress occupies exactly one terminal row however long it runs.
        assertThat(written()).isEqualTo(
                "\r  listing · 10 objects\u001B[K" + ERASE + "\r  listing · 20 objects\u001B[K");
        assertThat(written()).doesNotContain("\n");
    }

    @Test
    void aPlainChannelEmitsNoControlSequencesAtAll() {
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress(false);

        channel.frame("  listing · 10 objects");
        channel.frame("  listing · 20 objects");

        // The captured-log guarantee: a redirected stderr gets lines, never a carriage return and
        // never an erase. A file of \r is not a log, and this is what keeps it from becoming one.
        assertThat(written()).isEqualTo("  listing · 10 objects\n  listing · 20 objects\n");
        assertThat(written()).doesNotContain("\r").doesNotContain("\u001B");
    }

    @Test
    void aLogEventLandsOnACleanLineAndTheFrameComesBack() {
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress(true);
        channel.frame("  listing · 10 objects");

        coordinator.record(err -> err.println("12:00:00.000 WARN  swath - slow probe"));

        // Erase, the whole record, then the frame repainted: the operator reads the warning on its
        // own line and does not lose sight of a run that is still going.
        assertThat(written()).isEqualTo("\r  listing · 10 objects\u001B[K"
                + ERASE
                + "12:00:00.000 WARN  swath - slow probe\n"
                + "\r  listing · 10 objects\u001B[K");
    }

    @Test
    void aLogEventThroughTheLogbackStreamErasesOnceAndRepaintsOnce() {
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress(true);
        channel.frame("  listing · 10 objects");

        // Logback's appender writes an event as several writes then a flush; the frame must be
        // erased by the first of them and repainted by the flush, not once per write.
        var logStream = coordinator.logStream();
        write(logStream, "12:00:00.000 WARN  ");
        write(logStream, "swath - slow probe\n");
        flush(logStream);

        assertThat(written()).isEqualTo("\r  listing · 10 objects\u001B[K"
                + ERASE
                + "12:00:00.000 WARN  swath - slow probe\n"
                + "\r  listing · 10 objects\u001B[K");
    }

    @Test
    void finishingProgressErasesTheFrameAndLeavesNothingToRepaint() {
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress(true);
        channel.frame("  listing · 10 objects");

        coordinator.finishProgress();
        coordinator.record(err -> err.println("  1,204,993 objects in 4m12s"));

        // The summary is the last word: the frame is erased, and NOT repainted underneath it.
        assertThat(written()).isEqualTo("\r  listing · 10 objects\u001B[K"
                + ERASE
                + "  1,204,993 objects in 4m12s\n");
        assertThat(channel.frame("  listing · 30 objects")).isFalse();
    }

    @Test
    void closingTheChannelErasesTheFrameItLeftOnScreen() {
        StderrCoordinator.ProgressChannel channel = coordinator.openProgress(true);
        channel.frame("  listing · 10 objects");

        channel.close();

        assertThat(written()).isEqualTo("\r  listing · 10 objects\u001B[K" + ERASE);
    }

    @Test
    void aSupersededGenerationLeavesNoFrameForTheNewOneToErase() {
        StderrCoordinator.ProgressChannel first = coordinator.openProgress(true);
        first.frame("  seeding · 1/64 probes (2%)");

        StderrCoordinator.ProgressChannel second = coordinator.openProgress(true);
        second.frame("  listing · 10 objects");

        // Opening a generation erases the outgoing one's frame, so the phase transition does not
        // leave a stale seeding line for the listing frame to paint over half of.
        assertThat(written()).isEqualTo("\r  seeding · 1/64 probes (2%)\u001B[K"
                + ERASE
                + "\r  listing · 10 objects\u001B[K");
        assertThat(first.frame("  seeding · 2/64 probes (3%)")).isFalse();
    }

    @Test
    void aBrokenStreamOwesNoEraseToAStreamThatIsGone() {
        PrintStream broken = new PrintStream(java.io.OutputStream.nullOutputStream()) {
            @Override
            public boolean checkError() {
                return true;
            }
        };
        StderrCoordinator brokenCoordinator = new StderrCoordinator(() -> broken);
        StderrCoordinator.ProgressChannel channel = brokenCoordinator.openProgress(true);

        assertThat(channel.frame("  listing · 10 objects")).isFalse();

        // EPIPE disables the display; what must not survive it is the belief that a frame is still
        // painted, which would have the summary path erase against a dead stream.
        brokenCoordinator.finishProgress();
        assertThat(channel.isActive()).isFalse();
    }

    private static void write(java.io.OutputStream out, String text) {
        try {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void flush(java.io.OutputStream out) {
        try {
            out.flush();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }
}
