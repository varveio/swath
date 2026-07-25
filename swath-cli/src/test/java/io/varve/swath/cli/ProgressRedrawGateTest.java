/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;

/**
 * Which runs redraw, and how a frame is bounded once they do. The gate is deliberately narrower
 * than the one deciding whether progress appears at all, and these pin the difference.
 */
class ProgressRedrawGateTest {

    private static final IntSupplier WIDE = () -> 120;
    private static final IntSupplier UNKNOWN = () -> TerminalGeometry.UNKNOWN;

    /**
     * A capable terminal, stated rather than inherited: the gate reads {@code TERM} off the
     * {@link ProgressDisplay.Preferences} it is given, so these assertions hold on a developer
     * machine and a CI image alike — including one that exports {@code TERM=dumb}.
     */
    private static final String XTERM = "xterm-256color";

    private static final List<String> PARTS =
            List.of("listing", "1,204,993 objects", "4,781 keys/s", "1,208 API calls");

    private static ProgressDisplay.Preferences prefs(Boolean progress, boolean stderrIsTerminal) {
        return new ProgressDisplay.Preferences(progress, false, false, false, stderrIsTerminal, XTERM);
    }

    @Test
    void aTerminalWideEnoughToSaySoRedraws() {
        assertThat(ProgressDisplay.shouldRedraw(prefs(null, true), WIDE)).isTrue();
    }

    @Test
    void aTerminalThatDisclaimsCapabilityKeepsThePlainRecords() {
        // TERM=dumb is taken at its word, on the same reasoning --color=auto uses: a terminal that
        // says it cannot act on control sequences is not sent any. Width is irrelevant here — a
        // dumb terminal wide enough to redraw into still must not be redrawn into.
        assertThat(ProgressDisplay.shouldRedraw(
                new ProgressDisplay.Preferences(null, false, false, false, true, "dumb"), WIDE))
                .isFalse();
    }

    @Test
    void anUnsetTermIsNotTreatedAsDumb() {
        // Absent is not a disclaimer: only the literal "dumb" disables the redraw, so a terminal
        // that simply never exported TERM keeps the display it is otherwise entitled to.
        assertThat(ProgressDisplay.shouldRedraw(
                new ProgressDisplay.Preferences(null, false, false, false, true, null), WIDE))
                .isTrue();
    }

    @Test
    void aRedirectedStderrNeverRedrawsEvenWhenProgressIsForcedOn() {
        // --progress forces progress to APPEAR off a terminal; it does not force control sequences
        // into a file. This is the captured-log guarantee at the gate rather than at the writer.
        assertThat(ProgressDisplay.shouldRedraw(prefs(true, false), WIDE)).isFalse();
    }

    @Test
    void aTerminalThatWillNotReportItsWidthKeepsThePlainRecords() {
        // No provider, or one that stopped answering: an erase whose reach cannot be predicted is
        // worse than a line that scrolls.
        assertThat(ProgressDisplay.shouldRedraw(prefs(null, true), UNKNOWN)).isFalse();
    }

    @Test
    void aTerminalTooNarrowToSayAnythingUsefulKeepsThePlainRecords() {
        assertThat(ProgressDisplay.shouldRedraw(prefs(null, true),
                () -> TerminalGeometry.MIN_USABLE_WIDTH - 1)).isFalse();
        assertThat(ProgressDisplay.shouldRedraw(prefs(null, true),
                () -> TerminalGeometry.MIN_USABLE_WIDTH)).isTrue();
    }

    private static final AnsiPalette PLAIN = new AnsiPalette(false);

    /** The frame as the operator sees it, colour off so these pin content and width only. */
    private static String render(java.util.List<String> parts, int width) {
        return PLAIN.render(ProgressDisplay.frame(parts, width, PLAIN));
    }

    @Test
    void aFrameTooWideLosesWholeFieldsAndNeverHalfOfOne() {
        // 39 columns fits the phase and the object count, and not the rate. The dropped field goes
        // whole: "4,781 key" is the frame that looks broken, and is what this forecloses.
        String frame = render(PARTS, 39);

        assertThat(frame).isEqualTo("  listing · 1,204,993 objects");
        assertThat(frame.length()).isLessThanOrEqualTo(39);
    }

    @Test
    void aFrameThatFitsKeepsEveryField() {
        String frame = render(PARTS, 120);

        assertThat(frame).isEqualTo(
                "  listing · 1,204,993 objects · 4,781 keys/s · 1,208 API calls");
        assertThat(render(PARTS, frame.length())).isEqualTo(frame);
    }

    @Test
    void aTerminalNarrowerThanTheFirstFieldFallsBackToACut() {
        // The one case a field cannot be dropped to solve: something must still bound the line, or
        // it wraps and the erase strands a row.
        String frame = render(PARTS, 6);

        assertThat(frame).hasSize(6).isEqualTo("  list");
    }

    @Test
    void aFieldOfWideCharactersIsBoundedByColumnsNotCharacters() {
        // "日本語 objects" is 11 characters but 14 display columns, so with the 9-column indent and
        // phase plus a 3-column separator the frame needs 26. Bounded by String.length() it would
        // measure 23, fit at a width it actually overruns, wrap, and strand a row past the reach of
        // the erase -- the exact failure the redraw exists to prevent. No field carries text like
        // this today; the bound has to hold for the one that eventually does.
        List<String> wide = List.of("listing", "日本語 objects", "4,781 keys/s");

        assertThat(render(wide, 26)).isEqualTo("  listing · 日本語 objects");
        assertThat(render(wide, 25))
                .as("one column short is one column short, however few characters that is")
                .isEqualTo("  listing");
    }

    @Test
    void aStyledFrameCarriesOnlyWellFormedEscapes() {
        AnsiPalette colour = new AnsiPalette(true);

        String frame = colour.render(ProgressDisplay.frame(PARTS, 39, colour));

        // Strip every COMPLETE SGR sequence; anything left is a half-written one, which would
        // leave the terminal styled for the rest of the session. This is what cutting the rendered
        // string -- rather than the styled representation -- would produce.
        assertThat(frame.replaceAll("\u001B\\[[0-9;]*m", ""))
                .doesNotContain("\u001B");
        assertThat(frame).endsWith("\u001B[0m");
    }

    @Test
    void anUnknownWidthBoundsNothingRatherThanBoundingToNothing() {
        // The failure this forecloses: treating UNKNOWN (-1) as a width would cut every frame to
        // nothing, turning "we cannot measure" into "print nothing at all".
        assertThat(render(PARTS, TerminalGeometry.UNKNOWN))
                .isEqualTo("  listing · 1,204,993 objects · 4,781 keys/s · 1,208 API calls");
    }
}
