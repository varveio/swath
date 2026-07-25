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

    private static final List<String> PARTS =
            List.of("listing", "1,204,993 objects", "4,781 keys/s", "1,208 API calls");

    private static ProgressDisplay.Preferences prefs(Boolean progress, boolean stderrIsTerminal) {
        return new ProgressDisplay.Preferences(progress, false, false, false, stderrIsTerminal);
    }

    @Test
    void aTerminalWideEnoughToSaySoRedraws() {
        assertThat(ProgressDisplay.shouldRedraw(prefs(null, true), WIDE)).isTrue();
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

    @Test
    void aFrameTooWideLosesWholeFieldsAndNeverHalfOfOne() {
        // 39 columns fits the phase and the object count, and not the rate. The dropped field goes
        // whole: "4,781 key" is the frame that looks broken, and is what this forecloses.
        String frame = ProgressDisplay.fit(PARTS, 39);

        assertThat(frame).isEqualTo("  listing · 1,204,993 objects");
        assertThat(frame.length()).isLessThanOrEqualTo(39);
    }

    @Test
    void aFrameThatFitsKeepsEveryField() {
        String frame = ProgressDisplay.fit(PARTS, 120);

        assertThat(frame).isEqualTo(
                "  listing · 1,204,993 objects · 4,781 keys/s · 1,208 API calls");
        assertThat(ProgressDisplay.fit(PARTS, frame.length())).isEqualTo(frame);
    }

    @Test
    void aTerminalNarrowerThanTheFirstFieldFallsBackToACut() {
        // The one case a field cannot be dropped to solve: something must still bound the line, or
        // it wraps and the erase strands a row.
        String frame = ProgressDisplay.fit(PARTS, 6);

        assertThat(frame).hasSize(6).isEqualTo("  list");
    }

    @Test
    void anUnknownWidthBoundsNothingRatherThanBoundingToNothing() {
        // The failure this forecloses: treating UNKNOWN (-1) as a width would cut every frame to
        // nothing, turning "we cannot measure" into "print nothing at all".
        assertThat(ProgressDisplay.fit(PARTS, TerminalGeometry.UNKNOWN))
                .isEqualTo("  listing · 1,204,993 objects · 4,781 keys/s · 1,208 API calls");
    }
}
