/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.cli.AnsiPalette.Mode;
import org.junit.jupiter.api.Test;

/**
 * The {@code --color}/{@code NO_COLOR}/{@code TERM}/{@code CLICOLOR_FORCE} precedence (§4.9), and
 * the palette's dim/accent/red styling and its column-accurate truncation. {@link AnsiPalette#resolveEnabled} takes the raw env
 * values as parameters rather than reading {@code System.getenv} itself, so these are pure,
 * deterministic unit tests that never touch the real process environment.
 */
final class AnsiPaletteTest {

    // ---- resolveEnabled precedence ------------------------------------

    @Test
    void noColorSetToAnyValueDisablesColorUnderAuto() {
        assertThat(AnsiPalette.resolveEnabled(Mode.AUTO, "1", null, null, true)).isFalse();
        assertThat(AnsiPalette.resolveEnabled(Mode.AUTO, "", null, null, true))
                .as("NO_COLOR is a presence check, not a value check -- the spec's own test")
                .isFalse();
    }

    @Test
    void termDumbDisablesColorUnderAuto() {
        assertThat(AnsiPalette.resolveEnabled(Mode.AUTO, null, "dumb", null, true)).isFalse();
    }

    @Test
    void explicitColorAlwaysOverridesNoColor() {
        assertThat(AnsiPalette.resolveEnabled(Mode.ALWAYS, "1", null, null, false))
                .as("an explicit --color=always wins over NO_COLOR -- no-color.org: command-line "
                        + "arguments override the env var")
                .isTrue();
    }

    @Test
    void explicitColorNeverOverridesClicolorForceAndTty() {
        assertThat(AnsiPalette.resolveEnabled(Mode.NEVER, null, null, "1", true)).isFalse();
    }

    @Test
    void clicolorForceEnablesColorOnANonTerminal() {
        assertThat(AnsiPalette.resolveEnabled(Mode.AUTO, null, null, "1", false)).isTrue();
    }

    @Test
    void autoFollowsTheTargetFdWhenNoEnvOverrideApplies() {
        assertThat(AnsiPalette.resolveEnabled(Mode.AUTO, null, null, null, true)).isTrue();
        assertThat(AnsiPalette.resolveEnabled(Mode.AUTO, null, null, null, false)).isFalse();
    }

    @Test
    void alwaysColorsEvenOffATerminal() {
        assertThat(AnsiPalette.resolveEnabled(Mode.ALWAYS, null, null, null, false))
                .as("--color=always is what forcing color on a non-TTY means")
                .isTrue();
    }

    // ---- the palette itself --------------------------------------------

    @Test
    void enabledRendersTheExpectedSgrCodesAndResets() {
        AnsiPalette on = new AnsiPalette(true);

        // Pinned as exact bytes rather than "contains an escape": these three codes are the whole
        // palette, and swath renders them itself rather than consulting a terminfo database, so
        // they must not drift with the rendering library underneath.
        assertThat(on.render(on.dim("x"))).isEqualTo("\u001B[2mx\u001B[0m");
        assertThat(on.render(on.accent("x"))).isEqualTo("\u001B[36mx\u001B[0m");
        assertThat(on.render(on.red("x"))).isEqualTo("\u001B[31mx\u001B[0m");
    }

    @Test
    void disabledRendersTextUnchanged() {
        AnsiPalette off = new AnsiPalette(false);

        assertThat(off.render(off.dim("x"))).isEqualTo("x");
        assertThat(off.render(off.accent("x"))).isEqualTo("x");
        assertThat(off.render(off.red("x"))).isEqualTo("x");
        assertThat(off.render(off.plain("x"))).isEqualTo("x");
    }

    @Test
    void aStyledStringSurvivesBeingCutToAWidth() {
        AnsiPalette on = new AnsiPalette(true);

        // The reason styling and truncation share a representation: cutting the RENDERED string
        // would slice the leading escape and leave the terminal wearing it. Cutting by column
        // re-emits the style and its reset around whatever survived.
        String cut = on.render(on.dim("listing is long").columnSubSequence(0, 7));

        assertThat(cut).isEqualTo("\u001B[2mlisting\u001B[0m");
    }

    @Test
    void widthIsMeasuredInDisplayColumnsNotCharacters() {
        // The defect this forecloses. A frame bounded by String.length() believes this text is 6
        // wide when it occupies 9 terminal columns, so it under-truncates, the line wraps, and the
        // erase that follows reaches only one of the two rows it now occupies.
        assertThat(new AnsiPalette(false).plain("日本語 ok").columnLength()).isEqualTo(9);
    }
}
