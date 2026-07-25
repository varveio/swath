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
 * the palette's dim/accent/red wrapping. {@link AnsiPalette#resolveEnabled} takes the raw env
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
    void enabledWrapsWithTheExpectedSgrCodesAndResets() {
        AnsiPalette on = new AnsiPalette(true);

        assertThat(on.dim("x")).isEqualTo("\u001B[2mx\u001B[0m");
        assertThat(on.accent("x")).isEqualTo("\u001B[36mx\u001B[0m");
        assertThat(on.red("x")).isEqualTo("\u001B[31mx\u001B[0m");
    }

    @Test
    void disabledReturnsTextUnchanged() {
        AnsiPalette off = new AnsiPalette(false);

        assertThat(off.dim("x")).isEqualTo("x");
        assertThat(off.accent("x")).isEqualTo("x");
        assertThat(off.red("x")).isEqualTo("x");
    }
}
