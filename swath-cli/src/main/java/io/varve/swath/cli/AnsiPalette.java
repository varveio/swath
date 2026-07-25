/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

/**
 * The one place swath's SGR escape codes live, plus the {@code --color}/{@code NO_COLOR}/
 * {@code TERM}/{@code CLICOLOR_FORCE} precedence that decides whether they're ever emitted (spec
 * §4.9). The palette is deliberately small: dim for labels/units, one accent for the headline
 * figures, red for {@code INCOMPLETE}. Nothing else, and no 256-colour or truecolour codes.
 */
final class AnsiPalette {

    /** {@code --color}'s accepted values. */
    enum Mode { AUTO, ALWAYS, NEVER }

    private static final String RESET = "\u001B[0m";
    private static final String DIM = "\u001B[2m";
    private static final String ACCENT = "\u001B[36m";   // cyan: the block's one accent
    private static final String RED = "\u001B[31m";

    private final boolean enabled;

    AnsiPalette(boolean enabled) {
        this.enabled = enabled;
    }

    boolean enabled() {
        return enabled;
    }

    /** Labels and units — de-emphasized so the figures they describe read first. */
    String dim(String text) {
        return wrap(DIM, text);
    }

    /** The headline figures — the one accent color the palette allows. */
    String accent(String text) {
        return wrap(ACCENT, text);
    }

    /** The {@code INCOMPLETE} marker only. */
    String red(String text) {
        return wrap(RED, text);
    }

    private String wrap(String code, String text) {
        return enabled ? code + text + RESET : text;
    }

    /**
     * Resolve whether color is on, in precedence order (§4.9):
     *
     * <ol>
     *   <li>an explicit {@code --color=always}/{@code --color=never} wins over everything,
     *       including {@code NO_COLOR} — the no-color.org spec is explicit that command-line
     *       arguments override the env var;</li>
     *   <li>otherwise {@code NO_COLOR} (set to any value at all — the spec's own test) or
     *       {@code TERM=dumb} force it off;</li>
     *   <li>otherwise {@code CLICOLOR_FORCE} (any value; gh's convention, not {@code
     *       FORCE_COLOR} — that's JS-ecosystem, with no CLI-native precedent) forces it on;</li>
     *   <li>otherwise {@code auto} = color only when the target fd is a terminal.</li>
     * </ol>
     *
     * <p>Takes the raw env values as explicit parameters — mirroring {@code
     * MeterRegistries#resolve} — so this is a pure, directly unit-testable function rather than
     * one that reaches into {@code System.getenv} itself.
     *
     * @param mode the parsed {@code --color} value; {@link Mode#AUTO} when the flag was not
     *         passed (auto is also its own explicit spelling, and behaves identically either way)
     * @param noColorEnv the raw {@code NO_COLOR} value, or {@code null} if unset
     * @param termEnv the raw {@code TERM} value, or {@code null} if unset
     * @param clicolorForceEnv the raw {@code CLICOLOR_FORCE} value, or {@code null} if unset
     * @param fdIsTerminal whether the target fd (stderr, for the summary block) is a terminal
     */
    static boolean resolveEnabled(Mode mode, String noColorEnv, String termEnv,
            String clicolorForceEnv, boolean fdIsTerminal) {
        if (mode == Mode.ALWAYS) {
            return true;
        }
        if (mode == Mode.NEVER) {
            return false;
        }
        if (noColorEnv != null || "dumb".equals(termEnv)) {
            return false;
        }
        if (clicolorForceEnv != null) {
            return true;
        }
        return fdIsTerminal;
    }
}
