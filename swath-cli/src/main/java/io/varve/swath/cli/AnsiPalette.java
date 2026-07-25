/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import org.jline.utils.AttributedCharSequence;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;

/**
 * swath's operator palette, and the {@code --color}/{@code NO_COLOR}/{@code TERM}/{@code
 * CLICOLOR_FORCE} precedence that decides whether it is ever emitted (spec §4.9). The palette is
 * deliberately small: dim for labels and secondary lines, one accent for the headline figure, red
 * for {@code INCOMPLETE}. Nothing else, and no 256-colour or truecolour codes.
 *
 * <p><b>Styles, not escape codes.</b> Text is styled as an {@link AttributedString} and rendered to
 * ANSI only at the last moment, by {@link #render}. That indirection is not decoration: the live
 * progress frame must be cut to the terminal's width, and cutting a {@code String} that already
 * contains escape sequences can slice one in half and leave the terminal wearing a colour — or
 * worse, an incomplete control sequence — for the rest of the session. {@link
 * AttributedCharSequence#columnSubSequence} cuts by display column with the styles held separately,
 * so a truncated frame is still a well-formed one. Styling and truncation have to share a
 * representation to be safe, and this is it.
 *
 * <p>Colour resolution stays swath's own: JLine renders, it does not decide. {@link #render} emits
 * plain text when {@link #resolveEnabled} said no, so a caller never branches on colour itself and
 * no surface can forget to.
 */
final class AnsiPalette {

    /** {@code --color}'s accepted values. */
    enum Mode { AUTO, ALWAYS, NEVER }

    private static final AttributedStyle DIM = AttributedStyle.DEFAULT.faint();
    private static final AttributedStyle ACCENT =
            AttributedStyle.DEFAULT.foreground(AttributedStyle.CYAN);
    private static final AttributedStyle RED =
            AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);

    private final boolean enabled;

    AnsiPalette(boolean enabled) {
        this.enabled = enabled;
    }

    boolean enabled() {
        return enabled;
    }

    /** Labels, units and secondary lines — de-emphasized so the figures they describe read first. */
    AttributedString dim(String text) {
        return new AttributedString(text, DIM);
    }

    /** The headline figures — the one accent colour the palette allows. */
    AttributedString accent(String text) {
        return new AttributedString(text, ACCENT);
    }

    /** The {@code INCOMPLETE} marker only. */
    AttributedString red(String text) {
        return new AttributedString(text, RED);
    }

    /** Unstyled text that still goes through {@link #render}, so every path is uniform. */
    AttributedString plain(String text) {
        return new AttributedString(text);
    }

    /**
     * The last step before the bytes reach stderr: ANSI when this palette is enabled, and the bare
     * characters when it is not. {@link AttributedCharSequence#toAnsi()} is asked for no terminal,
     * so it emits the plain SGR codes this palette's four styles need and consults no terminfo
     * database — swath already decided, and a capability lookup could only disagree with it.
     */
    String render(AttributedCharSequence styled) {
        return enabled ? styled.toAnsi() : styled.toString();
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
