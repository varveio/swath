/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;

/**
 * Characterization goldens for {@code swath} usage/help text. They pin the deliberate public
 * option names and sectioned {@link picocli.CommandLine.ArgGroup} layout so later internal moves
 * cannot silently alter the public surface.
 *
 * <p>The golden text is the exact {@link CommandLine#getUsageMessage(Ansi)} output (ANSI off,
 * default width). Regenerate intentionally — after a deliberate help change, and only then —
 * with {@code -Dswath.goldens.update=true}, and review the resulting diff.
 */
class HelpUsageGoldenTest {

    private static final boolean UPDATE = Boolean.getBoolean("swath.goldens.update");

    @Test
    void rootUsageMatchesGolden() {
        assertUsageMatchesGolden(App.commandLine(), "swath.txt");
    }

    @Test
    void listUsageMatchesGolden() {
        assertUsageMatchesGolden(App.commandLine().getSubcommands().get("list"), "swath-list.txt");
    }

    @Test
    void resumeUsageMatchesGolden() {
        assertUsageMatchesGolden(App.commandLine().getSubcommands().get("resume"), "swath-resume.txt");
    }

    private static void assertUsageMatchesGolden(CommandLine command, String resource) {
        String actual = command.getUsageMessage(Ansi.OFF);
        if (UPDATE) {
            Path golden = Path.of("src/test/resources/help", resource);
            try {
                Files.createDirectories(golden.getParent());
                Files.writeString(golden, actual, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }
        assertThat(actual).isEqualTo(readGolden(resource));
    }

    private static String readGolden(String resource) {
        try (InputStream in = HelpUsageGoldenTest.class.getResourceAsStream("/help/" + resource)) {
            if (in == null) {
                throw new IllegalStateException("missing golden /help/" + resource
                        + " (generate with -Dswath.goldens.update=true)");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
