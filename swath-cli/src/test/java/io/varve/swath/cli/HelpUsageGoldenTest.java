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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.OptionSpec;

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

    @Test
    void tuneIsVisibleWhileDiagnosticSurfaceIsHiddenButRegistered() {
        CommandLine root = App.commandLine();
        CommandLine list = root.getSubcommands().get("list");
        CommandLine resume = root.getSubcommands().get("resume");

        assertThat(root.getSubcommands().get("dump-run").getCommandSpec().usageMessage().hidden())
                .isTrue();
        assertThat(list.getCommandSpec().findOption("--engine-toggle").hidden()).isTrue();
        assertThat(list.getCommandSpec().findOption("--tune").hidden()).isFalse();
        assertThat(resume.getCommandSpec().findOption("--tune").hidden()).isFalse();
    }

    @Test
    void everyVisibleOptionIsNamedOnTheCliSurfacePage() throws IOException {
        CommandLine root = App.commandLine();
        Set<String> visible = new TreeSet<>();
        for (CommandLine command : List.of(
                root.getSubcommands().get("list"), root.getSubcommands().get("resume"))) {
            for (OptionSpec option : command.getCommandSpec().options()) {
                if (!option.hidden()) {
                    String name = Arrays.stream(option.names())
                            .filter(candidate -> candidate.startsWith("--"))
                            .findFirst()
                            .orElse(option.names()[0]);
                    visible.add(name);
                }
            }
        }

        Path doc = Path.of("..", "docs", "cli.md");
        if (!Files.exists(doc)) {
            doc = Path.of("docs", "cli.md");
        }
        String documented = Files.readString(doc, StandardCharsets.UTF_8);

        assertThat(visible).hasSize(49);
        for (String name : visible) {
            assertThat(documented).as("documented visible option %s", name)
                    .contains("`" + name + "`");
        }
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
