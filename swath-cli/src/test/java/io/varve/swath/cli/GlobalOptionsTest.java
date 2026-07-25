/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Pins: {@code -v}/{@code -q} are accepted BEFORE or AFTER the verb —
 * {@code swath -v list …} and {@code swath list -v …} both work, and {@link
 * GlobalOptions#effectiveVerbosity} sees whichever placement the user actually used.
 */
class GlobalOptionsTest {

    @Test
    void verbosityBeforeTheVerbIsVisibleOnTheRootCommand() {
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("-v", "list", "s3://bucket/prefix");
        CommandLine list = cmd.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveVerbosity(list)).isEqualTo(1);
    }

    @Test
    void verbosityAfterTheVerbIsAlsoVisible() {
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("list", "s3://bucket/prefix", "-v");
        CommandLine list = cmd.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveVerbosity(list)).isEqualTo(1);
    }

    @Test
    void repeatedVFlagsStack() {
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("list", "s3://bucket/prefix", "-vvv");
        CommandLine list = cmd.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveVerbosity(list)).isEqualTo(3);
    }

    @Test
    void noVerbosityFlagIsZero() {
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("list", "s3://bucket/prefix");
        CommandLine list = cmd.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveVerbosity(list)).isZero();
    }

    @Test
    void quietParsesOnEitherSideOfTheVerbToo() {
        CommandLine before = App.commandLine();
        before.parseArgs("-q", "list", "s3://bucket/prefix");
        CommandLine beforeList = before.getSubcommands().get("list");
        ListCommand listBefore = (ListCommand) beforeList.getCommand();
        assertThat(listBefore.global.quiet).isEmpty();   // set on the ROOT App's mixin, not list's own
        assertThat(GlobalOptions.effectiveQuietLevel(beforeList)).isEqualTo(1);

        CommandLine after = App.commandLine();
        after.parseArgs("list", "s3://bucket/prefix", "-q");
        ListCommand listAfter = (ListCommand) after.getSubcommands().get("list").getCommand();
        assertThat(listAfter.globalOptions().quiet).hasSize(1);
    }

    @Test
    void repeatedQFlagsStack() {
        CommandLine longForm = App.commandLine();
        longForm.parseArgs("list", "s3://bucket/prefix", "--quiet", "--quiet");
        CommandLine longList = longForm.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveQuietLevel(longList)).isEqualTo(2);

        CommandLine shortForm = App.commandLine();
        shortForm.parseArgs("list", "s3://bucket/prefix", "-qq");
        CommandLine shortList = shortForm.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveQuietLevel(shortList)).isEqualTo(2);
    }

    @Test
    void quietSplitAcrossTheVerbStacksLikeARepeatedFlag() {
        // Both sides of the verb accept -q, so both mixins really are populated in one invocation:
        // the two occurrences must add up to -qq (OFF), not collapse to a single ERROR level.
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("-q", "list", "s3://bucket/prefix", "-q");
        CommandLine list = cmd.getSubcommands().get("list");
        ListCommand leaf = (ListCommand) list.getCommand();
        App root = (App) cmd.getCommand();

        assertThat(leaf.globalOptions().quiet).hasSize(1);
        assertThat(root.globalOptions().quiet).hasSize(1);
        assertThat(GlobalOptions.effectiveQuietLevel(list)).isEqualTo(2);
    }

    @Test
    void verbositySplitAcrossTheVerbStacksLikeARepeatedFlag() {
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("-v", "list", "s3://bucket/prefix", "-v");
        CommandLine list = cmd.getSubcommands().get("list");
        ListCommand leaf = (ListCommand) list.getCommand();
        App root = (App) cmd.getCommand();

        assertThat(leaf.globalOptions().verbosity).hasSize(1);
        assertThat(root.globalOptions().verbosity).hasSize(1);
        assertThat(GlobalOptions.effectiveVerbosity(list)).isEqualTo(2);   // DEBUG, as `-vv` would
    }

    @Test
    void noQuietFlagIsZero() {
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("list", "s3://bucket/prefix");
        CommandLine list = cmd.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveQuietLevel(list)).isZero();
    }

    @Test
    void colorBeforeOrAfterTheVerbResolvesIdentically() {
        CommandLine before = App.commandLine();
        before.parseArgs("--color=never", "list", "s3://bucket/prefix");
        CommandLine beforeList = before.getSubcommands().get("list");

        CommandLine after = App.commandLine();
        after.parseArgs("list", "s3://bucket/prefix", "--color=never");
        CommandLine afterList = after.getSubcommands().get("list");

        assertThat(GlobalOptions.effectiveColor(beforeList)).isEqualTo(AnsiPalette.Mode.NEVER);
        assertThat(GlobalOptions.effectiveColor(afterList))
                .isEqualTo(GlobalOptions.effectiveColor(beforeList));
    }

    @Test
    void noColorFlagIsAuto() {
        CommandLine cmd = App.commandLine();
        cmd.parseArgs("list", "s3://bucket/prefix");
        CommandLine list = cmd.getSubcommands().get("list");
        assertThat(GlobalOptions.effectiveColor(list)).isEqualTo(AnsiPalette.Mode.AUTO);
    }

    @Test
    void verbosityConfiguresTheSwathLogbackLogger() {
        Logger logger = (Logger)
                LoggerFactory.getLogger("io.varve.swath");
        Level originalLevel = logger.getLevel();
        try {
            CommandLine cmd = App.commandLine();
            cmd.execute("-vv", "--help");

            assertThat(logger.getLevel()).isEqualTo(Level.DEBUG);
        } finally {
            logger.setLevel(originalLevel);
        }
    }
}
