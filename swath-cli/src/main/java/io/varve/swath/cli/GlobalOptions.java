/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.util.function.ToIntFunction;
import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * The truly-global flags: {@code -v}/{@code -q}/{@code --color}, accepted BEFORE or
 * AFTER the verb ({@code swath -v list …} and {@code swath list -v …} both work), the same way
 * {@code gh}/{@code kubectl} do it. One mixin, applied to the root {@link App} AND to every
 * subcommand that accepts it — each occurrence is its own independent picocli option, so {@link
 * #effectiveVerbosity} merges whichever level(s) the user actually populated.
 *
 * <p>{@code -q} lowers the log level and suppresses {@link OutputOptions#echoResolvedOutput}'s startup
 * destination line: a single {@code -q} drops the level to ERROR, {@code -qq} (or higher) turns
 * logging off entirely. Quiet wins over verbosity when both are given — see {@link
 * CliLogging#configure}.
 *
 * <p>{@code --color} governs only the {@link SummaryRenderer} block on stderr; unlike {@code
 * --stats} (list-scoped, on {@link OutputOptions}) it is a global flag because it is not tied to
 * listing at all — the archive spec's D-P groups it with {@code -v}/{@code -q}.
 */
final class GlobalOptions {

    @Resume(ResumeClass.FREE)
    @Option(names = "-v", description = "Increase verbosity (-v INFO, -vv DEBUG, -vvv TRACE).")
    boolean[] verbosity = new boolean[0];

    @Resume(ResumeClass.FREE)
    @Option(names = {"-q", "--quiet"},
            description = "Decrease verbosity (-q ERROR, -qq silent); suppresses the startup "
                    + "destination echo.")
    boolean[] quiet = new boolean[0];

    @Resume(ResumeClass.FREE)
    @Option(names = "--color", paramLabel = "MODE",
            description = "Color the end-of-run summary: auto, always, or never (default: auto).")
    AnsiPalette.Mode color = AnsiPalette.Mode.AUTO;

    /** A command whose CLI surface carries a {@link GlobalOptions} mixin. */
    interface Carrier {
        GlobalOptions globalOptions();
    }

    /**
     * The effective verbosity level: {@code -v} placed after the verb, before it, or both.
     * See {@link #mergedLevel}.
     */
    static int effectiveVerbosity(CommandLine cmd) {
        return mergedLevel(cmd, GlobalOptions::verbosityOf);
    }

    /**
     * The effective {@code -q}/{@code --quiet} level — a root-level {@code swath -q list …} must
     * suppress the same things a leaf-level {@code swath list -q …} does (the startup destination
     * echo used to consult the leaf mixin only, so a root {@code -q} was silently ignored). See
     * {@link #mergedLevel}.
     */
    static int effectiveQuietLevel(CommandLine cmd) {
        return mergedLevel(cmd, GlobalOptions::quietOf);
    }

    /**
     * One repeatable global counter, merged across the levels of the command line that can carry
     * it — the leaf's own mixin instance for a flag placed after the verb, the root's for one
     * placed before it.
     *
     * <p>The occurrences are SUMMED, not maxed. Both placements are accepted in the same
     * invocation and picocli populates each level's mixin independently, so {@code swath -q list
     * … -q} really does populate both with one each: it asks for two levels of quiet and must turn
     * logging off, exactly as {@code swath list … -qq} does. Taking the max would silently discard
     * one of the two occurrences. A {@code cmd} that IS the root ({@code swath -vv --help}) has a
     * single mixin to read and is counted once, never doubled.
     */
    private static int mergedLevel(CommandLine cmd, ToIntFunction<Object> level) {
        CommandLine root = rootOf(cmd);
        int leaf = level.applyAsInt(cmd.getCommand());
        return cmd == root ? leaf : leaf + level.applyAsInt(root.getCommand());
    }

    /**
     * The effective {@code --color} mode across every level of the command line that could carry
     * it, mirroring {@link #effectiveVerbosity}/{@link #effectiveQuietLevel}. Unlike those two
     * (repeatable counters, where "unset" and "the default" are the same zero), {@code --color}
     * is single-valued with {@link AnsiPalette.Mode#AUTO} as both its default and its own explicit
     * spelling — so a leaf value other than {@code AUTO} is exactly the one the user actually
     * passed (picocli only populates the mixin instance at the level the flag was placed), and it
     * wins; otherwise the root's value stands, whether that is an explicit root-level flag or
     * root's own untouched default.
     */
    static AnsiPalette.Mode effectiveColor(CommandLine cmd) {
        AnsiPalette.Mode leaf = colorOf(cmd.getCommand());
        CommandLine root = rootOf(cmd);
        AnsiPalette.Mode rootColor = colorOf(root.getCommand());
        return leaf != AnsiPalette.Mode.AUTO ? leaf : rootColor;
    }

    private static CommandLine rootOf(CommandLine cmd) {
        CommandLine root = cmd;
        while (root.getParent() != null) {
            root = root.getParent();
        }
        return root;
    }

    private static int verbosityOf(Object command) {
        return command instanceof Carrier carrier ? carrier.globalOptions().verbosity.length : 0;
    }

    private static int quietOf(Object command) {
        return command instanceof Carrier carrier ? carrier.globalOptions().quiet.length : 0;
    }

    private static AnsiPalette.Mode colorOf(Object command) {
        return command instanceof Carrier carrier ? carrier.globalOptions().color : AnsiPalette.Mode.AUTO;
    }
}
