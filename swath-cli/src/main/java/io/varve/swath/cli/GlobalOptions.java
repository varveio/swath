/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * The truly-global flags: {@code -v}/{@code -q}, accepted BEFORE or
 * AFTER the verb ({@code swath -v list …} and {@code swath list -v …} both work), the same way
 * {@code gh}/{@code kubectl} do it. One mixin, applied to the root {@link App} AND to every
 * subcommand that accepts it — each occurrence is its own independent picocli option, so {@link
 * #effectiveVerbosity} merges whichever level(s) the user actually populated.
 *
 * <p>{@code -q} has one real consumer today (suppresses {@link OutputOptions#echoResolvedOutput}'s
 * startup destination line) and will grow more as a stderr run summary lands.
 */
final class GlobalOptions {

    @Resume(ResumeClass.FREE)
    @Option(names = "-v", description = "Increase verbosity (-v INFO, -vv DEBUG, -vvv TRACE).")
    boolean[] verbosity = new boolean[0];

    @Resume(ResumeClass.FREE)
    @Option(names = {"-q", "--quiet"}, description = "Suppress the startup destination echo.")
    boolean quiet;

    /** A command whose CLI surface carries a {@link GlobalOptions} mixin. */
    interface Carrier {
        GlobalOptions globalOptions();
    }

    /**
     * The effective verbosity level across every level of the command line that could carry it —
     * a subcommand's own mixin instance if the user placed {@code -v} after the verb, OR the
     * root's if placed before it. At most one is ever actually populated per invocation; take the
     * max so either placement works identically.
     */
    static int effectiveVerbosity(CommandLine cmd) {
        int leaf = verbosityOf(cmd.getCommand());
        CommandLine root = rootOf(cmd);
        int rootLevel = verbosityOf(root.getCommand());
        return Math.max(leaf, rootLevel);
    }

    /**
     * The effective {@code -q}/{@code --quiet} across every level of the command line that could
     * carry it — a root-level {@code swath -q list …} must suppress the same things a leaf-level
     * {@code swath list -q …} does (the startup destination echo used to consult the leaf mixin
     * only, so a root {@code -q} was silently ignored).
     */
    static boolean effectiveQuiet(CommandLine cmd) {
        return quietOf(cmd.getCommand()) || quietOf(rootOf(cmd).getCommand());
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

    private static boolean quietOf(Object command) {
        return command instanceof Carrier carrier && carrier.globalOptions().quiet;
    }
}
