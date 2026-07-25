/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.error.InvalidArgsException;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * {@code --engine-toggle} startup validation through the real
 * picocli parse path — an unknown name, a malformed value, or a contradictory combination must fail
 * fast at exit 2, before any network/checkpoint work (mirrors {@code
 * MaxParallelListingsValidationTest}'s idiom). {@link io.varve.swath.engine.EngineTogglesParseTest}
 * covers the parse contract itself in detail; this pins the CLI wiring
 * ({@link ListCommand#resolveEngineToggles()}) end to end.
 */
class EngineToggleCliValidationTest {

    /**
     * {@code --checkpoint none} keeps this test offline (no SQLite/S3 needed) while {@code
     * resolveEngineToggles()} is still reached first, in {@code call()}'s pure-validation prologue
     * — see {@link #resolveEngineTogglesFailsBeforeAnyCheckpointOrNetworkWork} for the
     * complementary check that the default ({@code --checkpoint auto}) path also fails this early.
     */
    private static ListCommand parsed(String... extraArgs) {
        ListCommand cmd = new ListCommand();
        String[] args = new String[extraArgs.length + 3];
        args[0] = "s3://bucket/prefix";
        args[1] = "--checkpoint";
        args[2] = "none";
        System.arraycopy(extraArgs, 0, args, 3, extraArgs.length);
        new CommandLine(cmd).parseArgs(args);
        cmd.syncArgGroups();
        return cmd;
    }

    @Test
    void defaultsToAllToggles() throws Exception {
        ListCommand cmd = parsed();
        assertThat(cmd.engine.resolveToggles()).isEqualTo(EngineToggles.DEFAULT);
    }

    @Test
    void singleToggleParsesThroughTheRealCommandLine() throws Exception {
        ListCommand cmd = parsed("--engine-toggle", "radix_bands=off");
        assertThat(cmd.engine.resolveToggles().radixBands()).isFalse();
    }

    @Test
    void repeatableAccumulatesThroughTheRealCommandLine() throws Exception {
        ListCommand cmd = parsed("--engine-toggle", "structure_probes=off",
                "--engine-toggle", "far_ahead=off");
        EngineToggles toggles = cmd.engine.resolveToggles();
        assertThat(toggles.structureProbes()).isFalse();
        assertThat(toggles.farAhead()).isFalse();
        assertThat(toggles.densityEwma()).isTrue();
    }

    @Test
    void removedOwnerSplitAliasIsRejected() {
        ListCommand cmd = new ListCommand();
        assertThatThrownBy(() -> new CommandLine(cmd).parseArgs(
                "s3://bucket/prefix", "--no-owner-split"))
                .isInstanceOf(CommandLine.UnmatchedArgumentException.class);
    }

    @Test
    void unknownToggleNameFailsAtCall() {
        ListCommand cmd = parsed("--engine-toggle", "not_a_real_toggle=off");
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("not_a_real_toggle");
    }

    @Test
    void malformedValueFailsAtCall() {
        ListCommand cmd = parsed("--engine-toggle", "owner_split=maybe");
        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("on|off");
    }

    @Test
    void resolveEngineTogglesFailsBeforeAnyCheckpointOrNetworkWork() {
        // --checkpoint auto (default) would open a SQLite DB and build an S3Config before ever
        // touching the seed/engine — proving the bad toggle is rejected during call()'s pure
        // validation prologue (like validateSummaryJsonFlags), not after those side effects.
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--engine-toggle", "bogus=off");
        assertThatThrownBy(cmd::call).isInstanceOf(InvalidArgsException.class);
    }
}
