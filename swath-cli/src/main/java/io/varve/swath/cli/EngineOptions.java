/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.SeedMode;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.observability.TraceSink;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * WorkStealingScan engine controls: the seed source, diagnostic ablation toggles, and the
 * {@code --trace} flight recorder. The resolved {@link
 * EngineToggles} namespace is computed once (fail-fast) and read by the command's downstream call
 * sites.
 */
final class EngineOptions {

    String seed = "shallow";

    boolean noOwnerSplit;

    List<String> engineToggle;

    String trace;

    /**
     * The resolved {@code --engine-toggle} namespace, set once by {@link
     * #resolveToggles()} at the top of the run and read by every downstream call site instead of
     * re-parsing {@link #engineToggle}/{@link #noOwnerSplit}.
     */
    EngineToggles toggles = EngineToggles.DEFAULT;

    /**
     * Resolve the diagnostic engine-ablation namespace.
     * Pure validation (no I/O) — run before any network/store work so a bad/unknown/contradictory
     * toggle fails fast at exit 2 rather than after a checkpoint DB or S3 client is open.
     */
    EngineToggles resolveToggles() throws InvalidArgsException {
        return EngineToggles.parse(engineToggle, noOwnerSplit);
    }

    SeedMode resolveSeedMode() throws InvalidArgsException {
        String s = seed == null ? "shallow" : seed.trim().toLowerCase(Locale.ROOT);
        return switch (s) {
            case "shallow" -> SeedMode.SHALLOW;
            case "none" -> SeedMode.NONE;
            case "hints" -> SeedMode.HINTS;
            default -> throw new InvalidArgsException(
                    "--tune seed.mode must be one of: shallow, none, hints");
        };
    }

    /**
     * Opens the {@code --trace} JSONL sink (V1), or the always-on no-op default when the flag was not
     * given. Called only from the {@code WorkStealingScan} dispatch — that includes
     * {@code --checkpoint none} (an ephemeral, non-durable checkpoint store still drives the identical
     * engine, so there is a real ready queue/thief to trace; only durability differs).
     */
    TraceSink openTraceSink() throws IOException {
        if (trace == null) {
            return TraceSink.NONE;
        }
        return TraceSink.jsonl(Path.of(trace));
    }
}
