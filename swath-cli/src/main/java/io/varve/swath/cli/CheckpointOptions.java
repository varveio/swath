/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.nio.file.Path;
import picocli.CommandLine.Option;

/** Durable-checkpoint location and the resume/restart mode flags. */
final class CheckpointOptions {

    @Resume(ResumeClass.FREE)
    @Option(names = "--checkpoint", paramLabel = "PATH|none|auto",
            description = "auto (default) co-locates the checkpoint in a directory output "
                    + "(<dir>/.swath/checkpoint.sqlite) and keeps nothing for stdout; a path pins the "
                    + "SQLite file; none runs ephemeral.")
    String location = "auto";

    boolean resume;

    @Resume(ResumeClass.FREE)
    @Option(names = "--restart", description = "Discard any prior checkpoint and start fresh.")
    boolean restart;

    @Resume(ResumeClass.FREE)
    @Option(names = {"--overwrite", "--force"},
            description = "Discard a COMPLETED run for this destination and re-list it.")
    boolean overwrite;

    CheckpointMode mode() {
        return CheckpointMode.parse(location);
    }

    /** Directory name of the run-handle checkpoint co-located inside a directory dataset. */
    static final String COLOCATED_DIR = ".swath";

    /** File name of the co-located run-handle checkpoint. */
    static final String COLOCATED_FILE = "checkpoint.sqlite";

    /**
     * The checkpoint storage mode: {@code auto} co-locates the checkpoint inside a directory-dataset
     * output at {@code <dir>/.swath/checkpoint.sqlite} (the output dir is the whole run handle) and is
     * ephemeral for stdout / single-file (nothing on disk); {@code none} → in-memory (no resume);
     * otherwise an explicit DB path.
     */
    record CheckpointMode(String raw) {
        static CheckpointMode parse(String s) {
            return new CheckpointMode(s == null ? "auto" : s);
        }

        boolean isNone() {
            return raw.equalsIgnoreCase("none");
        }

        boolean isAuto() {
            return raw.equalsIgnoreCase("auto");
        }

        /**
         * The durable checkpoint file for this mode, or {@code null} for an ephemeral run that keeps
         * nothing on disk. {@code auto} co-locates inside a directory-dataset output at
         * {@code <dir>/.swath/checkpoint.sqlite}; a stdout or single-file {@code auto} run is
         * ephemeral (zero litter). An explicit path is used verbatim.
         */
        Path resolve(String destination, OutputOptions.DestinationKind kind) {
            if (isNone()) {
                return null;
            }
            if (isAuto()) {
                return kind == OutputOptions.DestinationKind.DIRECTORY
                        ? colocatedCheckpoint(Path.of(destination))
                        : null;
            }
            return Path.of(raw);
        }

        /** The co-located run-handle checkpoint path for a directory-dataset output. */
        static Path colocatedCheckpoint(Path outputDir) {
            return outputDir.resolve(COLOCATED_DIR).resolve(COLOCATED_FILE);
        }
    }
}
