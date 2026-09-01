/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.output.dataset.DurableFiles;
import java.io.IOException;
import java.nio.file.Path;

/**
 * fsync helpers for the sort package's crash-durability points (I6), mirroring the
 * output.dataset {@code DurableFiles} discipline: forcing a file's bytes is not enough — the directory
 * entry that names it (or a rename that commits it) must also be durable. v1 targets
 * POSIX/Linux/macOS; directory fsync uses the shared filesystem-aware support classification.
 */
final class Durability {

    private Durability() {
    }

    /** fsync {@code dir} so a newly-created or just-renamed entry within it is durable. */
    static void directory(Path dir) throws IOException {
        if (dir == null) {
            return;
        }
        DurableFiles.directory(dir);
    }
}
