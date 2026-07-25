/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * fsync helpers for the sort package's crash-durability points (I6), mirroring the
 * output.parquet {@code Fsync} discipline: forcing a file's bytes is not enough — the directory
 * entry that names it (or a rename that commits it) must also be durable. v1 targets
 * POSIX/Linux/macOS; directory fsync degrades to a debug-logged no-op where unsupported.
 */
final class Durability {

    private static final Logger log = LoggerFactory.getLogger(Durability.class);

    private Durability() {
    }

    /** fsync {@code dir} so a newly-created or just-renamed entry within it is durable. */
    static void directory(Path dir) throws IOException {
        if (dir == null) {
            return;
        }
        try (FileChannel ch = FileChannel.open(dir)) {   // no options ⇒ READ: a directory fd
            ch.force(true);
        } catch (UnsupportedOperationException | FileSystemException e) {
            log.debug("directory fsync unsupported for {}; continuing without directory barrier", dir, e);
        }
    }
}
