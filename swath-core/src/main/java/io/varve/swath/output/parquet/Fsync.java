/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * fsync helpers for crash-durability (contract I6 / RES-4). Forcing a file's bytes to
 * disk is not enough: the <i>directory entry</i> that names the file (or the rename that
 * commits it) must also be durable, or after power loss a part we treated as finalized —
 * or a manifest we atomically renamed — can vanish even though its bytes survived. On
 * POSIX the durable barrier for a directory entry is {@code fsync()} on the directory fd.
 * v1 targets POSIX/Linux/macOS; directory fsync degrades to a debug-logged no-op
 * where the filesystem/OS does not support it. File fsync failures are still fatal.
 */
final class Fsync {

    private static final Logger log = LoggerFactory.getLogger(Fsync.class);

    private Fsync() {
    }

    /**
     * fsync {@code dir} so a newly-created or just-renamed entry within it is durable.
     * A directory is opened read-only (the only portable way to obtain its fd) and forced.
     */
    static void directory(Path dir) throws IOException {
        directory(dir, path -> {
            try (FileChannel ch = FileChannel.open(path)) {   // no options ⇒ READ: a directory fd
                ch.force(true);
            }
        });
    }

    @FunctionalInterface
    interface DirectoryForcer {
        void force(Path dir) throws IOException;
    }

    static void directory(Path dir, DirectoryForcer forcer) throws IOException {
        try {
            forcer.force(dir);
        } catch (UnsupportedOperationException | FileSystemException e) {
            log.debug("directory fsync unsupported for {}; continuing without directory barrier", dir, e);
        }
    }
}
