/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.dataset;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Crash-durability barriers shared by every local dataset format. */
public final class DurableFiles {
    private static final Logger log = LoggerFactory.getLogger(DurableFiles.class);

    private DurableFiles() {
    }

    /** Force a completed file's bytes and then the parent entry that names it. */
    public static void fileAndParent(Path file) throws IOException {
        fileAndParent(file, path -> forceFile(path, true), DurableFiles::directory);
    }

    private static void forceFile(Path file, boolean metadata) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(metadata);
        }
    }

    @FunctionalInterface
    interface FileForcer {
        void force(Path file) throws IOException;
    }

    static void fileAndParent(Path file, FileForcer fileForcer, DirectoryForcer directoryForcer)
            throws IOException {
        fileForcer.force(file);
        directoryForcer.force(file.getParent());
    }

    /**
     * Force directory-entry changes where supported. File-data failures remain fatal; unsupported
     * directory forcing is a documented debug-logged portability degradation.
     */
    public static void directory(Path directory) throws IOException {
        directory(directory, path -> {
            try (FileChannel channel = FileChannel.open(path)) {
                channel.force(true);
            }
        });
    }

    @FunctionalInterface
    interface DirectoryForcer {
        void force(Path directory) throws IOException;
    }

    static void directory(Path directory, DirectoryForcer forcer) throws IOException {
        try {
            forcer.force(directory);
        } catch (UnsupportedOperationException | FileSystemException e) {
            log.debug("directory fsync unsupported for {}; continuing without directory barrier",
                    directory, e);
        }
    }
}
