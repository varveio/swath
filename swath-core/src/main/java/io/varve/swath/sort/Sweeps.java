/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.function.Consumer;

/** Shared deletion primitives for sorter-owned staging files and trees. */
final class Sweeps {

    private Sweeps() {
    }

    /** Delete every entry matching any glob; a no-op when {@code dir} is absent. */
    static void sweep(Path dir, Consumer<Path> beforeDelete, String... globs) throws IOException {
        if (!Files.isDirectory(dir)) {
            return;
        }
        for (String glob : globs) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
                for (Path stale : stream) {
                    beforeDelete.accept(stale);
                    Files.deleteIfExists(stale);
                }
            }
        }
    }

    /** Recursively delete one caller-owned tree; a no-op when it is absent. */
    static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            try {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
    }
}
