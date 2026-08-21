/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * The single source of truth for swath's on-disk dataset layout. Given the
 * dataset root (the {@code -o} directory), it resolves every well-known path — the pure-parquet
 * {@code data/} subdirectory and the individual part files inside it, plus the root-level
 * {@code manifest.json}, {@code .swath-state.json}, {@code _SUCCESS}, and {@code symlink.txt}.
 *
 * <p>Everything that reads or writes the layout — the part writers, {@link ParquetResume}'s
 * disk-sweep, and the tests — goes through this type rather than hardcoding {@code "data"} or a
 * marker filename, so the layout is defined in exactly one place. The bare filename constants
 * still live on {@link Manifest}; this type is where they are turned into paths.
 */
public record DatasetLayout(Path root) {

    public static DatasetLayout of(Path root) {
        return new DatasetLayout(root);
    }

    /** The pure-parquet {@code data/} subdirectory (part files live here — no markers, no manifest). */
    public Path dataDir() {
        return root.resolve(Manifest.DATA_DIR);
    }

    /** The absolute path of a part file, given its bare filename (e.g. {@code part-w0-00000.parquet}). */
    public Path dataFile(String fileName) {
        return dataDir().resolve(fileName);
    }

    /**
     * The canonical {@code data/}-prefixed relative key for a part's bare filename — the identical
     * form recorded in the consumer manifest {@code files[].key} and the checkpoint {@code
     * part_file.path}, so the manifest, the checkpoint, and {@link #resolveKey} all agree.
     */
    public static String key(String fileName) {
        return Manifest.DATA_DIR + "/" + fileName;
    }

    /** Resolve a stored {@code data/<part>} key back to its absolute path under this root. */
    public Path resolveKey(String key) {
        return root.resolve(key);
    }

    public Path manifest() {
        return root.resolve(Manifest.FILE_NAME);
    }

    public Path state() {
        return root.resolve(Manifest.STATE_FILE_NAME);
    }

    public Path success() {
        return root.resolve(Manifest.SUCCESS_FILE_NAME);
    }

    public Path symlink() {
        return root.resolve(Manifest.SYMLINK_FILE_NAME);
    }

    /**
     * The sorted parquet part files currently under {@code data/} (empty if {@code data/} does not
     * yet exist) — the one shared implementation of "list this dataset's parts", used by the resume
     * disk-sweep and the tests instead of each re-rolling a {@code Files.list(...).filter(...)}.
     */
    public List<Path> dataParts() throws IOException {
        return dataParts(".parquet");
    }

    /** Finalized parts for a specific dataset format suffix. */
    public List<Path> dataParts(String suffix) throws IOException {
        Path data = dataDir();
        if (!Files.isDirectory(data)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(data)) {
            return entries
                    .filter(p -> p.getFileName().toString().startsWith("part-"))
                    .filter(p -> p.getFileName().toString().endsWith(suffix))
                    .sorted()
                    .toList();
        }
    }
}
