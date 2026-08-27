/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Resolves one Parquet file or the direct Parquet parts of a directory in lexical name order. */
public final class ParquetParts {

    private ParquetParts() {
    }

    /**
     * Returns {@code path} itself when it is a regular file; otherwise returns its direct
     * {@code *.parquet} children ordered by file name. A directory with no matching files resolves
     * to an empty list. A missing or non-directory path fails with the filesystem exception.
     */
    public static List<Path> resolve(Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            return List.of(path);
        }
        List<Path> parts = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.parquet")) {
            for (Path part : stream) {
                parts.add(part);
            }
        }
        parts.sort(Comparator.comparing(part -> part.getFileName().toString()));
        return parts;
    }
}
