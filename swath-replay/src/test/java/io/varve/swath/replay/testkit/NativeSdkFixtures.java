/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.testkit;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfigs;
import java.nio.file.Files;
import java.nio.file.Path;

/** Small real sorted-Parquet fixture for official provider SDK tests. */
public final class NativeSdkFixtures {
    private NativeSdkFixtures() { }

    public static Path sorted(Path root, ObjectEntry... objects) throws Exception {
        Path capture = Files.createDirectories(root.resolve("capture"));
        ParquetFixtures.write(capture.resolve("part.parquet"), objects);
        Path sorted = Files.createDirectories(root.resolve("sorted"));
        new CaptureSorter(SortConfigs.manySmallRowGroups()).sort(capture, sorted);
        return sorted;
    }
}
