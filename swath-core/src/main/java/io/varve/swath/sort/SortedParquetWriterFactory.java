/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.observability.RunMetrics;
import io.varve.swath.output.dataset.DatasetDataSyncMetrics;
import java.io.IOException;
import java.nio.file.Path;

/**
 * The stamped {@link SortedFileWriterFactory}: every final file it creates is a
 * {@link SortedParquetWriter} carrying the same {@link SortConfig} and {@link SortMode} — the
 * mode is fixed per {@code --sort} run ({@code --all-versions} known once at CLI parse time),
 * not per file. Callers pass this in place of
 * {@link SortedFileWriterFactory#DEFAULT} when constructing {@link SortTransform} for a real run;
 * {@code DEFAULT} remains the plain, unstamped path used by tests.
 */
public final class SortedParquetWriterFactory implements SortedFileWriterFactory {

    private final SortConfig config;
    private final SortMode mode;
    private final long writebackBytes;
    private final DatasetDataSyncMetrics syncMetrics;

    public SortedParquetWriterFactory(SortConfig config, SortMode mode) {
        this(config, mode, 0L, null);
    }

    public SortedParquetWriterFactory(
            SortConfig config, SortMode mode, long writebackBytes, RunMetrics metrics) {
        this.config = config;
        this.mode = mode;
        this.writebackBytes = writebackBytes;
        this.syncMetrics = metrics == null ? null : new DatasetDataSyncMetrics(
                metrics, "parquet", DatasetDataSyncMetrics.Classification.SORTED_PARQUET);
    }

    @Override
    public SortedFileWriter create(Path path, int fileIndex) throws IOException {
        return new SortedParquetWriter(path, config, mode, fileIndex, writebackBytes, syncMetrics);
    }
}
