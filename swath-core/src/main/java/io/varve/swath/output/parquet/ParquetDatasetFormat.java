/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.output.dataset.DatasetFormat;
import io.varve.swath.output.dataset.DatasetPartWriter;
import io.varve.swath.output.dataset.PeriodicDataSync;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.parquet.schema.MessageType;

/** Parquet encoding adapter for the format-neutral dataset writer pool. */
public final class ParquetDatasetFormat implements DatasetFormat {
    private final MessageType schema;
    private final long writebackBytes;
    private final SyncableLocalOutputFile.DataForcer dataForcer;

    public ParquetDatasetFormat(MessageType schema) {
        this(schema, 0L);
    }

    public ParquetDatasetFormat(MessageType schema, long writebackBytes) {
        this(schema, writebackBytes, null);
    }

    private ParquetDatasetFormat(MessageType schema, long writebackBytes,
                                 SyncableLocalOutputFile.DataForcer dataForcer) {
        this.schema = Objects.requireNonNull(schema, "schema");
        PeriodicDataSync.requireValidInterval(writebackBytes);
        this.writebackBytes = writebackBytes;
        this.dataForcer = dataForcer;
    }

    @Override public String partSuffix() { return ".parquet"; }
    @Override public String manifestFormat() { return "Parquet"; }
    @Override public String manifestSchema() { return schema.toString(); }
    @Override public DatasetPartWriter openPart(Path path) throws IOException {
        return dataForcer == null
                ? new PartWriter(path, schema, writebackBytes)
                : new PartWriter(path, schema, writebackBytes, dataForcer);
    }

    DatasetFormat withDataForcer(SyncableLocalOutputFile.DataForcer forcer) {
        return new ParquetDatasetFormat(schema, writebackBytes,
                Objects.requireNonNull(forcer, "forcer"));
    }
}
