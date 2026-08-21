/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.output.dataset.DatasetFormat;
import io.varve.swath.output.dataset.DatasetPartWriter;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.schema.MessageType;

/** Parquet encoding adapter for the format-neutral dataset writer pool. */
public record ParquetDatasetFormat(MessageType schema) implements DatasetFormat {
    @Override public String partSuffix() { return ".parquet"; }
    @Override public String manifestFormat() { return "Parquet"; }
    @Override public String manifestSchema() { return schema.toString(); }
    @Override public DatasetPartWriter openPart(Path path) throws IOException { return new PartWriter(path, schema); }
}
