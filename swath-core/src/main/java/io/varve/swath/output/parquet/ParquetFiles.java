/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.SwathReadOptions;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

/** Shared Hadoop-independent configuration and open boundary for local Parquet files. */
public final class ParquetFiles {

    private ParquetFiles() {
    }

    public static ParquetFileReader open(Path path) throws IOException {
        CompressionCodecFactory codecs = newCodecFactory();
        ParquetReadOptions options = SwathReadOptions.create(newConfiguration(), codecs);
        try {
            return ParquetFileReader.open(new LocalInputFile(path), options);
        } catch (IOException | RuntimeException | Error failure) {
            codecs.release();
            throw failure;
        }
    }

    static ParquetConfiguration newConfiguration() {
        return new PlainParquetConfiguration();
    }

    static CompressionCodecFactory newCodecFactory() {
        return new ParquetCodecs();
    }
}
