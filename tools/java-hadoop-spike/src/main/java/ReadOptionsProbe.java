/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

import java.nio.file.Path;

/** Executes in a fresh JVM with no Hadoop classes on its classpath. */
public final class ReadOptionsProbe {
    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.apache.hadoop.conf.Configuration");
            throw new AssertionError("Hadoop present");
        } catch (ClassNotFoundException expected) {
        }
        var codecs = new ProbeCodecs();
        try {
            // Stock path must demonstrate whether caller overrides are reachable.
            var options =
                    args[0].equals("stock")
                            ? ParquetReadOptions.builder(new PlainParquetConfiguration())
                                    .withCodecFactory(codecs)
                                    .withRecordFilter(FilterCompat.NOOP)
                                    .build()
                            : (ParquetReadOptions)
                                    Class.forName("PatchedOptions")
                                            .getMethod("create", ProbeCodecs.class)
                                            .invoke(null, codecs);
            try (var reader =
                    ParquetFileReader.open(new LocalInputFile(Path.of(args[1])), options)) {
                CanonicalProbe.verify(reader);
            }
            System.out.println(
                    "PASS no Hadoop runtime; canonical rows, footer, column indexes, offset indexes"
                            + " and page decoding");
        } finally {
            codecs.release();
        }
    }
}
