/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;

public final class PatchedOptions {
    public static ParquetReadOptions create(ProbeCodecs codecs) {
        return create(new PlainParquetConfiguration(), codecs);
    }

    public static ParquetReadOptions create(
            org.apache.parquet.conf.ParquetConfiguration conf, ProbeCodecs codecs) {
        return new ParquetReadOptions.Builder(conf, codecs, FilterCompat.NOOP).build();
    }
}
