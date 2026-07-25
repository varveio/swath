/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.output.parquet.PartWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Single-sink streaming Parquet {@link EntryFormatter} (§4): one canonical-schema
 * {@link PartWriter} whose footer is fsynced on {@link #close()} — at which point the part is
 * COMPLETE-able (I6) — and an atomically-committed one-part {@code manifest.json}.
 *
 * <p>This is the one-instance-per-sink realization of the sealed SPI. The {@code list} CLI's
 * default Parquet path is the decoupled multi-writer {@link io.varve.swath.output.parquet.ParquetWriterPool}
 * (§4.1/§7 memory model: 2–4 writers decoupled from {@code T}); this formatter is the
 * single-writer sink that the {@code EntryFormatter} contract describes. Both emit the same
 * canonical schema and the same manifest shape, so either is readable by the INT-6 readers.
 */
public final class ParquetFormatter implements EntryFormatter {

    private final Path dir;
    private final DatasetLayout layout;
    private final String argsHash;
    private final String bucket;
    private final PartWriter writer;

    /**
     * @param dir      dataset root for {@code data/}, {@code manifest.json}, and the completion markers
     * @param bucket   source bucket recorded in the consumer manifest's {@code sourceBucket}
     * @param argsHash run-identity hash recorded in {@code .swath-state.json} (resume idempotence, §4.1)
     */
    public ParquetFormatter(Path dir, String bucket, String argsHash) throws IOException {
        this.dir = dir;
        this.layout = DatasetLayout.of(dir);
        this.bucket = bucket;
        this.argsHash = argsHash;
        Files.createDirectories(layout.dataDir());   // parts live under <root>/data/
        this.writer = new PartWriter(layout.dataFile("part-00000.parquet"), ParquetSchema.canonical());
    }

    /** Parquet has no header rows; the schema lives in the footer written on {@link #close()}. */
    @Override
    public void writeHeader() {
    }

    @Override
    public void write(ListEntry e) throws IOException {
        writer.write(e);
    }

    /**
     * Write the footer + fsync (the part becomes COMPLETE-able, I6), then commit the full output
     * layout: consumer {@code manifest.json}, {@code .swath-state.json}, {@code
     * symlink.txt}, and the {@code _SUCCESS} marker LAST.
     */
    @Override
    public void close() throws IOException {
        writer.close();
        String relPath = DatasetLayout.key(writer.path().getFileName().toString());
        String md5;
        try (var in = Files.newInputStream(writer.path())) {
            md5 = DigestUtils.md5Hex(in);
        }
        List<PartInfo> parts = List.of(new PartInfo(relPath, 0, writer.rows(), Files.size(writer.path()), md5));
        Manifest.write(dir, bucket, ParquetSchema.canonical().toString(), parts, false, null);
        Manifest.writeState(dir, argsHash, null);
        Manifest.writeSymlink(dir, parts);
        Manifest.writeSuccess(dir);
    }
}
