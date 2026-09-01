/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.spill;

import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * The single page-run file encoder. Callers append already-packed pages, then finish with a
 * CRC-protected fixed trailer. A successful finish forces and closes the file, fsyncs its
 * directory, and only then records completion for the supplied {@link PageRunKind}.
 */
final class PageRunEncoder implements AutoCloseable {

    private final Path path;
    private final FileChannel channel;
    private final SortMetrics metrics;
    private final SortMode orderingMode;
    private int totalRecords;
    private long totalEntries;
    private int maxRecordLen;
    private int maxRawPayloadLength;
    private int maxKeyLength;
    private byte[] previousPageMax;
    private boolean closed;

    private PageRunEncoder(Path path, FileChannel channel, SortMetrics metrics,
                                  SortMode orderingMode) {
        this.path = path;
        this.channel = channel;
        this.metrics = metrics;
        this.orderingMode = orderingMode;
    }

    static PageRunEncoder open(Path path, SortMetrics metrics,
                                      SortMode orderingMode) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(metrics, "metrics");
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        try {
            PageRunHeader.write(channel, orderingMode);
            return new PageRunEncoder(path, channel, metrics, orderingMode);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    void append(PageBlock page) throws IOException {
        requireOpen();
        byte[] pageMin = page.firstKeyUnsafe();
        byte[] pageMax = page.lastKeyUnsafe();
        if (previousPageMax != null) {
            int comparison = Arrays.compareUnsigned(previousPageMax, pageMin);
            if (comparison > 0 || (comparison == 0 && orderingMode == SortMode.OBJECTS)) {
                metrics.recordStealReason("SORT", "buffer_page_overlap");
                throw new PageRunCorruptionException(path,
                        PageRunCorruptionException.PAGE_RUN_PAGE_OVERLAP,
                        "adjacent pages are not disjoint under " + orderingMode
                                + " ordering (previous maxKey must be below next minKey)");
            }
        }
        byte[] body = page.serialize();
        writeFrame(channel, body);
        maxRecordLen = Math.max(maxRecordLen, body.length);
        maxRawPayloadLength = Math.max(maxRawPayloadLength, page.rawPayloadLength());
        maxKeyLength = Math.max(maxKeyLength,
                Math.max(pageMin.length, pageMax.length));
        totalEntries += page.count();
        totalRecords++;
        previousPageMax = pageMax;
    }

    long finish(PageRunKind kind) throws IOException {
        requireOpen();
        Objects.requireNonNull(kind, "kind");
        long trailerStart = channel.position();
        writeTrailer(channel, trailerStart, totalRecords, totalEntries, maxRecordLen,
                maxRawPayloadLength, maxKeyLength);
        channel.force(true);
        channel.close();
        closed = true;
        Durability.directory(path.getParent());
        recordCompletion(kind);
        return totalEntries;
    }

    private void recordCompletion(PageRunKind kind) {
        switch (kind) {
            case LISTING, FIXTURE_CHUNK ->
                    metrics.recordStealReason("SORT", "segment_flushed");
            case CASCADE_INTERMEDIATE -> { }
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("page-run segment encoder is already closed");
        }
    }

    private static void writeFrame(FileChannel channel, byte[] body) throws IOException {
        CRC32C crc = new CRC32C();
        crc.update(body, 0, body.length);
        ByteBuffer header = ByteBuffer.allocate(8);
        header.putInt(body.length);
        header.putInt((int) crc.getValue());
        writeFully(channel, header.flip());
        writeFully(channel, ByteBuffer.wrap(body));
    }

    private static void writeTrailer(FileChannel channel, long trailerStart, int totalRecords,
                                     long totalEntries, int maxRecordLen,
                                     int maxRawPayloadLength, int maxKeyLength) throws IOException {
        ByteBuffer trailer = ByteBuffer.allocate(PageRunWriter.TRAILER_FIXED_TAIL_BYTES);
        trailer.putLong(trailerStart);
        trailer.putInt(totalRecords);
        trailer.putLong(totalEntries);
        trailer.putInt(maxRecordLen);
        trailer.putInt(maxRawPayloadLength);
        trailer.putInt(maxKeyLength);
        CRC32C crc = new CRC32C();
        crc.update(trailer.array(), 0, PageRunWriter.TRAILER_FIELDS_BYTES);
        trailer.putInt((int) crc.getValue());
        trailer.putInt(PageRunWriter.MAGIC);
        writeFully(channel, trailer.flip());
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            channel.close();
            closed = true;
        }
    }
}
