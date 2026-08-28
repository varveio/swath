/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * The single page-run file encoder. Callers append already-packed pages, then finish with either a
 * retained boundary sample or {@code null} for an extension-free cascade/fixture segment. A
 * successful finish writes the trailer, forces and closes the file, fsyncs its directory, and only
 * then records completion for the supplied {@link SegmentKind}.
 */
final class PageRunSegmentEncoder implements AutoCloseable {

    private static final byte[] EMPTY_KEY = new byte[0];

    private final Path path;
    private final FileChannel channel;
    private final SortMetrics metrics;
    private byte[] segmentMin = EMPTY_KEY;
    private byte[] segmentMax = EMPTY_KEY;
    private int totalRecords;
    private long totalEntries;
    private int maxRecordLen;
    private boolean closed;

    private PageRunSegmentEncoder(Path path, FileChannel channel, SortMetrics metrics) {
        this.path = path;
        this.channel = channel;
        this.metrics = metrics;
    }

    static PageRunSegmentEncoder open(Path path, SortMetrics metrics) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(metrics, "metrics");
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        try {
            writeHeader(channel);
            return new PageRunSegmentEncoder(path, channel, metrics);
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
        if (totalRecords == 0 || Arrays.compareUnsigned(pageMin, segmentMin) < 0) {
            segmentMin = pageMin;
        }
        if (totalRecords == 0 || Arrays.compareUnsigned(pageMax, segmentMax) > 0) {
            segmentMax = pageMax;
        }

        byte[] body = page.serialize();
        writeFrame(channel, body);
        maxRecordLen = Math.max(maxRecordLen, body.length);
        totalEntries += page.count();
        totalRecords++;
    }

    long finish(List<byte[]> boundarySample, SegmentKind kind) throws IOException {
        requireOpen();
        Objects.requireNonNull(kind, "kind");
        long trailerStart = channel.position();
        writeTrailer(channel, segmentMin, segmentMax, boundarySample, trailerStart,
                totalRecords, totalEntries, maxRecordLen);
        channel.force(true);
        channel.close();
        closed = true;
        Durability.directory(path.getParent());
        recordCompletion(kind);
        return totalEntries;
    }

    private void recordCompletion(SegmentKind kind) {
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

    private static void writeHeader(FileChannel channel) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(PageRunSegmentWriter.HEADER_BYTES);
        header.putInt(PageRunSegmentWriter.MAGIC);
        header.putShort(PageRunSegmentWriter.FORMAT_VERSION);
        writeFully(channel, header.flip());
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

    private static void writeTrailer(FileChannel channel, byte[] segmentMin, byte[] segmentMax,
                                     List<byte[]> boundarySample, long trailerStart, int totalRecords,
                                     long totalEntries, int maxRecordLen) throws IOException {
        ByteBuffer bounds = ByteBuffer.allocate(2 + segmentMin.length + 2 + segmentMax.length);
        bounds.putShort((short) segmentMin.length).put(segmentMin);
        bounds.putShort((short) segmentMax.length).put(segmentMax);
        writeFully(channel, bounds.flip());
        if (boundarySample != null) {
            PageRunBoundarySample.write(channel, boundarySample);
        }
        ByteBuffer trailer = ByteBuffer.allocate(PageRunSegmentWriter.TRAILER_FIXED_TAIL_BYTES);
        trailer.putLong(trailerStart);
        trailer.putInt(totalRecords);
        trailer.putLong(totalEntries);
        trailer.putInt(maxRecordLen);
        trailer.putInt(PageRunSegmentWriter.MAGIC);
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
