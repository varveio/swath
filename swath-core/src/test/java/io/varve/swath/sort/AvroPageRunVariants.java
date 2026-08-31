/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/**
 * PR 6b deep dive: production-shaped Avro OCF variants measured against the spike's arm.
 *
 * <p>Three read shapes are provided.
 *
 * <ul>
 *   <li>{@code REUSE} keeps the spike's {@code GenericRecord} path but reuses the datum object
 *       through {@code DataFileStream.next(D)} and drops the defensive {@code ByteBuffer -> byte[]}
 *       copies for the routing fields.
 *   <li>{@code RAW} iterates OCF blocks with the public {@code hasNext()/nextBlock()} pair, so the
 *       block payload is handed over undecoded, and decodes the single record in the block with one
 *       reused {@link BinaryDecoder}. No {@code GenericRecord}, no schema resolution in the loop.
 *   <li>{@code RAW2} additionally reorders the schema so the routing fields precede the opaque page
 *       payload, which lets the header pass read only the first bytes of each block and seek over
 *       the payload, matching what the custom frame's positional header pass does.
 * </ul>
 */
final class AvroPageRunVariants {

    static final String FORMAT = "swath-pageseg-avro-spike-v1";
    static final String FORMAT_META = "swath.format";
    static final String PAGE_CODEC_META = "swath.page-codec";

    /** Routing metadata first, opaque payload last: the header pass can stop after the prefix. */
    private static final String REORDERED_SCHEMA_JSON = """
            {
              "type":"record", "name":"PageRunFrame", "namespace":"io.varve.swath.spike2",
              "fields":[
                {"name":"kind", "type":{"type":"enum", "name":"FrameKind",
                  "symbols":["PAGE","SEAL"]}},
                {"name":"minKey", "type":"bytes"},
                {"name":"maxKey", "type":"bytes"},
                {"name":"count", "type":"int"},
                {"name":"rawPayloadLength", "type":"int"},
                {"name":"totalEntries", "type":"long"},
                {"name":"totalRecords", "type":"long"},
                {"name":"lastKey", "type":"bytes"},
                {"name":"page", "type":"bytes"}
              ]
            }
            """;

    static final Schema REORDERED_SCHEMA = new Schema.Parser().parse(REORDERED_SCHEMA_JSON);
    private static final Schema REORDERED_KIND = REORDERED_SCHEMA.getField("kind").schema();

    /** Enough to cover kind + both keys + counters + lastKey for the 109-byte key corpus. */
    private static final int HEADER_PREFIX_BYTES = 512;

    private AvroPageRunVariants() {
    }

    // ---------------------------------------------------------------- writer

    /**
     * The spike writer with two production-shaped knobs: {@code setSyncInterval} sized to the block
     * we actually emit (we call {@code sync()} per page anyway, so the 64 KB default buffer is dead
     * weight) and the optional reordered schema.
     */
    static final class Writer implements Closeable {

        private final Path path;
        private final Schema schema;
        private final Schema kindSchema;
        private final DataFileWriter<GenericRecord> file;
        private final GenericData.Record frame;
        private long totalEntries;
        private long totalRecords;
        private byte[] lastKey = new byte[0];
        private boolean sealed;

        Writer(Path path, PageCodec pageCodec, boolean reordered, int syncInterval)
                throws IOException {
            this.path = path;
            this.schema = reordered ? REORDERED_SCHEMA : AvroPageRunContainer.SCHEMA;
            this.kindSchema = reordered ? REORDERED_KIND : schema.getField("kind").schema();
            this.file = new DataFileWriter<>(new GenericDatumWriter<>(schema));
            file.setCodec(CodecFactory.nullCodec());
            if (syncInterval > 0) {
                file.setSyncInterval(syncInterval);
            }
            file.setMeta(FORMAT_META, FORMAT);
            file.setMeta(PAGE_CODEC_META, pageCodec.name().toLowerCase(Locale.ROOT));
            this.frame = new GenericData.Record(schema);
            try {
                file.create(schema, path.toFile());
                file.sync();
            } catch (IOException | RuntimeException failure) {
                file.close();
                throw failure;
            }
        }

        void append(PageBlock page) throws IOException {
            byte[] body = page.serialize();
            PageBlockCodec.Header header = PageBlockCodec.parseHeader(body);
            reset("PAGE");
            frame.put("page", ByteBuffer.wrap(body));
            frame.put("minKey", ByteBuffer.wrap(header.minKey()));
            frame.put("maxKey", ByteBuffer.wrap(header.maxKey()));
            frame.put("count", header.count());
            frame.put("rawPayloadLength", header.rawPayloadLength());
            file.append(frame);
            file.sync();
            totalRecords++;
            totalEntries += header.count();
            lastKey = header.maxKey();
        }

        void seal() throws IOException {
            reset("SEAL");
            frame.put("totalEntries", totalEntries);
            frame.put("totalRecords", totalRecords);
            frame.put("lastKey", ByteBuffer.wrap(lastKey));
            file.append(frame);
            file.sync();
            file.close();
            sealed = true;
            force(path);
        }

        private void reset(String kind) {
            frame.put("kind", new GenericData.EnumSymbol(kindSchema, kind));
            frame.put("page", ByteBuffer.wrap(new byte[0]));
            frame.put("minKey", ByteBuffer.wrap(new byte[0]));
            frame.put("maxKey", ByteBuffer.wrap(new byte[0]));
            frame.put("count", 0);
            frame.put("rawPayloadLength", 0);
            frame.put("totalEntries", -1L);
            frame.put("totalRecords", -1L);
            frame.put("lastKey", ByteBuffer.wrap(new byte[0]));
        }

        @Override
        public void close() throws IOException {
            if (!sealed) {
                file.close();
                sealed = true;
            }
        }
    }

    private static void force(Path path) throws IOException {
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE)) {
            file.force(true);
        }
        try (FileChannel directory = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    // ------------------------------------------------------- decoded record

    /** One decoded frame; {@code page} is null on the header pass and for SEAL. */
    static final class Frame {
        String kind;
        byte[] minKey = new byte[0];
        byte[] maxKey = new byte[0];
        int count;
        int rawPayloadLength;
        long totalEntries;
        long totalRecords;
        byte[] lastKey = new byte[0];
        byte[] page;
    }

    // ------------------------------------------------- GenericRecord, reused

    /**
     * The spike's shape with datum reuse. {@code DataFileStream.next(D)} lets
     * {@code GenericDatumReader} refill the same record and the same {@link ByteBuffer}s, and the
     * routing fields are compared straight out of the buffer instead of through a fresh
     * {@code byte[]}.
     */
    static final class ReuseReader implements Closeable {

        private final Path path;
        private final DataFileReader<GenericRecord> file;
        private GenericRecord reuse;
        private long pages;
        private long entries;
        private byte[] previousMax;
        private boolean done;

        ReuseReader(Path path, boolean projected) throws IOException {
            this.path = path;
            Schema write = AvroPageRunContainer.SCHEMA;
            this.file = new DataFileReader<>(path.toFile(),
                    projected ? new GenericDatumReader<>(write, headerProjection())
                            : new GenericDatumReader<>(write));
            validateMetadata(path, file);
        }

        private static Schema headerProjection() {
            return new Schema.Parser().parse("""
                    {
                      "type":"record", "name":"PageRunFrame", "namespace":"io.varve.swath.spike",
                      "fields":[
                        {"name":"kind", "type":{"type":"enum", "name":"FrameKind",
                          "symbols":["PAGE","SEAL"]}},
                        {"name":"minKey", "type":"bytes"},
                        {"name":"maxKey", "type":"bytes"},
                        {"name":"count", "type":"int"},
                        {"name":"rawPayloadLength", "type":"int"},
                        {"name":"totalEntries", "type":"long"},
                        {"name":"totalRecords", "type":"long"},
                        {"name":"lastKey", "type":"bytes"}
                      ]
                    }
                    """);
        }

        /** Returns false at the validated seal. */
        boolean next(Frame out, boolean wantPage) throws IOException {
            if (done) {
                return false;
            }
            if (!file.hasNext()) {
                throw corrupt(path, "missing final SEAL record");
            }
            reuse = file.next(reuse);
            String kind = reuse.get("kind").toString();
            if (kind.equals("SEAL")) {
                checkSeal(path, (long) reuse.get("totalRecords"),
                        (long) reuse.get("totalEntries"), copy(reuse, "lastKey"),
                        pages, entries, previousMax);
                if (file.hasNext()) {
                    throw corrupt(path, "SEAL is not the last OCF record");
                }
                done = true;
                return false;
            }
            if (!kind.equals("PAGE")) {
                throw corrupt(path, "unknown frame kind " + kind);
            }
            ByteBuffer min = (ByteBuffer) reuse.get("minKey");
            ByteBuffer max = (ByteBuffer) reuse.get("maxKey");
            out.kind = kind;
            out.count = (int) reuse.get("count");
            out.rawPayloadLength = (int) reuse.get("rawPayloadLength");
            out.minKey = toArray(min);
            out.maxKey = toArray(max);
            if (wantPage) {
                out.page = toArray((ByteBuffer) reuse.get("page"));
            }
            if (out.count <= 0 || out.rawPayloadLength <= 0
                    || Arrays.compareUnsigned(out.minKey, out.maxKey) > 0
                    || previousMax != null
                    && Arrays.compareUnsigned(previousMax, out.minKey) >= 0) {
                throw corrupt(path, "invalid PAGE metadata");
            }
            pages++;
            entries += out.count;
            previousMax = out.maxKey;
            return true;
        }

        private static byte[] copy(GenericRecord record, String field) {
            return toArray((ByteBuffer) record.get(field));
        }

        @Override
        public void close() throws IOException {
            file.close();
        }
    }

    private static byte[] toArray(ByteBuffer buffer) {
        byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return out;
    }

    // -------------------------------------------------- raw block iteration

    /**
     * Raw-block reader: {@code hasNext()} pulls and (null-codec) hands back the whole block, then
     * {@code nextBlock()} returns the undecoded payload. One reused {@link BinaryDecoder} parses the
     * single record.
     */
    static final class RawReader implements Closeable {

        private final Path path;
        private final DataFileReader<GenericRecord> file;
        private final boolean reordered;
        private BinaryDecoder decoder;
        private long pages;
        private long entries;
        private byte[] previousMax;
        private boolean done;

        RawReader(Path path, boolean reordered) throws IOException {
            this.path = path;
            this.reordered = reordered;
            Schema schema = reordered ? REORDERED_SCHEMA : AvroPageRunContainer.SCHEMA;
            // The datum reader is never used on this path; DataFileReader still needs one.
            this.file = new DataFileReader<>(path.toFile(), new GenericDatumReader<>(schema));
            validateMetadata(path, file);
        }

        boolean next(Frame out, boolean wantPage) throws IOException {
            if (done) {
                return false;
            }
            if (!file.hasNext()) {
                throw corrupt(path, "missing final SEAL record");
            }
            if (file.getBlockCount() != 1) {
                throw corrupt(path, "expected exactly one record per OCF block, saw "
                        + file.getBlockCount());
            }
            ByteBuffer block = file.nextBlock();
            decoder = DecoderFactory.get().binaryDecoder(block.array(),
                    block.arrayOffset() + block.position(), block.remaining(), decoder);
            decode(out, wantPage, decoder);
            if (out.kind.equals("SEAL")) {
                checkSeal(path, out.totalRecords, out.totalEntries, out.lastKey,
                        pages, entries, previousMax);
                if (file.hasNext()) {
                    throw corrupt(path, "SEAL is not the last OCF record");
                }
                done = true;
                return false;
            }
            if (out.count <= 0 || out.rawPayloadLength <= 0
                    || Arrays.compareUnsigned(out.minKey, out.maxKey) > 0
                    || previousMax != null
                    && Arrays.compareUnsigned(previousMax, out.minKey) >= 0) {
                throw corrupt(path, "invalid PAGE metadata");
            }
            pages++;
            entries += out.count;
            previousMax = out.maxKey;
            return true;
        }

        private void decode(Frame out, boolean wantPage, BinaryDecoder in) throws IOException {
            out.page = null;
            out.kind = in.readEnum() == 0 ? "PAGE" : "SEAL";
            if (reordered) {
                out.minKey = readBytes(in);
                out.maxKey = readBytes(in);
                out.count = in.readInt();
                out.rawPayloadLength = in.readInt();
                out.totalEntries = in.readLong();
                out.totalRecords = in.readLong();
                out.lastKey = readBytes(in);
                if (wantPage) {
                    out.page = readBytes(in);
                }
            } else {
                if (wantPage) {
                    out.page = readBytes(in);
                } else {
                    in.skipBytes();
                }
                out.minKey = readBytes(in);
                out.maxKey = readBytes(in);
                out.count = in.readInt();
                out.rawPayloadLength = in.readInt();
                out.totalEntries = in.readLong();
                out.totalRecords = in.readLong();
                out.lastKey = readBytes(in);
            }
        }

        @Override
        public void close() throws IOException {
            file.close();
        }
    }

    private static byte[] readBytes(BinaryDecoder in) throws IOException {
        int length = (int) in.readLong();
        byte[] out = new byte[length];
        in.readFixed(out, 0, length);
        return out;
    }

    // --------------------------------------- seek-over-payload header pass

    /**
     * Header pass for the reordered schema: walk OCF block framing on a plain channel, read only the
     * leading {@value #HEADER_PREFIX_BYTES} bytes of each block, and seek over the opaque payload.
     * This is the shape the custom frame's positional header pass already has.
     */
    static AvroPageRunContainer.HeaderSummary scanHeadersSeeking(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            OcfHeader header = readOcfHeader(channel, path);
            long size = channel.size();
            long position = header.firstBlock;
            long pages = 0;
            long entries = 0;
            byte[] previousMax = null;
            boolean sealed = false;
            ByteBuffer prefix = ByteBuffer.allocate(HEADER_PREFIX_BYTES + 20)
                    .order(ByteOrder.BIG_ENDIAN);
            BinaryDecoder decoder = null;
            Frame frame = new Frame();
            while (position < size) {
                prefix.clear();
                readAt(channel, prefix, position, path);
                prefix.flip();
                long recordCount = readVarLong(prefix, path);
                long blockSize = readVarLong(prefix, path);
                if (recordCount != 1) {
                    throw corrupt(path, "expected one record per block, saw " + recordCount);
                }
                int dataStart = prefix.position();
                int available = (int) Math.min(prefix.remaining(), blockSize);
                decoder = DecoderFactory.get().binaryDecoder(prefix.array(), dataStart,
                        available, decoder);
                frame.kind = decoder.readEnum() == 0 ? "PAGE" : "SEAL";
                frame.minKey = readBytes(decoder);
                frame.maxKey = readBytes(decoder);
                frame.count = decoder.readInt();
                frame.rawPayloadLength = decoder.readInt();
                frame.totalEntries = decoder.readLong();
                frame.totalRecords = decoder.readLong();
                frame.lastKey = readBytes(decoder);
                if (frame.kind.equals("PAGE")) {
                    if (sealed) {
                        throw corrupt(path, "PAGE follows SEAL");
                    }
                    if (frame.count <= 0 || frame.rawPayloadLength <= 0
                            || Arrays.compareUnsigned(frame.minKey, frame.maxKey) > 0
                            || previousMax != null
                            && Arrays.compareUnsigned(previousMax, frame.minKey) >= 0) {
                        throw corrupt(path, "invalid projected PAGE metadata");
                    }
                    pages++;
                    entries += frame.count;
                    previousMax = frame.maxKey;
                } else {
                    checkSeal(path, frame.totalRecords, frame.totalEntries, frame.lastKey,
                            pages, entries, previousMax);
                    sealed = true;
                }
                position += dataStart + blockSize + 16;
                if (sealed && position < size) {
                    throw corrupt(path, "SEAL is not the last OCF block");
                }
            }
            if (!sealed) {
                throw corrupt(path, "missing final SEAL record");
            }
            return new AvroPageRunContainer.HeaderSummary(pages, entries);
        }
    }

    private record OcfHeader(long firstBlock, Map<String, byte[]> meta) {
    }

    private static OcfHeader readOcfHeader(FileChannel channel, Path path) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(64 << 10);
        readAt(channel, buffer, 0, path);
        buffer.flip();
        byte[] magic = new byte[4];
        buffer.get(magic);
        if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j' || magic[3] != 1) {
            throw corrupt(path, "not an Avro OCF file");
        }
        Map<String, byte[]> meta = new LinkedHashMap<>();
        while (true) {
            long entries = readVarLong(buffer, path);
            if (entries == 0) {
                break;
            }
            if (entries < 0) {
                entries = -entries;
                readVarLong(buffer, path);
            }
            for (long index = 0; index < entries; index++) {
                int keyLength = (int) readVarLong(buffer, path);
                byte[] key = new byte[keyLength];
                buffer.get(key);
                int valueLength = (int) readVarLong(buffer, path);
                byte[] value = new byte[valueLength];
                buffer.get(value);
                meta.put(new String(key, StandardCharsets.UTF_8), value);
            }
        }
        byte[] codec = meta.get("avro.codec");
        if (codec != null && !"null".equals(new String(codec, StandardCharsets.UTF_8))) {
            throw corrupt(path, "Avro codec must be null");
        }
        byte[] format = meta.get(FORMAT_META);
        if (format == null || !FORMAT.equals(new String(format, StandardCharsets.UTF_8))) {
            throw corrupt(path, "missing or unsupported " + FORMAT_META);
        }
        return new OcfHeader(buffer.position() + 16L, meta);
    }

    private static void readAt(FileChannel channel, ByteBuffer buffer, long position, Path path)
            throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + buffer.position());
            if (read < 0) {
                break;
            }
        }
    }

    private static long readVarLong(ByteBuffer buffer, Path path) throws IOException {
        long value = 0;
        int shift = 0;
        while (true) {
            if (!buffer.hasRemaining()) {
                throw corrupt(path, "truncated varint");
            }
            int b = buffer.get() & 0xff;
            value |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw corrupt(path, "varint overflow");
            }
        }
        return value >>> 1 ^ -(value & 1);
    }

    // ------------------------------------------------------------- helpers

    private static void validateMetadata(Path path, DataFileReader<GenericRecord> file)
            throws IOException {
        if (!FORMAT.equals(file.getMetaString(FORMAT_META))) {
            file.close();
            throw corrupt(path, "missing or unsupported " + FORMAT_META);
        }
        if (!"null".equals(file.getMetaString("avro.codec"))) {
            file.close();
            throw corrupt(path, "Avro codec must be null");
        }
    }

    private static void checkSeal(Path path, long declaredPages, long declaredEntries,
            byte[] lastKey, long pages, long entries, byte[] previousMax) throws IOException {
        if (declaredPages != pages || declaredEntries != entries
                || !Arrays.equals(lastKey, previousMax == null ? new byte[0] : previousMax)) {
            throw corrupt(path, "SEAL totals or last key do not match preceding PAGE records");
        }
    }

    private static IOException corrupt(Path path, String message) {
        return new IOException("Avro page-run segment " + path + ": " + message);
    }
}
