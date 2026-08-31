/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/** Test-only Avro OCF candidate for the PR 6b container decision spike. */
final class AvroPageRunContainer {

    private static final String FORMAT = "swath-pageseg-avro-spike-v1";
    private static final String FORMAT_META = "swath.format";
    private static final String PAGE_CODEC_META = "swath.page-codec";
    private static final String SCHEMA_JSON = """
            {
              "type":"record", "name":"PageRunFrame", "namespace":"io.varve.swath.spike",
              "fields":[
                {"name":"kind", "type":{"type":"enum", "name":"FrameKind",
                  "symbols":["PAGE","SEAL"]}},
                {"name":"page", "type":"bytes"},
                {"name":"minKey", "type":"bytes"},
                {"name":"maxKey", "type":"bytes"},
                {"name":"count", "type":"int"},
                {"name":"rawPayloadLength", "type":"int"},
                {"name":"totalEntries", "type":"long"},
                {"name":"totalRecords", "type":"long"},
                {"name":"lastKey", "type":"bytes"}
              ]
            }
            """;
    private static final String HEADER_SCHEMA_JSON = """
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
            """;

    static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);
    private static final Schema HEADER_SCHEMA = new Schema.Parser().parse(HEADER_SCHEMA_JSON);
    private static final Schema KIND_SCHEMA = SCHEMA.getField("kind").schema();

    private AvroPageRunContainer() {
    }

    static Writer openWriter(Path path, PageCodec pageCodec) throws IOException {
        return new Writer(path, pageCodec);
    }

    static Reader openReader(Path path) throws IOException {
        return new Reader(path);
    }

    static HeaderSummary scanHeaders(Path path) throws IOException {
        long pages = 0;
        long entries = 0;
        byte[] previousMax = null;
        boolean sealed = false;
        try (DataFileReader<GenericRecord> file = openProjected(path)) {
            while (file.hasNext()) {
                GenericRecord frame = file.next();
                String kind = frame.get("kind").toString();
                if (kind.equals("PAGE")) {
                    if (sealed) {
                        throw corrupt(path, "PAGE follows SEAL");
                    }
                    byte[] min = bytes(frame, "minKey");
                    byte[] max = bytes(frame, "maxKey");
                    int count = (int) frame.get("count");
                    int rawLength = (int) frame.get("rawPayloadLength");
                    if (count <= 0 || rawLength <= 0
                            || Arrays.compareUnsigned(min, max) > 0
                            || previousMax != null
                            && Arrays.compareUnsigned(previousMax, min) >= 0) {
                        throw corrupt(path, "invalid projected PAGE metadata");
                    }
                    pages++;
                    entries += count;
                    previousMax = max;
                } else if (kind.equals("SEAL")) {
                    validateSeal(path, frame, pages, entries, previousMax);
                    sealed = true;
                    if (file.hasNext()) {
                        throw corrupt(path, "SEAL is not the last OCF record");
                    }
                } else {
                    throw corrupt(path, "unknown frame kind " + kind);
                }
            }
        } catch (EOFException e) {
            throw corrupt(path, "truncated OCF", e);
        }
        if (!sealed) {
            throw corrupt(path, "missing final SEAL record");
        }
        return new HeaderSummary(pages, entries);
    }

    static ResyncResult resync(Path path, long damagedOffset) throws IOException {
        try (DataFileReader<GenericRecord> file = openFull(path)) {
            file.sync(damagedOffset);
            long alignedAt = file.previousSync();
            if (!file.hasNext()) {
                throw corrupt(path, "no block after resync offset " + damagedOffset);
            }
            GenericRecord frame = file.next();
            return new ResyncResult(alignedAt, frame.get("kind").toString(),
                    bytes(frame, "minKey"));
        }
    }

    static String inspect(Path path) throws IOException {
        StringBuilder out = new StringBuilder();
        try (DataFileReader<GenericRecord> file = openFull(path)) {
            List<String> keys = new ArrayList<>(file.getMetaKeys());
            keys.sort(Comparator.naturalOrder());
            out.append("file=").append(path).append('\n');
            for (String key : keys) {
                byte[] value = file.getMeta(key);
                out.append("metadata ").append(key).append('=');
                if (key.equals("avro.schema")) {
                    out.append("<schema, ").append(value.length).append(" bytes>");
                } else {
                    out.append(new String(value, java.nio.charset.StandardCharsets.UTF_8));
                }
                out.append('\n');
            }
            int block = 0;
            while (file.hasNext()) {
                GenericRecord frame = file.next();
                block++;
                out.append("block ").append(block)
                        .append(" sync=").append(file.previousSync())
                        .append(" records=1 kind=").append(frame.get("kind"));
                if (frame.get("kind").toString().equals("PAGE")) {
                    out.append(" entries=").append(frame.get("count"))
                            .append(" pageBytes=").append(bytes(frame, "page").length);
                } else {
                    out.append(" totalRecords=").append(frame.get("totalRecords"))
                            .append(" totalEntries=").append(frame.get("totalEntries"));
                }
                out.append('\n');
            }
            out.append("blocks=").append(block).append(" records=").append(block).append('\n');
        }
        return out.toString();
    }

    private static DataFileReader<GenericRecord> openFull(Path path) throws IOException {
        DataFileReader<GenericRecord> file = new DataFileReader<>(
                path.toFile(), new GenericDatumReader<>(SCHEMA));
        try {
            validateMetadata(path, file);
            return file;
        } catch (IOException | RuntimeException failure) {
            file.close();
            throw failure;
        }
    }

    private static DataFileReader<GenericRecord> openProjected(Path path) throws IOException {
        DataFileReader<GenericRecord> file = new DataFileReader<>(path.toFile(),
                new GenericDatumReader<>(SCHEMA, HEADER_SCHEMA));
        try {
            validateMetadata(path, file);
            return file;
        } catch (IOException | RuntimeException failure) {
            file.close();
            throw failure;
        }
    }

    private static void validateMetadata(Path path, DataFileReader<GenericRecord> file)
            throws IOException {
        if (!FORMAT.equals(file.getMetaString(FORMAT_META))) {
            throw corrupt(path, "missing or unsupported " + FORMAT_META);
        }
        if (!"null".equals(file.getMetaString("avro.codec"))) {
            throw corrupt(path, "Avro codec must be null");
        }
    }

    private static void validateSeal(Path path, GenericRecord frame, long pages, long entries,
            byte[] previousMax) throws IOException {
        long declaredPages = (long) frame.get("totalRecords");
        long declaredEntries = (long) frame.get("totalEntries");
        byte[] lastKey = bytes(frame, "lastKey");
        if (declaredPages != pages || declaredEntries != entries
                || !Arrays.equals(lastKey, previousMax == null ? new byte[0] : previousMax)) {
            throw corrupt(path, "SEAL totals or last key do not match preceding PAGE records");
        }
    }

    private static byte[] bytes(GenericRecord record, String field) {
        ByteBuffer source = ((ByteBuffer) record.get(field)).duplicate();
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        return bytes;
    }

    private static IOException corrupt(Path path, String message) {
        return new IOException("Avro page-run segment " + path + ": " + message);
    }

    private static IOException corrupt(Path path, String message, Throwable cause) {
        return new IOException("Avro page-run segment " + path + ": " + message, cause);
    }

    record WriteResult(long headerEnd, List<Long> pageBoundaries, long sealBoundary) {
    }

    record HeaderSummary(long pages, long entries) {
    }

    record ResyncResult(long alignedAt, String kind, byte[] minKey) {
    }

    static final class Writer implements Closeable {

        private final Path path;
        private final DataFileWriter<GenericRecord> file;
        private final List<Long> pageBoundaries = new ArrayList<>();
        private final long headerEnd;
        private long totalEntries;
        private long totalRecords;
        private byte[] lastKey = new byte[0];
        private boolean sealed;

        Writer(Path path, PageCodec pageCodec) throws IOException {
            this.path = Objects.requireNonNull(path, "path");
            this.file = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA));
            file.setCodec(CodecFactory.nullCodec());
            file.setMeta(FORMAT_META, FORMAT);
            file.setMeta(PAGE_CODEC_META, pageCodec.name().toLowerCase(java.util.Locale.ROOT));
            try {
                file.create(SCHEMA, path.toFile());
                headerEnd = file.sync();
            } catch (IOException | RuntimeException failure) {
                file.close();
                throw failure;
            }
        }

        void append(PageBlock page) throws IOException {
            requireOpen();
            byte[] body = page.serialize();
            PageBlockCodec.Header header = PageBlockCodec.parseHeader(body);
            GenericRecord frame = baseFrame("PAGE");
            frame.put("page", ByteBuffer.wrap(body));
            frame.put("minKey", ByteBuffer.wrap(header.minKey()));
            frame.put("maxKey", ByteBuffer.wrap(header.maxKey()));
            frame.put("count", header.count());
            frame.put("rawPayloadLength", header.rawPayloadLength());
            file.append(frame);
            pageBoundaries.add(file.sync());
            totalRecords++;
            totalEntries += header.count();
            lastKey = header.maxKey();
        }

        WriteResult seal() throws IOException {
            requireOpen();
            GenericRecord frame = baseFrame("SEAL");
            frame.put("totalEntries", totalEntries);
            frame.put("totalRecords", totalRecords);
            frame.put("lastKey", ByteBuffer.wrap(lastKey));
            file.append(frame);
            long sealBoundary = file.sync();
            file.close();
            sealed = true;
            force(path);
            return new WriteResult(headerEnd, List.copyOf(pageBoundaries), sealBoundary);
        }

        private GenericRecord baseFrame(String kind) {
            GenericRecord frame = new GenericData.Record(SCHEMA);
            frame.put("kind", new GenericData.EnumSymbol(KIND_SCHEMA, kind));
            frame.put("page", ByteBuffer.wrap(new byte[0]));
            frame.put("minKey", ByteBuffer.wrap(new byte[0]));
            frame.put("maxKey", ByteBuffer.wrap(new byte[0]));
            frame.put("count", 0);
            frame.put("rawPayloadLength", 0);
            frame.put("totalEntries", -1L);
            frame.put("totalRecords", -1L);
            frame.put("lastKey", ByteBuffer.wrap(new byte[0]));
            return frame;
        }

        private void requireOpen() {
            if (sealed) {
                throw new IllegalStateException("Avro page-run writer is sealed");
            }
        }

        @Override
        public void close() throws IOException {
            if (!sealed) {
                file.close();
                sealed = true;
            }
        }
    }

    static final class Reader implements Closeable {

        private final Path path;
        private final DataFileReader<GenericRecord> file;
        private long pages;
        private long entries;
        private byte[] previousMax;
        private boolean done;

        Reader(Path path) throws IOException {
            this.path = path;
            this.file = openFull(path);
        }

        PageBlock nextPage() throws IOException {
            if (done) {
                return null;
            }
            if (!file.hasNext()) {
                throw corrupt(path, "missing final SEAL record");
            }
            GenericRecord frame = file.next();
            String kind = frame.get("kind").toString();
            if (kind.equals("SEAL")) {
                validateSeal(path, frame, pages, entries, previousMax);
                if (file.hasNext()) {
                    throw corrupt(path, "SEAL is not the last OCF record");
                }
                done = true;
                return null;
            }
            if (!kind.equals("PAGE")) {
                throw corrupt(path, "unknown frame kind " + kind);
            }
            byte[] body = bytes(frame, "page");
            PageBlockCodec.Header header;
            try {
                header = PageRunSegmentIo.parsePageHeader(body);
            } catch (IllegalArgumentException failure) {
                throw corrupt(path, "malformed PageBlock", failure);
            }
            if (!Arrays.equals(header.minKey(), bytes(frame, "minKey"))
                    || !Arrays.equals(header.maxKey(), bytes(frame, "maxKey"))
                    || header.count() != (int) frame.get("count")
                    || header.rawPayloadLength() != (int) frame.get("rawPayloadLength")) {
                throw corrupt(path, "PageBlock metadata disagrees with OCF record metadata");
            }
            if (previousMax != null
                    && Arrays.compareUnsigned(previousMax, header.minKey()) >= 0) {
                throw corrupt(path, "PAGE ranges overlap or regress");
            }
            pages++;
            entries += header.count();
            previousMax = header.maxKey();
            return PageBlockCodec.deserialize(body, header, path);
        }

        @Override
        public void close() throws IOException {
            file.close();
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
}
