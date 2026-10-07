/*
 * Copyright 2026 Varve Systems Ltd
 * SPDX-License-Identifier: Apache-2.0
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.sun.management.OperatingSystemMXBean;

import java.io.BufferedWriter;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Isolated native Zstd comparison over the exact encoded payloads extracted by Go. */
public final class CodecInterop {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final OperatingSystemMXBean CPU =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private static final class Block {
        final ObjectNode metadata;
        final byte[] raw;
        final byte[] encoded;
        int length;

        Block(ObjectNode metadata, byte[] raw) {
            this.metadata = metadata;
            this.raw = raw;
            this.encoded = new byte[Math.toIntExact(Zstd.compressBound(raw.length))];
        }
    }

    private CodecInterop() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException(
                    "bench|verify BLOCKS_JSONL FRAMES_JSONL [warmup iterations]");
        }
        switch (args[0]) {
            case "bench" ->
                    bench(
                            Path.of(args[1]),
                            Path.of(args[2]),
                            args.length > 3 ? Integer.parseInt(args[3]) : 10,
                            args.length > 4 ? Integer.parseInt(args[4]) : 20);
            case "verify" -> verify(Path.of(args[1]), Path.of(args[2]));
            case "source-verify" -> verifySource(Path.of(args[1]), Path.of(args[2]));
            default -> throw new IllegalArgumentException("unknown codec mode " + args[0]);
        }
    }

    private static List<ObjectNode> records(Path path) throws Exception {
        List<ObjectNode> records = new ArrayList<>();
        try (var lines = Files.lines(path)) {
            for (String line : (Iterable<String>) lines::iterator) {
                records.add((ObjectNode) JSON.readTree(line));
            }
        }
        return records;
    }

    private static byte[] bytes(ObjectNode record) {
        var field = record.get("data");
        return field == null || field.isNull()
                ? new byte[0]
                : Base64.getDecoder().decode(field.asText());
    }

    private static long pass(ZstdCompressCtx codec, List<Block> blocks) {
        long size = 0;
        for (Block block : blocks) {
            block.length =
                    codec.compressByteArray(
                            block.encoded, 0, block.encoded.length, block.raw, 0, block.raw.length);
            size += block.length;
        }
        return size;
    }

    private static Map<String, Long> timing(long wall, long cpu, long bytes) {
        return Map.of("wall_ns", wall, "cpu_ns", cpu, "compressed_bytes", bytes);
    }

    private static void bench(Path input, Path frames, int warmup, int iterations)
            throws Exception {
        if (warmup < 0 || iterations <= 0) {
            throw new IllegalArgumentException("invalid iteration counts");
        }
        List<Block> blocks = new ArrayList<>();
        Map<String, List<Block>> byColumn = new TreeMap<>();
        long rawBytes = 0;
        for (ObjectNode record : records(input)) {
            byte[] raw = bytes(record);
            if (raw.length != record.path("expected_bytes").asInt(-1)) {
                throw new IllegalArgumentException("extracted raw length mismatch");
            }
            Block block = new Block(record, raw);
            blocks.add(block);
            byColumn.computeIfAbsent(record.path("column").asText(), ignored -> new ArrayList<>())
                    .add(block);
            rawBytes += raw.length;
        }
        if (blocks.isEmpty()) {
            throw new IllegalArgumentException("empty block corpus");
        }
        List<Map<String, Long>> runs = new ArrayList<>();
        Map<String, Map<String, Long>> columns = new LinkedHashMap<>();
        try (ZstdCompressCtx codec = new ZstdCompressCtx().setLevel(3).setChecksum(false)) {
            for (int iteration = 0; iteration < warmup; iteration++) {
                pass(codec, blocks);
            }
            for (int iteration = 0; iteration < iterations; iteration++) {
                long cpu = CPU.getProcessCpuTime();
                long start = System.nanoTime();
                long size = pass(codec, blocks);
                long wall = System.nanoTime() - start;
                long elapsedCPU = CPU.getProcessCpuTime() - cpu;
                runs.add(timing(wall, elapsedCPU, size));
            }
            for (var column : byColumn.entrySet()) {
                for (int iteration = 0; iteration < warmup; iteration++) {
                    pass(codec, column.getValue());
                }
                long cpu = CPU.getProcessCpuTime();
                long start = System.nanoTime();
                long size = 0;
                for (int iteration = 0; iteration < iterations; iteration++) {
                    size = pass(codec, column.getValue());
                }
                long wall = System.nanoTime() - start;
                long elapsedCPU = CPU.getProcessCpuTime() - cpu;
                columns.put(
                        column.getKey(), timing(wall / iterations, elapsedCPU / iterations, size));
            }
        }
        try (ZstdDecompressCtx decoder = new ZstdDecompressCtx()) {
            for (Block block : blocks) {
                byte[] decoded = new byte[block.raw.length];
                int length =
                        decoder.decompressByteArray(
                                decoded, 0, decoded.length, block.encoded, 0, block.length);
                if (length != decoded.length || !Arrays.equals(decoded, block.raw)) {
                    throw new IllegalStateException("native codec round-trip mismatch");
                }
            }
        }
        try (BufferedWriter writer = Files.newBufferedWriter(frames)) {
            for (Block block : blocks) {
                ObjectNode record = block.metadata.deepCopy();
                record.put(
                        "data",
                        Base64.getEncoder()
                                .encodeToString(Arrays.copyOf(block.encoded, block.length)));
                writer.write(JSON.writeValueAsString(record));
                writer.newLine();
            }
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("codec", "zstd-jni reused ZstdCompressCtx level 3, checksum false");
        report.put("warmup", warmup);
        report.put("iterations", iterations);
        report.put("blocks", blocks.size());
        report.put("raw_bytes", rawBytes);
        report.put("runs", runs);
        report.put("columns", columns);
        report.put("validation", "every compressed block round-trips byte-exact outside clocks");
        report.put(
                "timing",
                "total per-iteration runs; column wall/CPU means over batched iterations; loading,"
                    + " allocation, decoding, JSON excluded; JVM process CPU clock may be coarse");
        System.out.println(JSON.writeValueAsString(report));
    }

    private static void verify(Path rawPath, Path framesPath) throws Exception {
        List<ObjectNode> raw = records(rawPath);
        List<ObjectNode> frames = records(framesPath);
        if (raw.isEmpty() || raw.size() != frames.size()) {
            throw new IllegalArgumentException("frame/block count mismatch");
        }
        try (ZstdDecompressCtx decoder = new ZstdDecompressCtx()) {
            for (int index = 0; index < raw.size(); index++) {
                ObjectNode original = raw.get(index);
                ObjectNode frame = frames.get(index);
                if (!original.path("column").equals(frame.path("column"))
                        || !original.path("kind").equals(frame.path("kind"))) {
                    throw new IllegalArgumentException("frame/block identity mismatch");
                }
                byte[] expected = bytes(original);
                byte[] encoded = bytes(frame);
                byte[] decoded = new byte[expected.length];
                int length =
                        decoder.decompressByteArray(
                                decoded, 0, decoded.length, encoded, 0, encoded.length);
                if (length != decoded.length || !Arrays.equals(decoded, expected)) {
                    throw new IllegalStateException("cross-codec mismatch at block " + index);
                }
            }
        }
        System.out.println(
                JSON.writeValueAsString(
                        Map.of(
                                "verified",
                                true,
                                "blocks",
                                raw.size(),
                                "decoder",
                                "native zstd-jni",
                                "frames",
                                framesPath.toString())));
    }

    private static void verifySource(Path blocksPath, Path parquetPath) throws Exception {
        List<ObjectNode> blocks = records(blocksPath);
        long rawBytes = 0;
        try (RandomAccessFile source = new RandomAccessFile(parquetPath.toFile(), "r");
                ZstdDecompressCtx decoder = new ZstdDecompressCtx()) {
            for (ObjectNode block : blocks) {
                byte[] expected = bytes(block);
                int declared = block.path("expected_bytes").asInt(-1);
                int length = block.path("source_value_bytes").asInt(-1);
                long offset =
                        block.path("page_offset").asLong(-1)
                                + block.path("page_header_bytes").asInt(-1)
                                + block.path("excluded_level_bytes").asInt(-1);
                if (declared != expected.length
                        || length < 0
                        || offset < 4
                        || offset > source.length()
                        || length > source.length() - offset) {
                    throw new IllegalArgumentException(
                            "source block extent/decoded length mismatch");
                }
                source.seek(offset);
                byte[] original = new byte[length];
                source.readFully(original);
                byte[] decoded = original;
                if (block.path("source_compressed").asBoolean()) {
                    decoded = new byte[declared];
                    int count =
                            decoder.decompressByteArray(
                                    decoded, 0, decoded.length, original, 0, original.length);
                    if (count != declared) {
                        throw new IllegalStateException("native original-body length mismatch");
                    }
                }
                if (!Arrays.equals(decoded, expected)) {
                    throw new IllegalStateException(
                            "native original-body decode disagrees with extraction");
                }
                rawBytes += decoded.length;
            }
        }
        System.out.println(
                JSON.writeValueAsString(
                        Map.of(
                                "verified",
                                true,
                                "blocks",
                                blocks.size(),
                                "raw_bytes",
                                rawBytes,
                                "validation",
                                "native decoder independently reads original Parquet compressed"
                                    + " value bodies and checks extracted bytes and declared"
                                    + " lengths")));
    }
}
