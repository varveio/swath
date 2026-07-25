/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.benchmarks;

import io.varve.swath.model.KeyBytes;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * The designated performance-benchmark suite for {@link KeyBytes#compareUnsigned(byte[], byte[])},
 * over representative key-pair shapes.
 *
 * <ul>
 *   <li>{@code lastByteDiffers} — equal-length keys differing in a single byte, five bytes before the end</li>
 *   <li>{@code highByteUtf8} — two CJK ideographs (3-byte UTF-8, bytes &ge; 0x80)</li>
 *   <li>{@code prefixRelationship} — shorter key is a byte-prefix of longer key</li>
 *   <li>{@code supplementaryPlane} — two emoticons encoded as 4-byte UTF-8 sequences</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class CompareUnsignedBench {

    // equal-length keys differing in a single byte, five bytes before the end
    byte[] lastByteA;
    byte[] lastByteB;

    // high byte (>= 0x80): CJK ideographs U+4E00 (一) and U+4E01 (丁), 3-byte UTF-8
    byte[] highByteA;
    byte[] highByteB;

    // different lengths: "prefix/" is a proper byte-prefix of "prefix/child"
    byte[] prefixA;
    byte[] prefixB;

    // supplementary-plane: U+1F600 (😀) and U+1F601 (😁), 4-byte UTF-8
    byte[] suppA;
    byte[] suppB;

    @Setup(Level.Trial)
    public void setup() {
        lastByteA = "logs/2024-01/events-a.json".getBytes(StandardCharsets.UTF_8);
        lastByteB = "logs/2024-01/events-z.json".getBytes(StandardCharsets.UTF_8);

        highByteA = "一".getBytes(StandardCharsets.UTF_8);
        highByteB = "丁".getBytes(StandardCharsets.UTF_8);

        prefixA = "data/objects/".getBytes(StandardCharsets.UTF_8);
        prefixB = "data/objects/report.csv".getBytes(StandardCharsets.UTF_8);

        // Java: surrogate pair for U+1F600 / U+1F601
        suppA = "😀".getBytes(StandardCharsets.UTF_8);
        suppB = "😁".getBytes(StandardCharsets.UTF_8);
    }

    /** Equal-length keys differing in a single byte, five bytes before the end. */
    @Benchmark
    public void lastByteDiffers(Blackhole bh) {
        bh.consume(KeyBytes.compareUnsigned(lastByteA, lastByteB));
    }

    /** High-byte (>= 0x80) CJK keys — 3-byte UTF-8 code units. */
    @Benchmark
    public void highByteUtf8(Blackhole bh) {
        bh.consume(KeyBytes.compareUnsigned(highByteA, highByteB));
    }

    /** Prefix relationship: shorter key is a byte-exact prefix of the longer one. */
    @Benchmark
    public void prefixRelationship(Blackhole bh) {
        bh.consume(KeyBytes.compareUnsigned(prefixA, prefixB));
    }

    /** Supplementary-plane keys — 4-byte UTF-8 code units. */
    @Benchmark
    public void supplementaryPlane(Blackhole bh) {
        bh.consume(KeyBytes.compareUnsigned(suppA, suppB));
    }
}
