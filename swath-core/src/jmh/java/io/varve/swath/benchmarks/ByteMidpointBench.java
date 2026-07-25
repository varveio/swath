/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.benchmarks;

import io.varve.swath.model.ByteMidpoint;
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
 * The designated performance-benchmark suite for {@link ByteMidpoint#between(byte[], byte[])},
 * over representative bound-pair shapes.
 *
 * <ul>
 *   <li>{@code adjacentDense} — "a" vs "z", many safe scalars available</li>
 *   <li>{@code straddleC0Block} — lower bound is below the safe range (U+001F),
 *       upper bound is well above (U+0041 'A')</li>
 *   <li>{@code controlSliver} — empty vs U+0001: only unsafe scalars lie between,
 *       {@code between()} returns {@code null}</li>
 *   <li>{@code highByteUtf8} — two multi-byte (>= 0x80) code points</li>
 *   <li>{@code longKeys} — near-1024-byte ASCII keys diverging near the midpoint; the
 *       pivot stays far under the 1024-byte cap, so this exercises decode/common-prefix-scan
 *       cost, not cap-fallback</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ByteMidpointBench {

    // Adjacent dense bounds — plenty of safe scalars between 'a' and 'z'
    byte[] denseA;
    byte[] denseB;

    // Straddle C0 block: lo = U+001F (below safe range), hi = U+0041 'A'
    byte[] c0A;
    byte[] c0B;

    // Control sliver: empty < U+0001 — no safe scalar between them; between() returns null
    byte[] sliverA;
    byte[] sliverB;

    // High-byte UTF-8: U+00A0 (NBSP, 2 bytes) vs U+00FF (ÿ, 2 bytes)
    byte[] highByteA;
    byte[] highByteB;

    // Near-1024-byte ASCII keys diverging near the midpoint — the pivot stays far under
    // the 1024-byte cap; this exercises decode/scan cost, not cap-fallback
    byte[] longA;
    byte[] longB;

    @Setup(Level.Trial)
    public void setup() {
        denseA = "a".getBytes(StandardCharsets.UTF_8);
        denseB = "z".getBytes(StandardCharsets.UTF_8);

        // U+001F (0x1F) < U+0041 'A' (0x41); between() returns U+0030 '0'
        c0A = new byte[]{0x1F};
        c0B = "A".getBytes(StandardCharsets.UTF_8);

        // empty < U+0001: only unsafe scalars (C0 controls) between them; returns null
        sliverA = new byte[0];
        sliverB = new byte[]{0x01};

        // U+00A0 (0xC2 0xA0) vs U+00FF (0xC3 0xBF) — multi-byte code points
        highByteA = " ".getBytes(StandardCharsets.UTF_8);
        highByteB = "ÿ".getBytes(StandardCharsets.UTF_8);

        // Build ~1022-byte ASCII keys that differ at position 510
        StringBuilder sbA = new StringBuilder(1022);
        StringBuilder sbB = new StringBuilder(1022);
        for (int i = 0; i < 510; i++) {
            sbA.append('a');
            sbB.append('a');
        }
        sbA.append('m');   // lower divergence char at position 510
        sbB.append('z');   // higher divergence char at position 510
        for (int i = 0; i < 511; i++) {
            sbA.append('a');
            sbB.append('a');
        }
        longA = sbA.toString().getBytes(StandardCharsets.UTF_8);
        longB = sbB.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Adjacent dense bounds with many safe scalars available. */
    @Benchmark
    public void adjacentDense(Blackhole bh) {
        bh.consume(ByteMidpoint.between(denseA, denseB));
    }

    /** Lower bound below the safe range (C0 block), upper bound well above it. */
    @Benchmark
    public void straddleC0Block(Blackhole bh) {
        bh.consume(ByteMidpoint.between(c0A, c0B));
    }

    /**
     * Control sliver: only unsafe scalars between the bounds; between() returns
     * {@code null}. {@code bh.consume(null)} is valid JMH usage.
     */
    @Benchmark
    public void controlSliver(Blackhole bh) {
        bh.consume(ByteMidpoint.between(sliverA, sliverB));
    }

    /** Multi-byte (>= 0x80) UTF-8 code points — exercises the high-byte path. */
    @Benchmark
    public void highByteUtf8(Blackhole bh) {
        bh.consume(ByteMidpoint.between(highByteA, highByteB));
    }

    /**
     * Near-1024-byte ASCII keys diverging near the midpoint — the pivot stays far under the
     * cap, so this exercises decode/common-prefix-scan cost, not cap-fallback.
     */
    @Benchmark
    public void longKeys(Blackhole bh) {
        bh.consume(ByteMidpoint.between(longA, longB));
    }
}
