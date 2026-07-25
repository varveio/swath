/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.benchmarks;

import io.varve.swath.output.ControlCharEscaper;
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
 * The designated performance-benchmark suite for {@link ControlCharEscaper#escape(String)},
 * covering both the no-alloc fast path (clean ASCII returns the same String instance) and the
 * escape path (control characters present; builds a StringBuilder).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ControlCharEscaperBench {

    /** Clean ASCII object key; fast path returns the same String instance, no allocation. */
    String cleanKey;

    /**
     * Key with control characters: NUL (0x00), LF (0x0A), TAB (0x09),
     * ESC (0x1B), DEL (0x7F). Escape path builds a new String via StringBuilder.
     * Assembled via char casts at @Setup time so the source file contains no
     * literal control-character bytes.
     */
    String dirtyKey;

    @Setup(Level.Trial)
    public void setup() {
        cleanKey = "bucket/logs/2024-01-15/events-aggregated.json.gz";
        // Build via char casts — keeps source clean; exercised by escapePath
        dirtyKey = "bucket/logs" + (char) 0x00
                + "/2024" + (char) 0x0A        // LF
                + "01" + (char) 0x09           // TAB
                + "events" + (char) 0x1B       // ESC
                + ".json" + (char) 0x7F;       // DEL
    }

    /**
     * Fast path: no control characters; escape() returns the input instance unchanged.
     * The common case in production (most S3 keys are clean ASCII/UTF-8).
     */
    @Benchmark
    public void fastPath(Blackhole bh) {
        bh.consume(ControlCharEscaper.escape(cleanKey));
    }

    /**
     * Escape path: NUL, LF, TAB, ESC, and DEL present; escape() allocates a
     * StringBuilder and returns the escaped result.
     */
    @Benchmark
    public void escapePath(Blackhole bh) {
        bh.consume(ControlCharEscaper.escape(dirtyKey));
    }
}
