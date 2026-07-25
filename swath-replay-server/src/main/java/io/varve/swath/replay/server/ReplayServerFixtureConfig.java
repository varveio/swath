/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import java.time.Duration;
import java.util.function.BiFunction;

/**
 * The optional wiring for a {@link ReplayServer} built directly around an already-constructed
 * {@code ListingFixture} — the fixture the server owns and closes, the concurrency permit count,
 * and per-request latency injection. This is the direct-injection seam the tests exercise (the
 * production path opens its fixture through {@link ReplayServingFactory} instead), so it is
 * package-private, never a public surface. Callers pass {@link #DEFAULT} with the single knob they
 * vary derived via a {@code withX} — e.g. {@code DEFAULT.withMaxConcurrentReads(2)}.
 *
 * @param ownedFixture an {@code AutoCloseable} the server closes on shutdown ({@code null} = none)
 * @param maxConcurrentReads the fixture-read concurrency bound ({@code 0} = unbounded)
 * @param latency per-request injected delay, seeing the request and the result it produced
 *                (default {@code Duration.ZERO}, i.e. off)
 */
record ReplayServerFixtureConfig(AutoCloseable ownedFixture, int maxConcurrentReads,
                                 BiFunction<S3ListRequest, S3ListResult, Duration> latency) {

    /** No owned fixture, unbounded reads, no latency injection. */
    static final ReplayServerFixtureConfig DEFAULT =
            new ReplayServerFixtureConfig(null, 0, (req, result) -> Duration.ZERO);

    ReplayServerFixtureConfig withOwnedFixture(AutoCloseable ownedFixture) {
        return new ReplayServerFixtureConfig(ownedFixture, maxConcurrentReads, latency);
    }

    ReplayServerFixtureConfig withMaxConcurrentReads(int maxConcurrentReads) {
        return new ReplayServerFixtureConfig(ownedFixture, maxConcurrentReads, latency);
    }

    ReplayServerFixtureConfig withLatency(BiFunction<S3ListRequest, S3ListResult, Duration> latency) {
        return new ReplayServerFixtureConfig(ownedFixture, maxConcurrentReads, latency);
    }
}
