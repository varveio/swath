/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.replay.testkit.HttpProbe;
import io.varve.swath.testkit.LatencyModels;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * {@link ReplayServer}/{@link ReplayHandler}'s optional per-request latency injection,
 * reusing swath-core's canned {@link LatencyModels} profiles via {@link ReplayLatencyAdapter}.
 * Default-off is already covered by every other {@code ReplayServerTest} case (none of them pass
 * a latency function); this file only proves the opt-in path actually delays the HTTP response.
 */
class ReplayServerLatencyInjectionTest {

    @Test
    void absorbsAFixtureReadIntoTheInjectedDelayRatherThanAddingToIt() throws Exception {
        Duration profile = Duration.ofMillis(300);
        Duration readCost = Duration.ofMillis(150);
        ReplayLatencyAdapter latency = new ReplayLatencyAdapter(
                LatencyModels.uniformFast(1L, profile, profile));

        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket",
                slowFixture(readCost), ReplayServerFixtureConfig.DEFAULT.withLatency(latency))) {
            server.start();

            long start = System.nanoTime();
            HttpResponse<String> response = HttpProbe.response(server, "/bucket?list-type=2");
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            assertThat(response.statusCode()).isEqualTo(200);
            // The profile says this backend answers in 300ms. A fixture that took 150ms of that
            // must still answer in ~300ms, not 450ms -- otherwise every client is charged the
            // server's own cost on top of the cost it was told to simulate, and charged a
            // different amount depending on how expensive its own access pattern happens to be.
            assertThat(elapsed).isGreaterThanOrEqualTo(profile);
            assertThat(elapsed).isLessThan(profile.plus(readCost));
        }
    }

    @Test
    void countsAnOverrunWhenTheServerIsSlowerThanTheProfileItImitates() throws Exception {
        Duration profile = Duration.ofMillis(50);
        Duration readCost = Duration.ofMillis(250);
        ReplayLatencyAdapter latency = new ReplayLatencyAdapter(
                LatencyModels.uniformFast(1L, profile, profile));

        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket",
                slowFixture(readCost), ReplayServerFixtureConfig.DEFAULT.withLatency(latency))) {
            server.start();

            HttpProbe.response(server, "/bucket?list-type=2");

            // Past the profile there is nothing to wait out, and the client is now observing the
            // server rather than the backend. Counted, so a run can be told apart afterwards from
            // one that stayed inside its profile.
            assertThat(server.metrics().registry()
                    .find("swath.replay.inject.overrun").tag("shape", "worker_page").counter())
                    .isNotNull()
                    .satisfies(counter -> assertThat(counter.count()).isEqualTo(1.0));
        }
    }

    @Test
    void countsNoOverrunWhenInjectionIsOff() throws Exception {
        try (ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket",
                slowFixture(Duration.ofMillis(80)))) {
            server.start();

            HttpProbe.response(server, "/bucket?list-type=2");

            // No profile means no floor to overrun. A slow read with injection off is just a slow
            // read, and must not be reported as a fidelity failure.
            assertThat(server.metrics().registry().find("swath.replay.inject.overrun").counter())
                    .isNull();
        }
    }

    /** A fixture whose read costs a known amount, so the delay arithmetic is observable. */
    private static io.varve.swath.replay.protocol.ListingFixture slowFixture(Duration readCost) {
        return request -> {
            try {
                Thread.sleep(readCost.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return new S3ListResult(request, List.of(new S3ResultEntry.ObjectResult(
                    new ListedObject(bytes("a/1"), 1, 0, "etag", "STANDARD", null, null, null, null))),
                    false, null);
        };
    }

    @Test
    void injectsConfiguredLatencyBeforeRespondingWhenALatencyModelIsWired() throws Exception {
        Duration delay = Duration.ofMillis(150);
        ReplayLatencyAdapter latency = new ReplayLatencyAdapter(
                LatencyModels.uniformFast(1L, delay, delay));

        try (ReplayServer server = new ReplayServer(
                "127.0.0.1", 0, "bucket", request -> new S3ListResult(request, List.of(
                new S3ResultEntry.ObjectResult(new ListedObject(bytes("a/1"), 1, 0, "etag", "STANDARD",
                        null, null, null, null))),
                false, null),
                ReplayServerFixtureConfig.DEFAULT.withLatency(latency))) {
            server.start();

            long start = System.nanoTime();
            HttpResponse<String> response = HttpProbe.response(server, "/bucket?list-type=2");
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(elapsed).isGreaterThanOrEqualTo(delay);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
