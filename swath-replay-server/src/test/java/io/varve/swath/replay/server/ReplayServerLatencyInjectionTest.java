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
