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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsEndpointTest {

    @TempDir
    Path temp;

    @Test
    void servesTheRegistryAsJsonOnItsOwnPort() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "sorted", System.nanoTime())) {

                HttpProbe.response(server, "/bucket?list-type=2");
                String body = scrape(endpoint, "/metrics");

                assertThat(body).contains("\"schema_version\":1");
                assertThat(body).contains("\"serving_mode\":\"sorted\"");
                assertThat(body).contains("\"swath.replay.http.requests\"");
                assertThat(body).contains("\"swath.replay.fixture.list.latency\"");
                // The timer fields a headroom check reads, present and typed as numbers.
                assertThat(body).containsPattern("\"name\":\"swath.replay.page.read.latency\".*?\"p99_ms\":");
            }
        }
    }

    @Test
    void scrapingDoesNotPerturbWhatItMeasures() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "duckdb", System.nanoTime())) {

                HttpProbe.response(server, "/bucket?list-type=2");
                long afterOneList = server.metrics().snapshot().httpRequests();
                scrape(endpoint, "/metrics");
                scrape(endpoint, "/metrics");

                // The whole reason the endpoint has its own connector: a harness may poll it
                // through a measured window without becoming part of the measurement.
                assertThat(server.metrics().snapshot().httpRequests()).isEqualTo(afterOneList);
            }
        }
    }

    @Test
    void separatesRequestLatencyByShape() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "sorted", System.nanoTime())) {

                HttpProbe.response(server, "/bucket?list-type=2");                    // worker page
                HttpProbe.response(server, "/bucket?list-type=2&max-keys=1");         // pivot probe
                HttpProbe.response(server, "/bucket?list-type=2&delimiter=%2F");      // structure probe
                String body = scrape(endpoint, "/metrics");

                // One timer per shape, so an average is never taken over a mixture that belongs to
                // the client rather than to the server.
                assertThat(body).contains("\"shape\":\"worker_page\"");
                assertThat(body).contains("\"shape\":\"pivot_probe\"");
                assertThat(body).contains("\"shape\":\"structure_probe\"");
            }
        }
    }

    @Test
    void answersHealthzAndRefusesAnythingElse() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "sorted", System.nanoTime())) {

                assertThat(scrape(endpoint, "/healthz")).isEqualTo("ok\n");
                assertThat(status(endpoint, "/bucket?list-type=2")).isEqualTo(404);
            }
        }
    }

    @Test
    void servesRuntimeAttestationWithoutPerturbingListingCounters() throws Exception {
        Path proc = Files.createDirectories(temp.resolve("proc/self"));
        Path cgroup = Files.createDirectories(temp.resolve("cgroup/server"));
        Files.writeString(proc.resolve("cgroup"), "0::/server\n");
        Files.writeString(proc.resolve("status"), "Cpus_allowed_list:\t1-2\n");
        Files.writeString(cgroup.resolve("cpuset.cpus.effective"), "1-2\n");
        Files.writeString(cgroup.resolve("memory.max"), "1073741824\n");
        Files.writeString(cgroup.resolve("memory.swap.max"), "max\n");
        RuntimeAttestation attestation = new RuntimeAttestation(
                proc.resolve("cgroup"), proc.resolve("status"), temp.resolve("cgroup"));

        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "sorted", System.nanoTime(), attestation)) {
                long before = server.metrics().snapshot().httpRequests();

                String body = scrape(endpoint, "/runtime-attestation");

                assertThat(body).contains("\"schema_version\":\"runtime-attestation-v1\"");
                assertThat(body).contains("\"cpuset_cpus_effective\":{\"value\":\"1-2\","
                        + "\"error\":null}");
                assertThat(server.metrics().snapshot().httpRequests()).isEqualTo(before);
            }
        }
    }

    private static ReplayServer server() {
        return new ReplayServer("127.0.0.1", 0, "bucket", request -> new S3ListResult(request, List.of(
                new S3ResultEntry.ObjectResult(new ListedObject(
                        "a/1".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        1, 0, "etag", "STANDARD", null, null, null, null))),
                false, null));
    }

    private static String scrape(MetricsEndpoint endpoint, String path) throws Exception {
        return send(endpoint, path).body();
    }

    private static int status(MetricsEndpoint endpoint, String path) throws Exception {
        return send(endpoint, path).statusCode();
    }

    private static HttpResponse<String> send(MetricsEndpoint endpoint, String path) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + endpoint.port() + path)).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }

    /**
     * The sidecar case: a consumer in the container beside this one cannot read the server's cgroup,
     * so if these are not on the wire they are not obtainable at all, and "was the server saturated?"
     * goes back to being inferred from throughput times service time.
     *
     * <p>{@code process.cpu.usage} is asserted to be a real number rather than merely present:
     * micrometer publishes the gauge whether or not the platform bean can answer it, and a NaN
     * serialises to a shape a reader would take for a measurement.
     */
    @Test
    void publishesTheProcessOwnCpuHeapAndThreadsForASidecarConsumer() throws Exception {
        try (ReplayServer server = server()) {
            server.start();
            try (MetricsEndpoint endpoint = MetricsEndpoint.start("127.0.0.1", 0,
                    server.metrics().registry(), "sorted", System.nanoTime())) {

                HttpProbe.response(server, "/bucket?list-type=2");
                String body = scrape(endpoint, "/metrics");

                assertThat(body).contains("\"process.cpu.usage\"");
                assertThat(body).contains("\"system.cpu.count\"");
                assertThat(body).contains("\"jvm.memory.used\"");
                assertThat(body).contains("\"jvm.threads.live\"");

                assertThat(body).doesNotContain("NaN");
                // Not [^}]* -- an empty tag map serialises as `"tags":{}` and ends the class early.
                assertThat(body).containsPattern("\"name\":\"process.cpu.usage\".*?\"value\":-?[0-9]");
                assertThat(body).containsPattern("\"name\":\"system.cpu.count\".*?\"value\":[0-9]");
            }
        }
    }
}
