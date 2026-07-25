/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.varve.swath.engine.TransientRetryFetcher;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.protocol.ListingFixture;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.s3.S3ClientFactory;
import io.varve.swath.store.s3.S3Config;
import io.varve.swath.store.s3.S3PageFetcher;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The response-side entry bound, driven end to end: a replay server standing in for an endpoint
 * that ignores the {@code MaxKeys} it was sent, against swath's real {@link S3PageFetcher}. The
 * page must be refused outright — a listing swath cannot trust is not silently truncated, and the
 * refusal is not retried, so a permanently misbehaving endpoint cannot be turned into a spin.
 */
class OversizedPageRefusalIT {

    @Test
    void refusesPageCarryingMoreObjectsThanTheRequestedMaxKeys() throws Exception {
        try (ReplayServer server = server(overServing(3, 0));
             S3Client client = client(server)) {
            S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

            assertThatThrownBy(() -> fetcher.fetchPage(PageRequest.objects(null, null, 2)))
                    .isInstanceOf(ProtocolViolationException.class)
                    .hasMessageContaining("returned 3 entries (3 keys + 0 common prefixes)")
                    .hasMessageContaining("max_keys=2");
        }
    }

    @Test
    void countsCommonPrefixesTowardTheMaxKeysBound() throws Exception {
        try (ReplayServer server = server(overServing(2, 1));
             S3Client client = client(server)) {
            S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

            assertThatThrownBy(() -> fetcher.fetchPage(
                    PageRequest.objectsDelimited(null, bytes("/"), null, 2)))
                    .isInstanceOf(ProtocolViolationException.class)
                    .hasMessageContaining("returned 3 entries (2 keys + 1 common prefix)");
        }
    }

    /** The refusal is fatal: typed for the run summary, and not the throttle class the retry loops ride out. */
    @Test
    void refusalIsTypedFatalRatherThanTransient() throws Exception {
        try (ReplayServer server = server(overServing(3, 0));
             S3Client client = client(server)) {
            S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

            Throwable thrown = catchThrowable(() -> fetcher.fetchPage(PageRequest.objects(null, null, 2)));

            assertThat(thrown).isInstanceOf(ListingException.class).isNotInstanceOf(ThrottleException.class);
            assertThat(((ListingException) thrown).errorClass()).isEqualTo("oversized_page");
            assertThat(((ListingException) thrown).exitCode()).isEqualTo(1);
        }
    }

    /**
     * The retry decorator every fetch is wrapped in must let the refusal through on the first
     * page: one HTTP request reached the endpoint, not a retry storm.
     */
    @Test
    void retryDecoratorDoesNotRetryTheRefusal() throws Exception {
        try (ReplayServer server = server(overServing(3, 0));
             S3Client client = client(server)) {
            var retrying = new TransientRetryFetcher(new S3PageFetcher(client, "bucket"), null);

            assertThatThrownBy(() -> retrying.fetchPage(PageRequest.objects(null, null, 2)))
                    .isInstanceOf(ProtocolViolationException.class);
            assertThat(server.metrics().snapshot().httpRequests()).isEqualTo(1);
        }
    }

    /** The bound is exclusive: a page exactly at {@code MaxKeys} is what a conforming store returns. */
    @Test
    void acceptsPageExactlyAtTheRequestedMaxKeys() throws Exception {
        try (ReplayServer server = server(overServing(2, 0));
             S3Client client = client(server)) {
            S3PageFetcher fetcher = new S3PageFetcher(client, "bucket");

            assertThat(fetcher.fetchPage(PageRequest.objects(null, null, 2)).entries()).hasSize(2);
        }
    }

    /** A fixture that serves {@code objects + commonPrefixes} entries whatever page size it was asked for. */
    private static ListingFixture overServing(int objects, int commonPrefixes) {
        return request -> {
            List<S3ResultEntry> entries = new ArrayList<>(objects + commonPrefixes);
            for (int i = 0; i < objects; i++) {
                entries.add(new S3ResultEntry.ObjectResult(new ListedObject(
                        bytes("k/" + i), 1, 0, "etag", "STANDARD", null, null, null, null)));
            }
            for (int i = 0; i < commonPrefixes; i++) {
                entries.add(new S3ResultEntry.CommonPrefixResult(bytes("p" + i + "/")));
            }
            return new S3ListResult(request, entries, false, null);
        };
    }

    private static ReplayServer server(ListingFixture fixture) throws Exception {
        ReplayServer server = new ReplayServer("127.0.0.1", 0, "bucket", fixture);
        server.start();
        return server;
    }

    private static S3Client client(ReplayServer server) {
        return S3ClientFactory.create(new S3Config(
                Region.US_EAST_1,
                URI.create("http://127.0.0.1:" + server.port()),
                true,
                4,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(5),
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                AnonymousCredentialsProvider.create(),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
