/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.fail;

import io.varve.swath.engine.RetryPolicy;
import io.varve.swath.engine.TransientRetryFetcher;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.store.PageRequest;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

final class SeedDeadEndpointFailFastTest {

    @Test
    @Timeout(10)
    void connectionRefusedSeedProbeDoesNotEnterSwathRetryLoop() throws Exception {
        int deadPort;
        try (ServerSocket socket = new ServerSocket(
                0, 1, InetAddress.getByName("127.0.0.1"))) {
            deadPort = socket.getLocalPort();
        }
        S3Config config = new S3Config(
                Region.US_EAST_1,
                URI.create("http://127.0.0.1:" + deadPort),
                true,
                1,
                1,
                Duration.ofSeconds(2),
                Duration.ofSeconds(4),
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")),
                Duration.ofSeconds(2));

        try (var client = S3ClientFactory.create(config)) {
            S3PageFetcher delegate = new S3PageFetcher(client, "bucket");
            TransientRetryFetcher fetcher = TransientRetryFetcher.forSeed(
                    delegate,
                    new CancellationToken(),
                    null,
                    millis -> fail("dead seed endpoint must fail before swath backoff"),
                    RetryPolicy.RIDE_OUT);

            Throwable failure = catchThrowable(() -> fetcher.fetchPage(
                    PageRequest.objectsDelimited(
                            new byte[0], new byte[] {'/'}, null, 1000)));

            assertThat(failure)
                    .isInstanceOf(ListingException.class)
                    .isNotInstanceOf(ThrottleException.class)
                    .hasRootCauseInstanceOf(ConnectException.class);
            assertThat(delegate.apiCalls()).isEqualTo(1L);
        }
    }
}
