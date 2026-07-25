/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Shared LocalStack S3 helpers for the integration tier. */
public final class LocalStackSupport {

    public static final DockerImageName IMAGE = DockerImageName.parse("localstack/localstack:3.8");

    /** LocalStack S3 container for no-DNS/air-gapped CI: SKIP_SSL_CERT_DOWNLOAD avoids a ~80s
     *  blocking fetch to api.localstack.cloud that pushes "Ready." past Testcontainers' 60s wait. */
    public static LocalStackContainer s3Container() {
        return new LocalStackContainer(IMAGE)
                .withServices(Service.S3)
                .withEnv("SKIP_SSL_CERT_DOWNLOAD", "1");
    }

    private LocalStackSupport() {
    }

    public static S3Config config(LocalStackContainer localstack, int targetConcurrency) {
        return new S3Config(
                Region.of(localstack.getRegion()),
                localstack.getEndpoint(),
                true,                       // path-style for LocalStack
                targetConcurrency,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                Duration.ofSeconds(30),
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())),
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT);
    }

    public static S3Client client(LocalStackContainer localstack) {
        return S3ClientFactory.create(config(localstack, 64));
    }

    public static void createBucket(S3Client s3, String bucket) {
        s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    }

    /** Put many tiny objects in parallel (bounded) so the fixture builds quickly. */
    public static void putObjects(S3Client s3, String bucket, List<byte[]> keys, int parallelism) {
        Semaphore permits = new Semaphore(parallelism);
        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(keys.size());
            for (byte[] key : keys) {
                permits.acquireUninterruptibly();
                futures.add(exec.submit(() -> {
                    try {
                        s3.putObject(
                                PutObjectRequest.builder().bucket(bucket)
                                        .key(new String(key, StandardCharsets.UTF_8)).build(),
                                RequestBody.fromBytes(new byte[]{1}));
                    } finally {
                        permits.release();
                    }
                }));
            }
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    throw new RuntimeException("put failed", e);
                }
            }
        }
    }
}
