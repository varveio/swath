/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Mutates only an explicitly supplied local emulator; credentials are fixed fake values. */
public final class S3Smoke {
    public static void main(String[] args) throws Exception {
        String endpoint = args[0];
        if (!java.net.URI.create(endpoint).getHost().equals("127.0.0.1")
                && !java.net.URI.create(endpoint).getHost().equals("localhost")) {
            throw new IllegalArgumentException("local emulator endpoint required");
        }
        var client =
                software.amazon.awssdk.services.s3.S3Client.builder()
                        .endpointOverride(java.net.URI.create(endpoint))
                        .forcePathStyle(true)
                        .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                        .credentialsProvider(
                                software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
                                        .create(
                                                software.amazon.awssdk.auth.credentials
                                                        .AwsBasicCredentials.create(
                                                        "swathspike", "swathspike-secret")))
                        .build();
        try (client) {
            client.createBucket(CreateBucketRequest.builder().bucket("bucket").build());
            byte[] original = Files.readAllBytes(Path.of(args[1]));
            client.putObject(
                    PutObjectRequest.builder().bucket("bucket").key("file").build(),
                    RequestBody.fromBytes(original));
            try (var pinned = new SdkSources.S3(endpoint)) {
                if (!Arrays.equals(pinned.read(11, 37), Arrays.copyOfRange(original, 11, 48)))
                    throw new AssertionError("range mismatch");
                client.putObject(
                        PutObjectRequest.builder().bucket("bucket").key("file").build(),
                        RequestBody.fromBytes(new byte[original.length]));
                try {
                    pinned.read(11, 37);
                    throw new AssertionError("replacement accepted");
                } catch (java.io.IOException error) {
                    if (!(error.getCause() instanceof S3Exception s3) || s3.statusCode() != 412)
                        throw error;
                }
            }
            System.out.println("S3 emulator exact range and overwritten-object412 passed");
        }
    }
}
