/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

final class SdkSources {
    static void require(boolean condition, String message) throws IOException {
        if (!condition) throw new IOException(message);
    }

    static void bounds(long pos, int count, long size) throws IOException {
        require(
                pos >= 0 && count > 0 && pos <= size && count <= size - pos,
                "range outside object");
    }

    static byte[] exact(InputStream stream, int count) throws IOException {
        byte[] bytes;
        try {
            bytes = stream.readNBytes(count);
        } catch (IOException failure) {
            throw new IOException("truncated range", failure);
        }
        require(bytes.length == count, "truncated range");
        require(stream.read() == -1, "oversized range");
        return bytes;
    }

    static final class S3 implements ConditionalInput.Source {
        final S3Client client;
        final long size;
        final String etag;
        final String version;
        final AtomicReference<ResponseInputStream<GetObjectResponse>> active =
                new AtomicReference<>();
        volatile boolean closed;

        S3(String endpoint) {
            client =
                    S3Client.builder()
                            .endpointOverride(URI.create(endpoint))
                            .forcePathStyle(true)
                            .overrideConfiguration(
                                    config ->
                                            config.apiCallTimeout(java.time.Duration.ofSeconds(5))
                                                    .apiCallAttemptTimeout(
                                                            java.time.Duration.ofSeconds(5)))
                            .region(Region.US_EAST_1)
                            .credentialsProvider(
                                    StaticCredentialsProvider.create(
                                            AwsBasicCredentials.create(
                                                    "swathspike", "swathspike-secret")))
                            .build();
            try {
                var head =
                        client.headObject(
                                HeadObjectRequest.builder().bucket("bucket").key("file").build());
                if (head.contentLength() == null
                        || head.contentLength() < 0
                        || head.eTag() == null
                        || head.eTag().isEmpty()) {
                    throw new IllegalStateException("S3 HEAD missing size or identity");
                }
                size = head.contentLength();
                etag = head.eTag();
                version = head.versionId();
            } catch (RuntimeException | Error error) {
                client.close();
                throw error;
            }
        }

        public long length() {
            return size;
        }

        public synchronized byte[] read(long pos, int count) throws IOException {
            require(!closed, "source closed");
            bounds(pos, count, size);
            ResponseInputStream<GetObjectResponse> body;
            try {
                body =
                        client.getObject(
                                GetObjectRequest.builder()
                                        .bucket("bucket")
                                        .key("file")
                                        .ifMatch(etag)
                                        .range("bytes=" + pos + "-" + (pos + count - 1))
                                        .build());
            } catch (RuntimeException e) {
                throw new IOException("S3 conditional range", e);
            }
            active.set(body);
            try {
                require(!closed, "source closed");
                var response = body.response();
                require(response.sdkHttpResponse().statusCode() == 206, "ignored range");
                require(
                        ("bytes " + pos + "-" + (pos + count - 1) + "/" + size)
                                .equals(response.contentRange()),
                        "incorrect range");
                require(
                        Long.valueOf(count).equals(response.contentLength())
                                && etag.equals(response.eTag()),
                        "changed identity");
                require(
                        response.contentEncoding() == null
                                || response.contentEncoding().equals("identity"),
                        "encoded range");
                require(
                        version == null || version.equals(response.versionId()),
                        "changed S3 version");
                return exact(body, count);
            } finally {
                active.compareAndSet(body, null);
                body.abort();
            }
        }

        public void close() {
            closed = true;
            var body = active.getAndSet(null);
            if (body != null) body.abort();
            client.close();
        }
    }

    static final class Gcs implements ConditionalInput.Source {
        final Storage client;
        final ApacheHttpTransport transport;
        final long size;
        final long generation;
        volatile boolean closed;

        Gcs(String endpoint) throws IOException {
            transport =
                    new ApacheHttpTransport(
                            ApacheHttpTransport.newDefaultHttpClientBuilder()
                                    .disableContentCompression()
                                    .build());
            client =
                    new Storage.Builder(
                                    transport,
                                    GsonFactory.getDefaultInstance(),
                                    request -> {
                                        request.setConnectTimeout(5000);
                                        request.setReadTimeout(5000);
                                        request.setNumberOfRetries(0);
                                    })
                            .setRootUrl(endpoint + "/")
                            .setApplicationName("swath-java-range-spike")
                            .build();
            try {
                var object = client.objects().get("bucket", "file").execute();
                size = object.getSize().longValueExact();
                generation = object.getGeneration();
            } catch (IOException | RuntimeException | Error error) {
                try {
                    transport.shutdown();
                } catch (IOException closeError) {
                    error.addSuppressed(closeError);
                }
                throw error;
            }
        }

        public long length() {
            return size;
        }

        public synchronized byte[] read(long pos, int count) throws IOException {
            require(!closed, "source closed");
            bounds(pos, count, size);
            var get = client.objects().get("bucket", "file").setIfGenerationMatch(generation);
            get.getRequestHeaders().setRange("bytes=" + pos + "-" + (pos + count - 1));
            get.getRequestHeaders().setAcceptEncoding("identity");
            HttpResponse response = get.executeMedia();
            Throwable failure = null;
            try {
                require(!closed, "source closed");
                require(response.getStatusCode() == 206, "ignored range");
                require(
                        ("bytes " + pos + "-" + (pos + count - 1) + "/" + size)
                                .equals(response.getHeaders().getContentRange()),
                        "incorrect range");
                require(
                        response.getHeaders().getContentEncoding() == null
                                || response.getHeaders().getContentEncoding().equals("identity"),
                        "encoded range");
                require(
                        Long.valueOf(count).equals(response.getHeaders().getContentLength()),
                        "wrong length");
                require(
                        Long.toString(generation)
                                .equals(
                                        response.getHeaders()
                                                .getFirstHeaderStringValue("x-goog-generation")),
                        "changed generation");
                return exact(response.getContent(), count);
            } catch (IOException | RuntimeException | Error error) {
                failure = error;
                throw error;
            } finally {
                try {
                    response.disconnect();
                } catch (IOException closeError) {
                    if (failure != null) failure.addSuppressed(closeError);
                    else throw closeError;
                }
            }
        }

        public void close() throws IOException {
            closed = true;
            transport.shutdown();
            // Closing the owned pool aborts in-flight body/header reads. The read finally block
            // owns response cleanup.
        }
    }
}
