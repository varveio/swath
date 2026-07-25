/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

/**
 * A single configurable fake {@link S3Client} for store/s3 tests — replaces the ~8
 * hand-rolled per-test stub classes ({@code CapturingS3Client}, {@code ThrowingS3Client}, {@code
 * EmptyPageS3Client}, {@code PaginatingFakeS3Client}, ...) that duplicated the same
 * {@code listObjectsV2}/{@code serviceName}/{@code close} boilerplate across the module's tests.
 *
 * <p>Every {@code listObjectsV2} call is recorded — see {@link #capturedRequests()} / {@link
 * #lastRequest()} — regardless of which response mode is configured:
 * <ul>
 *   <li>{@link #captureOnly()} — capture the request, always return an empty, non-truncated page.
 *   <li>{@link #respondingWith(ListObjectsV2Response)} — capture, always return the same response.
 *   <li>{@link #throwing(RuntimeException)} — capture, always throw the same fault (SDK or not).
 *   <li>{@link #builder()} + {@link Builder#then}/{@link Builder#thenThrow} — a scripted sequence
 *       of responses/faults consumed in call order, the last entry sticking once exhausted (e.g.
 *       two throttles then a successful page).
 *   <li>{@link #respondingWith(Responder)} — a fully dynamic per-request responder, for a fake
 *       that must compute its response from the incoming request (e.g. simulating real S3
 *       {@code start-after} pagination over a fixed keyspace).
 * </ul>
 */
public final class FakeS3Client implements S3Client {

    /** Computes a response for a captured request; may throw to model a fault. */
    @FunctionalInterface
    public interface Responder {
        ListObjectsV2Response respond(ListObjectsV2Request request);
    }

    private final List<ListObjectsV2Request> capturedRequests = new ArrayList<>();
    private final Responder responder;

    private FakeS3Client(Responder responder) {
        this.responder = Objects.requireNonNull(responder, "responder");
    }

    /** Captures requests and always returns an empty, non-truncated page. */
    public static FakeS3Client captureOnly() {
        return respondingWith(ListObjectsV2Response.builder().isTruncated(false).build());
    }

    /** Captures requests and always returns {@code response}. */
    public static FakeS3Client respondingWith(ListObjectsV2Response response) {
        return respondingWith(request -> response);
    }

    /** Captures requests and always throws {@code toThrow} (an {@code SdkException} or not). */
    public static FakeS3Client throwing(RuntimeException toThrow) {
        return respondingWith(request -> {
            throw toThrow;
        });
    }

    /** Captures requests and computes each response dynamically from the incoming request. */
    public static FakeS3Client respondingWith(Responder responder) {
        return new FakeS3Client(responder);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
        capturedRequests.add(request);
        return responder.respond(request);
    }

    @Override
    public String serviceName() {
        return "s3";
    }

    @Override
    public void close() {
    }

    /** Every request this client has seen, in call order. */
    public List<ListObjectsV2Request> capturedRequests() {
        return List.copyOf(capturedRequests);
    }

    /** The most recent request this client has seen. */
    public ListObjectsV2Request lastRequest() {
        return capturedRequests.get(capturedRequests.size() - 1);
    }

    /** Builds a {@link FakeS3Client} that replays a scripted sequence of responses/faults. */
    public static final class Builder {
        private final List<Responder> steps = new ArrayList<>();

        private Builder() {
        }

        /** The next call returns {@code response}. */
        public Builder then(ListObjectsV2Response response) {
            steps.add(request -> response);
            return this;
        }

        /** The next call throws {@code toThrow}. */
        public Builder thenThrow(RuntimeException toThrow) {
            steps.add(request -> {
                throw toThrow;
            });
            return this;
        }

        /** The last configured step sticks: every call past the scripted sequence repeats it. */
        public FakeS3Client build() {
            if (steps.isEmpty()) {
                throw new IllegalStateException("at least one then()/thenThrow() step is required");
            }
            List<Responder> ordered = List.copyOf(steps);
            return new FakeS3Client(new Responder() {
                private int index;

                @Override
                public ListObjectsV2Response respond(ListObjectsV2Request request) {
                    Responder step = ordered.get(Math.min(index, ordered.size() - 1));
                    index++;
                    return step.respond(request);
                }
            });
        }
    }
}
