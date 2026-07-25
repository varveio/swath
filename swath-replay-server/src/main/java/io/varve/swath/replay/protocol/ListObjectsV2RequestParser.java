/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/**
 * Parses a raw S3 ListObjectsV2 query string into a typed {@link S3ListRequest}, isolating S3
 * query validation from the Jetty transport. Byte-level key parameters are preserved verbatim
 * ({@code prefix}, {@code delimiter}, {@code start-after}); {@code list-type=2} and a non-negative
 * integer {@code max-keys} are enforced, rejecting violations with a 400 {@code InvalidArgument}.
 */
public final class ListObjectsV2RequestParser {

    private ListObjectsV2RequestParser() {
    }

    /** Parses the query for the given bucket, or throws {@link S3Error} on an invalid request. */
    public static S3ListRequest parse(String bucket, String rawQuery) {
        S3Query query = S3Query.parse(rawQuery);
        if (!"2".equals(query.value("list-type"))) {
            throw new S3Error(400, "InvalidArgument", "only ListObjectsV2 is supported");
        }
        return new S3ListRequest(
                bucket,
                bytes(query, "prefix"),
                bytes(query, "delimiter"),
                bytes(query, "start-after"),
                query.value("continuation-token"),
                requestedMaxKeys(query.value("max-keys")),
                "url".equals(query.value("encoding-type")),
                Boolean.parseBoolean(query.value("fetch-owner")));
    }

    private static byte[] bytes(S3Query query, String name) {
        byte[] value = query.bytes(name);
        return value == null || value.length == 0 ? null : value;
    }

    private static int requestedMaxKeys(String value) {
        if (value == null || value.isBlank()) {
            return 1000;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new S3Error(400, "InvalidArgument", "max-keys must be non-negative");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new S3Error(400, "InvalidArgument", "max-keys must be an integer");
        }
    }
}
