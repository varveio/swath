/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.InvalidUriException;
import io.varve.swath.observability.SafeInput;
import java.nio.charset.StandardCharsets;

/**
 * A parsed {@code s3://bucket[/prefix]} URI. The prefix is kept as raw bytes
 * (UTF-8 of the textual prefix) for the byte-exact engine. The path is taken
 * literally: {@code a%20b} names key bytes for {@code a%20b}, not {@code a b}.
 */
public record S3Uri(String bucket, byte[] prefix) {

    public S3Uri {
        prefix = prefix == null ? new byte[0] : prefix.clone();
    }

    public static S3Uri parse(String uri) throws InvalidUriException {
        if (uri == null || !uri.startsWith("s3://")) {
            throw new InvalidUriException("expected an s3:// URI, got: "
                    + SafeInput.logText(uri));
        }
        String rest = uri.substring("s3://".length());
        if (rest.isEmpty()) {
            throw new InvalidUriException("missing bucket in URI: "
                    + SafeInput.logText(uri));
        }
        int slash = rest.indexOf('/');
        String bucket = slash < 0 ? rest : rest.substring(0, slash);
        String prefix = slash < 0 ? "" : rest.substring(slash + 1);
        if (bucket.isEmpty()) {
            throw new InvalidUriException("missing bucket in URI: "
                    + SafeInput.logText(uri));
        }
        for (int i = 0; i < bucket.length(); i++) {
            if (Character.isISOControl(bucket.charAt(i))) {
                throw new InvalidUriException("invalid s3:// URI: bucket contains control characters");
            }
        }
        return new S3Uri(bucket, prefix.getBytes(StandardCharsets.UTF_8));
    }

    public String prefixAsString() {
        return new String(prefix, StandardCharsets.UTF_8);
    }

    /** Public boundary copy; parsed CLI prefixes are not on the engine hot path. */
    @Override
    public byte[] prefix() {
        return prefix.clone();
    }
}
