/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidUriException;
import org.junit.jupiter.api.Test;

class S3UriTest {

    @Test
    void parsesBucketAndPrefix() throws Exception {
        S3Uri u = S3Uri.parse("s3://my-bucket/a/b/c");
        assertThat(u.bucket()).isEqualTo("my-bucket");
        assertThat(u.prefixAsString()).isEqualTo("a/b/c");
    }

    @Test
    void parsesBucketWithoutPrefix() throws Exception {
        S3Uri u = S3Uri.parse("s3://my-bucket");
        assertThat(u.bucket()).isEqualTo("my-bucket");
        assertThat(u.prefix()).isEmpty();
    }

    @Test
    void treatsPercentEscapesLiterally() throws Exception {
        S3Uri u = S3Uri.parse("s3://my-bucket/a%20b");
        assertThat(u.prefixAsString()).isEqualTo("a%20b");
        assertThat(u.prefix()).containsExactly('a', '%', '2', '0', 'b');
    }

    @Test
    void prefixAccessorReturnsDefensiveCopy() throws Exception {
        S3Uri u = S3Uri.parse("s3://my-bucket/abc");
        byte[] prefix = u.prefix();

        prefix[0] = 'z';

        assertThat(u.prefixAsString()).isEqualTo("abc");
        assertThat(u.prefix()).containsExactly('a', 'b', 'c');
    }

    @Test
    void rejectsNonS3AndEmptyBucket() {
        assertThatThrownBy(() -> S3Uri.parse("http://x/y")).isInstanceOf(InvalidUriException.class);
        assertThatThrownBy(() -> S3Uri.parse("s3://")).isInstanceOf(InvalidUriException.class);
        assertThatThrownBy(() -> S3Uri.parse(null)).isInstanceOf(InvalidUriException.class);
    }
}
