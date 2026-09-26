/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ServeConfigTest {
    @Test
    void gcsNamesFollowCurrentBucketBoundsWhileS3OnlyKeepsLegacyNameAcceptance() {
        for (String invalid : new String[] {"ABc", "ab", "192.168.1.4", "goog-test",
                "test-google", "test-g00gle", "foo..bar", "a".repeat(64),
                "a".repeat(64) + ".example"}) {
            assertThatThrownBy(() -> config(invalid, Set.of(Protocol.GCS)))
                    .as(invalid).isInstanceOf(IllegalArgumentException.class);
            assertThat(config(invalid, Set.of(Protocol.S3)).bucket()).isEqualTo(invalid);
        }
        assertThat(config("a".repeat(63) + "." + "b".repeat(63) + "." + "c".repeat(63)
                + "." + "d".repeat(30), Set.of(Protocol.GCS)).bucket()).hasSize(222);
    }

    @Test
    void onlyEnabledGcsReservesStorageAndAzureAccountDoesNotConflictWithBucket() {
        assertThat(config("storage", Set.of(Protocol.S3)).bucket()).isEqualTo("storage");
        assertThat(config("storage", Set.of(Protocol.GCS)).bucket()).isEqualTo("storage");
        assertThatThrownBy(() -> config("storage", Set.of(Protocol.S3, Protocol.GCS)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(config("replay", Set.of(Protocol.S3, Protocol.AZURE)).azureAccount())
                .isEqualTo("replay");
    }

    @Test
    void azureAdvertisedHostIsAConcreteHostWithoutSchemeOrPort() {
        for (String invalid : new String[] {"http://example.test", "example.test:8080", "example/path",
                "::", "[::]", "0.0.0.0", "00.0.0.0", "999.999.999.999", "::ffff:127.0.0.1",
                "fe80::1%eth0"}) {
            assertThatThrownBy(() -> azure("127.0.0.1", invalid))
                    .as(invalid).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(azure("127.0.0.1", "example.test").advertisedHost()).isEqualTo("example.test");
        assertThat(azure("127.0.0.1", "2001:db8::1").advertisedHost()).isEqualTo("2001:db8::1");
        assertThatThrownBy(() -> azure("0:0:0:0:0:0:0:0", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(azure("0:0:0:0:0:0:0:0", "example.test").advertisedHost())
                .isEqualTo("example.test");
    }

    private static ServeConfig azure(String host, String advertised) {
        return new ServeConfig(Path.of("fixture"), host, 0, "bucket", ServingMode.SORTED,
                1, 512, Set.of(Protocol.AZURE), null, 256L * 1024 * 1024, 64 * 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), advertised,
                (request, result) -> Duration.ZERO);
    }

    private static ServeConfig config(String bucket, Set<Protocol> protocols) {
        return new ServeConfig(Path.of("fixture"), "127.0.0.1", 0, bucket, ServingMode.SORTED,
                1, 512, protocols, null, 256L * 1024 * 1024, 64 * 1024 * 1024,
                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30), null,
                (request, result) -> Duration.ZERO);
    }
}
