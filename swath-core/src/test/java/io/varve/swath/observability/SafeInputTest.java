/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import java.util.List;
import org.junit.jupiter.api.Test;

final class SafeInputTest {

    @Test
    void acceptsPlainEndpointAndRejectsEveryCredentialCarrierWithoutEchoingIt() throws Exception {
        assertThat(SafeInput.endpoint("--endpoint-url", "http://localhost:4566/base").toString())
                .isEqualTo("http://localhost:4566/base");

        for (String unsafe : List.of(
                "https://user:top-secret@example.test/path",
                "https://example.test/path?X-Amz-Signature=top-secret",
                "https://example.test/path?harmless=top-secret",
                "https://example.test/path#top-secret",
                "https:user:top-secret@example.test",
                "https:/user:top-secret@example.test",
                "//user:top-secret@example.test/path",
                "file://example.test/top-secret")) {
            assertThatThrownBy(() -> SafeInput.endpoint("--endpoint-url", unsafe))
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("invalid --endpoint-url")
                    .hasMessageNotContaining("top-secret")
                    .satisfies(error -> assertThat(error.getCause()).isNull());
        }
    }

    @Test
    void persistedArgvRedactsBothEndpointSyntaxesAndEscapesControls() {
        List<String> safe = SafeInput.argv(List.of(
                "list", "s3://bucket/prefix", "--endpoint-url",
                "https://user:secret@example.test", "--metrics-endpoint=https://collector?token=secret",
                "--include", "first\nforged\u009bline"));

        assertThat(safe).containsExactly(
                "list", "s3://bucket/prefix", "--endpoint-url", SafeInput.REDACTED_ENDPOINT,
                "--metrics-endpoint=" + SafeInput.REDACTED_ENDPOINT,
                "--include", "first\\x0aforged\\x9bline");
        assertThat(String.join(" ", safe)).doesNotContain("secret").doesNotContain("\n");
    }
}
