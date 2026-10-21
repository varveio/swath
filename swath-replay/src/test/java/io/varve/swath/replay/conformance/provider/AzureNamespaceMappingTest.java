/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AzureNamespaceMappingTest {
    @Test
    void nativeHostAndReplayAccountPathAreMappedExplicitly() {
        var mapping = new AzureNamespaceMapping("{account}", "{container}", "replay", "bucket");
        assertThatCode(() -> mapping.assertMapped("/{container}", "/replay/bucket",
                "https://{account}.blob.core.windows.net/", "http://127.0.0.1:19090/replay/"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> mapping.assertMapped("/{account}/{container}", "/replay/bucket",
                "https://{account}.blob.core.windows.net/", "http://127.0.0.1:19090/replay/"))
                .hasMessageContaining("mapping differs");
        assertThatThrownBy(() -> mapping.assertMapped("/{container}", "/replay/bucket",
                "http://{account}.blob.core.windows.net/", "http://127.0.0.1:19090/replay/"))
                .hasMessageContaining("native ServiceEndpoint");
        assertThatThrownBy(() -> mapping.assertMapped("/{container}", "/replay/bucket",
                "https://{account}.blob.core.windows.net/", "http://127.0.0.1:19090/wrong/"))
                .hasMessageContaining("replay ServiceEndpoint");
    }
}
