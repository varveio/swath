/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ProviderEvidenceTest {
    @Test
    void gcsKeepsObjectAndPrefixArraysSeparateAndChecksWireTypes() throws Exception {
        var page = GcsEvidence.parse(bytes("""
                {"kind":"storage#objects","items":[{"name":"a/2","size":"12"}],
                 "prefixes":["a/"],"nextPageToken":"opaque"}
                """));
        assertThat(page.objects()).containsExactly(new GcsEvidence.ObjectRow("a/2", 12));
        assertThat(page.prefixes()).containsExactly("a/");
        assertThat(page.nextToken()).isEqualTo("opaque");
        assertThatThrownBy(() -> GcsEvidence.parse(bytes("""
                {"kind":"storage#objects","items":[{"name":"a/2","size":12}]}
                """))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void azureKeepsElementOrderExactNameTextAndEncodedAttribute() throws Exception {
        var page = AzureEvidence.parse(bytes("""
                <?xml version="1.0" encoding="utf-8"?>
                <EnumerationResults ServiceEndpoint="https://account.blob.core.windows.net/" ContainerName="test">
                  <Blobs><BlobPrefix><Name Encoded="true">a%2Bb/</Name></BlobPrefix>
                    <Blob><Name> leading and trailing </Name><Properties/></Blob></Blobs><NextMarker>opaque</NextMarker>
                </EnumerationResults>
                """));
        assertThat(page.entries()).containsExactly(
                new AzureEvidence.Entry("BlobPrefix", "a%2Bb/", "true"),
                new AzureEvidence.Entry("Blob", " leading and trailing ", null));
        assertThat(page.nextMarker()).isEqualTo("opaque");
        assertThat(page.serviceEndpoint()).isEqualTo("https://account.blob.core.windows.net/");
        assertThat(page.containerName()).isEqualTo("test");
    }

    @Test
    void azureRefusesDoctypeAndUnexpectedEntryKinds() {
        assertThatThrownBy(() -> AzureEvidence.parse(bytes("""
                <!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
                <EnumerationResults><Blobs><Blob><Name>&xxe;</Name></Blob></Blobs></EnumerationResults>
                """))).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> AzureEvidence.parse(bytes("""
                <EnumerationResults><Blobs><Deleted><Name>x</Name></Deleted></Blobs></EnumerationResults>
                """))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tokenWalkerFollowsEachEndpointsOwnOpaqueTokensAndRejectsCycles() {
        AtomicInteger calls = new AtomicInteger();
        assertThat(InventoryOracle.walk(token -> {
            calls.incrementAndGet();
            if (calls.get() == 2) {
                assertThat(token).isEqualTo("live-1");
            }
            return token == null ? new InventoryOracle.Page<>(List.of("a"), "live-1")
                    : new InventoryOracle.Page<>(List.of("b"), null);
        }, 3, 2).inventory()).containsExactly("a", "b");
        assertThat(calls).hasValue(2);
        assertThatThrownBy(() -> InventoryOracle.walk(token ->
                new InventoryOracle.Page<>(List.of("a"), "loop"), 3, 10))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("repeated");
        assertThatThrownBy(() -> InventoryOracle.walk(token ->
                new InventoryOracle.Page<>(List.of(), token == null ? "1" : token + "1"), 3, 10))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("page limit");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
