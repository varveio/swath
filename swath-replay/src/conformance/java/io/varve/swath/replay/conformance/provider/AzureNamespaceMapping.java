/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import java.net.URI;

/** Explicit mapping from Azure's native account host to replay's local account path. */
public record AzureNamespaceMapping(String nativeAccount, String nativeContainer,
                                    String replayAccount, String replayContainer) {
    public void assertMapped(String nativePath, String replayPath,
                             String nativeServiceEndpoint, String replayServiceEndpoint) {
        String expectedNativePath = "/" + nativeContainer;
        String expectedReplayPath = "/" + replayAccount + "/" + replayContainer;
        if (!expectedNativePath.equals(nativePath) || !expectedReplayPath.equals(replayPath)) {
            throw new IllegalArgumentException("Azure native-host to replay-account-path mapping differs");
        }
        String expectedNativeEndpoint = "https://" + nativeAccount + ".blob.core.windows.net/";
        if (!expectedNativeEndpoint.equals(nativeServiceEndpoint)) {
            throw new IllegalArgumentException("Azure native ServiceEndpoint shape differs");
        }
        URI replay = URI.create(replayServiceEndpoint);
        if (!"http".equals(replay.getScheme()) && !"https".equals(replay.getScheme())
                || replay.getHost() == null || !replay.getPath().equals("/" + replayAccount + "/")
                || replay.getRawQuery() != null || replay.getRawFragment() != null) {
            throw new IllegalArgumentException("Azure replay ServiceEndpoint shape differs");
        }
    }
}
