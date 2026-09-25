/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/** Parses one provider's request and renders that provider's failures. */
public interface ListingProtocolHandler {
    Protocol protocol();

    ListingOperation parse(ListingHttpRequest request);

    RenderedResponse error(ReplayFailure failure, ListingHttpRequest request);
}
