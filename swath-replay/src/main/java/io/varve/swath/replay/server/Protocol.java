/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

/** The replay server's built-in listing wire protocols. */
public enum Protocol {
    S3("s3"), GCS("gcs"), AZURE("azure"), REPLAY("replay");

    private final String metricName;

    Protocol(String metricName) {
        this.metricName = metricName;
    }

    public String metricName() { return metricName; }
}
