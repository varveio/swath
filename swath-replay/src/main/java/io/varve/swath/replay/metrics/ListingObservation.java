/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.metrics;

/** Rows and grouped prefixes selected for one native listing page. */
public record ListingObservation(ObservationShape shape, long objects, long prefixes) {
    public static final ListingObservation UNKNOWN = new ListingObservation(ObservationShape.UNKNOWN, 0, 0);

    public ListingObservation {
        if (shape == null || objects < 0 || prefixes < 0) {
            throw new IllegalArgumentException("invalid listing observation");
        }
    }
}
