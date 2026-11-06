/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.provider;

/** A capture bound refused the next attempt, so the run is incomplete rather than failed open. */
public final class CaptureBoundException extends RuntimeException {
    private final String bound;

    public CaptureBoundException(String bound) {
        super("capture bound reached: " + bound);
        this.bound = bound;
    }

    public String bound() {
        return bound;
    }

    /** Returns the first bound exception in a cause chain, or {@code null}. */
    static CaptureBoundException find(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof CaptureBoundException bound) {
                return bound;
            }
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed instanceof CaptureBoundException bound) {
                    return bound;
                }
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return null;
    }
}
