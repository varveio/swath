/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

/** Invalid configuration value (bad knob, unreadable hints file, …). Exit 2. */
public final class InvalidConfigException extends SwathException {
    public InvalidConfigException(String message) {
        super(message);
    }

    public InvalidConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int exitCode() {
        return 2;
    }
}
