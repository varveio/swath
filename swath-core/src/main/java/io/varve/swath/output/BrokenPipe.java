/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import java.io.IOException;

/**
 * Broken-pipe detection (downstream reader closed the pipe — e.g. piping to
 * {@code head}). The JDK surfaces it as an {@link IOException} with a
 * platform-dependent message. A broken pipe is a <b>clean</b> stop (exit 0),
 * not an error.
 *
 * <p>This only fires if output goes to a stream that actually throws.
 * {@code System.out} (a {@code PrintStream}) swallows the error, so the output
 * stage writes to {@code FileDescriptor.out} directly.
 */
public final class BrokenPipe {

    private BrokenPipe() {
    }

    public static boolean is(IOException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String m = msg.toLowerCase();
        return m.contains("broken pipe") || m.contains("stream closed") || m.contains("pipe closed");
    }
}
