/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.RunContext;

/**
 * Placeholder producer for the pipeline skeleton: emits no items. The
 * real {@code WorkStealingScan} engine replaces it; the pipeline
 * appends the single {@code End} envelope on its behalf.
 */
public final class NoOpEngine<T> implements Pipeline.Producer<T> {

    @Override
    public void produce(RunContext ctx, Channel<T> out) {
        // No work; the pipeline emits End after this returns.
    }
}
