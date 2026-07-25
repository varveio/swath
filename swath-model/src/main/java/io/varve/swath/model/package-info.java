/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * swath's listing entity and value types — the leaf module every other module depends on and that
 * depends on none of them.
 *
 * <p>The types here are the vocabulary the rest of the system is written in: a {@link
 * io.varve.swath.model.ListEntry} (an {@link io.varve.swath.model.ObjectEntry}, {@link
 * io.varve.swath.model.DeleteMarkerEntry}, or {@link io.varve.swath.model.CommonPrefixEntry}), the
 * {@link io.varve.swath.model.PageBatch} that carries a page of them through the pipeline, and
 * {@link io.varve.swath.model.KeyBytes}, the raw-bytes key representation everything compares and
 * splits on.
 *
 * <p>Two invariants govern this package:
 *
 * <ul>
 *   <li><b>Keys are bytes, not strings.</b> S3 object keys are arbitrary byte sequences; decoding
 *       them to {@code String} loses information and breaks ordering. {@link
 *       io.varve.swath.model.KeyBytes} and unsigned byte comparison are the canonical spelling, and
 *       {@link io.varve.swath.model.ByteMidpoint} does range splitting in that same byte domain.</li>
 *   <li><b>This module imports no other internal swath package.</b> It is deliberately a leaf, so
 *       the engine, the stores, and the CLI can all share these types without a dependency cycle.
 *       Adding an internal dependency here is a design change, not a convenience.</li>
 * </ul>
 */
package io.varve.swath.model;
