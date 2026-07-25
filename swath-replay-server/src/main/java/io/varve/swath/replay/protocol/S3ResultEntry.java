/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

/** A single ListObjectsV2 result entry: either an object or a rolled-up common prefix. */
public sealed interface S3ResultEntry permits S3ResultEntry.ObjectResult, S3ResultEntry.CommonPrefixResult {

    byte[] key();

    record ObjectResult(ListedObject object) implements S3ResultEntry {
        @Override
        public byte[] key() {
            return object.key();
        }
    }

    record CommonPrefixResult(byte[] key) implements S3ResultEntry {
    }
}
