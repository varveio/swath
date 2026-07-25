/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * The canonical Parquet schema — one {@link MessageType} used by
 * every writer. The superset of the fleet canonical schema; the fleet view
 * selects its columns.
 */
public final class ParquetSchema {

    public static final String NAME = "swath_listing";

    private ParquetSchema() {
    }

    public static MessageType canonical() {
        return Types.buildMessage()
                .required(PrimitiveTypeName.BINARY).named("key")                       // raw key bytes
                .optional(PrimitiveTypeName.INT64).named("size")                        // null for prefixes/markers
                .optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("last_modified")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("etag")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("storage_class")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("version_id")
                .optional(PrimitiveTypeName.BOOLEAN).named("is_latest")
                .required(PrimitiveTypeName.BOOLEAN).named("is_delete_marker")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("owner_id")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("owner_display_name")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("checksum_algorithm")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("checksum_type")
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("row_type")
                .named(NAME);
    }
}
