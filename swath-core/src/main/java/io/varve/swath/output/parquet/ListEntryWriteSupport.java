/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import io.varve.swath.error.InvalidKeyEncodingException;
import io.varve.swath.error.OutputException;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.RowType;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/** Writes a {@link ListEntry} as a row of the canonical {@link ParquetSchema}. */
public final class ListEntryWriteSupport extends WriteSupport<ListEntry> {

    private final MessageType schema;
    private RecordConsumer rc;

    public ListEntryWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    @SuppressWarnings("deprecation")
    public WriteContext init(Configuration configuration) {
        return context();
    }

    @Override
    public WriteContext init(ParquetConfiguration configuration) {
        return context();
    }

    private WriteContext context() {
        return new WriteContext(schema, Map.of());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.rc = recordConsumer;
    }

    @Override
    public void write(ListEntry e) {
        byte[] key = e.key().rawUnsafe();
        if (!KeyBytes.isValidUtf8(key)) {
            throw new UncheckedOutputException(InvalidKeyEncodingException.forKey(key));
        }
        rc.startMessage();
        binary("key", 0, key);
        switch (e) {
            case ObjectEntry o -> {
                longField("size", 1, o.size());
                timestamp("last_modified", 2, o.lastModifiedEpochMicros());
                str("etag", 3, o.etag());
                str("storage_class", 4, o.storageClass());
                str("version_id", 5, o.versionId());
                if (o.versionId() != null) {
                    bool("is_latest", 6, o.isLatest());
                }
                bool("is_delete_marker", 7, false);
                str("owner_id", 8, o.ownerId());
                str("owner_display_name", 9, o.ownerDisplayName());
                str("checksum_algorithm", 10, o.checksumAlgorithm());
                str("checksum_type", 11, o.checksumType());
            }
            case DeleteMarkerEntry d -> {
                timestamp("last_modified", 2, d.lastModifiedEpochMicros());
                str("version_id", 5, d.versionId());
                bool("is_latest", 6, d.isLatest());
                bool("is_delete_marker", 7, true);
                str("owner_id", 8, d.ownerId());
            }
            case CommonPrefixEntry ignored -> bool("is_delete_marker", 7, false);
        }
        str("row_type", 12, RowType.of(e).name());
        rc.endMessage();
    }

    private void binary(String name, int index, byte[] value) {
        rc.startField(name, index);
        rc.addBinary(Binary.fromConstantByteArray(value));
        rc.endField(name, index);
    }

    private void str(String name, int index, String value) {
        if (value == null) {
            return;
        }
        rc.startField(name, index);
        rc.addBinary(Binary.fromString(value));
        rc.endField(name, index);
    }

    private void longField(String name, int index, long value) {
        rc.startField(name, index);
        rc.addLong(value);
        rc.endField(name, index);
    }

    private void timestamp(String name, int index, long micros) {
        if (micros == 0L) {
            return;   // treat the sentinel as null (no timestamp)
        }
        rc.startField(name, index);
        rc.addLong(micros);
        rc.endField(name, index);
    }

    private void bool(String name, int index, boolean value) {
        rc.startField(name, index);
        rc.addBoolean(value);
        rc.endField(name, index);
    }
}

/** Checked-output-error bridge for parquet-mr's {@code WriteSupport.write} callback. */
final class UncheckedOutputException extends RuntimeException {

    UncheckedOutputException(OutputException cause) {
        super(cause);
    }

    @Override
    public synchronized OutputException getCause() {
        return (OutputException) super.getCause();
    }
}
