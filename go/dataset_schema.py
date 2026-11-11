# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Schema dispatch shared by the independent dataset readers."""
from collections import Counter

S3_CURRENT = "swath/s3-objects/v1"
S3_VERSIONS = "swath/s3-versions/v1"
GCS_CURRENT = "swath/gcs-objects/v1"
GCS_VERSIONS = "swath/gcs-generations/v1"
AZBLOB_CURRENT = "swath/azblob-objects/v1"
AZBLOB_VERSIONS = "swath/azblob-versions/v1"
LOCAL = "swath/local-entries/v1"
S3_COLUMNS = ["key", "size", "last_modified", "etag", "storage_class", "owner_id",
              "owner_display_name", "checksum_algorithm", "checksum_type"]
GCS_COLUMNS = ["key", "size", "last_modified", "etag", "generation", "metageneration",
               "storage_class", "md5", "crc32c", "content_type", "time_created"]
# The Azure Blob columns added to the schema id after the first nine: a part
# written before lacks them (docs/dataset.md, "Schema registry").
AZBLOB_ADDED = ["access_tier_inferred", "access_tier_change_time", "archive_status", "rehydrate_priority",
                "last_access_time", "content_encoding", "server_encrypted", "encryption_scope",
                "customer_provided_key_sha256", "lease_state", "tag_count", "metadata", "tags",
                "immutability_policy_until", "immutability_policy_mode", "legal_hold"]
AZBLOB_COLUMNS = ["key", "size", "last_modified", "etag", "content_md5", "content_type", "blob_type",
                  "access_tier", "creation_time"] + AZBLOB_ADDED
COLUMNS = {
    S3_CURRENT: S3_COLUMNS,
    S3_VERSIONS: S3_COLUMNS + ["version_id", "is_latest", "is_delete_marker"],
    GCS_CURRENT: GCS_COLUMNS,
    GCS_VERSIONS: GCS_COLUMNS + ["time_deleted"],
    AZBLOB_CURRENT: AZBLOB_COLUMNS,
    AZBLOB_VERSIONS: AZBLOB_COLUMNS + ["version_id", "is_latest", "snapshot", "is_deleted", "deleted_time",
                                       "remaining_retention_days"],
}
VERSION_IDS = {S3_VERSIONS, GCS_VERSIONS, AZBLOB_VERSIONS}
SUPPORTED = set(COLUMNS) | {LOCAL, "swath/canonical"}


def schema_id(manifest):
    sid = manifest.get("schema_id", manifest.get("schemaId"))
    if sid not in SUPPORTED:
        raise ValueError(f"unsupported schema id: {sid!r}")
    return sid


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def compare_rows(sid, a, b):
    """Whole-row fixture oracle: order, multiplicity, metadata and nullness."""
    if sid not in VERSION_IDS:
        raise ValueError("whole-row fixture comparison requires a versions schema")
    columns = COLUMNS[sid]
    aa = [tuple(row[c] for c in columns) for row in a]
    bb = [tuple(row[c] for c in columns) for row in b]
    return aa == bb, Counter(aa) == Counter(bb)
