# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Schema dispatch shared by the independent dataset readers."""
from collections import Counter

S3_CURRENT = "swath/s3-objects/v1"
S3_VERSIONS = "swath/s3-versions/v1"
GCS_CURRENT = "swath/gcs-objects/v1"
GCS_VERSIONS = "swath/gcs-generations/v1"
LOCAL = "swath-local/v1"
S3_COLUMNS = ["key", "size", "last_modified", "etag", "storage_class", "owner_id",
              "owner_display_name", "checksum_algorithm", "checksum_type"]
GCS_COLUMNS = ["key", "size", "last_modified", "etag", "generation", "metageneration",
               "storage_class", "md5", "crc32c", "content_type", "time_created"]
COLUMNS = {
    S3_CURRENT: S3_COLUMNS,
    S3_VERSIONS: S3_COLUMNS + ["version_id", "is_latest", "is_delete_marker"],
    GCS_CURRENT: GCS_COLUMNS,
    GCS_VERSIONS: GCS_COLUMNS + ["time_deleted"],
}
VERSION_IDS = {S3_VERSIONS, GCS_VERSIONS}
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
