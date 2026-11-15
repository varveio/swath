// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"slices"
	"testing"
)

func TestSchema(t *testing.T) {
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzureObjectsV1, AzureVersionsV1, LocalV1} {
		s, ok := Schema(id)
		if !ok || s.ID != id || s.Key.Name != "key" || !s.Key.Required || len(s.Columns) == 0 {
			t.Fatalf("%s: %+v, %v", id, s, ok)
		}
		grouped := id == S3VersionsV1 || id == GCSGenerationsV1 || id == AzureVersionsV1
		if s.UniqueKeys == grouped {
			t.Errorf("%s: UniqueKeys %v", id, s.UniqueKeys)
		}
		if c, ok := s.Column("size"); !ok || c.Type != TypeInt64 || c.Required {
			t.Errorf("%s: size %+v", id, c)
		}
		s.Columns[0].Name = "changed"
		if again, _ := Schema(id); again.Columns[0].Name != "size" {
			t.Fatalf("%s: a caller's change reached the spec", id)
		}
	}
	if _, ok := Schema("swath/canonical"); ok {
		t.Fatal("the Java-era schema id has a spec")
	}
	local, _ := Schema(LocalV1)
	if local.Key.Type != TypeBinary || local.MaxKeyBytes != 0 {
		t.Errorf("local key %+v, max %d", local.Key, local.MaxKeyBytes)
	}
	az, _ := Schema(AzureObjectsV1)
	if az.Key.Type != TypeString || az.MaxKeyBytes != 3072 || len(az.Columns) != 24 {
		t.Errorf("azure %+v", az)
	}
	for _, name := range []string{"md5", "storage_class", "blob_type"} {
		if c, ok := az.Column(name); !ok || c.Type != TypeString || c.Required {
			t.Errorf("azure %s: %+v", name, c)
		}
	}
	for _, c := range az.Columns {
		if c.Required {
			t.Errorf("azure column %+v", c)
		}
	}
	// The versions schema: the current columns, then the entry's.
	av, _ := Schema(AzureVersionsV1)
	if names := av.ColumnNames(); !slices.Equal(names[:24], az.ColumnNames()) ||
		!slices.Equal(names[24:], []string{"version_id", "is_latest", "snapshot", "is_deleted", "deleted_at", "remaining_retention_days"}) {
		t.Errorf("azure versions columns %q", names)
	}
	gv, _ := Schema(GCSGenerationsV1)
	if names := gv.ColumnNames(); !slices.Equal(names[len(names)-2:], []string{"is_latest", "deleted_at"}) {
		t.Errorf("gcs generations columns %q", names)
	}
}

// TestColumnNames: one concept, one column name in every schema that has
// it: each time column of a source is created_at, deleted_at, accessed_at,
// last_modified, metadata_changed_at or storage_class_changed_at, and the
// content hash is md5.
func TestColumnNames(t *testing.T) {
	times := []string{"last_modified", "created_at", "deleted_at", "accessed_at", "metadata_changed_at", "storage_class_changed_at",
		"immutability_policy_until"}
	for _, id := range SchemaIDs() {
		s, _ := Schema(id)
		for _, c := range s.Columns {
			if c.Type == TypeTimestampMicros && !slices.Contains(times, c.Name) {
				t.Errorf("%s: time column %s", id, c.Name)
			}
		}
		for _, old := range []string{"time_created", "creation_time", "birth_time", "time_deleted", "deleted_time", "last_access_time",
			"atime", "content_md5", "access_tier"} {
			if _, ok := s.Column(old); ok {
				t.Errorf("%s: column %s", id, old)
			}
		}
	}
}

// TestVersionSpecs: the versions schemas name their role columns, each a
// column of the schema with the type and nullability its role takes; the
// current-object schemas name none.
func TestVersionSpecs(t *testing.T) {
	want := map[SchemaID]VersionSpec{
		S3VersionsV1:     {ID: "version_id", Latest: "is_latest", DeleteMarker: "is_delete_marker"},
		GCSGenerationsV1: {ID: "generation", Latest: "is_latest"},
		AzureVersionsV1:  {ID: "version_id", Latest: "is_latest"},
	}
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzureObjectsV1, AzureVersionsV1, LocalV1} {
		s, _ := Schema(id)
		if s.Versions != want[id] {
			t.Errorf("%s: versions %+v, want %+v", id, s.Versions, want[id])
		}
		if s.Versions == (VersionSpec{}) {
			continue
		}
		if s.UniqueKeys {
			t.Errorf("%s: a versions schema with unique keys", id)
		}
		if c, ok := s.Column(s.Versions.ID); !ok || c.Type != TypeString && c.Type != TypeInt64 {
			t.Errorf("%s: id %+v", id, c)
		}
		for _, name := range []string{s.Versions.Latest, s.Versions.DeleteMarker} {
			if c, ok := s.Column(name); name != "" && (!ok || c.Type != TypeBool || !c.Required) {
				t.Errorf("%s: %s %+v", id, name, c)
			}
		}
	}
}
