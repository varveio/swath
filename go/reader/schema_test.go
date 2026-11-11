// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"slices"
	"testing"
)

func TestSchema(t *testing.T) {
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzblobObjectsV1, AzblobVersionsV1, LocalV1} {
		s, ok := Schema(id)
		if !ok || s.ID != id || s.Key.Name != "key" || !s.Key.Required || len(s.Columns) == 0 {
			t.Fatalf("%s: %+v, %v", id, s, ok)
		}
		grouped := id == S3VersionsV1 || id == GCSGenerationsV1 || id == AzblobVersionsV1
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
	az, _ := Schema(AzblobObjectsV1)
	if az.Key.Type != TypeString || az.MaxKeyBytes != 3072 || len(az.Columns) != 24 {
		t.Errorf("azblob %+v", az)
	}
	for _, name := range []string{"content_md5", "access_tier", "blob_type"} {
		if c, ok := az.Column(name); !ok || c.Type != TypeString || c.Required || c.MayBeAbsent {
			t.Errorf("azblob %s: %+v", name, c)
		}
	}
	if c, ok := az.Column("creation_time"); !ok || c.Type != TypeTimestampMicros {
		t.Errorf("azblob creation_time: %+v", c)
	}
	var absent []string
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzblobObjectsV1, AzblobVersionsV1, LocalV1} {
		s, _ := Schema(id)
		for _, c := range s.Columns {
			if c.MayBeAbsent {
				absent = append(absent, string(id)+" "+c.Name)
			}
		}
	}
	// The Azure Blob columns added after the first eight: the properties
	// every listing carries, then those a run asks for.
	added := []string{"access_tier_inferred", "access_tier_change_time", "archive_status", "rehydrate_priority",
		"last_access_time", "content_encoding", "server_encrypted", "encryption_scope", "customer_provided_key_sha256",
		"lease_state", "tag_count", "metadata", "tags", "immutability_policy_until", "immutability_policy_mode", "legal_hold"}
	want := []string{"swath/gcs-objects/v1 time_created", "swath/gcs-generations/v1 time_created"}
	for _, id := range []string{"swath/azblob-objects/v1 ", "swath/azblob-versions/v1 "} {
		for _, name := range added {
			want = append(want, id+name)
		}
	}
	// The versions schema: the current columns, then the entry's.
	av, _ := Schema(AzblobVersionsV1)
	if names := av.ColumnNames(); !slices.Equal(names[:24], az.ColumnNames()) ||
		!slices.Equal(names[24:], []string{"version_id", "is_latest", "snapshot", "is_deleted", "deleted_time", "remaining_retention_days"}) {
		t.Errorf("azblob versions columns %q", names)
	}
	if !slices.Equal(absent, want) {
		t.Errorf("columns a part may lack: %q, want %q", absent, want)
	}
	for i, c := range az.Columns {
		if c.MayBeAbsent != (i >= 8) || c.Required {
			t.Errorf("azblob column %d %+v", i, c)
		}
	}
}

// TestVersionSpecs: the versions schemas name their role columns, each a
// column of the schema with the type and nullability its role takes; the
// current-object schemas name none.
func TestVersionSpecs(t *testing.T) {
	want := map[SchemaID]VersionSpec{
		S3VersionsV1:     {ID: "version_id", Latest: "is_latest", DeleteMarker: "is_delete_marker"},
		GCSGenerationsV1: {ID: "generation", DeletedAt: "time_deleted"},
		AzblobVersionsV1: {ID: "version_id", Latest: "is_latest"},
	}
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzblobObjectsV1, AzblobVersionsV1, LocalV1} {
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
		if c, ok := s.Column(s.Versions.DeletedAt); s.Versions.DeletedAt != "" && (!ok || c.Type != TypeTimestampMicros || c.Required) {
			t.Errorf("%s: deleted at %+v", id, c)
		}
	}
}
