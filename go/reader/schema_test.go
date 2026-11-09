// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import "testing"

func TestSchema(t *testing.T) {
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzblobObjectsV1, LocalV1} {
		s, ok := Schema(id)
		if !ok || s.ID != id || s.Key.Name != "key" || !s.Key.Required || len(s.Columns) == 0 {
			t.Fatalf("%s: %+v, %v", id, s, ok)
		}
		grouped := id == S3VersionsV1 || id == GCSGenerationsV1
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
	if az.Key.Type != TypeString || az.MaxKeyBytes != 3072 || len(az.Columns) != 8 {
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
	for _, id := range []SchemaID{S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzblobObjectsV1, LocalV1} {
		s, _ := Schema(id)
		for _, c := range s.Columns {
			if c.MayBeAbsent {
				absent = append(absent, string(id)+" "+c.Name)
			}
		}
	}
	if len(absent) != 2 || absent[0] != "swath/gcs-objects/v1 time_created" || absent[1] != "swath/gcs-generations/v1 time_created" {
		t.Errorf("columns a part may lack: %q", absent)
	}
}
