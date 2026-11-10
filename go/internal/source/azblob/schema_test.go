// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"reflect"
	"slices"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
)

// TestSchemaBinding pins the producer's specification and accessors, so
// reader binding cannot silently change the columns written to a dataset.
func TestSchemaBinding(t *testing.T) {
	wantInfo := schema.Info{Name: "swath_listing", ID: "swath/azblob-objects/v1", Key: schema.String, MaxKeyBytes: 3072, StorageClass: "access_tier"}
	if !reflect.DeepEqual(Schema.Info, wantInfo) || Schema.MaxKeyBytes != MaxKeyBytes {
		t.Fatalf("info: %+v, want %+v", Schema.Info, wantInfo)
	}
	wantSpecs := []schema.Spec{
		{Name: "key", Type: schema.String, Required: true},
		{Name: "size", Type: schema.Int64},
		{Name: "last_modified", Type: schema.TimestampMicros},
		{Name: "etag", Type: schema.String},
		{Name: "content_md5", Type: schema.String},
		{Name: "content_type", Type: schema.String},
		{Name: "blob_type", Type: schema.String},
		{Name: "access_tier", Type: schema.String},
		{Name: "creation_time", Type: schema.TimestampMicros},
	}
	if got := Schema.Specs(); !slices.Equal(got, wantSpecs) {
		t.Fatalf("specs: %+v, want %+v", got, wantSpecs)
	}
	o := Object{Size: 7, Mtime: 11, CreationTime: 13, ETag: "etag", ContentMD5: "md5", ContentType: "type", BlobType: "blob", AccessTier: "tier"}
	wantInts := map[string]int64{"size": 7, "last_modified": 11, "creation_time": 13}
	wantStrings := map[string]string{"etag": "etag", "content_md5": "md5", "content_type": "type", "blob_type": "blob", "access_tier": "tier"}
	for _, c := range Schema.Columns {
		if c.Delta != (c.Name == "etag") || c.Bool != nil {
			t.Fatalf("%s: encoding or accessor changed", c.Name)
		}
		if c.Int != nil {
			if v, ok := c.Int(&o); !ok || v != wantInts[c.Name] {
				t.Fatalf("%s: (%d, %v)", c.Name, v, ok)
			}
			if v, ok := c.Int(&Object{}); v != 0 || ok != (c.Name == "size") {
				t.Fatalf("%s zero: (%d, %v)", c.Name, v, ok)
			}
		} else {
			if v, ok := c.Str(&o); !ok || v != wantStrings[c.Name] {
				t.Fatalf("%s: (%q, %v)", c.Name, v, ok)
			}
			if v, ok := c.Str(&Object{}); v != "" || ok {
				t.Fatalf("%s zero: (%q, %v)", c.Name, v, ok)
			}
		}
	}
	if Schema.Heap(&o) != 19 || Schema.Bytes(&o) != 43 || Schema.Heap(&Object{}) != 0 || Schema.Bytes(&Object{}) != 24 {
		t.Fatalf("heap %d, bytes %d", Schema.Heap(&o), Schema.Bytes(&o))
	}
	if _, ok := Schema.Codec.(objectCodec); !ok {
		t.Fatalf("codec: %T", Schema.Codec)
	}
}
