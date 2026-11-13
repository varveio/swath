// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"reflect"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source/azblob/client"
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
		{Name: "access_tier_inferred", Type: schema.Bool},
		{Name: "access_tier_change_time", Type: schema.TimestampMicros},
		{Name: "archive_status", Type: schema.String},
		{Name: "rehydrate_priority", Type: schema.String},
		{Name: "last_access_time", Type: schema.TimestampMicros},
		{Name: "content_encoding", Type: schema.String},
		{Name: "server_encrypted", Type: schema.Bool},
		{Name: "encryption_scope", Type: schema.String},
		{Name: "customer_provided_key_sha256", Type: schema.String},
		{Name: "lease_state", Type: schema.String},
		{Name: "tag_count", Type: schema.Int64},
		{Name: "metadata", Type: schema.String},
		{Name: "tags", Type: schema.String},
		{Name: "immutability_policy_until", Type: schema.TimestampMicros},
		{Name: "immutability_policy_mode", Type: schema.String},
		{Name: "legal_hold", Type: schema.Bool},
	}
	if got := Schema.Specs(); !slices.Equal(got, wantSpecs) {
		t.Fatalf("specs: %+v, want %+v", got, wantSpecs)
	}
	o := Object{Size: 7, Mtime: 11, CreationTime: 13, ETag: "etag", ContentMD5: "md5", ContentType: "type", BlobType: "blob", AccessTier: "tier",
		Props: client.Props{AccessTierInferred: client.True, AccessTierChangeTime: 17, ArchiveStatus: "arch", RehydratePriority: "prio",
			LastAccessTime: 19, ContentEncoding: "enc", ServerEncrypted: client.False, EncryptionScope: "scope", CustomerProvidedKeySHA256: "cpk",
			LeaseState: "lease", TagCount: 23, Metadata: "{}", Tags: "{}", ImmutabilityUntil: 29, ImmutabilityMode: "mode", LegalHold: client.True}}
	wantInts := map[string]int64{"size": 7, "last_modified": 11, "creation_time": 13, "access_tier_change_time": 17, "last_access_time": 19,
		"tag_count": 23, "immutability_policy_until": 29}
	wantStrings := map[string]string{"etag": "etag", "content_md5": "md5", "content_type": "type", "blob_type": "blob", "access_tier": "tier",
		"archive_status": "arch", "rehydrate_priority": "prio", "content_encoding": "enc", "encryption_scope": "scope",
		"customer_provided_key_sha256": "cpk", "lease_state": "lease", "metadata": "{}", "tags": "{}", "immutability_policy_mode": "mode"}
	wantBools := map[string]bool{"access_tier_inferred": true, "server_encrypted": false, "legal_hold": true}
	for _, c := range Schema.Columns {
		if c.Delta != (c.Name == "etag" || c.Name == "metadata" || c.Name == "tags") {
			t.Fatalf("%s: encoding changed", c.Name)
		}
		if c.Bool != nil {
			if v, ok := c.Bool(&o); !ok || v != wantBools[c.Name] {
				t.Fatalf("%s: (%v, %v)", c.Name, v, ok)
			}
			if _, ok := c.Bool(&Object{}); ok {
				t.Fatalf("%s zero: not null", c.Name)
			}
			continue
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
	// Heap: the strings' bytes; Bytes: the first three integers, the
	// four later ones with a value, three booleans and the heap.
	if Schema.Heap(&o) != 51 || Schema.Bytes(&o) != 24+32+3+51 || Schema.Heap(&Object{}) != 0 || Schema.Bytes(&Object{}) != 24 {
		t.Fatalf("heap %d, bytes %d", Schema.Heap(&o), Schema.Bytes(&o))
	}
	if _, ok := Schema.Codec.(objectCodec); !ok {
		t.Fatalf("codec: %T", Schema.Codec)
	}
}
