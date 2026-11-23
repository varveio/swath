// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestFilesystemBoundaryMetadata(t *testing.T) {
	for _, tc := range []struct {
		extra    string
		present  bool
		count    int64
		complete bool
	}{
		{"", false, 0, false},
		{`,"filesystem_boundaries":null`, false, 0, false},
		{`,"filesystem_boundaries":{"keys_base64":[],"total_count":0,"complete":true}`, true, 0, true},
		{`,"filesystem_boundaries":{"keys_base64":["YmFk/y8="],"total_count":1,"complete":true}`, true, 1, true},
		{`,"filesystem_boundaries":{"keys_base64":["YmFk/y8="],"total_count":5000,"complete":false}`, true, 5000, false},
	} {
		m, err := ParseMetadata(manifestWith(`{"swath":{"version":1,"coverage":{"complete":true` + tc.extra + `}}}`))
		if err != nil {
			t.Fatal(err)
		}
		b := m.Coverage.FilesystemBoundaries
		if (b != nil) != tc.present {
			t.Fatalf("presence: %+v", b)
		}
		if b == nil {
			continue
		}
		if b.TotalCount != tc.count || b.Complete != tc.complete || b.KeysBase64 == nil {
			t.Fatalf("parsed %+v", b)
		}
		if len(b.KeysBase64) != 0 {
			key, err := base64.StdEncoding.DecodeString(b.KeysBase64[0])
			if err != nil || string(key) != "bad\xff/" {
				t.Fatalf("key %q %v", key, err)
			}
		}
	}
}

func TestFilesystemBoundaryMetadataInvalid(t *testing.T) {
	for _, value := range []string{
		`{}`, `{"keys_base64":[],"total_count":0}`, `{"keys_base64":null,"total_count":0,"complete":true}`,
		`{"keys_base64":[],"total_count":-1,"complete":false}`,
		`{"keys_base64":[],"total_count":1,"complete":true}`,
		`{"keys_base64":["bS8="],"total_count":0,"complete":false}`,
		`{"keys_base64":["not base64"],"total_count":1,"complete":true}`,
		`{"keys_base64":["L20v"],"total_count":1,"complete":true}`,
		`{"keys_base64":["bQ=="],"total_count":1,"complete":true}`,
	} {
		var b FilesystemBoundaries
		if err := json.Unmarshal([]byte(value), &b); err == nil {
			t.Fatalf("accepted invalid coverage %s", value)
		}
	}
}
