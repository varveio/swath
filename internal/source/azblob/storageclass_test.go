// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"slices"
	"testing"

	"github.com/varveio/swath/internal/filter"
)

// TestStorageClassIsTheAccessTier: --storage-class filters Azure Blob rows
// by their access tier, the column the schema declares its storage class,
// case-insensitively; a blob without a tier is not in any class.
func TestStorageClassIsTheAccessTier(t *testing.T) {
	keep, err := filter.Compile(&filter.Spec{StorageClasses: []string{"hot", "ARCHIVE"}}, &Schema)
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, r := range []Row{
		{Key: "a", Data: Object{AccessTier: "Hot"}},
		{Key: "b", Data: Object{AccessTier: "Cool"}},
		{Key: "c", Data: Object{AccessTier: "Archive"}},
		{Key: "d", Data: Object{}},
	} {
		if keep(&r) {
			kept = append(kept, r.Key)
		}
	}
	if !slices.Equal(kept, []string{"a", "c"}) {
		t.Fatalf("kept %q, want [a c]", kept)
	}
}
