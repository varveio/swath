// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// Exercise schema-derived projections independently of provider identity.
// The fixture has all metadata; the view must read only what the supplied
// schema declares, including schemas with just one of the optional fields.
func TestObjectMetadataProjection(t *testing.T) {
	for _, modified := range []bool{false, true} {
		for _, class := range []bool{false, true} {
			t.Run(fmt.Sprintf("modified=%t/class=%t", modified, class), func(t *testing.T) {
				records := objects()[:6]
				inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: records}))
				spec, _ := reader.Schema(reader.S3ObjectsV1)
				spec.Columns = slices.DeleteFunc(spec.Columns, func(c reader.ColumnSpec) bool {
					return c.Name == "last_modified" && !modified || c.Name == "storage_class" && !class
				})
				inv.rows = shapeOf(spec)
				want := slices.Clone(records)
				for i := range want {
					if !modified {
						want[i].modified = nil
					}
					if !class {
						want[i].class = nil
					}
					got, err := inv.Lookup(context.Background(), []byte(want[i].key))
					if err != nil || got.Entry == nil || !sameEntry(*got.Entry, want[i], false) {
						t.Fatalf("lookup %s: %+v, %v; want %+v", want[i].key, got.Entry, err, want[i])
					}
				}
				for _, split := range []listing.Split{{}, {StorageClass: true}, {StorageClass: true, SizeBounds: []int64{10, 100}}} {
					sums, _, err := inv.Summarize(context.Background(), []listing.Target{{Subtree: true}}, listing.SummaryOptions{Split: split})
					if err != nil || len(sums) != 1 {
						t.Fatalf("summarize: %+v %v", sums, err)
					}
					all := func(rec) bool { return true }
					if expected := totalsOf(want, false, nil, all); !sameTotals(sums[0].Totals, expected) {
						t.Fatalf("totals %s, want %s", showTotals(sums[0].Totals), showTotals(expected))
					}
					if expected := groupsOf(want, false, nil, all, split); !sameGroups(sums[0].Groups, expected) {
						t.Fatalf("groups %s, want %s", showGroups(sums[0].Groups), showGroups(expected))
					}
				}
			})
		}
	}
}

// The object projection must preserve a schema's required-size contract in
// both row lookups and batch summaries, even when a malformed part has a null.
func TestRequiredObjectSize(t *testing.T) {
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: []rec{{key: "missing"}}}))
	spec, _ := reader.Schema(reader.S3ObjectsV1)
	for i := range spec.Columns {
		if spec.Columns[i].Name == "size" {
			spec.Columns[i].Required = true
		}
	}
	inv.rows = shapeOf(spec)
	if _, err := inv.Lookup(context.Background(), []byte("missing")); !errors.Is(err, reader.ErrRow) {
		t.Errorf("lookup accepted required null size: %v", err)
	}
	if _, _, err := inv.Summarize(context.Background(), []listing.Target{{Subtree: true}}, listing.SummaryOptions{}); !errors.Is(err, reader.ErrRow) {
		t.Errorf("summary accepted required null size: %v", err)
	}
}

// Pin the common view's projection for each public schema: schema changes
// must not silently remove metadata from existing inventories.
func TestRegisteredObjectProjections(t *testing.T) {
	object := rowShape{columns: []string{"size", "last_modified", "storage_class"}, modified: 1, class: 2}
	want := map[reader.SchemaID]rowShape{
		reader.S3ObjectsV1: object, reader.S3VersionsV1: object,
		reader.GCSObjectsV1: object, reader.GCSGenerationsV1: object,
		reader.AzureObjectsV1: object, reader.AzureVersionsV1: object,
		reader.VercelObjectsV1: {columns: []string{"size"}, modified: -1, class: -1, sizeRequired: true},
		reader.LocalV1:         {local: true, columns: []string{"size", "last_modified", "kind", "nlink", "inode", "dev", "allocated_bytes", "link_target"}, modified: 1, class: -1},
	}
	for _, id := range reader.SchemaIDs() {
		spec, _ := reader.Schema(id)
		expected, ok := want[id]
		if got := shapeOf(spec); !ok || !reflect.DeepEqual(got, expected) {
			t.Errorf("%s: projection %+v, want %+v (known %t)", id, got, expected, ok)
		}
	}
}
