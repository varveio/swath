// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package legacy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
)

// keyIndexOrders returns the key column's declared boundary order in each
// row group of the part at path.
func keyIndexOrders(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, _ := f.Stat()
	file, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := file.Schema().Lookup("key")
	var orders []string
	for g, rg := range file.RowGroups() {
		index, err := rg.ColumnChunks()[leaf.ColumnIndex].ColumnIndex()
		if err != nil {
			t.Fatal(err)
		}
		order := "UNORDERED"
		switch {
		case index.IsAscending():
			order = "ASCENDING"
		case index.IsDescending():
			order = "DESCENDING"
		}
		orders = append(orders, fmt.Sprintf("%d:%d pages %s", g, index.NumPages(), order))
	}
	return orders
}

// TestTestWriterDeclaresAscendingKeyIndex requires the test writer's key
// column index to declare ASCENDING, as the Java writer's sorted output
// does and the Java server's rollup requires: one-page and many-page row
// groups. A many-page group whose page bounds descend keeps the order its
// bounds have.
func TestTestWriterDeclaresAscendingKeyIndex(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("k%04d", i))
	}
	dir := t.TempDir()
	cases := map[string]legacytest.Part{
		"one page":   {Rows: rowsOf(keys[:5])},
		"many pages": {Rows: rowsOf(keys), PageBytes: 512, GroupRows: 150},
	}
	for name, part := range cases {
		part.Stamps = legacytest.Stamps(1, true)
		path := filepath.Join(dir, name+".parquet")
		if err := legacytest.Write(path, part); err != nil {
			t.Fatal(err)
		}
		for _, order := range keyIndexOrders(t, path) {
			if want := "ASCENDING"; order[len(order)-len(want):] != want {
				t.Fatalf("%s: key index %s", name, order)
			}
		}
	}
	reversed := make([]string, len(keys))
	for i, k := range keys {
		reversed[len(keys)-1-i] = k
	}
	path := filepath.Join(dir, "descending.parquet")
	if err := legacytest.Write(path, legacytest.Part{Rows: rowsOf(reversed), PageBytes: 512}); err != nil {
		t.Fatal(err)
	}
	if orders := keyIndexOrders(t, path); orders[0][len(orders[0])-10:] != "DESCENDING" {
		t.Fatalf("descending bounds: key index %s", orders[0])
	}
}
