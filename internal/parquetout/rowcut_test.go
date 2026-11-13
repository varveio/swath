// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/dataset"
)

func routeMaxRows(t *testing.T, rows []row, encoders int, partSize, maxRows int64) ([]dataset.Part, int64) {
	t.Helper()
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(Options{Out: out, PartSize: partSize, Encoders: encoders, MaxRows: maxRows}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(rows); i += 1500 {
		if err := r.Write(context.Background(), [][]row{rows[i:min(i+1500, len(rows))]}); err != nil {
			t.Fatal(err)
		}
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	return parts, r.RowCuts()
}

// TestMaxRowsRotation: --part-rotation-max-rows closes a part at N rows
// besides the size target; the boundaries depend on the rows only (the same
// parts at any encoder count), the cuts are counted, and a size-cut part
// still sets the ratio: with a row bound that never binds, the parts are
// those of the size rule alone.
func TestMaxRowsRotation(t *testing.T) {
	rows := testRows(20_000, 3)
	parts, cuts := routeMaxRows(t, rows, 1, 64<<20, 3000)
	if len(parts) != 7 || cuts != 6 {
		t.Fatalf("%d parts, %d row cuts", len(parts), cuts)
	}
	for i, p := range parts {
		if want := int64(min(3000, 20_000-3000*i)); p.Rows != want {
			t.Fatalf("part %d: %d rows, want %d", i, p.Rows, want)
		}
	}
	again, _ := routeMaxRows(t, rows, 4, 64<<20, 3000)
	for i := range parts {
		if again[i].SHA256 != parts[i].SHA256 {
			t.Fatalf("part %d differs across encoder counts", i)
		}
	}
	if got := readKeys(t, parts); !slices.Equal(got, keysOf(rows)) {
		t.Fatal("keys differ")
	}
	bySize, _ := routeMaxRows(t, rows, 1, 16<<10, 0)
	loose, cuts := routeMaxRows(t, rows, 1, 16<<10, 1<<40)
	if cuts != 0 || len(loose) != len(bySize) {
		t.Fatalf("an unbinding row bound changed the parts: %d vs %d, %d cuts", len(loose), len(bySize), cuts)
	}
	for i := range loose {
		if loose[i].SHA256 != bySize[i].SHA256 {
			t.Fatalf("part %d differs", i)
		}
	}
}
