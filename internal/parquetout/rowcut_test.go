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

// TestWriterAccountAtListingDone: the writer's account recorded when
// listing ends holds the parts published by then and their rows, and the
// encoders' time split into encoding and output (the part files' writes,
// syncs and closes; not asserted positive, as a coarse clock (Windows)
// reads a short write as 0); later parts do not change it. Listing ends here while
// the last part publishes: parts 0 and 1 are published, in order, before.
func TestWriterAccountAtListingDone(t *testing.T) {
	out := t.TempDir()
	var r *Router[tr]
	publish := func(ordinal int, p dataset.Part, _, _ int64, _ bool) (dataset.Part, error) {
		if ordinal == 2 {
			r.ListingDone()
		}
		return p, nil
	}
	r, err := NewRouter(Options{Out: out, PartSize: 64 << 20, Encoders: 1, MaxRows: 3000, Publish: publish}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.AtListingDone(); ok {
		t.Fatal("an account before listing ended")
	}
	rows := testRows(7000, 5)
	for i := 0; i < len(rows); i += 1500 {
		if err := r.Write(context.Background(), [][]row{rows[i:min(i+1500, len(rows))]}); err != nil {
			t.Fatal(err)
		}
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	w, ok := r.AtListingDone()
	if len(parts) != 3 || !ok || w.Parts != 2 || w.Rows != 6000 || w.EncodeNs < 0 || w.OutputNs < 0 {
		t.Fatalf("%d parts; at listing done (%v): %+v", len(parts), ok, w)
	}
}
