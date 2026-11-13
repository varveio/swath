// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/schema"
)

// groupedSchema is testSchema in group mode: keys repeat, a key's rows
// consecutive.
var groupedSchema = func() schema.Schema[tr] {
	s := testSchema
	s.Info.ID, s.Info.Groups = "test/all-types-grouped", true
	return s
}()

// groupedRows repeats each of testRows' keys: every seventh key 40 times
// (larger than the row cap below), the rest one to three times.
func groupedRows(n int) []row {
	var out []row
	for i, r := range testRows(n, 5) {
		reps := 1 + i%3
		if i%7 == 3 {
			reps = 40
		}
		for j := range reps {
			r.Data.Size = int64(j)
			out = append(out, r)
		}
	}
	return out
}

func routeGrouped(t *testing.T, rows []row, chunk int, opt func(*Options)) ([]dataset.Part, *Router[tr], error) {
	t.Helper()
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	o := Options{Out: out, PartSize: 64 << 20, Encoders: 2}
	if opt != nil {
		opt(&o)
	}
	r, err := NewRouter(o, &groupedSchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(rows); i += chunk {
		if err := r.Write(context.Background(), [][]row{rows[i:min(i+chunk, len(rows))]}); err != nil {
			r.Abort()
			return nil, r, err
		}
	}
	parts, err := r.Close()
	return parts, r, err
}

// checkGroupBoundaries: the parts hold every row in order, and no key
// spans two parts.
func checkGroupBoundaries(t *testing.T, parts []dataset.Part, rows []row) {
	t.Helper()
	if got := readKeys(t, parts); !slices.Equal(got, keysOf(rows)) {
		t.Fatalf("parts hold %d keys, want %d (or out of order)", len(got), len(rows))
	}
	for i := 1; i < len(parts); i++ {
		if parts[i].MinKey <= parts[i-1].MaxKey {
			t.Fatalf("part %d starts at %q, inside part %d (max %q)", i, parts[i].MinKey, i-1, parts[i-1].MaxKey)
		}
	}
}

// TestGroupedRowCutsWaitForTheGroup: a row cap (and a size target)
// reached inside a key's group closes the part at the group's end, in
// chunks that end inside groups too; the cuts are counted once each.
func TestGroupedRowCutsWaitForTheGroup(t *testing.T) {
	rows := groupedRows(3000)
	for _, chunk := range []int{1, 7, 1500} {
		parts, r, err := routeGrouped(t, rows, chunk, func(o *Options) { o.MaxRows = 25 })
		if err != nil {
			t.Fatal(err)
		}
		checkGroupBoundaries(t, parts, rows)
		if len(parts) < 10 || r.DeferredRows() == 0 {
			t.Fatalf("chunk %d: %d parts, %d deferred rows: the cap did not engage inside groups", chunk, len(parts), r.DeferredRows())
		}
		if r.RowCuts() != int64(len(parts)-1) {
			t.Fatalf("chunk %d: %d row cuts for %d parts", chunk, r.RowCuts(), len(parts))
		}
		bySize, _, err := routeGrouped(t, rows, chunk, func(o *Options) { o.PartSize = 4 << 10 })
		if err != nil {
			t.Fatal(err)
		}
		checkGroupBoundaries(t, bySize, rows)
		if len(bySize) < 3 {
			t.Fatalf("chunk %d: %d size parts", chunk, len(bySize))
		}
	}
}

// TestGroupedIntervalWaitsForTheGroup: the interval asks for a cut while
// a key's rows are still arriving; the cut lands at the next key.
func TestGroupedIntervalWaitsForTheGroup(t *testing.T) {
	rows := groupedRows(400)
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(Options{Out: out, PartSize: 64 << 20, Encoders: 2, Interval: 5 * time.Millisecond}, &groupedSchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := r.Write(context.Background(), [][]row{rows[i : i+1]}); err != nil {
			t.Fatal(err)
		}
		if i%50 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	checkGroupBoundaries(t, parts, rows)
	if r.TimeCuts() == 0 || len(parts) < 2 {
		t.Fatalf("%d time cuts, %d parts: the interval did not engage", r.TimeCuts(), len(parts))
	}
}

// TestGroupedPinnedAmbiguousTail: a claimed part whose recorded row count
// ends inside a key's group (the listing now has more rows of that key
// than the claim saw) fails before the part is published; a claim that
// ends at a group's end reproduces.
func TestGroupedPinnedAmbiguousTail(t *testing.T) {
	rows := groupedRows(300)
	cold, _, err := routeGrouped(t, rows, 13, func(o *Options) { o.MaxRows = 25 })
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{0, 2} {
		got, _, err := routeGrouped(t, rows, 13, func(o *Options) { o.MaxRows, o.Cut = 25, &Cut{Ordinal: at, Rows: cold[at].Rows} })
		if err != nil {
			t.Fatalf("pinned part %d: %v", at, err)
		}
		if got[at].SHA256 != cold[at].SHA256 {
			t.Fatalf("pinned part %d differs", at)
		}
	}
	// The same claim over a listing where the claimed part's last key has
	// one more version: the boundary is no longer a group's end.
	last := int(cold[0].Rows) - 1
	grown := slices.Insert(slices.Clone(rows), last+1, rows[last])
	_, _, err = routeGrouped(t, grown, 13, func(o *Options) { o.MaxRows, o.Cut = 25, &Cut{Ordinal: 0, Rows: cold[0].Rows} })
	if !errors.Is(err, ErrAmbiguousTail) {
		t.Fatalf("%v, want ErrAmbiguousTail", err)
	}
}

// TestGroupedWriterAcceptsRepeatsNotDescents: in group mode the writer
// takes a repeated key and refuses a descending one.
func TestGroupedWriterAcceptsRepeatsNotDescents(t *testing.T) {
	rows := []row{{Key: "a"}, {Key: "b"}, {Key: "b"}, {Key: "a"}}
	for i := range rows {
		rows[i].Data.Kind = "file"
	}
	if _, _, err := routeGrouped(t, rows[:3], 3, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := routeGrouped(t, rows, 4, nil); err == nil {
		t.Fatal("a descending key was written")
	}
}

// TestDeferredRowCutCalibratesBySize: A row cut deferred through a hot group becomes
// a size cut when the group reaches the size target.
func TestDeferredRowCutCalibratesBySize(t *testing.T) {
	rows := groupedRows(30)
	_, r, err := routeGrouped(t, rows, 1, func(o *Options) { o.MaxRows = 1; o.PartSize = 100 })
	if err != nil {
		t.Fatal(err)
	}
	var rowCuts, promoted int64
	for _, p := range r.parts {
		if !p.final {
			if p.bySize {
				promoted++
			} else {
				rowCuts++
			}
		}
		if !p.final && p.logical >= int64(float64(r.opt.PartSize)/initialRatio) && !p.bySize {
			t.Fatalf("oversize completed part %d (logical=%d initial target=%d) was not marked bySize; limit=%d", p.ordinal, p.logical, int64(float64(r.opt.PartSize)/initialRatio), p.limit)
		}
	}
	if promoted == 0 || r.RowCuts() != rowCuts {
		t.Fatalf("promoted=%d rowCuts=%d want=%d", promoted, r.RowCuts(), rowCuts)
	}

}

func TestDeferredTimerCutPromotedBySize(t *testing.T) {
	r, err := NewRouter(Options{Out: t.TempDir(), PartSize: 100, Encoders: 1, Interval: time.Millisecond}, &groupedSchema)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Abort()
	rows := groupedRows(4)
	// The fourth key is hot. Start just that group, and let the real
	// ticker request a cut before it reaches the size target.
	group := rows[len(rows)-40:]
	if err := r.Write(context.Background(), [][]row{group[:1]}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.Lock()
		pending := r.full && r.byTime
		r.mu.Unlock()
		if pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ticker did not engage")
		}
		time.Sleep(time.Millisecond)
	}
	if err := r.Write(context.Background(), [][]row{group[1:]}); err != nil {
		t.Fatal(err)
	}
	next := group[0]
	next.Key += "z"
	if err := r.Write(context.Background(), [][]row{{next}}); err != nil {
		t.Fatal(err)
	}
	var parts []dataset.Part
	parts, err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || !r.parts[0].bySize || r.parts[0].limit == 0 || r.TimeCuts() != 0 || r.DeferredRows() != 39 {
		t.Fatalf("parts=%d bySize=%v limit=%d timerCuts=%d deferred=%d", len(parts), r.parts[0].bySize, r.parts[0].limit, r.TimeCuts(), r.DeferredRows())
	}
}
