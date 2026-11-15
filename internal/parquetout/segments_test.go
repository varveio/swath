// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
)

// segmentRun writes rows through a router: serially until the ratio is
// frozen and half of the open part is written, then the rest as segments
// along the serial run's own boundaries (base), the later segments written
// and closed first, the first last and slowly. It returns the parts and
// the published ordinals in order; abortFirst leaves the first segment
// unfinished and aborts instead.
func segmentRun(t *testing.T, rows []row, base []dataset.Part, abortFirst bool) ([]dataset.Part, []int, error) {
	t.Helper()
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var published []int
	r, err := NewRouter(Options{Out: out, PartSize: 16 << 10, Encoders: len(base) + 1, // every segment open at once
		Publish: func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
			mu.Lock()
			defer mu.Unlock()
			published = append(published, ordinal)
			return p, nil
		}}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Serially: part 0 and half of part 1.
	serial := int(base[0].Rows + base[1].Rows/2)
	for i := 0; i < serial; i += 1000 {
		if err := r.Write(ctx, [][]row{rows[i:min(i+1000, serial)]}); err != nil {
			t.Fatal(err)
		}
	}
	pl, ok := r.Plan()
	if !ok || !pl.Open || pl.Rows != base[1].Rows/2 {
		t.Fatalf("plan %+v, %v", pl, ok)
	}
	if _, ok := r.BeginSegments(); !ok {
		t.Fatal("segments could not begin")
	}
	if err := r.Write(ctx, [][]row{rows[serial : serial+1]}); err == nil {
		t.Fatal("Write after the segments began")
	}
	// The segments: the rest of part 1, then every later part.
	var segs []*Segment[tr]
	var spans [][2]int
	at := serial
	end := int(base[0].Rows + base[1].Rows)
	for i := 1; i < len(base); i++ {
		s, err := r.OpenSegment(ctx, i == 1)
		if err != nil {
			t.Fatal(err)
		}
		segs, spans = append(segs, s), append(spans, [2]int{at, end})
		at = end
		if i+1 < len(base) {
			end += int(base[i+1].Rows)
		}
	}
	for j := len(segs) - 1; j >= 0; j-- { // the last part first: it is fast, the first slow
		if j == 0 {
			time.Sleep(50 * time.Millisecond)
			if abortFirst {
				_ = segs[0].Write(ctx, [][]row{rows[spans[0][0] : spans[0][0]+10]})
				r.Abort()
				mu.Lock()
				defer mu.Unlock()
				return nil, published, nil
			}
		}
		a, b := spans[j][0], spans[j][1]
		for i := a; i < b; i += 700 {
			if err := segs[j].Write(ctx, [][]row{rows[i:min(i+700, b)]}); err != nil {
				t.Fatal(err)
			}
		}
		if j == len(segs)-1 {
			segs[j].Hold(false) // Close marks it final
		} else {
			segs[j].Close(ctx, false)
		}
	}
	parts, err := r.Close()
	mu.Lock()
	defer mu.Unlock()
	return parts, published, err
}

// segmentFixture is a few small parts (every segment's writer is open at
// once, each with its codec: keep them few, under -race too).
func segmentFixture(t *testing.T) ([]row, []dataset.Part) {
	rows := testRows(5_000, 2)
	_, base := route(t, rows, 1, 16<<10)
	if len(base) < 4 || len(base) > 8 {
		t.Fatalf("want 4 to 8 parts, got %d", len(base))
	}
	return rows, base
}

// TestSegmentsAreTheSerialParts: parts written as segments, the last ones
// first, are the serial run's byte for byte, published in ordinal order.
func TestSegmentsAreTheSerialParts(t *testing.T) {
	rows, base := segmentFixture(t)
	parts, published, err := segmentRun(t, rows, base, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != len(base) {
		t.Fatalf("%d parts vs %d", len(parts), len(base))
	}
	for i := range parts {
		if parts[i].SHA256 != base[i].SHA256 || parts[i].Rows != base[i].Rows {
			t.Fatalf("part %d differs", i)
		}
	}
	if !slices.IsSorted(published) || len(published) != len(base) {
		t.Fatalf("published %v", published)
	}
}

// TestSegmentAbortPublishesNoPartialPart: the first segment is written in
// part when the router aborts. Its part is never published, nor any after
// it, although they were complete and encoded first.
func TestSegmentAbortPublishesNoPartialPart(t *testing.T) {
	rows, base := segmentFixture(t)
	_, published, _ := segmentRun(t, rows, base, true)
	for _, o := range published {
		if o >= 1 {
			t.Fatalf("published %v: part %d after the unfinished part 1", published, o)
		}
	}
}

// keepRun writes rows through a router as a seeded run's drainer does when
// it rolls: serially until the ratio is frozen and half of part 1 is
// written; then a stretch (the rest of part 1 as a segment continuing the
// open part, and the first keep rows of part 2 as a new segment, kept for
// Write with the rule's state after them, as order's cut states it);
// then the rest through Write again. full says the kept part reached the
// size target at its last row (a cut pending). It returns the parts.
func keepRun(t *testing.T, rows []row, base []dataset.Part, keep int, full bool) ([]dataset.Part, error) {
	t.Helper()
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(Options{Out: out, PartSize: 16 << 10, Encoders: 3}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	serial := int(base[0].Rows + base[1].Rows/2)
	if err := r.Write(ctx, [][]row{rows[:serial]}); err != nil {
		t.Fatal(err)
	}
	pl, ok := r.BeginSegments()
	if !ok {
		t.Fatal("segments could not begin")
	}
	end1 := int(base[0].Rows + base[1].Rows)
	s0, err := r.OpenSegment(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := r.OpenSegment(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s0.Write(ctx, [][]row{rows[serial:end1]}); err != nil {
		t.Fatal(err)
	}
	s0.Close(ctx, false)
	kept := rows[end1 : end1+keep]
	if err := s1.Write(ctx, [][]row{kept}); err != nil {
		t.Fatal(err)
	}
	st := dataset.SegmentPlan{Open: true, Rows: int64(keep), Full: full}
	for i := range kept {
		st.Logical += testSchema.LogicalBytes(&kept[i])
	}
	if full != (st.Logical >= pl.Limit) {
		t.Fatalf("kept %d rows of %d logical bytes against the limit %d: full %v", keep, st.Logical, pl.Limit, full)
	}
	if err := s1.Keep(false, st, kept[keep-1].Key); err != nil {
		t.Fatal(err)
	}
	r.AbandonSegments()
	if err := r.Write(ctx, [][]row{rows[end1+keep:]}); err != nil {
		t.Fatal(err)
	}
	return r.Close()
}

// TestKeptPartContinuesAsWrite: a stretch's last part kept for Write
// (Segment.Keep) with the rule's state after its last row, partway through
// the part and exactly at its size target (a cut pending), continues as
// the serial run's: every part is the serial run's, byte for byte. Kept at
// rows its segment did not write, it is refused before anything changes.
func TestKeptPartContinuesAsWrite(t *testing.T) {
	rows, base := segmentFixture(t)
	end1 := int(base[0].Rows + base[1].Rows)
	var limit, logical int64 // the frozen limit, and part 2's logical bytes until it reaches it
	{
		out := t.TempDir()
		os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755)
		r, err := NewRouter(Options{Out: out, PartSize: 16 << 10, Encoders: 1}, &testSchema)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Write(context.Background(), [][]row{rows[:end1+1]}); err != nil {
			t.Fatal(err)
		}
		pl, _ := r.Plan()
		limit = pl.Limit
		r.Abort()
	}
	atLimit := 0
	for i := end1; i < len(rows); i++ {
		if logical += testSchema.LogicalBytes(&rows[i]); logical >= limit {
			atLimit = i - end1 + 1
			break
		}
	}
	if atLimit == 0 || int64(atLimit) != base[2].Rows {
		t.Fatalf("part 2 reaches the limit at %d rows, holds %d", atLimit, base[2].Rows)
	}
	for _, keep := range []struct {
		rows int
		full bool
	}{{atLimit / 2, false}, {atLimit, true}} {
		parts, err := keepRun(t, rows, base, keep.rows, keep.full)
		if err != nil {
			t.Fatal(err)
		}
		if len(parts) != len(base) {
			t.Fatalf("kept at %d: %d parts vs %d", keep.rows, len(parts), len(base))
		}
		for i := range parts {
			if parts[i].SHA256 != base[i].SHA256 || parts[i].Rows != base[i].Rows {
				t.Fatalf("kept at %d: part %d differs", keep.rows, i)
			}
		}
	}
	out := t.TempDir()
	os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755)
	r, err := NewRouter(Options{Out: out, PartSize: 16 << 10, Encoders: 2}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Abort()
	ctx := context.Background()
	if err := r.Write(ctx, [][]row{rows[:end1+1]}); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.BeginSegments(); !ok {
		t.Fatal("segments could not begin")
	}
	s, err := r.OpenSegment(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, [][]row{rows[end1+1 : end1+11]}); err != nil {
		t.Fatal(err)
	}
	var inv *dataset.InvariantError
	if err := s.Keep(false, dataset.SegmentPlan{Open: true, Rows: 12}, rows[end1+10].Key); !errors.As(err, &inv) {
		t.Fatalf("kept at 12 rows of 11: %v", err)
	}
}
