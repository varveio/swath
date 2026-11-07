// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
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
			segs[j].Close(false)
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
		if parts[i].MD5 != base[i].MD5 || parts[i].Rows != base[i].Rows {
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
