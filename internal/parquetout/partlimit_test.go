// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
)

// TestRouterRefusesPartPastLimit: part names have five digits, so the
// router refuses to open part 100,000 (dataset.ErrPartLimit) once parts
// 0 to 99,999 exist, after publishing every earlier part: the journal's
// last record is 99,999. A resume of 99,998 parts stands in for the run
// before, and a one-row cap closes each new part (owner decision,
// 2026-11-06: before, ordinal 100,000 was written as part-100000, a name
// that breaks the five-digit contract).
func TestRouterRefusesPartPastLimit(t *testing.T) {
	const resumed = dataset.MaxParts - 2
	parts := make([]dataset.Part, resumed)
	parts[resumed-1] = dataset.Part{PartInfo: dataset.PartInfo{Rows: 1, MinKey: "a", MaxKey: "a"}}
	var mu sync.Mutex
	var published []int
	opt := Options{Out: t.TempDir(), PartSize: 1 << 20, MaxRows: 1, Encoders: 2,
		Resume: &Resume{Parts: parts, Limit: 1 << 20, Rows: resumed},
		Publish: func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
			mu.Lock()
			defer mu.Unlock()
			published = append(published, ordinal)
			return p, nil
		}}
	r, err := NewRouter(opt, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Abort()
	var werr error
	for _, k := range []string{"b", "c", "d"} {
		if werr = r.Write(context.Background(), [][]row{{{Key: k, Data: tr{Kind: "FILE"}}}}); werr != nil {
			break
		}
	}
	if !errors.Is(werr, dataset.ErrPartLimit) {
		t.Fatalf("write error %v, want ErrPartLimit", werr)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(published) != 2 || published[0] != dataset.MaxParts-2 || published[1] != dataset.MaxParts-1 {
		t.Fatalf("published ordinals %v, want %d and %d", published, dataset.MaxParts-2, dataset.MaxParts-1)
	}
}

// TestSegmentPastLimitCancels: a resume one part short of the ceiling
// continues part 99,999 as its first segment and opens a second, which the
// router refuses (dataset.ErrPartLimit) once part 99,999 is published. If
// the first segment fails instead, its part is never closed, so never
// published, until the pipeline's Abort, which waits for every segment
// worker: the refusal's wait must end on cancellation and give its encoder
// slot back (sol review 2, P1: it waited for the publication alone, and the
// run hung).
func TestSegmentPastLimitCancels(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		const resumed = dataset.MaxParts - 1
		parts := make([]dataset.Part, resumed)
		parts[resumed-1] = dataset.Part{PartInfo: dataset.PartInfo{Rows: 1, MinKey: "a", MaxKey: "a"}}
		r, err := NewRouter(Options{Out: t.TempDir(), PartSize: 1 << 20, Encoders: 2,
			Resume: &Resume{Parts: parts, Limit: 1 << 20, Rows: resumed}}, &testSchema)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		if err := r.Write(ctx, [][]row{{{Key: "b", Data: tr{Kind: "FILE"}}}}); err != nil {
			t.Fatal(err)
		}
		if _, ok := r.BeginSegments(); !ok {
			t.Fatal("segments could not begin")
		}
		first, err := r.OpenSegment(ctx, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Write(ctx, [][]row{{{Key: "c", Data: tr{Kind: "FILE"}}}}); err != nil {
			t.Fatal(err)
		}
		got := make(chan error, 1)
		go func() {
			_, err := r.OpenSegment(ctx, false)
			got <- err
		}()
		cause := errors.New("the first segment failed")
		if failFirst {
			time.Sleep(20 * time.Millisecond) // the second is waiting for the first's publication
			cancel(cause)                     // what the order stage does when a segment fails
		} else {
			first.Close(ctx, false)
		}
		select {
		case err := <-got:
			if failFirst && !errors.Is(err, cause) || !failFirst && !errors.Is(err, dataset.ErrPartLimit) {
				t.Fatalf("failFirst %v: %v", failFirst, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("failFirst %v: the part past the limit waits for a part that is never published", failFirst)
		}
		if n := len(r.slots); n > 1 {
			t.Fatalf("failFirst %v: %d slots held, want at most the first segment's", failFirst, n)
		}
		r.Abort()
		cancel(nil)
	}
}
