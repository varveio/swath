// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
)

// TestIntervalClosesAnAgedPart: with Interval set, a part open that long
// with rows is closed and published although far from full, with no new
// row arriving (the frontier stalled), and not before it is that old, nor
// again while no row arrives; with it off, nothing is published before
// Close. Rows stay in order and complete either way.
//
// The stall waits for each publication instead of sleeping a fixed time
// and asserting how soon it came: past the interval, the time to the
// publication is the ticker's wake-up and the encoder's file write,
// scheduling and disk latency that a loaded host stretches without bound
// (part 0 took 491 ms at load 30, against a 350 ms bound; the old bound
// also counted from the first row, so each oversleep of the 400 ms stall
// was charged to the next part). What the test proves is that the
// interval, and nothing else, cut the part: the row is alone, the part is
// far from full, the cut is counted, it comes no sooner than the interval
// and no further part follows without a row.
func TestIntervalClosesAnAgedPart(t *testing.T) {
	for _, interval := range []time.Duration{0, 100 * time.Millisecond} {
		t.Run(fmt.Sprint(interval), func(t *testing.T) {
			var mu sync.Mutex
			var published []time.Time
			signal := make(chan struct{}, 8)
			r, err := NewRouter(Options{Out: t.TempDir(), PartSize: 64 << 20, Encoders: 2, Interval: interval,
				Publish: func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
					mu.Lock()
					published = append(published, time.Now())
					mu.Unlock()
					signal <- struct{}{}
					return p, nil
				}}, &testSchema)
			if err != nil {
				t.Fatal(err)
			}
			count := func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(published)
			}
			for i := range 3 { // three trickles, then a stall each
				wrote := time.Now()
				if err := r.Write(context.Background(), [][]row{{{Key: fmt.Sprintf("k%d", i), Data: tr{Kind: "FILE"}}}}); err != nil {
					t.Fatal(err)
				}
				if interval == 0 {
					time.Sleep(400 * time.Millisecond)
					continue
				}
				select {
				case <-signal:
				case <-time.After(30 * time.Second):
					t.Fatalf("part %d: not published %v after its row, with no new row", i, time.Since(wrote))
				}
				mu.Lock()
				at := published[i]
				mu.Unlock()
				if d := at.Sub(wrote); d < interval {
					t.Fatalf("part %d published %v after its row, before the interval", i, d)
				}
				time.Sleep(2 * interval) // no row: no further part
				if n, cuts := count(), r.TimeCuts(); n != i+1 || cuts != int64(i+1) {
					t.Fatalf("after trickle %d: %d parts published, %d time cuts", i, n, cuts)
				}
			}
			early := count()
			parts, err := r.Close()
			if err != nil {
				t.Fatal(err)
			}
			if interval == 0 {
				if early != 0 || len(parts) != 1 || r.TimeCuts() != 0 {
					t.Fatalf("by size only: %d early, %d parts", early, len(parts))
				}
				return
			}
			if early != 3 || len(parts) != 3 || r.TimeCuts() != 3 {
				t.Fatalf("%d parts published while stalled, %d in all, %d time cuts", early, len(parts), r.TimeCuts())
			}
		})
	}
}
