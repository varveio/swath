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
// row arriving (the frontier stalled); with it off, nothing is published
// before Close. Rows stay in order and complete either way.
func TestIntervalClosesAnAgedPart(t *testing.T) {
	for _, interval := range []time.Duration{0, 100 * time.Millisecond} {
		t.Run(fmt.Sprint(interval), func(t *testing.T) {
			var mu sync.Mutex
			var published []time.Time
			r, err := NewRouter(Options{Out: t.TempDir(), PartSize: 64 << 20, Encoders: 2, Interval: interval,
				Publish: func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
					mu.Lock()
					published = append(published, time.Now())
					mu.Unlock()
					return p, nil
				}}, &testSchema)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			for i := range 3 { // three trickles, then a stall each
				if err := r.Write(context.Background(), [][]row{{{Key: fmt.Sprintf("k%d", i), Data: tr{Kind: "FILE"}}}}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(400 * time.Millisecond)
			}
			mu.Lock()
			early := append([]time.Time(nil), published...)
			mu.Unlock()
			parts, err := r.Close()
			if err != nil {
				t.Fatal(err)
			}
			if interval == 0 {
				if len(early) != 0 || len(parts) != 1 || r.TimeCuts() != 0 {
					t.Fatalf("by size only: %d early, %d parts", len(early), len(parts))
				}
				return
			}
			if len(early) != 3 || len(parts) != 3 || r.TimeCuts() != 3 {
				t.Fatalf("%d parts published while stalled, %d in all, %d time cuts", len(early), len(parts), r.TimeCuts())
			}
			for i, at := range early { // each within its trickle's window
				if d := at.Sub(start) - time.Duration(i)*400*time.Millisecond; d > 350*time.Millisecond {
					t.Fatalf("part %d published %v after its row", i, d)
				}
			}
		})
	}
}
