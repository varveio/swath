// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"github.com/varveio/swath/go/internal/dataset"
	"testing"
	"time"
)

func TestReviewDeferredRowCutCalibratesBySize(t *testing.T) {
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
