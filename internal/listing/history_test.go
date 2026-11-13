// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"testing"
	"time"
)

// TestHistoryBounded: however many samples arrive, the history keeps at
// most HistoryCap, the first and the latest among them, in order; each
// time it fills it halves.
func TestHistoryBounded(t *testing.T) {
	s := &sampler{}
	halvings := 0
	for i := range 10_000 {
		if s.keep(Sample{RowsFetched: int64(i)}) {
			halvings++
		}
		h := s.hist.Samples
		if len(h) >= HistoryCap || h[0].RowsFetched != 0 || h[len(h)-1].RowsFetched != int64(i) {
			t.Fatalf("after %d: %d samples, first %d, last %d", i, len(h), h[0].RowsFetched, h[len(h)-1].RowsFetched)
		}
		for j := 1; j < len(h); j++ {
			if h[j].RowsFetched <= h[j-1].RowsFetched {
				t.Fatalf("after %d: out of order at %d", i, j)
			}
		}
	}
	if halvings == 0 {
		t.Fatal("never halved")
	}
}

// TestHistoryFinish: a sampler records its first sample at the start and
// a last one at finish, which is idempotent, and watches the free space.
func TestHistoryFinish(t *testing.T) {
	n := int64(0)
	s := startHistory(func() Sample { n++; return Sample{RowsEmitted: n} }, t.TempDir())
	time.Sleep(10 * time.Millisecond)
	h, free := s.finish()
	h2, _ := s.finish()
	if len(h.Samples) != 2 || len(h2.Samples) != 2 || h.IntervalMs != HistoryEvery.Milliseconds() || h.Samples[1].RowsEmitted != 2 {
		t.Fatalf("history %+v", h)
	}
	if free == nil || *free <= 0 || h.Samples[0].FreeDiskBytes == nil {
		t.Fatalf("free %v", free)
	}
}
