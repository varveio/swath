// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"fmt"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/source/s3"
)

// TestArmBPeakCountsTheSpillingBuffer (B5): while a full buffer is being
// spilled its rows stay live beside the next filling buffer, so arm B's
// peak buffered bytes are both. Before, the peak saw only the filling
// buffer (the spilled one's bytes were reset at the hand-off).
func TestArmBPeakCountsTheSpillingBuffer(t *testing.T) {
	batch := func(from, n int) []row {
		rows := make([]row, n)
		for i := range rows {
			k := fmt.Sprintf("r0000/obj-%012d", from+i)
			rows[i] = row{Key: k, Data: s3.Object{Size: int64(i)}}
		}
		return rows
	}
	a, b := batch(0, 200), batch(200, 100)
	st := &BStats{FirstRunMs: -1}
	s := &sorter{s: &s3.Schema, dir: t.TempDir(), limit: s3.Schema.RowsBytes(a), bufLast: map[int]int64{}, emitted: map[int]int64{},
		returned: map[int]bool{}, ck: &checkpoint{Cursor: map[int]int64{}, Done: map[int]bool{}}, st: st, spillGate: make(chan struct{})}
	s.cond = sync.NewCond(&s.mu)
	if err := s.add(0, a); err != nil { // fills the buffer: its spill starts and holds
		t.Fatal(err)
	}
	if err := s.add(0, b); err != nil { // fills the next one while it does
		t.Fatal(err)
	}
	close(s.spillGate)
	s.wg.Wait()
	if want := s3.Schema.RowsBytes(a) + s3.Schema.RowsBytes(b); st.PeakBuffered != want {
		t.Fatalf("peak buffered %d bytes, want %d (both buffers live)", st.PeakBuffered, want)
	}
	if s.err != nil {
		t.Fatal(s.err)
	}
}
