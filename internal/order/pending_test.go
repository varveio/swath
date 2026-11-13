// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// pendingFailure emits firstRows rows, then three more, then fails with
// failure (or succeeds when it is nil). Its rows must reach send on the
// emitter's own goroutine, so that a failure returned after them leaves
// them validated and batched but never flushed: only a range the drainer
// expands directly does that. When the worker claims the root instead, the
// rows it emitted would be drained and then flushed by the drainer's wait
// on the frontier, so the root hands them to a child (1), which the
// drainer expands directly: the child stays in the root's pending list
// until the root finishes, so no worker can claim it.
type pendingFailure struct {
	failure   error
	firstRows int
	frontier  <-chan struct{} // closed at the drainer's first frontier wait
	childDone chan struct{}   // closed when the child's Expand returns
}

func (p pendingFailure) Root() int { return 0 }
func (p pendingFailure) Expand(ctx context.Context, r int, e source.Emitter[int, tp]) error {
	direct := e.(*emitter[int, tp]).direct
	if r == 0 && !direct {
		// The drainer waits on the frontier with the root on its path, so
		// the child publishes at once.
		select {
		case <-p.frontier:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := e.Child(1); err != nil {
			return err
		}
		select {
		case <-p.childDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r == 1 {
		defer close(p.childDone)
	}
	if !direct {
		return fmt.Errorf("range %d expanded by a worker, want the drainer", r)
	}
	if p.firstRows > 0 {
		rows := make([]schema.Row[tp], p.firstRows)
		for i := range rows {
			rows[i].Key = "a"
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
	}
	// Direct: send has validated these before Rows returns; fewer than
	// batchRows of them, they are not flushed.
	if err := e.Rows([]schema.Row[tp]{{Key: "a"}, {Key: "a"}, {Key: "b"}}); err != nil {
		return err
	}
	return p.failure
}

func TestDistinctCountsOnlyHandedRows(t *testing.T) {
	fail := errors.New("injected provider failure")
	for _, tc := range []struct {
		name           string
		firstRows      int
		failure        error
		rows, distinct int64
	}{
		{"pending-only", 0, fail, 0, 0},
		{"after-flush", batchRows, fail, batchRows, 1},
		{"success-across-flush", batchRows, nil, batchRows + 3, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frontier := make(chan struct{})
			var once sync.Once
			hookWait = func(site string) {
				if site == waitFrontier {
					once.Do(func() { close(frontier) })
				}
			}
			t.Cleanup(func() { hookWait = nil })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sink := &collect{}
			p := pendingFailure{failure: tc.failure, firstRows: tc.firstRows, frontier: frontier, childDone: make(chan struct{})}
			st, err := Run(ctx, p, sink, &testSchema, Options{Workers: 1, Budget: 1 << 20, Groups: true})
			if !errors.Is(err, tc.failure) {
				t.Fatalf("error=%v", err)
			}
			if st.Received != int64(tc.firstRows+3) || st.Rows != tc.rows || len(sink.keys) != int(tc.rows) || st.DistinctKeys != tc.distinct {
				t.Fatalf("received=%d rows=%d distinct=%d sink_rows=%d; want received=%d rows=%d distinct=%d", st.Received, st.Rows, st.DistinctKeys, len(sink.keys), tc.firstRows+3, tc.rows, tc.distinct)
			}
		})
	}
}
