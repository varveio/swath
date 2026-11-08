// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

type pendingFailure struct {
	g         *Gauges
	failure   error
	firstRows int
}

func (p pendingFailure) Root() int { return 0 }
func (p pendingFailure) Expand(ctx context.Context, _ int, e source.Emitter[int, tp]) error {
	if p.firstRows > 0 {
		rows := make([]schema.Row[tp], p.firstRows)
		for i := range rows {
			rows[i].Key = "a"
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
	}
	if err := e.Rows([]schema.Row[tp]{{Key: "a"}, {Key: "a"}, {Key: "b"}}); err != nil {
		return err
	}
	// Wait until send has validated the batch but has not flushed it.
	for p.g.Snapshot().AheadRows != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			runtime.Gosched()
		}
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
			g := &Gauges{}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sink := &collect{}
			st, err := Run(ctx, pendingFailure{g, tc.failure, tc.firstRows}, sink, &testSchema, Options{Workers: 1, Budget: 1 << 20, Groups: true, Gauges: g})
			if !errors.Is(err, tc.failure) {
				t.Fatalf("error=%v", err)
			}
			if st.Received != int64(tc.firstRows+3) || st.Rows != tc.rows || len(sink.keys) != int(tc.rows) || st.DistinctKeys != tc.distinct {
				t.Fatalf("received=%d rows=%d distinct=%d sink_rows=%d; want received=%d rows=%d distinct=%d", st.Received, st.Rows, st.DistinctKeys, len(sink.keys), tc.firstRows+3, tc.rows, tc.distinct)
			}
		})
	}
}
