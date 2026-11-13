// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/order"
)

// rangeTraceEnv names a file the experiment's range trace (#449, not a
// product feature) appends one JSON line per Expand to: the range's
// bounds, when it started and ended, the rows it emitted, the children it
// handed on (a split), and its adapter's responses, items read, rows
// discarded past a span's end and rows it left buffered (abandoned).
const rangeTraceEnv = "SWATH_EXP_RANGE_TRACE"

type rangeTrace struct {
	mu    sync.Mutex
	f     *os.File
	start time.Time
}

func openRangeTrace() (*rangeTrace, error) {
	path := os.Getenv(rangeTraceEnv)
	if path == "" {
		return nil, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", rangeTraceEnv, err)
	}
	return &rangeTrace{f: f, start: time.Now()}, nil
}

type traceLine struct {
	Lo        string `json:"lo"`
	Hi        string `json:"hi"`
	Open      bool   `json:"open"`
	Born      string `json:"born"`
	T0        int64  `json:"t0_us"`
	T1        int64  `json:"t1_us"`
	Rows      int64  `json:"rows"`
	Children  int64  `json:"children"`
	Responses int64  `json:"responses"`
	Items     int64  `json:"items"`
	Overlap   int64  `json:"overlap"`
	Abandoned int    `json:"abandoned"`
	Err       string `json:"err,omitempty"`
}

// traceExpand is Expand traced.
func traceExpand[P any](t *rangeTrace, ctx context.Context, r Range, e source.Emitter[Range, P], a *order.Adapter,
	expand func(context.Context, Range, source.Emitter[Range, P]) error) error {
	t0 := time.Now()
	c := &countEm[P]{e: e}
	var em source.Emitter[Range, P] = c
	if f, ok := e.(source.Fencer); ok {
		em = &countFenceEm[P]{countEm: c, f: f}
	}
	err := expand(ctx, r, em)
	lo, hi, open, born := r.Bounds()
	l := traceLine{Lo: lo, Hi: hi, Open: open, Born: born, T0: t0.Sub(t.start).Microseconds(), T1: time.Since(t.start).Microseconds(),
		Rows: c.rows.Load(), Children: c.children.Load(), Responses: a.Tally.Responses.Load(), Items: a.Tally.Items.Load(),
		Overlap: a.Tally.Overlap.Load(), Abandoned: a.Buffered()}
	if err != nil {
		l.Err = err.Error()
	}
	b, _ := json.Marshal(l)
	t.mu.Lock()
	t.f.Write(append(b, '\n'))
	t.mu.Unlock()
	return err
}

type countEm[P any] struct {
	e              source.Emitter[Range, P]
	rows, children atomic.Int64
}

func (c *countEm[P]) Rows(rows []schema.Row[P]) error {
	c.rows.Add(int64(len(rows)))
	return c.e.Rows(rows)
}

func (c *countEm[P]) Child(r Range) error {
	c.children.Add(1)
	return c.e.Child(r)
}

type countFenceEm[P any] struct {
	*countEm[P]
	f source.Fencer
}

func (c *countFenceEm[P]) Span(lo, hi string, open bool) { c.f.Span(lo, hi, open) }
func (c *countFenceEm[P]) Fence(x string, top bool)      { c.f.Fence(x, top) }
