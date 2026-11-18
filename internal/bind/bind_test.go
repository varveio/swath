// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package bind_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// object is a payload of one column, its size.
type object struct{ size int64 }

var objects = schema.Schema[object]{
	Info: schema.Info{Name: "objects", ID: "test/objects", Key: schema.String},
	Columns: []schema.Column[object]{
		{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(p *object) (int64, bool) { return p.size, true }},
	},
	Bytes: func(*object) int64 { return 8 },
	Heap:  func(*object) int64 { return 0 },
}

// tiles is a provider of n ranges below its root, each of ten keys in
// ascending order; key i has size i.
type tiles struct{ n int }

func (t tiles) Root() int { return -1 }

func (t tiles) Expand(_ context.Context, r int, e source.Emitter[int, object]) error {
	if r < 0 {
		for i := range t.n {
			if err := e.Child(i); err != nil {
				return err
			}
		}
		return nil
	}
	rows := make([]schema.Row[object], 10)
	for i := range rows {
		k := r*10 + i
		rows[i] = schema.Row[object]{Key: fmt.Sprintf("k%04d", k), Data: object{int64(k)}}
	}
	return e.Rows(rows)
}

// collect is a caller's own sink: it keeps the keys of the batches it is
// handed, which come concurrently.
type collect struct {
	mu   sync.Mutex
	keys []string
}

func (c *collect) Write(_ context.Context, segs [][]schema.Row[object]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, seg := range segs {
		for _, r := range seg {
			c.keys = append(c.keys, r.Key)
		}
	}
	return nil
}

// TestCountIntoCallersSink: an unordered listing hands a sink of the
// caller's every row the filters keep, from independent ranges, and counts
// the rows dropped, handed on and their object bytes as a run does.
func TestCountIntoCallersSink(t *testing.T) {
	b, err := bind.Provider[int, object](tiles{8}, &objects)
	if err != nil {
		t.Fatal(err)
	}
	var filtered, handed atomic.Int64
	sizes := &bind.Sizes{}
	l, err := b.Prepare(bind.Env{Order: order.Options{Workers: 4}, Filter: &filter.Spec{MinSize: "40"}, Filtered: &filtered, Ordered: &handed, Sizes: sizes})
	if err != nil {
		t.Fatal(err)
	}

	var c collect
	st, err := l.Count(context.Background(), &c)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(c.keys)
	var want []string
	for k := 40; k < 80; k++ {
		want = append(want, fmt.Sprintf("k%04d", k))
	}
	if !slices.Equal(c.keys, want) {
		t.Fatalf("the sink got %d keys %q, want k0040..k0079", len(c.keys), c.keys)
	}
	if st.Rows != 40 || handed.Load() != 40 || filtered.Load() != 40 {
		t.Errorf("%d rows handed on (counted %d), %d dropped; want 40, 40", st.Rows, handed.Load(), filtered.Load())
	}
	if n, ok := sizes.Known(); !ok || n != (40+79)*40/2 {
		t.Errorf("object bytes %d (known %v), want %d", n, ok, (40+79)*40/2)
	}
}

// TestPrepareChecksBeforeAnySink: a filter the schema cannot answer is
// refused by Prepare, before the caller makes a sink.
func TestPrepareChecksBeforeAnySink(t *testing.T) {
	b, err := bind.Provider[int, object](tiles{1}, &objects)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Prepare(bind.Env{Filter: &filter.Spec{Since: "2026-01-01T00:00:00Z"}}); err == nil || !strings.Contains(err.Error(), "last_modified") {
		t.Fatalf("a time filter on a schema without times: %v", err)
	}
}
