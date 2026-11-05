// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// tree is a synthetic provider: range r covers keys "r/..." with fan-out
// children; each range streams its rows in several Rows calls, like an
// object-store page sequence, with children interleaved.
type tree struct {
	depth, fanout, rowsPer int
	expanded               atomic.Int64
	slow                   time.Duration
	failAt                 string
}

type rng struct {
	prefix string
	level  int
}

func (t *tree) Root() rng { return rng{} }

func (t *tree) Expand(ctx context.Context, r rng, e source.Emitter[rng, tp]) error {
	t.expanded.Add(1)
	if r.prefix == t.failAt && t.failAt != "" {
		return errors.New("provider failed")
	}
	if t.slow > 0 {
		time.Sleep(t.slow)
	}
	for i := range t.fanout {
		// rows "p/iNN" then, for i, a child "p/iNN/" subtree
		rows := make([]schema.Row[tp], 0, t.rowsPer)
		for j := range t.rowsPer {
			rows = append(rows, schema.Row[tp]{Key: fmt.Sprintf("%s%02d-%03d", r.prefix, i, j)})
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
		if r.level < t.depth {
			if err := e.Child(rng{prefix: fmt.Sprintf("%s%02d/", r.prefix, i), level: r.level + 1}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *tree) keys() []string {
	var out []string
	var walk func(prefix string, level int)
	walk = func(prefix string, level int) {
		for i := range t.fanout {
			for j := range t.rowsPer {
				out = append(out, fmt.Sprintf("%s%02d-%03d", prefix, i, j))
			}
			if level < t.depth {
				walk(fmt.Sprintf("%s%02d/", prefix, i), level+1)
			}
		}
	}
	walk("", 0)
	return out
}

// The tests state budgets in rows: count each row as one unit.
func init() { countRows = true }

// tp is the tests' row payload: one integer and one string, so spill
// frames carry a payload through the codec.
type tp struct {
	Size int64
	ETag string
}

type tpCodec struct{}

func (tpCodec) Append(dst []byte, p *tp) []byte {
	return schema.AppendString(binary.AppendVarint(dst, p.Size), p.ETag)
}

func (tpCodec) Decode(d *schema.Decoder, p *tp) { p.Size, p.ETag = d.Varint(), d.Str() }

var testSchema = schema.Schema[tp]{
	Info:  schema.Info{Name: "t", ID: "test", Key: schema.String},
	Bytes: func(p *tp) int64 { return 8 + int64(len(p.ETag)) },
	Heap:  func(p *tp) int64 { return int64(len(p.ETag)) },
	Codec: tpCodec{},
}

type collect struct {
	mu   sync.Mutex
	keys []string
	data []tp
	fail int // fail after this many rows (0 = never)
}

func (c *collect) Write(_ context.Context, segs [][]schema.Row[tp]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		for _, r := range s {
			c.keys = append(c.keys, r.Key)
			c.data = append(c.data, r.Data)
		}
	}
	if c.fail > 0 && len(c.keys) >= c.fail {
		return errors.New("sink failed")
	}
	return nil
}

func TestOrderedOutput(t *testing.T) {
	tr := &tree{depth: 3, fanout: 5, rowsPer: 7}
	want := tr.keys()
	if !slices.IsSorted(want) {
		t.Fatal("oracle not sorted")
	}
	for _, tc := range []struct{ workers, budget int }{{1, 1}, {4, 3}, {8, 50}, {3, 1 << 20}} {
		t.Run(fmt.Sprintf("w%d_b%d", tc.workers, tc.budget), func(t *testing.T) {
			c := &collect{}
			st, err := Run[rng, tp](context.Background(), tr, c, &testSchema, Options{Workers: tc.workers, Budget: int64(tc.budget)})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(c.keys, want) {
				t.Fatalf("got %d keys, want %d", len(c.keys), len(want))
			}
			if st.Rows != int64(len(want)) {
				t.Fatalf("rows %d", st.Rows)
			}
			// Budget + one publish batch per worker (here a whole range, it is
			// below publishRows) + the rows pending on the drain path (at most
			// one range's content per level).
			rng := tr.fanout * tr.rowsPer
			if limit := int64(tc.budget + tc.workers*rng + (tr.depth+1)*rng); st.MaxBuffered > limit {
				t.Errorf("buffered %d > %d", st.MaxBuffered, limit)
			}
		})
	}
}

func TestSinkFailureStopsPromptly(t *testing.T) {
	tr := &tree{depth: 5, fanout: 6, rowsPer: 20} // 9331 ranges
	c := &collect{fail: 100}
	_, err := Run[rng, tp](context.Background(), tr, c, &testSchema, Options{Workers: 4, Budget: 500})
	if err == nil || err.Error() != "sink failed" {
		t.Fatalf("err = %v", err)
	}
	if n := tr.expanded.Load(); n > 200 {
		t.Fatalf("walk kept going after the sink failed: %d ranges expanded", n)
	}
}

func TestProviderFailure(t *testing.T) {
	tr := &tree{depth: 3, fanout: 4, rowsPer: 3, failAt: "02/01/"}
	_, err := Run[rng, tp](context.Background(), tr, &collect{}, &testSchema, Options{Workers: 3, Budget: 10})
	if err == nil || err.Error() != "provider failed" {
		t.Fatalf("err = %v", err)
	}
}

func TestCancellation(t *testing.T) {
	tr := &tree{depth: 6, fanout: 6, rowsPer: 5, slow: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	start := time.Now()
	_, err := Run[rng, tp](ctx, tr, &collect{}, &testSchema, Options{Workers: 4, Budget: 1000})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancellation was not prompt")
	}
}

// failingRoot's root emits nothing and fails, the shape where the drainer
// could find the tree finished between the node's completion and the
// failure being recorded.
type failingRoot struct{}

func (failingRoot) Root() rng { return rng{} }
func (failingRoot) Expand(context.Context, rng, source.Emitter[rng, tp]) error {
	return errors.New("first LIST failed")
}

// TestWorkerFailureNeverSucceeds: a worker failure on the final node is
// always reported, never a successful empty run (Sol P1-1).
func TestWorkerFailureNeverSucceeds(t *testing.T) {
	// Force the window: a worker (not the drainer) expands the root, and
	// the worker pauses after expand returns while the drainer runs.
	hookBeforeDrain = func() { time.Sleep(2 * time.Millisecond) }
	hookAfterExpand = func() { time.Sleep(2 * time.Millisecond) }
	defer func() { hookBeforeDrain, hookAfterExpand = nil, nil }()
	for i := range 200 {
		if _, err := Run[rng, tp](context.Background(), failingRoot{}, &collect{}, &testSchema, Options{Workers: 4, Budget: 10}); err == nil {
			t.Fatalf("iteration %d: a failed listing reported success", i)
		}
	}
}
