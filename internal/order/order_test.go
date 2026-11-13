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

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
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

// stalledRoot's root emits nothing for wait, then fails with err, or
// waits for cancellation when err is nil: a frontier that never answers.
type stalledRoot struct {
	wait time.Duration
	err  error
}

func (stalledRoot) Root() rng { return rng{} }
func (s stalledRoot) Expand(ctx context.Context, _ rng, _ source.Emitter[rng, tp]) error {
	if s.err == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	time.Sleep(s.wait)
	return s.err
}

// TestInterruptedRunReportsStall: a run stopped while its frontier has
// handed nothing on still reports that interval as FrontierStuck, on a
// cancellation and on a provider error (the drain's early exits), not
// zero as when only the successful tail closed the interval. The drain
// starts a little after the clock does, so the bound is half the stall.
func TestInterruptedRunReportsStall(t *testing.T) {
	const stall = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(stall, cancel)
	st, err := Run[rng, tp](ctx, stalledRoot{}, &collect{}, &testSchema, Options{Workers: 2, Budget: 1000})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: err = %v", err)
	}
	if st.FrontierStuck < stall/2 {
		t.Fatalf("cancelled after %v with nothing handed on: FrontierStuck %v", stall, st.FrontierStuck)
	}
	st, err = Run[rng, tp](context.Background(), stalledRoot{wait: stall, err: errors.New("provider failed")}, &collect{}, &testSchema, Options{Workers: 2, Budget: 1000})
	if err == nil || err.Error() != "provider failed" {
		t.Fatalf("failed: err = %v", err)
	}
	if st.FrontierStuck < stall/2 {
		t.Fatalf("failed after %v with nothing handed on: FrontierStuck %v", stall, st.FrontierStuck)
	}
}

// blockedSink accepts nothing: every Write waits for the run's context.
type blockedSink struct{}

func (blockedSink) Write(ctx context.Context, _ [][]schema.Row[tp]) error {
	<-ctx.Done()
	return context.Cause(ctx)
}

// TestFinalFlushReportsStall: a run whose one row is batched until the
// drain's final flush, and whose sink then blocks until the deadline,
// reports that wait as FrontierStuck (the interval is closed after the
// flush, not before it).
func TestFinalFlushReportsStall(t *testing.T) {
	const stall = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), stall)
	defer cancel()
	st, err := Run[rng, tp](ctx, &tree{depth: 0, fanout: 1, rowsPer: 1}, blockedSink{}, &testSchema, Options{Workers: 1, Budget: 1000})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if st.FrontierStuck < stall/2 {
		t.Fatalf("the final write blocked for %v: FrontierStuck %v", stall, st.FrontierStuck)
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
// always reported, never a successful empty run (durability P1-1).
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

// siblings is a root with two children: "a" emits a row and fails after
// "b", its right sibling, has finished listing.
type siblings struct{ bDone chan struct{} }

func (s *siblings) Root() rng { return rng{} }

func (s *siblings) Expand(_ context.Context, r rng, e source.Emitter[rng, tp]) error {
	switch r.prefix {
	case "":
		if err := e.Child(rng{prefix: "a"}); err != nil {
			return err
		}
		return e.Child(rng{prefix: "b"})
	case "a":
		<-s.bDone
		if err := e.Rows([]schema.Row[tp]{{Key: "a1"}}); err != nil {
			return err
		}
		return errors.New("provider failed")
	}
	defer close(s.bDone)
	return e.Rows([]schema.Row[tp]{{Key: "b1"}, {Key: "b2"}})
}

// TestFailedRangeStopsTheStream: when a range fails, nothing after it
// reaches the sink, however soon after the failure the drainer passes its
// node: the rows handed on stay a prefix of the keyspace with no gap, the
// prefix a resume continues after. The drainer must not pass a failed
// node before it sees the run stopped. Every run forces the window with
// barriers, not sleeps: the drainer starts only after the workers have
// expanded the root and both siblings ("a" failed after "b" finished;
// hookAfterExpand counts the three), and the context's stop callback is
// held until Run has returned (hookStopCallback), so only the latch taken
// with the failure can stop the drainer at the failed node. It covers a
// failed leaf next to a finished sibling, not nested split failures,
// cancellation mid-drain or spill-trailer errors.
func TestFailedRangeStopsTheStream(t *testing.T) {
	defer func() { hookBeforeDrain, hookAfterExpand, hookStopCallback = nil, nil, nil }()
	for i := range 100 {
		var expanded atomic.Int32
		allExpanded, release := make(chan struct{}), make(chan struct{})
		hookAfterExpand = func() {
			if expanded.Add(1) == 3 { // the root, "b", then "a"
				close(allExpanded)
			}
		}
		hookBeforeDrain = func() {
			select {
			case <-allExpanded:
			case <-time.After(10 * time.Second):
				t.Error("the workers did not expand the three ranges")
			}
		}
		hookStopCallback = func() { <-release }
		c := &collect{}
		_, err := Run[rng, tp](context.Background(), &siblings{bDone: make(chan struct{})}, c, &testSchema, Options{Workers: 2, Budget: 1 << 20})
		close(release)
		if err == nil || err.Error() != "provider failed" {
			t.Fatalf("run %d: err = %v", i, err)
		}
		if slices.ContainsFunc(c.keys, func(k string) bool { return k[0] == 'b' }) {
			t.Fatalf("run %d: the sink received %q after the range before them failed", i, c.keys)
		}
	}
}
