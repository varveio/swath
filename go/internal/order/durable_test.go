// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// fz is a fencedTree range: i -1 the root, else a range; replay >= 0 a
// replay range (its certificate).
type fz struct{ i, replay int }

// fencedTree is a keyrange-like provider for the durable spill: ranges
// r0000..., each a run of pages declaring its span, a fence after every
// page and one at its end. Range 0 is the slow frontier, held after its
// first page until every other range has ended (hold), so the others
// spill. spans, if set, are replay ranges emitted at their positions in
// place of the ranges they cover (a resume's tiling: whole ranges only).
type fencedTree struct {
	ranges, pages int
	rows          func(i, p int) int // rows on page p of range i (0: a wholly filtered page)
	hold          chan struct{}
	left          atomic.Int64
	spans         []Span
	noEnd         bool          // no fence at a range's end
	slow          time.Duration // range 0's delay per page (a slow frontier without the hold)
}

const fzWidth = 4

func (f *fencedTree) lo(i int) string         { return fmt.Sprintf("r%0*d", fzWidth, i) }
func (f *fencedTree) key(i, p, j int) string  { return fmt.Sprintf("r%0*d/%06d-%05d", fzWidth, i, p, j) }
func (f *fencedTree) Root() fz                { return fz{-1, -1} }
func (f *fencedTree) Fences() bool            { return true }
func (f *fencedTree) Replay(r fz) (int, bool) { return r.replay, r.replay >= 0 }
func (f *fencedTree) covered(i int) (int, bool) {
	for k, s := range f.spans {
		if s.Lo <= f.lo(i) && (s.HiOpen || f.lo(i+1) <= s.Hi) {
			return k, true
		}
	}
	return 0, false
}

func (f *fencedTree) Expand(ctx context.Context, r fz, e source.Emitter[fz, tp]) error {
	if r.i < 0 {
		f.left.Store(int64(f.ranges - 1))
		last := -1
		for i := range f.ranges {
			if k, ok := f.covered(i); ok {
				if k != last {
					if err := e.Child(fz{i, k}); err != nil {
						return err
					}
					last = k
				}
				continue
			}
			if err := e.Child(fz{i, -1}); err != nil {
				return err
			}
		}
		return nil
	}
	fence := e.(source.Fencer)
	fence.Span(f.lo(r.i), f.lo(r.i+1), false)
	for p := range f.pages {
		if r.i == 0 {
			time.Sleep(f.slow)
		}
		n := f.rows(r.i, p)
		rows := make([]schema.Row[tp], n)
		for j := range rows {
			rows[j] = schema.Row[tp]{Key: f.key(r.i, p, j), Data: tp{Size: int64(j), ETag: "etag"}}
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
		fence.Fence(f.key(r.i, p, 99999), false)
		if f.hold != nil && r.i == 0 && p == 0 {
			select {
			case <-f.hold:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if f.hold != nil && r.i > 0 && f.left.Add(-1) == 0 {
		close(f.hold)
	}
	if !f.noEnd {
		fence.Fence(f.lo(r.i+1), false)
	}
	return nil
}

// keys is every row in order; in, if set, keeps those it accepts.
func (f *fencedTree) keys(in func(string) bool) []string {
	var out []string
	for i := range f.ranges {
		for p := range f.pages {
			for j := range f.rows(i, p) {
				if k := f.key(i, p, j); in == nil || in(k) {
					out = append(out, k)
				}
			}
		}
	}
	return out
}

const testUUID = "00112233445566778899aabbccddeeff"

// newDurable is a fresh durable spill under a temporary output.
func newDurable(t *testing.T) (*Cache, *os.Root, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".swath"), 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	c, err := CreateDurable(root, nil, testUUID, "ident", "test")
	if err != nil {
		t.Fatal(err)
	}
	return c, root, dir
}

// ledgerOrder asserts (hookLedgerLocked) that the ledger mutex is only
// taken on the cache's own paths (flusher, recovery, close), never by the
// order stage's goroutines, which hold its mutex.
func ledgerOrder(t *testing.T) {
	t.Helper()
	var bad atomic.Value
	hookLedgerLocked = func() {
		buf := make([]byte, 64<<10)
		st := string(buf[:runtime.Stack(buf, false)])
		for _, f := range []string{".worker(", ".drainTree(", ".expandBy(", ".publish(", ".spill(", ".closeNode("} {
			if strings.Contains(st, f) {
				bad.Store(st)
			}
		}
	}
	t.Cleanup(func() {
		hookLedgerLocked = nil
		if st, ok := bad.Load().(string); ok {
			t.Errorf("the ledger mutex taken by the order stage:\n%s", st)
		}
	})
}

// recoverAll closes c and recovers its spill afresh with K (nil: ⊥).
func recoverAll(t *testing.T, c *Cache, root *os.Root, k *string) *Cache {
	t.Helper()
	c.Close()
	r, err := RecoverDurable(root, nil, testUUID, "ident", "test", k)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// replayAll replays every accepted certificate of c (started) and returns
// the rows handed on, per certificate.
func replayAll(t *testing.T, c *Cache, after *string) ([]string, Stats) {
	t.Helper()
	spans := c.Spans()
	p := &replayOnly{n: len(spans)}
	sink := &collect{}
	st, err := Run[fz, tp](context.Background(), p, sink, &testSchema, Options{Workers: 2, Budget: 1 << 20, SpillDir: t.TempDir(), Durable: c, After: after})
	if err != nil {
		t.Fatal(err)
	}
	return sink.keys, st
}

// replayOnly is a resume whose every range is a replay range.
type replayOnly struct{ n int }

func (p *replayOnly) Root() fz                { return fz{-1, -1} }
func (p *replayOnly) Fences() bool            { return true }
func (p *replayOnly) Replay(r fz) (int, bool) { return r.replay, r.replay >= 0 }
func (p *replayOnly) Expand(_ context.Context, r fz, e source.Emitter[fz, tp]) error {
	for i := range p.n {
		if err := e.Child(fz{i, i}); err != nil {
			return err
		}
	}
	return nil
}

// inSpans is membership in the union of spans.
func inSpans(spans []Span, low string) func(string) bool {
	return func(k string) bool {
		if k <= low {
			return false
		}
		for _, s := range spans {
			if k > s.Lo && (s.HiOpen || k <= s.Hi) {
				return true
			}
		}
		return false
	}
}

// TestDurableCertifiesExactRows is the capture's gate: behind a held
// frontier the other ranges spill into durable files and seal; recovered
// (K ⊥), every certificate validates and its replay hands on exactly the
// oracle's rows of (cs, ce], at page sizes that disagree with the publish
// chunk (1, 999, 1,025, 3,000 against 1,024): the capture start falls
// between chunks, no frame straddles it. The live output is the oracle.
func TestDurableCertifiesExactRows(t *testing.T) {
	ledgerOrder(t)
	for _, page := range []int{1, 999, 1025, 3000} {
		t.Run(fmt.Sprint(page), func(t *testing.T) {
			f := &fencedTree{ranges: 6, pages: 4, rows: func(int, int) int { return page }, hold: make(chan struct{})}
			c, root, _ := newDurable(t)
			if err := c.Start(64, 0, nil); err != nil {
				t.Fatal(err)
			}
			sink := &collect{}
			_, err := Run[fz, tp](context.Background(), f, sink, &testSchema, Options{Workers: 4, Budget: 2 * int64(page), SpillDir: t.TempDir(), Durable: c})
			if err != nil {
				t.Fatal(err)
			}
			if want := f.keys(nil); !slices.Equal(sink.keys, want) {
				t.Fatalf("%d keys, want %d", len(sink.keys), len(want))
			}
			sealed := c.st.SealedFiles.Load()
			if sealed == 0 {
				t.Fatalf("nothing sealed: %+v", c.Report())
			}
			r := recoverAll(t, c, root, nil)
			if err := r.Start(64, 0, nil); err != nil {
				t.Fatal(err)
			}
			if got := r.st.Accepted.Load(); got != sealed {
				t.Fatalf("%d accepted of %d sealed (rejected %+v)", got, sealed, r.Report().Resume.Rejected)
			}
			spans := r.Spans()
			got, st := replayAll(t, r, nil)
			want := f.keys(inSpans(spans, ""))
			if !slices.Equal(got, want) {
				t.Fatalf("replayed %d rows, the certificates hold %d", len(got), len(want))
			}
			if st.ReplayedRows != int64(len(want)) {
				t.Fatalf("replayed %d, counted %d", len(want), st.ReplayedRows)
			}
			r.Close()
			t.Logf("page %d: %d sealed, %d certified rows, capture at start %d / fence %d", page, sealed, len(want), c.st.CaptureAtStart.Load(), c.st.CaptureAtFence.Load())
		})
	}
}

// TestDurableReplayAboveK: a recovery replay emits only keys above
// max(cs, K): with K inside a certificate, its rows up to K are skipped.
func TestDurableReplayAboveK(t *testing.T) {
	f := &fencedTree{ranges: 5, pages: 3, rows: func(int, int) int { return 300 }, hold: make(chan struct{})}
	c, root, _ := newDurable(t)
	c.Start(64, 0, nil)
	sink := &collect{}
	if _, err := Run[fz, tp](context.Background(), f, sink, &testSchema, Options{Workers: 4, Budget: 400, SpillDir: t.TempDir(), Durable: c}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	probe, _ := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
	probe.Start(64, 0, nil)
	spans := probe.Spans()
	probe.Close()
	if len(spans) == 0 {
		t.Fatal("nothing sealed")
	}
	// K: a key inside the first certificate.
	inside := f.keys(inSpans(spans[:1], ""))
	k := inside[len(inside)/2]
	r, err := RecoverDurable(root, nil, testUUID, "ident", "test", &k)
	if err != nil {
		t.Fatal(err)
	}
	r.Start(64, 0, nil)
	got, st := replayAll(t, r, &k)
	want := f.keys(inSpans(r.Spans(), k))
	if !slices.Equal(got, want) || len(want) == 0 {
		t.Fatalf("replayed %d rows above K, want %d", len(got), len(want))
	}
	if r.st.ReplaySkippedRows.Load() == 0 || st.ReplayedRows != int64(len(want)) {
		t.Fatalf("skipped %d, replayed %d", r.st.ReplaySkippedRows.Load(), st.ReplayedRows)
	}
	r.Close()
}

// harness is a run's state with one node and its emitter, driven by hand:
// full forces a spill at the next publish.
type harness struct {
	o     *run[fz, tp]
	n     *node[fz, tp]
	e     *emitter[fz, tp]
	c     *Cache
	root  *os.Root
	close func()
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	c, root, _ := newDurable(t)
	if err := c.Start(64, 0, nil); err != nil {
		t.Fatal(err)
	}
	o, closeRun, err := newRun[fz, tp](context.Background(), &fencedTree{}, &collect{}, &testSchema, Options{Workers: 1, Budget: 1 << 20, SpillDir: t.TempDir(), Durable: c})
	if err != nil {
		t.Fatal(err)
	}
	n := &node[fz, tp]{r: fz{1, -1}, state: expanding}
	h := &harness{o: o, n: n, e: &emitter[fz, tp]{o: o, n: n}, c: c, root: root, close: closeRun}
	t.Cleanup(func() { closeRun(); c.Close() })
	return h
}

// rows emits n rows from key index from on, page p.
func (h *harness) rows(t *testing.T, p, from, n int) {
	t.Helper()
	rows := make([]schema.Row[tp], n)
	for j := range rows {
		rows[j] = schema.Row[tp]{Key: fmt.Sprintf("r0001/%06d-%05d", p, from+j)}
	}
	if err := h.e.Rows(rows); err != nil {
		t.Fatal(err)
	}
}

// publish publishes the emitter's list, full (spill) or not.
func (h *harness) publish(t *testing.T, full bool) {
	t.Helper()
	h.o.mu.Lock()
	if full {
		h.o.buffered = h.o.opt.Budget
	}
	err := h.e.publish()
	h.o.buffered = 0
	h.o.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

// frames reads a durable file's frame types and its trailer.
func frames(t *testing.T, root *os.Root, d *Cert) (string, trailer) {
	t.Helper()
	b, err := root.ReadFile(filepath.Join(DurableDir, d.name))
	if err != nil {
		t.Fatal(err)
	}
	var types []byte
	var tr trailer
	for off := 0; off+8 <= len(b)-tailSize; {
		n := int(binary.LittleEndian.Uint32(b[off:]))
		typ := b[off+8]
		types = append(types, typ)
		if typ == frameTrailer {
			tr, _ = parseTrailer(b[off+9 : off+8+n])
		}
		off += 8 + n
	}
	return string(types), tr
}

// TestDurableAbandonThenSpill: a capture abandoned (the drainer awaits the
// node: its rows go to memory) and then more spill in the same node leave
// one trailer, at the end, uncertified, and the slot and history returned.
func TestDurableAbandonThenSpill(t *testing.T) {
	h := newHarness(t)
	h.e.Span("r0001", "r0002", false)
	h.rows(t, 0, 0, 10)
	h.e.Fence("r0001/000000-99999", false)
	h.publish(t, true) // spills: capture from the span's start
	if !h.n.spill.capturing {
		t.Fatal("not capturing")
	}
	h.n.awaited.Store(true)
	h.rows(t, 1, 0, 10)
	h.publish(t, true) // awaited: to memory, abandoned
	if h.n.spill.capturing || !h.n.spill.abandoned || h.c.st.AbandonedAwaited.Load() != 1 {
		t.Fatalf("not abandoned: %+v", h.c.Report())
	}
	h.n.awaited.Store(false)
	h.rows(t, 2, 0, 10)
	h.e.Fence("r0001/000002-99999", false)
	h.publish(t, true) // spills again, uncertified
	h.e.Fence("r0002", false)
	if err := h.e.complete(); err != nil {
		t.Fatal(err)
	}
	types, tr := frames(t, h.root, h.n.spill.d)
	if strings.Count(types, "T") != 1 || !strings.HasSuffix(types, "T") || tr.certified {
		t.Fatalf("frames %q, trailer %+v", types, tr)
	}
	h.c.mu.Lock()
	slots, hist := h.c.slots, h.c.hist
	h.c.mu.Unlock()
	if slots != 0 || hist != 0 {
		t.Fatalf("slots %d, history %d after the abandon", slots, hist)
	}
}

// TestDurableZeroRowCertificate: rows spilled before the list's only fence
// are pre-capture; the capture starts at that fence and the rest of the
// range is wholly filtered (fences, no rows): a certificate of no rows.
// Thousands of filtered pages leave the emitter's state constant (a fence
// allocates nothing) and charge nothing.
func TestDurableZeroRowCertificate(t *testing.T) {
	h := newHarness(t)
	h.e.Span("r0001", "r0002", false)
	h.rows(t, 0, 0, 1024) // a publish batch: to memory, the list then holds no fence
	h.rows(t, 0, 1024, 476)
	h.e.Fence("r0001/000000-99999", false)
	if allocs := testing.AllocsPerRun(10_000, func() { h.e.Fence("r0001/000001-99999", false) }); allocs != 0 {
		t.Fatalf("a fence allocates %v", allocs)
	}
	if len(h.e.items) != 1 {
		t.Fatalf("%d items after the filtered pages", len(h.e.items))
	}
	h.publish(t, true) // the 476 rows are pre-capture, C after them
	h.e.Fence("r0002", false)
	if err := h.e.complete(); err != nil {
		t.Fatal(err)
	}
	d := h.n.spill.d
	types, tr := frames(t, h.root, d)
	if types != "HRCT" && types != "HDCT" && types != "HYCT" && types != "HZCT" {
		t.Fatalf("frames %q", types)
	}
	if !tr.certified || tr.captured != 0 || tr.rows != 476 || tr.cs != "r0001/000000-99999" || tr.ce != "r0002" {
		t.Fatalf("trailer %+v", tr)
	}
}

// TestDurablePendingCapture: a node that spills with no fence in its list
// waits for one (pending); its rows published to memory meanwhile drop the
// pending capture (counted), and the next spill with a fence arms it.
func TestDurablePendingCapture(t *testing.T) {
	h := newHarness(t)
	h.e.Span("r0001", "r0002", false)
	h.rows(t, 0, 0, 1024) // to memory: the next list holds no fence
	h.rows(t, 0, 1024, 10)
	h.publish(t, true) // spill, no fence: pending
	if h.n.spill.capturing {
		t.Fatal("captured without a fence")
	}
	h.rows(t, 0, 2000, 10)
	h.publish(t, false) // to memory: pending dropped
	if h.c.st.CapturePendingDropped.Load() != 1 {
		t.Fatalf("pending dropped %d", h.c.st.CapturePendingDropped.Load())
	}
	h.e.Fence("r0001/000000-99999", false)
	h.rows(t, 1, 0, 10)
	h.publish(t, true)
	if !h.n.spill.capturing || h.n.spill.d.cs != "r0001/000000-99999" {
		t.Fatal("the next spill did not arm the capture at the fence")
	}
}

// TestDurableHistoryRace: captures racing for the last history records
// never take more than H.
func TestDurableHistoryRace(t *testing.T) {
	c, _, _ := newDurable(t)
	c.Start(maxCertificates, 0, nil)
	defer c.Close()
	c.mu.Lock()
	c.hist = historyCap - 11
	c.mu.Unlock()
	var won atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.reserve() {
				won.Add(1)
			}
		}()
	}
	wg.Wait()
	if won.Load() != 5 || c.hist > historyCap || c.st.HistoryExhausted.Load() != 59 {
		t.Fatalf("%d reservations, history %d of %d, exhausted %d", won.Load(), c.hist, historyCap, c.st.HistoryExhausted.Load())
	}
}

// TestDurableSlotsFull: with every slot held by undrained files ahead of
// the frontier, further ranges spill disposably (no_slot) and the frontier
// completes: admission never waits.
func TestDurableSlotsFull(t *testing.T) {
	f := &fencedTree{ranges: 40, pages: 3, rows: func(int, int) int { return 200 }, hold: make(chan struct{})}
	c, _, _ := newDurable(t)
	c.Start(minCertificates, 0, nil)
	defer c.Close()
	sink := &collect{}
	if _, err := Run[fz, tp](context.Background(), f, sink, &testSchema, Options{Workers: 8, Budget: 300, SpillDir: t.TempDir(), Durable: c}); err != nil {
		t.Fatal(err)
	}
	if want := f.keys(nil); !slices.Equal(sink.keys, want) {
		t.Fatalf("%d keys, want %d", len(sink.keys), len(want))
	}
	if c.st.NoSlot.Load() == 0 || c.st.SealedFiles.Load() > minCertificates {
		t.Fatalf("no_slot %d, sealed %d", c.st.NoSlot.Load(), c.st.SealedFiles.Load())
	}
}

// TestDurableRetireDefer: retirement never waits on publication: K
// covering a file still being read marks it, and the reader's close
// retires it; a file drained first waits for K.
func TestDurableRetireDefer(t *testing.T) {
	c, _, _ := newDurable(t)
	c.Start(64, 0, nil)
	defer c.Close()
	a := &Cert{name: "a.spill", ce: "b", certified: true, slot: true}
	b := &Cert{name: "b.spill", ce: "d", certified: true, slot: true}
	c.slots = 2
	c.SetK("c") // publication passes a while its reader is open: no wait
	c.drainedFile(a)
	c.drainedFile(b)
	c.mu.Lock()
	if !a.retiring || b.retiring || len(c.drained) != 1 {
		c.mu.Unlock()
		t.Fatalf("a retiring %v, b retiring %v, %d waiting", a.retiring, b.retiring, len(c.drained))
	}
	c.mu.Unlock()
	c.SetK("d")
	if !b.retiring || c.st.RetiredPublished.Load() != 2 || c.st.RetireDeferred.Load() != 1 {
		t.Fatalf("b retiring %v, retired %d, deferred %d", b.retiring, c.st.RetiredPublished.Load(), c.st.RetireDeferred.Load())
	}
}

// TestDurableCreditWithoutOrderLock: an unlink's credit lowers the live
// spill bytes and wakes the waiters through the waker, never taking the
// order stage's mutex: with that mutex held throughout, an eviction is
// unlinked and its credit returned; the wake follows its release.
func TestDurableCreditWithoutOrderLock(t *testing.T) {
	ledgerOrder(t)
	h := newHarness(t)
	h.e.Span("r0001", "r0002", false)
	h.rows(t, 0, 0, 100)
	h.e.Fence("r0001/000000-99999", false)
	h.publish(t, true)
	h.e.Fence("r0002", false)
	if err := h.e.complete(); err != nil {
		t.Fatal(err)
	}
	d := h.n.spill.d
	h.c.drainedFile(d)
	live := h.o.sp.live.Load()
	if live == 0 {
		t.Fatal("nothing charged")
	}
	var woke atomic.Bool
	go func() {
		h.o.mu.Lock()
		h.o.drain.Wait()
		woke.Store(true)
		h.o.mu.Unlock()
	}()
	time.Sleep(10 * time.Millisecond)
	h.o.mu.Lock()
	if !h.c.evict() {
		t.Fatal("nothing to evict")
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.o.sp.live.Load() >= live && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	gone := h.o.sp.live.Load() < live
	if _, err := h.root.Stat(filepath.Join(DurableDir, d.name)); !errors.Is(err, os.ErrNotExist) || !gone {
		h.o.mu.Unlock()
		t.Fatalf("with the order mutex held: unlinked %v, credit returned %v", err, gone)
	}
	h.o.mu.Unlock()
	for !woke.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !woke.Load() {
		t.Fatal("the waiter was not woken")
	}
}

// TestDurableLedgerFailureEvicts (Sol 3): a ledger append torn part way
// (a partial write that fails), while drained certificates occupy the
// spill budget and the run publishes: the ledger latches failed (no new
// slot), retirements still unlink, refused frames evict, the run
// progresses to the oracle's output. (TestDurableEviction holds the same
// path deterministically; listing's TestDurableLedgerTornWrite resumes
// after it.)
func TestDurableLedgerFailureEvicts(t *testing.T) {
	ledgerOrder(t)
	var writes atomic.Int64
	defer faults.SetIO(func(op, name string, b []byte) (int, error) {
		if op == "write" && strings.HasSuffix(name, ledgerName) && writes.Add(1) == 2 {
			return len(b) / 2, errors.New("injected EIO")
		}
		return 0, nil
	})()
	// A slow frontier (not held: a held one and a spill cap wait for each
	// other by design): drained certificates, never retired without K,
	// fill the cap behind it.
	f := &fencedTree{ranges: 60, pages: 4, rows: func(int, int) int { return 200 }, slow: 30 * time.Millisecond}
	c, _, _ := newDurable(t)
	c.Start(1000, 0, nil)
	defer c.Close()
	sink := &collect{}
	// The spill cap holds a few ranges: the rest refuse, and wait or evict.
	st, err := Run[fz, tp](context.Background(), f, sink, &testSchema, Options{Workers: 8, Budget: 300, SpillDir: t.TempDir(), Durable: c, SpillMax: 24 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if want := f.keys(nil); !slices.Equal(sink.keys, want) {
		t.Fatalf("%d keys, want %d", len(sink.keys), len(want))
	}
	c.Close()
	rep := c.Report()
	if !rep.LedgerFailed || c.st.Unlinked.Load() == 0 || st.SpillRefused == 0 {
		t.Fatalf("ledger failed %v, unlinked %d, refused %d, evicted %d", rep.LedgerFailed, c.st.Unlinked.Load(), st.SpillRefused, rep.Retired["evicted"])
	}
	t.Logf("refused %d, evicted %d, unlinked %d, sealed %d", st.SpillRefused, rep.Retired["evicted"], c.st.Unlinked.Load(), rep.SealedFiles)
}

// TestDurableEviction: a frame refused by the spill cap while a drained
// certificate is retained evicts that certificate (never an undrained
// file): its unlink returns the credit, through the flusher, and the
// retried frame is admitted. Deterministic (no timing premise): the
// drained certificate is held by hand and the cap set so the next frame is
// refused; it passes GOMAXPROCS=1 -count=10. (TestDurableLedgerFailureEvicts runs the same path in a run.)
func TestDurableEviction(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("ledger-failed=%v", failed), func(t *testing.T) { evictOnce(t, failed) })
	}
}

// evictOnce is TestDurableEviction, with the ledger latched failed after
// the seal or not: unlinks and credit never depend on the ledger.
func evictOnce(t *testing.T, failed bool) {
	h := newHarness(t)
	h.e.Span("r0001", "r0002", false)
	h.rows(t, 0, 0, 100)
	h.e.Fence("r0001/000000-99999", false)
	h.publish(t, true)
	h.e.Fence("r0002", false)
	if err := h.e.complete(); err != nil {
		t.Fatal(err)
	}
	d := h.n.spill.d
	sp := h.o.sp
	undrained := sp.file()  // an undrained disposable file: never evicted
	sp.max = sp.live.Load() // the next frame is refused
	if err := sp.write(undrained, []schema.Row[tp]{{Key: "r0003/a"}}, false); err != errSpillFull {
		t.Fatalf("not refused while nothing is drained: %v", err)
	}
	if h.c.st.RetiredEvicted.Load() != 0 {
		t.Fatal("an undrained file evicted")
	}
	h.c.drainedFile(d)
	if failed {
		h.c.ledgerFailed() // after the seal: the certificate is held, the ledger gone
	}
	live := sp.live.Load()
	if err := sp.write(undrained, []schema.Row[tp]{{Key: "r0003/a"}}, false); err != errSpillFull {
		t.Fatalf("not refused: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for sp.live.Load() >= live && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.c.st.RetiredEvicted.Load() != 1 || sp.live.Load() >= live || sp.full.Load() {
		t.Fatalf("evicted %d, live %d (was %d), full %v", h.c.st.RetiredEvicted.Load(), sp.live.Load(), live, sp.full.Load())
	}
	if _, err := h.root.Stat(filepath.Join(DurableDir, d.name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the evicted file stays: %v", err)
	}
	// Progress: the refused frame, retried on the credit, is admitted.
	if err := sp.write(undrained, []schema.Row[tp]{{Key: "r0003/a"}}, false); err != nil {
		t.Fatalf("the retry after the eviction's credit: %v", err)
	}
	if undrained.written != 1 {
		t.Fatalf("%d rows written after the credit", undrained.written)
	}
}

// TestDurableSealErrorStopsRun: a seal that fails (its file's sync), with
// the stop's callback delayed, stops the run before the node is done: the
// output is a prefix of the oracle (no gap), no record names the file.
func TestDurableSealErrorStopsRun(t *testing.T) {
	var once atomic.Bool
	defer faults.Set(func(p string) error {
		if p == "spill-seal" && once.CompareAndSwap(false, true) {
			return errors.New("injected sync failure")
		}
		return nil
	})()
	hookStopCallback = func() { time.Sleep(20 * time.Millisecond) }
	defer func() { hookStopCallback = nil }()
	f := &fencedTree{ranges: 12, pages: 3, rows: func(int, int) int { return 200 }, hold: make(chan struct{})}
	c, root, _ := newDurable(t)
	c.Start(64, 0, nil)
	sink := &collect{}
	_, err := Run[fz, tp](context.Background(), f, sink, &testSchema, Options{Workers: 4, Budget: 300, SpillDir: t.TempDir(), Durable: c})
	if err == nil || !strings.Contains(err.Error(), "injected sync failure") {
		t.Fatalf("run error %v", err)
	}
	want := f.keys(nil)
	if len(sink.keys) > len(want) || !slices.Equal(sink.keys, want[:len(sink.keys)]) {
		t.Fatalf("the output is not a prefix of the oracle (%d rows)", len(sink.keys))
	}
	r := recoverAll(t, c, root, nil)
	r.Start(64, 0, nil)
	defer r.Close()
	if r.st.RejectedCorrupt.Load()+r.st.RejectedMissing.Load() != 0 {
		t.Fatalf("a record names the failed file: %+v", r.Report().Resume)
	}
}

// TestDurableLedgerPrefix: recovery reads the ledger to its first invalid
// frame: a flipped byte, a torn tail or a lost (zeroed) earlier frame with
// later ones intact (reordered persistence) all leave a shorter prefix,
// truncated before anything is appended; records after it are dropped (their
// files orphans, unlinked); recovery repeated classifies identically; a
// foreign header discards the ledger and every file.
func TestDurableLedgerPrefix(t *testing.T) {
	build := func(t *testing.T) (*os.Root, string, []int) {
		f := &fencedTree{ranges: 8, pages: 3, rows: func(int, int) int { return 200 }, hold: make(chan struct{})}
		c, root, dir := newDurable(t)
		c.Start(64, 0, nil)
		if _, err := Run[fz, tp](context.Background(), f, &collect{}, &testSchema, Options{Workers: 4, Budget: 300, SpillDir: t.TempDir(), Durable: c}); err != nil {
			t.Fatal(err)
		}
		c.Close()
		b, _ := os.ReadFile(filepath.Join(dir, DurableDir, ledgerName))
		var offs []int
		for off := 0; off+8 <= len(b); off += 8 + int(binary.LittleEndian.Uint32(b[off:])) {
			offs = append(offs, off)
		}
		if len(offs) < 4 {
			t.Fatalf("%d ledger frames", len(offs))
		}
		return root, filepath.Join(dir, DurableDir, ledgerName), offs
	}
	for _, damage := range []string{"flipped", "torn", "reordered", "foreign"} {
		t.Run(damage, func(t *testing.T) {
			root, path, offs := build(t)
			b, _ := os.ReadFile(path)
			cut := offs[2] // records 2.. are lost or damaged
			switch damage {
			case "flipped":
				b[cut+10] ^= 0xff
			case "torn":
				b = b[:cut+5]
			case "reordered":
				for i := cut; i < offs[3]; i++ {
					b[i] = 0 // record 2 never reached the disk, 3.. did
				}
			case "foreign":
				b[20] ^= 0xff // the header's run UUID
			}
			os.WriteFile(path, b, 0o644)
			var classes []string
			for attempt := range 2 {
				r, err := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
				if err != nil {
					t.Fatal(err)
				}
				r.Start(64, 0, nil)
				rep := r.Report().Resume
				classes = append(classes, fmt.Sprint(rep.Accepted))
				size, _ := os.Stat(path)
				switch {
				case damage == "foreign" && rep.Accepted != 0:
					t.Fatalf("attempt %d: a foreign ledger's files reused: %+v", attempt, rep)
				case damage != "foreign" && attempt == 0 && (rep.Accepted != 1 || rep.LedgerDroppedBytes == 0 || size.Size() < int64(cut)):
					t.Fatalf("attempt %d: %+v (ledger %d bytes, prefix %d)", attempt, rep, size.Size(), cut)
				}
				r.Close()
			}
			if classes[0] != classes[1] {
				t.Fatalf("classification changed: %v", classes)
			}
		})
	}
}

// TestDurableCorruption: a recorded file flipped, truncated, with frames
// swapped, its tail lost, or a frame claiming more rows than the limit, is
// rejected (corrupt) before anything sized by it is allocated; another
// run's file renamed into a recorded ID is rejected as foreign; a missing
// one as missing.
func TestDurableCorruption(t *testing.T) {
	for _, damage := range []string{"flipped", "truncated", "swapped", "tail", "claim", "foreign", "missing"} {
		t.Run(damage, func(t *testing.T) {
			f := &fencedTree{ranges: 4, pages: 3, rows: func(int, int) int { return 200 }, hold: make(chan struct{})}
			c, root, dir := newDurable(t)
			c.Start(64, 0, nil)
			if _, err := Run[fz, tp](context.Background(), f, &collect{}, &testSchema, Options{Workers: 4, Budget: 300, SpillDir: t.TempDir(), Durable: c}); err != nil {
				t.Fatal(err)
			}
			c.Close()
			ents, _ := os.ReadDir(filepath.Join(dir, DurableDir))
			var victim string
			for _, e := range ents {
				if strings.HasSuffix(e.Name(), ".spill") {
					victim = filepath.Join(dir, DurableDir, e.Name())
					break
				}
			}
			b, _ := os.ReadFile(victim)
			n0 := int(binary.LittleEndian.Uint32(b))
			switch damage {
			case "flipped":
				b[len(b)/2] ^= 1
			case "truncated":
				b = b[:len(b)-40]
			case "swapped":
				// the header and the next frame change places
				n1 := int(binary.LittleEndian.Uint32(b[8+n0:]))
				h, nx := append([]byte(nil), b[:8+n0]...), append([]byte(nil), b[8+n0:16+n0+n1]...)
				copy(b, nx)
				copy(b[len(nx):], h)
			case "tail":
				copy(b[len(b)-8:], "XXXXXXXX")
			case "claim":
				// a rows frame claiming 2,000,000 rows, its CRC valid
				body := append(binary.AppendUvarint(nil, 2_000_000), make([]byte, 64)...)
				fr := frame2(nil, frameRows, body)
				b = append(append(append([]byte(nil), b[:8+n0]...), fr...), b[8+n0:]...)
			case "foreign":
				b[18] ^= 1      // the header's run UUID: another run's file under a recorded ID
				seal2(b[:8+n0]) // its frame CRC valid
			case "missing":
				os.Remove(victim)
			}
			if damage != "missing" {
				os.WriteFile(victim, b, 0o644)
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			r, err := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatal(err)
			}
			r.Start(64, 0, nil)
			defer r.Close()
			rep := r.Report().Resume
			want := map[string]string{"foreign": "foreign", "missing": "missing"}[damage]
			if want == "" {
				want = "corrupt"
			}
			if rep.Rejected[want] != 1 {
				t.Fatalf("%+v", rep)
			}
			if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 32<<20 {
				t.Fatalf("recovery allocated %d bytes", alloc)
			}
			if _, err := os.Stat(victim); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the rejected file stays: %v", err)
			}
		})
	}
}

// TestDurableFrameClaims: a rows frame claiming more rows than the
// header's limit, or more storage than the decode bound, is refused by its
// claims before anything sized by them is allocated.
func TestDurableFrameClaims(t *testing.T) {
	v := &validator{maxRows: publishRows, maxDecode: 1 << 20}
	if _, _, ok := v.claims(frameRows, append(binary.AppendUvarint(nil, 2_000_000), make([]byte, 64)...)); ok {
		t.Fatal("2,000,000 rows accepted")
	}
	if _, _, ok := v.claims(frameDelta, append(binary.AppendUvarint(binary.AppendUvarint(nil, 10), 1<<30), 0)); ok {
		t.Fatal("1 GiB of storage accepted")
	}
	if n, _, ok := v.claims(frameRows, append(binary.AppendUvarint(nil, 10), make([]byte, 64)...)); !ok || n != 10 {
		t.Fatal("a legitimate frame refused")
	}
}

// sealedSpill runs a held frontier over ranges into a fresh durable spill
// and returns it closed, with its root.
func sealedSpill(t *testing.T, ranges int) (*os.Root, *fencedTree) {
	t.Helper()
	f := &fencedTree{ranges: ranges, pages: 3, rows: func(int, int) int { return 200 }, hold: make(chan struct{})}
	c, root, _ := newDurable(t)
	c.Start(1000, 0, nil)
	if _, err := Run[fz, tp](context.Background(), f, &collect{}, &testSchema, Options{Workers: 4, Budget: 300, SpillDir: t.TempDir(), Durable: c}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	return root, f
}

// TestDurableReducedCap: a resume with a smaller cap (or spill cap) retires
// the highest-keyed certificates first (rejected_budget) and keeps the rest.
func TestDurableReducedCap(t *testing.T) {
	root, _ := sealedSpill(t, 12)
	probe, _ := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
	probe.Start(1000, 0, nil)
	all := probe.Spans()
	probe.Close()
	if len(all) < 4 {
		t.Fatalf("%d certificates", len(all))
	}
	r, err := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Start(len(all)/2, 0, nil)
	defer r.Close()
	kept := r.Spans()
	if len(kept) != len(all)/2 || r.st.RejectedBudget.Load() != int64(len(all)-len(all)/2) || !slices.Equal(kept, all[:len(kept)]) {
		t.Fatalf("kept %d of %d (rejected %d): not the lowest-keyed", len(kept), len(all), r.st.RejectedBudget.Load())
	}
}

// TestDurableHistoryAcrossAttempts: the history a ledger retains counts on
// a resume: near H, new captures find it exhausted (they spill
// disposably), and the run still completes.
func TestDurableHistoryAcrossAttempts(t *testing.T) {
	root, f := sealedSpill(t, 6)
	// Pad the ledger with retirements of unknown files (records it keeps).
	lf, err := root.OpenFile(filepath.Join(DurableDir, ledgerName), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	var buf []byte
	for range historyCap - 40 {
		buf = appendLedgerFrame(buf, ledgerRec{cert: &Cert{id: randomID()}, retire: retireRecovery})
	}
	lf.Write(buf)
	lf.Close()
	r, err := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Start(1000, 0, nil)
	f2 := &fencedTree{ranges: 40, pages: 3, rows: func(int, int) int { return 200 }, hold: make(chan struct{})}
	_ = f
	if _, err := Run[fz, tp](context.Background(), f2, &collect{}, &testSchema, Options{Workers: 4, Budget: 300, SpillDir: t.TempDir(), Durable: r}); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if r.st.HistoryExhausted.Load() == 0 || r.hist > historyCap {
		t.Fatalf("history %d of %d, exhausted %d", r.hist, historyCap, r.st.HistoryExhausted.Load())
	}
}

// TestDurableReplacedOutput: the output replaced between a retirement's
// queueing and the flush: the flusher touches nothing (the file stays),
// stops, and cancels the run with the replacement's error.
func TestDurableReplacedOutput(t *testing.T) {
	c, root, _ := newDurable(t)
	replaced := errors.New("the output directory was moved or replaced")
	var bad atomic.Bool
	c.verify = func() error {
		if bad.Load() {
			return replaced
		}
		return nil
	}
	var cause atomic.Value
	c.Start(64, 0, func(err error) { cause.Store(err) })
	d := &Cert{id: randomID(), certified: true, slot: true}
	d.name = fileName(d.id)
	root.WriteFile(filepath.Join(DurableDir, d.name), []byte("x"), 0o644)
	bad.Store(true)
	c.mu.Lock()
	c.retireLocked(d, retireEvicted)
	c.mu.Unlock()
	c.Close()
	if _, err := root.Stat(filepath.Join(DurableDir, d.name)); err != nil {
		t.Fatalf("the file was touched: %v", err)
	}
	if err, _ := cause.Load().(error); !errors.Is(err, replaced) || !errors.Is(c.Err(), replaced) {
		t.Fatalf("cause %v, cache error %v", err, c.Err())
	}
}

var _ = io.EOF

// TestDurableReplacedMidCycle: ownership is verified before each mutation,
// not once per batch. A barrier at each step of a flusher cycle (the
// append, the ledger sync, the unlink) and of recovery's retirement (the
// ledger sync, the unlinks, the directory sync) replaces the output after
// the earlier steps ran: the next mutation is refused, no file is
// unlinked, and the replacement is fatal (the run's cause, recovery's
// error).
func TestDurableReplacedMidCycle(t *testing.T) {
	replaced := errors.New("the output directory was moved or replaced")
	for _, point := range []string{"spill-ledger-append", "spill-ledger-sync", "spill-unlink"} {
		t.Run(point, func(t *testing.T) {
			c, root, _ := newDurable(t)
			var bad atomic.Bool
			c.verify = func() error {
				if bad.Load() {
					return replaced
				}
				return nil
			}
			defer faults.Set(func(p string) error {
				if p == point {
					bad.Store(true) // the barrier: ownership changes here
				}
				return nil
			})()
			var cause atomic.Value
			c.Start(64, 0, func(err error) { cause.Store(err) })
			var ds []*Cert
			for range 3 {
				d := &Cert{id: randomID(), certified: true, slot: true}
				d.name = fileName(d.id)
				root.WriteFile(filepath.Join(DurableDir, d.name), []byte("x"), 0o644)
				ds = append(ds, d)
			}
			c.mu.Lock()
			for _, d := range ds {
				c.retireLocked(d, retireEvicted)
			}
			c.mu.Unlock()
			c.Close()
			for _, d := range ds {
				if _, err := root.Stat(filepath.Join(DurableDir, d.name)); err != nil {
					t.Fatalf("%s unlinked after the output was replaced: %v", d.name, err)
				}
			}
			if err, _ := cause.Load().(error); !errors.Is(err, replaced) || !errors.Is(c.Err(), replaced) {
				t.Fatalf("cause %v, cache error %v", err, c.Err())
			}
			if c.st.LedgerFailed.Load() != 0 {
				t.Fatal("a replacement latched as a ledger failure")
			}
		})
	}
	for _, point := range []string{"spill-recovery-sync", "spill-recovery-unlink", "spill-recovery-dirsync"} {
		t.Run(point, func(t *testing.T) {
			root, _ := sealedSpill(t, 6)
			files := func() int {
				ents, _ := root.FS().(fs.ReadDirFS).ReadDir(DurableDir)
				return len(ents)
			}
			before := files()
			if before < 3 {
				t.Fatalf("%d durable entries", before)
			}
			var bad atomic.Bool
			verify := func() error {
				if bad.Load() {
					return replaced
				}
				return nil
			}
			defer faults.Set(func(p string) error {
				if p == point {
					bad.Store(true)
				}
				return nil
			})()
			top := "~" // K above every certificate: recovery retires them all
			_, err := RecoverDurable(root, verify, testUUID, "ident", "test", &top)
			if !errors.Is(err, replaced) {
				t.Fatalf("recovery after the replacement: %v", err)
			}
			if point != "spill-recovery-dirsync" && files() != before {
				t.Fatalf("%d of %d entries left: unlinked after the replacement", files(), before)
			}
		})
	}
	// The ledger's truncation: a replacement at its crash point leaves the
	// ledger unshortened.
	t.Run("spill-ledger-truncate", func(t *testing.T) {
		root, _ := sealedSpill(t, 6)
		path := filepath.Join(DurableDir, ledgerName)
		b, _ := root.ReadFile(path)
		b = append(b, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) // a torn tail: the prefix is shorter
		root.WriteFile(path, b, 0o644)
		var bad atomic.Bool
		verify := func() error {
			if bad.Load() {
				return replaced
			}
			return nil
		}
		defer faults.Set(func(p string) error {
			if p == "spill-ledger-truncate" {
				bad.Store(true)
			}
			return nil
		})()
		_, err := RecoverDurable(root, verify, testUUID, "ident", "test", nil)
		if !errors.Is(err, replaced) {
			t.Fatalf("recovery after the replacement: %v", err)
		}
		if after, _ := root.ReadFile(path); len(after) != len(b) {
			t.Fatalf("ledger %d bytes, was %d: truncated after the replacement", len(after), len(b))
		}
	})
	// A durable file's write: replaced at its crash point (before the open)
	// nothing is written; replaced during a write that runs out of space,
	// the torn frame is not rolled back and the error is the replacement,
	// not a refused frame.
	for _, at := range []string{"crash-point", "enospc-rollback"} {
		t.Run("spill-write/"+at, func(t *testing.T) {
			h := newHarness(t)
			h.e.Span("r0001", "r0002", false)
			h.rows(t, 0, 0, 10)
			h.e.Fence("r0001/000000-99999", false)
			h.publish(t, true)
			f := h.n.spill
			if f == nil || f.d == nil {
				t.Fatal("no durable file")
			}
			var bad atomic.Bool
			h.o.sp.verify = func() error {
				if bad.Load() {
					return replaced
				}
				return nil
			}
			name := filepath.Join(DurableDir, f.d.name)
			fi, _ := h.root.Stat(name)
			before := fi.Size()
			if at == "crash-point" {
				defer faults.Set(func(p string) error {
					if p == "spill-write-frames" {
						bad.Store(true)
					}
					return nil
				})()
			} else {
				defer faults.SetIO(func(op, n string, b []byte) (int, error) {
					if op == "write" && strings.HasSuffix(n, ".spill") {
						bad.Store(true)
						return len(b) / 2, syscall.ENOSPC
					}
					return 0, nil
				})()
			}
			err := h.o.sp.write(f, []schema.Row[tp]{{Key: "r0001/000001-00000"}}, false)
			if !errors.Is(err, replaced) {
				t.Fatalf("write after the replacement: %v", err)
			}
			fi, _ = h.root.Stat(name)
			switch {
			case at == "crash-point" && fi.Size() != before:
				t.Fatalf("%d bytes written after the replacement", fi.Size()-before)
			case at == "enospc-rollback" && fi.Size() == before:
				t.Fatal("the torn frame was truncated after the replacement")
			}
		})
	}
}

// TestDurableCodecRejected: files of a row codec the resuming run does not
// replay, untouched (every frame, trailer and whole-file checksum valid),
// are rejected by recovery (rejected.codec), retired and unlinked: never
// accepted into the reuse plan, so replay never meets them.
func TestDurableCodecRejected(t *testing.T) {
	root, _ := sealedSpill(t, 6)
	probe, err := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	probe.Start(1000, 0, nil)
	n := len(probe.Spans())
	probe.Close()
	if n == 0 {
		t.Fatal("nothing sealed")
	}
	r, err := RecoverDurable(root, nil, testUUID, "ident", "test-v2", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Start(1000, 0, nil)
	defer r.Close()
	rep := r.Report().Resume
	if rep.Rejected["codec"] != int64(n) || rep.Accepted != 0 || rep.Rejected["corrupt"] != 0 || len(r.Spans()) != 0 {
		t.Fatalf("%d certificates of another codec: %+v", n, rep)
	}
	ents, _ := root.FS().(fs.ReadDirFS).ReadDir(DurableDir)
	if len(ents) != 1 { // the ledger
		t.Fatalf("%d entries left in the durable directory", len(ents))
	}
}

// TestDurableRecoveryBounds: recovery bounds its scan and its inventory by
// the persisted format, whatever a checksum-valid ledger holds. A ledger
// of more than 10,000 live sealed records is cut before the first record
// past that (no cap could have admitted it), and one of more than H
// records is cut at H: the scan reads at most H records, the inventory
// holds at most 10,000 entries, the ledger is truncated there
// (ledger_cut, ledger_dropped_bytes), and the files named after the cut
// are orphans.
func TestDurableRecoveryBounds(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sealed, extra int // sealed records, then retirements of unknown files
	}{
		{"live", maxCertificates + 64, 0},
		{"history", 10, historyCap + 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, root, dir := newDurable(t)
			c.Close()
			var buf []byte
			var named [][16]byte
			for i := range tc.sealed {
				d := &Cert{id: randomID(), cs: fmt.Sprintf("k%06d", 2*i), ce: fmt.Sprintf("k%06d", 2*i+1), captured: 1, last: fmt.Sprintf("k%06d", 2*i+1), bytes: 1}
				buf = appendLedgerFrame(buf, ledgerRec{cert: d})
				named = append(named, d.id)
			}
			for range tc.extra {
				buf = appendLedgerFrame(buf, ledgerRec{cert: &Cert{id: randomID()}, retire: retireRecovery})
			}
			path := filepath.Join(dir, DurableDir, ledgerName)
			lf, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			lf.Write(buf)
			lf.Close()
			// A file named after the cut: an orphan.
			last := fileName(named[len(named)-1])
			root.WriteFile(filepath.Join(DurableDir, last), []byte("x"), 0o644)
			r, err := newCache(root, nil, testUUID, "ident", "test")
			if err != nil {
				t.Fatal(err)
			}
			entries, err := r.readLedger()
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) > maxCertificates || r.hist > historyCap {
				t.Fatalf("%d entries (bound %d), %d records scanned (bound %d)", len(entries), maxCertificates, r.hist, historyCap)
			}
			if r.st.LedgerCut.Load() == 0 || r.st.LedgerDroppedBytes.Load() == 0 {
				t.Fatalf("not cut: dropped %d", r.st.LedgerDroppedBytes.Load())
			}
			fi, _ := os.Stat(path)
			if fi.Size() != r.lend {
				t.Fatalf("ledger %d bytes, prefix %d: not truncated", fi.Size(), r.lend)
			}
			if tc.name == "live" {
				if _, ok := entries[named[len(named)-1]]; ok {
					t.Fatal("a record past the 10,000th live certificate kept")
				}
				if err := r.sweep(entries); err != nil {
					t.Fatal(err)
				}
				if _, err := root.Stat(filepath.Join(DurableDir, last)); !errors.Is(err, os.ErrNotExist) || r.st.Orphans.Load() != 1 {
					t.Fatalf("the file past the cut is not an orphan: %v, %d orphans", err, r.st.Orphans.Load())
				}
			}
			r.ledger.Close()
		})
	}
}

// TestDurableNestedOverlaps: every member of a connected overlap group is
// rejected, not only adjacent pairs: a wide certificate followed by nested
// ones, an open-ended one followed by any, and a chain; disjoint ones
// (adjacent bounds included) after a group are kept.
func TestDurableNestedOverlaps(t *testing.T) {
	type iv struct {
		cs, ce string
		open   bool
	}
	for _, tc := range []struct {
		name string
		in   []iv
		want []bool
	}{
		{"nested", []iv{{"a", "z", false}, {"b", "c", false}, {"d", "e", false}}, []bool{true, true, true}},
		{"open-ended", []iv{{"a", "", true}, {"b", "c", false}, {"x", "y", false}}, []bool{true, true, true}},
		{"open-ended last", []iv{{"a", "b", false}, {"c", "d", false}, {"c", "", true}}, []bool{false, true, true}},
		{"nested then disjoint", []iv{{"a", "m", false}, {"b", "c", false}, {"m", "n", false}, {"o", "p", false}}, []bool{true, true, false, false}},
		{"chain", []iv{{"a", "d", false}, {"c", "f", false}, {"e", "g", false}, {"g", "h", false}}, []bool{true, true, true, false}},
		{"adjacent", []iv{{"a", "b", false}, {"b", "c", false}, {"c", "", true}}, []bool{false, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keep := make([]*Cert, len(tc.in))
			for i, v := range tc.in {
				keep[i] = &Cert{cs: v.cs, ce: v.ce, ceOpen: v.open}
			}
			if got := overlaps(keep); !slices.Equal(got, tc.want) {
				t.Fatalf("rejected %v, want %v", got, tc.want)
			}
		})
	}
}

// ledgerRecords counts the ledger's record frames (the header excluded).
func ledgerRecords(t *testing.T, root *os.Root) int {
	t.Helper()
	b, err := root.ReadFile(filepath.Join(DurableDir, ledgerName))
	if err != nil {
		t.Fatal(err)
	}
	n := -1
	for off := 0; off+8 <= len(b); off += 8 + int(binary.LittleEndian.Uint32(b[off:])) {
		n++
	}
	return n
}

// TestDurableRecoveryHistory: a checksum-valid ledger whose records fill
// H with certificates still live passes the scan, but recovery and Start
// then owe one retirement per live certificate (written now, or reserved
// for an accepted one). The prefix is cut so that the records kept plus
// those owed stay within H: through complete recovery and Start, with
// accepted certificates (reserved) and with missing files (retired now),
// and again on a further resume, the history and the ledger never pass H.
func TestDurableRecoveryHistory(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			root, _ := sealedSpill(t, 6)
			lf, err := root.OpenFile(filepath.Join(DurableDir, ledgerName), os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			var buf []byte
			for range historyCap - ledgerRecords(t, root) - 1 { // the scan alone would keep them all
				buf = appendLedgerFrame(buf, ledgerRec{cert: &Cert{id: randomID()}, retire: retireRecovery})
			}
			lf.Write(buf)
			lf.Close()
			if missing {
				ents, _ := root.FS().(fs.ReadDirFS).ReadDir(DurableDir)
				for _, e := range ents {
					if strings.HasSuffix(e.Name(), ".spill") {
						root.Remove(filepath.Join(DurableDir, e.Name()))
					}
				}
			}
			for attempt := range 2 {
				r, err := RecoverDurable(root, nil, testUUID, "ident", "test", nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Start(1000, 0, nil); err != nil {
					t.Fatal(err)
				}
				hist, reused := r.hist, r.Reused()
				r.Close()
				if n := ledgerRecords(t, root); hist > historyCap || n > historyCap || n+reused > historyCap {
					t.Fatalf("attempt %d: history %d, ledger %d records plus %d reserved (H %d)", attempt, hist, n, reused, historyCap)
				}
				if attempt == 0 && !missing && reused == 0 {
					t.Fatal("nothing accepted")
				}
			}
		})
	}
}
