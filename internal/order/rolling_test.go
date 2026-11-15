// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/source"
)

// rollFixture is a plan of tiles whose frontier tile (hold) is held after
// its first page until every other tile but the last is listed, so the
// drainer, released, finds the listed tiles ahead of it; the last tile is
// held too, until release is called, so listing goes on while they are
// written.
type rollFixture struct {
	p        *planned
	frontier int
	last     chan struct{}
	once     sync.Once
}

func newRollFixture(r *rand.Rand, groups bool) *rollFixture {
	p := newPlanned(24+r.IntN(16), 150+r.IntN(300), 20+r.IntN(60))
	if groups {
		p.rep = 2 + r.IntN(9)
	}
	return &rollFixture{p: p, frontier: 3 + r.IntN(3), last: make(chan struct{})}
}

// hold arms the holds (a run that rolls); the serial reference runs
// without them.
func (f *rollFixture) hold(t *testing.T) {
	front := make(chan struct{})
	f.p.hold[f.frontier] = front
	f.p.hold[f.p.tiles-1] = f.last
	var once sync.Once
	hookWait = func(string) {
		if f.p.finished.Load() >= int64(f.p.tiles-2) {
			once.Do(func() { close(front) })
		}
	}
	t.Cleanup(func() { hookWait = nil })
}

func (f *rollFixture) release() { f.once.Do(func() { close(f.last) }) }

// TestRollingCutWhereTheSinkWould: with a plan, the listed tiles ahead of
// the drainer are written as segments while listing goes on (rolling), the
// last part of each stretch kept open for the serial drain; the parts are
// the serial drain's row for row (every boundary, the row cuts, the last
// part final), with spill on and off, unique keys and groups (a pending
// cut kept across a stretch's end), a byte limit alone and with a row cap,
// for 2, 4 and 8 encoders. Each rolling run must keep a stretch before
// listing ends (the last tile is held until one is: a run that never rolls
// times out), close segment parts before listing ends and never run more
// segments at once than the encoders admitted. Some runs
// end listing in the middle of their first stretch (at segments-begun):
// the stretch is still kept, and the tail follows.
func TestRollingCutWhereTheSinkWould(t *testing.T) {
	anyShare(t)
	for seed := range uint64(8) {
		for _, groups := range []bool{false, true} {
			r := rand.New(rand.NewPCG(seed, 23))
			fx := newRollFixture(r, groups)
			want := fx.p.keys()
			limit := int64(2000 + r.IntN(4000))
			var maxRows int64
			if seed%3 == 0 {
				maxRows = int64(40 + r.IntN(200))
			}
			midStretch := seed%4 == 1
			run := func(procs, encoders int, spill, roll bool) (*partSink, Stats) {
				t.Helper()
				withProcs(t, procs)
				r := rand.New(rand.NewPCG(seed, 23))
				fresh := newRollFixture(r, groups)
				s := &partSink{limit: limit, maxRows: maxRows, encoders: encoders, groups: groups}
				var segsAtListed int
				listed := make(chan struct{})
				opt := Options{Workers: 6, Budget: 1 << 30, Groups: groups, ListingDone: func() {
					s.mu.Lock()
					segsAtListed = s.segClosed
					s.mu.Unlock()
					close(listed)
				}}
				if spill {
					opt.Budget, opt.SpillDir = 400, t.TempDir()
				}
				if roll {
					fresh.hold(t)
					restore := faults.Set(func(p string) error {
						switch {
						case midStretch && p == "segments-begun":
							fresh.release()
							<-listed
						case !midStretch && p == "stretch-kept": // every segment of the stretch closed
							fresh.release()
						}
						return nil
					})
					defer restore()
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				st, err := Run[int, tp](ctx, fresh.p, s, &testSchema, opt)
				if err != nil {
					t.Fatalf("seed %d groups %v encoders %d spill %v: %v (segments %+v, rolling %+v)", seed, groups, encoders, spill, err, st.Segments, *st.Segments.Rolling)
				}
				s.close()
				if roll && segsAtListed == 0 && !midStretch {
					t.Fatalf("seed %d groups %v encoders %d spill %v: no segment part closed before listing ended", seed, groups, encoders, spill)
				}
				return s, st
			}
			serial, _ := run(1, 1, false, false)
			if got := slices.Concat(serial.boundaries()...); !slices.Equal(got, want) {
				t.Fatalf("seed %d groups %v: the serial run's rows differ", seed, groups)
			}
			for _, k := range []int{2, 4, 8} {
				for _, spill := range []bool{false, true} {
					s, st := run(k+1, k, spill, true)
					at := func() string { return fmt.Sprintf("seed %d groups %v spill %v", seed, groups, spill) }
					if !slices.EqualFunc(s.boundaries(), serial.boundaries(), slices.Equal[[]string]) {
						t.Fatalf("%s k %d: %d parts, serial %d; or their rows differ (rolling %+v)", at(), k, len(s.parts), len(serial.parts), st.Segments.Rolling)
					}
					if s.rowCuts != serial.rowCuts {
						t.Fatalf("%s k %d: %d row cuts, serial %d", at(), k, s.rowCuts, serial.rowCuts)
					}
					for i, p := range s.parts {
						if !p.closed || p.final != (i == len(s.parts)-1) {
							t.Fatalf("%s k %d: part %d closed %v final %v of %d", at(), k, i, p.closed, p.final, len(s.parts))
						}
					}
					rl := st.Segments.Rolling
					if rl == nil || rl.Stretches == 0 || rl.Rows == 0 || s.kept != int(rl.Stretches) || s.maxWriting > k {
						t.Fatalf("%s k %d: rolling %+v, %d kept, %d segments at once", at(), k, rl, s.kept, s.maxWriting)
					}
					if st.Rows != int64(len(want)) || st.Received != st.Rows {
						t.Fatalf("%s k %d: %d rows handed on, %d received, want %d", at(), k, st.Rows, st.Received, len(want))
					}
				}
			}
		}
	}
}

// TestRollingTrailerFailureClosesNoPart: a spill file of a listed tile
// whose trailer disagrees with its rows is found before the stretch holding
// it opens any segment, while listing still goes on: the run fails with the
// invariant error, no part holding a row of that tile was closed (a part
// closed could be journaled) and no part is final. Earlier stretches may
// have written the tiles before it.
func TestRollingTrailerFailureClosesNoPart(t *testing.T) {
	withProcs(t, 5)
	fx := newRollFixture(rand.New(rand.NewPCG(3, 23)), false)
	fx.hold(t)
	t.Cleanup(fx.release)
	dir := t.TempDir()
	s := &partSink{limit: 3000, encoders: 4}
	tile := "" // the damaged file's tile, its keys' prefix (the drainer's goroutine alone)
	hookSegmenter = func(*sync.Mutex) {
		s.mu.Lock()
		ready := len(s.parts) >= 2 && !s.segmented
		reached := "" // a tile past every row the drainer can have reached
		if ready {
			p := s.parts[len(s.parts)-1]
			reached = fmt.Sprintf("t%04d", fx.frontier)
			if len(p.keys) > 0 {
				reached = max(reached, p.keys[len(p.keys)-1][:len("t0000")])
			}
		}
		s.mu.Unlock()
		if tile != "" || !ready {
			return
		}
		files, _ := filepath.Glob(filepath.Join(dir, "spill-*", "*.spill"))
		slices.Sort(files)
		for i := len(files) - 1; i >= 0 && tile == ""; i-- {
			if k := firstSpilledKey(t, files[i]); k != "" && k > reached+"~" && hasTrailer(files[i]) {
				damageTrailer(t, files[i])
				tile = k[:len("t0000")]
			}
		}
	}
	t.Cleanup(func() { hookSegmenter = nil })
	st, err := Run[int, tp](context.Background(), fx.p, s, &testSchema, Options{Workers: 6, Budget: 400, SpillDir: dir})
	var inv *dataset.InvariantError
	if tile == "" || !errors.As(err, &inv) {
		t.Fatalf("damaged %q: err %v, want the trailer's invariant error", tile, err)
	}
	if !st.ListingDone.IsZero() {
		t.Fatal("listing ended before the stretch was planned")
	}
	for i, p := range s.parts {
		if p.final {
			t.Fatalf("part %d final", i)
		}
		if p.closed && slices.ContainsFunc(p.keys, func(k string) bool { return strings.HasPrefix(k, tile) }) {
			t.Fatalf("part %d, holding rows of the damaged tile %s, was closed", i, tile)
		}
	}
}

// firstSpilledKey is the first key of the spill file at path, "" when it
// holds no rows frame yet.
func firstSpilledKey(t *testing.T, path string) string {
	sp, err := newSpiller(&testSchema, t.TempDir(), false, false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.close()
	fi, err := os.Stat(path)
	if err != nil {
		return ""
	}
	f := &spillFile{path: path, size: fi.Size()}
	f.most.Store(1 << 20)
	f.mostOwned.Store(1 << 30)
	rows, err := sp.next(f)
	sp.closeFile(f)
	if err != nil || len(rows) == 0 {
		return ""
	}
	return rows[0].Key
}

// hasTrailer reports whether the spill file at path ends with its trailer
// (its range is done).
func hasTrailer(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	for off := 0; off+8 <= len(b); {
		n := int(uint32(b[off]) | uint32(b[off+1])<<8 | uint32(b[off+2])<<16 | uint32(b[off+3])<<24)
		if off+8+n > len(b) || n == 0 {
			return false
		}
		if b[off+8] == frameTrailer {
			return true
		}
		off += 8 + n
	}
	return false
}

// TestRollingFailureMidStretch: a segment's part writer fails, or the run
// is cancelled, in a stretch written while listing goes on: the run fails
// with that error before listing ends, the failed part is never closed,
// no part is final, and every part closed is a run of the output.
func TestRollingFailureMidStretch(t *testing.T) {
	anyShare(t)
	withProcs(t, 5)
	for _, cancelled := range []bool{false, true} {
		fx := newRollFixture(rand.New(rand.NewPCG(5, 23)), false)
		fx.hold(t)
		s := &partSink{limit: 3000, encoders: 4}
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			restore := faults.Set(func(p string) error {
				if p == "segments-begun" {
					cancel()
				}
				return nil
			})
			t.Cleanup(restore)
		} else {
			s.failAt = 2
		}
		st, err := Run[int, tp](ctx, fx.p, s, &testSchema, Options{Workers: 6, Budget: 400, SpillDir: t.TempDir()})
		fx.release()
		cancel()
		switch {
		case cancelled && !errors.Is(err, context.Canceled):
			t.Fatalf("cancelled: err %v", err)
		case !cancelled && (err == nil || err.Error() != "part writer failed" || s.failed == nil || s.failed.closed):
			t.Fatalf("err %v, want the part writer's, its part unclosed", err)
		case !st.ListingDone.IsZero():
			t.Fatal("listing ended before the stretch failed")
		}
		want := fx.p.keys()
		pos := 0
		for i, p := range s.parts {
			if p.final {
				t.Fatalf("part %d final", i)
			}
			if !p.closed {
				continue
			}
			at := slices.Index(want, p.keys[0])
			if at < 0 || at < pos || !slices.Equal(want[at:at+len(p.keys)], p.keys) {
				t.Fatalf("part %d is not a run of the output", i)
			}
			pos = at
		}
	}
}

// TestRollingEngagesDuringListing: a seeded run (a plan) whose last tile is
// held until a stretch is kept writes parts with several segments at once
// before listing ends: the first segment's write waits until another
// segment writes (two encoders busy together), segment parts are closed
// before ListingDone, the report counts the stretch, its rows and the
// looks that wrote none, and the rows are the plan's, in order.
func TestRollingEngagesDuringListing(t *testing.T) {
	withProcs(t, 5)
	fx := newRollFixture(rand.New(rand.NewPCG(7, 23)), false)
	fx.hold(t)
	t.Cleanup(fx.release)
	defer faults.Set(func(p string) error {
		if p == "stretch-kept" {
			fx.release()
		}
		return nil
	})()
	s := &partSink{limit: 2500, encoders: 4, meet: make(chan struct{})}
	var closedAtListed int
	opt := Options{Workers: 6, Budget: 1 << 30, ListingDone: func() {
		s.mu.Lock()
		closedAtListed = s.segClosed
		s.mu.Unlock()
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := Run[int, tp](ctx, fx.p, s, &testSchema, opt)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	if got := slices.Concat(s.boundaries()...); !slices.Equal(got, fx.p.keys()) {
		t.Fatal("rows differ")
	}
	rl := st.Segments.Rolling
	if rl == nil || rl.Stretches == 0 || rl.Segments < 2 || rl.Rows == 0 || len(rl.NotEngaged) == 0 || s.maxWriting < 2 || rl.PeakWriters < 2 || closedAtListed == 0 {
		t.Fatalf("rolling %+v, %d segments at once, %d closed when listing ended", rl, s.maxWriting, closedAtListed)
	}
	t.Logf("rolling %+v; %d parts, %d closed by segments before listing ended", *rl, len(s.parts), closedAtListed)
}

// TestSegmentsLeaveIdleWorkersAlone: the tail's segments, writing rows held
// in memory, do not wake the listing workers, idle once listing is done:
// at 64 workers they wait at idle a few times each over the whole tail,
// not once per unit written (a broadcast per unit woke every worker for
// each, the tail slower by half at 2,048 workers).
func TestSegmentsLeaveIdleWorkersAlone(t *testing.T) {
	withProcs(t, 9)
	const workers = 64
	f := &frontier{slowPages: 2, siblings: 40, sibPages: 20, rows: 100}
	f.hold, f.left = make(chan struct{}), &atomic.Int64{}
	f.left.Store(int64(f.siblings))
	s := &partSink{limit: 20_000, encoders: 8}
	var listed atomic.Bool
	var idle atomic.Int64 // idle waits after listing is done
	hookWait = func(site string) {
		if site == waitIdle && listed.Load() {
			idle.Add(1)
		}
	}
	t.Cleanup(func() { hookWait = nil })
	st, err := Run[fr, tp](context.Background(), f, s, &testSchema, Options{Workers: workers, Budget: 1 << 30,
		ListingDone: func() { listed.Store(true) }})
	if err != nil {
		t.Fatal(err)
	}
	units := int64(f.siblings * f.sibPages)
	t.Logf("segments %d, %d rows; %d idle waits after listing, %d units", st.Segments.Segments, st.Segments.Rows, idle.Load(), units)
	if st.Segments.Reason != SegmentEngaged || st.Segments.Segments < 4 {
		t.Fatalf("segments %+v", st.Segments)
	}
	if idle.Load() > 4*workers {
		t.Fatalf("%d idle waits after listing at %d workers: the segments woke them", idle.Load(), workers)
	}
}

// certPlan is a plan over a certificate resume (certResume): tiled's
// ranges as tiles in key order, as keyrange's cutReuse makes them, the
// gaps above K cut into tiles of about gapRows rows and each replay range
// a tile of its own. The first gap tile (the frontier), when it is not the
// last, is held until every other gap tile but the last is listed, the
// last until release, so the drainer finds listed tiles holding
// certificates ahead of it while listing goes on.
type certPlan struct {
	*tiled
	tiles          []tz
	est            []int64
	front, last    int // tile indexes
	frontC, lastC  chan struct{}
	frontO, lastO  sync.Once
	gaps           int64
	finished       atomic.Int64 // gap tiles listed through
	mu             sync.Mutex
	next           int
	betweenReplays int // replay tiles between the frontier and the last
}

func newCertPlan(t *testing.T, cr *certResume, gapRows int) *certPlan {
	t.Helper()
	p := &certPlan{tiled: &tiled{rows: cr.rows, spans: cr.c.Spans(), k: cr.k}, front: -1, frontC: make(chan struct{}), lastC: make(chan struct{})}
	add := func(r tz) {
		var n int64
		for _, row := range cr.rows {
			if row.Key > r.lo && (r.open || row.Key <= r.hi) {
				n++
			}
		}
		p.tiles, p.est = append(p.tiles, r), append(p.est, n)
		if r.replay < 0 {
			p.gaps++
			if p.front < 0 {
				p.front = len(p.tiles) - 1
			}
		}
	}
	gap := func(lo, hi string, open bool) {
		n := 0
		for _, row := range cr.rows {
			if row.Key > lo && (open || row.Key <= hi) {
				if n++; n == gapRows {
					add(tz{lo: lo, hi: row.Key, replay: -1})
					lo, n = row.Key, 0
				}
			}
		}
		if open || hi > lo {
			add(tz{lo: lo, hi: hi, open: open, replay: -1})
		}
	}
	lo := cr.k
	for i, s := range p.spans {
		start := max(s.Lo, cr.k)
		if start > lo {
			gap(lo, start, false)
		}
		add(tz{lo: start, hi: s.Hi, open: s.HiOpen, replay: i})
		if s.HiOpen {
			t.Fatal("fixture: the last certificate is open, no gap tile ends the plan")
		}
		lo = s.Hi
	}
	gap(lo, "", true)
	p.last = len(p.tiles) - 1
	if p.front == p.last { // no gap before the certificates: nothing to hold at the frontier
		p.front = -1
		close(p.frontC)
	}
	for i := p.front + 1; i < p.last; i++ {
		if p.tiles[i].replay >= 0 {
			p.betweenReplays++
		}
	}
	if p.betweenReplays == 0 {
		t.Fatalf("fixture: no certificate between the frontier (tile %d) and the last tile (%d)", p.front, p.last)
	}
	return p
}

func (p *certPlan) Planner() source.Planner[tz] { return p }
func (p *certPlan) Sizes() source.PlanSizes     { return source.PlanSizes{} }
func (p *certPlan) Bound(bool)                  {}

func (p *certPlan) Start(context.Context, func()) func() { return func() {} }

func (p *certPlan) Next(admit func(int64) bool) (tz, bool, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.next == len(p.tiles):
		return tz{}, false, true, nil
	case !admit(p.est[p.next]):
		return tz{}, false, false, nil
	}
	p.next++
	return p.tiles[p.next-1], true, false, nil
}

func (p *certPlan) Expand(ctx context.Context, r tz, e source.Emitter[tz, tp]) error {
	var hold chan struct{}
	switch {
	case p.front >= 0 && r.lo == p.tiles[p.front].lo:
		hold = p.frontC
	case r.lo == p.tiles[p.last].lo:
		hold = p.lastC
	}
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	err := p.tiled.Expand(ctx, r, e)
	p.finished.Add(1)
	return err
}

func (p *certPlan) release() { p.lastO.Do(func() { close(p.lastC) }) }

// run resumes cr into s through p, with the frontier released once the
// other gap tiles are listed and the last tile once a stretch is kept;
// crash, if set, is the crash point stopping it (its first time). Each
// segment part closed moves K over the parts closed in order, as the
// journal would.
func (p *certPlan) run(t *testing.T, cr *certResume, s *partSink, crash string) (Stats, error) {
	t.Helper()
	withProcs(t, 5)
	anyShare(t)
	hookWait = func(string) {
		if p.front >= 0 && p.finished.Load() >= p.gaps-2 {
			p.frontO.Do(func() { close(p.frontC) })
		}
	}
	defer func() { hookWait = nil }()
	defer p.release()
	defer faults.Set(func(at string) error {
		if at == "stretch-kept" {
			p.release()
		}
		if at == crash {
			return faults.ErrCrash
		}
		return nil
	})()
	s.onClose = func(string) {
		if k, ok := s.closedPrefix(); ok {
			cr.c.SetK(k)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	k := cr.k
	return Run[tz, tp](ctx, p, s, &testSchema, Options{Workers: 4, Budget: 400, SpillDir: t.TempDir(), Durable: cr.c, After: &k})
}

// closedPrefix is the last key of the parts closed in order from the first
// (what a journal would have published), false if none is.
func (s *partSink) closedPrefix() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var k string
	ok := false
	for _, p := range s.parts {
		if !p.closed || len(p.keys) == 0 {
			break
		}
		k, ok = p.keys[len(p.keys)-1], true
	}
	return k, ok
}

// TestRollingReplaysCertificates: a seeded resume whose plan holds reused
// certificates (replay ranges as tiles, around which it lists the gaps)
// writes a stretch over listed tiles that hold certificates while listing
// goes on: a tile holding a replay range is listed once its other ranges
// are, and the stretch replays the certificates the drainer has not
// reached, as the tail does (scanned before the cut, read again by offset
// by their segments). The parts are the uninterrupted run's after K, every
// certified row above K handed on once, and every certificate retires once
// K covers it. Killed when a stretch holding certificates is planned with
// none of its parts opened, or when it is written with its last part kept,
// the run resumes (recovery with K the last part closed in order) to the
// same parts.
func TestRollingReplaysCertificates(t *testing.T) {
	for _, crash := range []string{"", "segments-begun", "stretch-kept"} {
		t.Run("crash="+crash, func(t *testing.T) {
			cr := newCertResume(t)
			spans := cr.c.Spans()
			p := newCertPlan(t, cr, 150)
			s := &partSink{limit: cr.limit, encoders: 4, stated: true}
			st, err := p.run(t, cr, s, crash)
			rl := st.Segments.Rolling
			if crash == "" {
				if err != nil {
					t.Fatalf("%v (segments %+v)", err, st.Segments)
				}
				s.close()
				if got := s.boundaries(); !slices.EqualFunc(got, cr.want, slices.Equal[[]string]) {
					t.Fatalf("%d parts, the uninterrupted run's after K %d; or their rows differ", len(got), len(cr.want))
				}
				in, all := cr.certified(spans)
				if rl == nil || rl.Stretches == 0 || rl.Certificates == 0 || st.Segments.Certificates < int(rl.Certificates) {
					t.Fatalf("rolling %+v, segments %+v: want a stretch replaying certificates (%d between the frontier and the last tile)", rl, st.Segments, p.betweenReplays)
				}
				if st.ReplayedRows != in || st.Received != st.Rows || st.Rows != all {
					t.Fatalf("replayed %d (want %d), received %d, handed on %d (want %d)", st.ReplayedRows, in, st.Received, st.Rows, all)
				}
				rs := &cr.c.st
				if rs.ReplayedCerts.Load() != int64(len(spans)) || rs.ReplayedRows.Load() != in {
					t.Fatalf("the cache counted %d of %d certificates, %d of %d rows replayed", rs.ReplayedCerts.Load(), len(spans), rs.ReplayedRows.Load(), in)
				}
				cr.c.SetK(spans[len(spans)-1].Hi)
				if n := rs.RetiredPublished.Load(); n != int64(len(spans)) {
					t.Fatalf("%d of %d certificates retired once K covers them", n, len(spans))
				}
				t.Logf("rolling %+v; %d tiles, %d certificates (%d by stretches, %d by segments in all)", *rl, len(p.tiles), len(spans), rl.Certificates, st.Segments.Certificates)
				return
			}
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("stopped with %v, want the crash at %s (rolling %+v)", err, crash, rl)
			}
			if rl == nil || rl.Certificates == 0 && crash == "stretch-kept" {
				t.Fatalf("rolling %+v: the crash at %s was not in a stretch replaying certificates", rl, crash)
			}
			// The resume: K is the last part closed in order; the parts after
			// it are the uninterrupted run's.
			k, ok := s.closedPrefix()
			var done [][]string
			for _, pt := range s.parts {
				if !pt.closed || len(pt.keys) == 0 {
					break
				}
				done = append(done, pt.keys)
			}
			if !ok {
				k = cr.k
			}
			cr.c = recoverAll(t, cr.c, cr.root, &k)
			cr.c.Start(slots(64), 0, nil)
			cr.k = k
			rp := newCertPlan(t, cr, 150)
			rs := &partSink{limit: cr.limit, encoders: 4, stated: true}
			if _, err := rp.run(t, cr, rs, ""); err != nil {
				t.Fatalf("resume: %v", err)
			}
			rs.close()
			got := append(done, rs.boundaries()...)
			if !slices.EqualFunc(got, cr.want, slices.Equal[[]string]) {
				t.Fatalf("%d parts before the crash and %d after, the uninterrupted run's %d; or their rows differ", len(done), len(rs.parts), len(cr.want))
			}
			t.Logf("crash at %s: %d parts kept, %d resumed over %d certificates", crash, len(done), len(rs.parts), len(cr.c.Spans()))
		})
	}
}
