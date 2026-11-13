// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortab

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"

	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3"
)

// Arm B is the run-based external sort of sortout (PR #248, 7d99970):
// rows accumulate in a buffer up to a byte bound, each full buffer is
// sorted (parallel chunk sort and pairwise merge, SortPtrs) and spilled to
// a sorted run while the next one fills (at most one run being written),
// and after listing the runs and the in-memory tail are k-way merged into
// the same part router and writer as arm A, parts published as they close
// and the manifest last.
//
// Durability is Java's, which sortout lacked: a run is synced, then a
// checkpoint (the runs and, per range, the last index in a durable run and
// whether the range is complete) is installed by an atomic rename. A
// resume keeps the checkpointed runs, deletes any other, relists only each
// unfinished range's non-durable tail, then merges. A crash during the
// merge reruns the merge from the runs (Java's finalization did the same).
// The buffer bound is half the row budget, so the filling buffer and the
// one being written together stay within the budget A gets; the runs hold
// every row and are not capped.

// checkpoint is B's durable state.
type checkpoint struct {
	Runs   []runFile     `json:"runs"`
	Cursor map[int]int64 `json:"cursor"` // range → last index in a durable run
	Done   map[int]bool  `json:"done"`   // ranges whose every row is in a durable run
}

type runFile struct {
	Name  string `json:"name"`
	Rows  int64  `json:"rows"`
	Bytes int64  `json:"bytes"`
}

const checkpointName = "checkpoint.json"

func loadCheckpoint(dir string) (*checkpoint, error) {
	ck := &checkpoint{Cursor: map[int]int64{}, Done: map[int]bool{}}
	b, err := os.ReadFile(filepath.Join(dir, checkpointName))
	if errors.Is(err, os.ErrNotExist) {
		return ck, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, ck); err != nil {
		return nil, err
	}
	return ck, nil
}

func saveCheckpoint(dir string, b []byte) error {
	return dataset.Replace(filepath.Join(dir, checkpointName), b, dataset.Durable)
}

// BStats are arm B's own measurements.
type BStats struct {
	Runs           int   `json:"runs"`             // runs at the merge (resumed included)
	RunsResumed    int   `json:"runs_resumed"`     // runs kept from the checkpoint
	RunRowsResumed int64 `json:"run_rows_resumed"` // rows in them
	RunBytesWrote  int64 `json:"run_bytes_written"`
	RunBytesRead   int64 `json:"run_bytes_read"`
	PeakStaging    int64 `json:"peak_staging_bytes"`  // live run bytes
	PeakBuffered   int64 `json:"peak_buffered_bytes"` // the filling buffer plus the one being spilled
	TailRows       int64 `json:"tail_rows"`           // rows merged from memory
	PeakNodes      int64 `json:"peak_nodes"`
	PeakReaders    int64 `json:"peak_readers"` // runs open in one merge (at most maxFanIn)
	Cascades       int   `json:"cascades"`     // intermediate merges (fan-in above maxFanIn)
	SealWaitNs     int64 `json:"seal_wait_ns"` // emitters waiting for the run being written
	SortNs         int64 `json:"sort_ns"`      // in-memory sorting, all buffers
	ListingMs      int64 `json:"listing_ms"`
	MergeMs        int64 `json:"merge_ms"` // merge, encode and publish
	EncoderWaitNs  int64 `json:"encoder_wait_ns"`
	FirstRunMs     int64 `json:"first_run_ms"` // first run durable (-1: none)
	Parts          int   `json:"parts"`
	Rows           int64 `json:"rows"`
	DatasetBytes   int64 `json:"dataset_bytes"`
}

type sorter struct {
	s     *schema.Schema[s3.Object]
	dir   string
	limit int64
	start time.Time

	mu       sync.Mutex
	cond     *sync.Cond
	buf      [][]row
	bufBytes int64
	// spillBytes are the rows of the buffer being spilled: live until its
	// run is written, beside the next filling buffer.
	spillBytes int64
	spillGate  chan struct{} // tests: a spill waits on it before it ends
	bufLast    map[int]int64 // range → last index in buf
	emitted    map[int]int64 // range → last index emitted
	returned   map[int]bool
	spilling   bool
	err        error
	ck         *checkpoint
	next       int // next run number
	live       int64
	st         *BStats
	wg         sync.WaitGroup
}

func (s *sorter) add(r int, rows []row) error {
	idx, err := indexOf(rows[len(rows)-1].Key)
	if err != nil {
		return err
	}
	bytes := s.s.RowsBytes(rows)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.buf = append(s.buf, rows)
	s.bufBytes += bytes
	s.st.PeakBuffered = max(s.st.PeakBuffered, s.bufBytes+s.spillBytes)
	s.bufLast[r], s.emitted[r] = idx, idx
	if s.bufBytes < s.limit {
		return nil
	}
	t := time.Now()
	for s.spilling && s.err == nil {
		s.cond.Wait()
	}
	s.st.SealWaitNs += int64(time.Since(t))
	if s.err != nil {
		return s.err
	}
	full, last := s.buf, s.bufLast
	s.spillBytes = s.bufBytes
	s.buf, s.bufBytes, s.bufLast = nil, 0, map[int]int64{}
	s.spilling = true
	s.wg.Add(1)
	go s.spill(full, last)
	return nil
}

func (s *sorter) rangeReturned(r int) {
	s.mu.Lock()
	s.returned[r] = true
	s.mu.Unlock()
}

// spill sorts and writes one buffer as a run, then checkpoints it.
func (s *sorter) spill(chunks [][]row, last map[int]int64) {
	defer s.wg.Done()
	t := time.Now()
	order := SortPtrs(pointers(chunks), runtime.GOMAXPROCS(0))
	sortNs := int64(time.Since(t))
	s.mu.Lock()
	name := fmt.Sprintf("run-%05d.bin", s.next)
	s.next++
	s.mu.Unlock()
	n, err := writeRun(filepath.Join(s.dir, name), order, s.s.Codec)
	if err == nil {
		err = dataset.SyncDir(s.dir)
	}
	var ck []byte
	s.mu.Lock()
	s.st.SortNs += sortNs
	s.st.RunBytesWrote += n
	if err == nil {
		s.live += n
		s.st.PeakStaging = max(s.st.PeakStaging, s.live)
		s.ck.Runs = append(s.ck.Runs, runFile{Name: name, Rows: int64(len(order)), Bytes: n})
		for r, i := range last {
			s.ck.Cursor[r] = i
		}
		s.markDone()
		ck, err = json.Marshal(s.ck)
	}
	s.mu.Unlock()
	if err == nil {
		// The run is durable once the checkpoint naming it is; the next
		// run waits for this (spilling), so checkpoints never race.
		err = saveCheckpoint(s.dir, ck)
	}
	if s.spillGate != nil {
		<-s.spillGate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.cond.Broadcast()
	s.spilling, s.spillBytes = false, 0
	if err != nil {
		s.err = err
		return
	}
	if s.st.FirstRunMs < 0 {
		s.st.FirstRunMs = time.Since(s.start).Milliseconds()
	}
}

// markDone marks the returned ranges whose last emitted row is durable.
func (s *sorter) markDone() {
	for r := range s.returned {
		if e, ok := s.emitted[r]; !ok || s.ck.Cursor[r] == e {
			s.ck.Done[r] = true
		}
	}
}

// bEmitter is one range's emitter in arm B: rows go to the sort buffer in
// arrival order, children to the work queue.
type bEmitter struct {
	s *sorter
	q *queue
	r int
}

func (e *bEmitter) Rows(rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	return e.s.add(e.r, rows)
}

func (e *bEmitter) Child(r Range) error { e.q.push(r); return nil }

// queue hands ranges to workers in key order, without any ordering of
// their output.
type queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []Range
	active int
	peak   int64
	stop   bool
}

func (q *queue) push(r Range) {
	q.mu.Lock()
	q.items = append(q.items, r)
	q.peak = max(q.peak, int64(len(q.items)+q.active))
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *queue) pop() (Range, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && q.active > 0 && !q.stop {
		q.cond.Wait()
	}
	if len(q.items) == 0 || q.stop {
		return Range{}, false
	}
	r := q.items[0]
	q.items = q.items[1:]
	q.active++
	return r, true
}

func (q *queue) finish() {
	q.mu.Lock()
	q.active--
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *queue) halt() {
	q.mu.Lock()
	q.stop = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// runB lists p into out with the external sort, staging runs in dir; a
// resume continues from dir's checkpoint. encoded and parts count rows
// encoded and parts published (progress).
func runB(ctx context.Context, p *Provider, out, dir string, lim Limits, resume bool, encoded, parts *atomic.Int64) (BStats, error) {
	start := time.Now()
	st := BStats{FirstRunMs: -1}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return st, err
	}
	ck := &checkpoint{Cursor: map[int]int64{}, Done: map[int]bool{}}
	if resume {
		var err error
		if ck, err = loadCheckpoint(dir); err != nil {
			return st, err
		}
	} else if err := os.Remove(filepath.Join(dir, checkpointName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return st, err
	}
	// Runs not in the checkpoint were not durable when it was written.
	keep := map[string]bool{checkpointName: true}
	next := 0
	var live int64
	for _, r := range ck.Runs {
		keep[r.Name] = true
		live += r.Bytes
		st.RunRowsResumed += r.Rows
		var k int
		if _, err := fmt.Sscanf(r.Name, "run-%05d.bin", &k); err == nil {
			next = max(next, k+1)
		}
	}
	st.RunsResumed = len(ck.Runs)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return st, err
	}
	for _, e := range ents {
		if !keep[e.Name()] {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return st, err
			}
		}
	}
	if resume {
		p.Start = func(r int, lo, hi int64) (int64, bool) {
			if ck.Done[r] {
				return 0, true
			}
			if c, ok := ck.Cursor[r]; ok {
				return c + 1, false
			}
			return lo, false
		}
	}
	s := &sorter{s: &s3.Schema, dir: dir, limit: max(1, lim.Memory/4), start: start, bufLast: map[int]int64{}, emitted: map[int]int64{},
		returned: map[int]bool{}, ck: ck, next: next, live: live, st: &st}
	s.cond = sync.NewCond(&s.mu)
	st.PeakStaging = live
	p.Done = s.rangeReturned

	// Listing: W workers, ranges in key order, rows in arrival order.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	q.active = 1 // the root
	if err := p.Expand(ctx, p.Root(), &bEmitter{s: s, q: q, r: -1}); err != nil {
		return st, err
	}
	q.finish()
	var wg sync.WaitGroup
	for range lim.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, ok := q.pop()
				if !ok {
					return
				}
				err := p.Expand(ctx, r, &bEmitter{s: s, q: q, r: r.R})
				q.finish()
				if err != nil {
					cancel(err)
					q.halt()
					return
				}
			}
		}()
	}
	wg.Wait()
	s.wg.Wait()
	st.PeakNodes = q.peak
	if err := context.Cause(ctx); err != nil {
		return st, err
	}
	if s.err != nil {
		return st, s.err
	}
	st.ListingMs = time.Since(start).Milliseconds()

	// Merge: the runs and the in-memory tail, into the router; first,
	// cascade the runs down to the fan-in (Java's cascade; sortout merged
	// every run at once).
	t := time.Now()
	if err := cascade(s, &st); err != nil {
		return st, err
	}
	tail := SortPtrs(pointers(s.buf), runtime.GOMAXPROCS(0))
	st.SortNs += int64(time.Since(t))
	st.TailRows = int64(len(tail))
	if err := os.RemoveAll(out); err != nil {
		return st, err
	}
	if err := dataset.Prepare(out, "", s3.Schema.ID, false); err != nil {
		return st, err
	}
	interval := time.Duration(0)
	switch {
	case lim.PartIntervalMs > 0:
		interval = msDuration(lim.PartIntervalMs)
	case lim.PartIntervalMs == 0:
		interval = 60 * time.Second // listing.DefaultPartInterval
	}
	var accounted atomic.Int64
	ropt := parquetout.Options{Out: out, PartSize: lim.PartSize, Encoders: lim.Encoders, Interval: interval, Encoded: encoded,
		Account: func(n int64) { accounted.Add(n) },
		Publish: func(ordinal int, part dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
			part, err := dataset.PublishPart(out, ordinal, part)
			if err == nil {
				parts.Add(1)
			}
			return part, err
		}}
	router, err := parquetout.NewRouter(ropt, &s3.Schema)
	if err != nil {
		return st, err
	}
	sources := []mergeSource{&memSource{order: tail}}
	var readers []*runReader
	defer func() {
		for _, rr := range readers {
			rr.Close()
		}
	}()
	for _, r := range ck.Runs {
		rr, err := openRun(filepath.Join(dir, r.Name), s.s.Codec, &st.RunBytesRead)
		if err != nil {
			router.Abort()
			return st, err
		}
		readers = append(readers, rr)
		sources = append(sources, rr)
	}
	st.Runs, st.PeakReaders = len(ck.Runs), max(st.PeakReaders, int64(len(readers)))
	next2 := mergeSources(sources)
	var n int64
	for {
		batch := make([]row, 0, frameRows)
		for len(batch) < frameRows {
			r, err := next2()
			if err != nil {
				router.Abort()
				return st, err
			}
			if r == nil {
				break
			}
			batch = append(batch, *r)
		}
		if len(batch) == 0 {
			break
		}
		n += int64(len(batch))
		if err := router.Write(ctx, [][]row{batch}); err != nil {
			router.Abort()
			return st, err
		}
	}
	if router.Routed() != n {
		router.Abort()
		return st, fmt.Errorf("router routed %d rows of %d", router.Routed(), n)
	}
	ps, err := router.Close()
	if err != nil {
		return st, err
	}
	st.EncoderWaitNs = int64(router.EncoderWait())
	now := time.Now()
	meta := dataset.Meta{SchemaID: s3.Schema.ID, Source: dataset.Source{Kind: "sortab", URI: "sortab://" + p.l.Name}, Scope: dataset.Scope{Kind: "whole"},
		Coverage: dataset.Coverage{Complete: true, StopReason: "completed", ListingStartedAt: start.UTC(), ListingFinishedAt: now.UTC()}}
	if err := dataset.Publish(out, meta, ps, nil); err != nil {
		return st, err
	}
	st.MergeMs = time.Since(t).Milliseconds()
	st.Parts, st.Rows = len(ps), n
	for _, part := range ps {
		st.DatasetBytes += part.Bytes
	}
	// Committed: the staging goes.
	for _, rr := range readers {
		rr.Close()
	}
	readers = nil
	return st, os.RemoveAll(dir)
}

var _ source.Emitter[Range, s3.Object] = (*bEmitter)(nil)

// maxFanIn bounds the runs one merge reads at once (each an open file, a
// 64 KiB buffer and one decoded frame of up to frameRows rows).
var maxFanIn = 64

// cascade merges the oldest maxFanIn runs into one until at most
// maxFanIn-1 remain (the tail is the merge's last input). Each merged run
// is synced and checkpointed before its inputs are removed, so a crash
// keeps either the inputs or the merged run.
func cascade(s *sorter, st *BStats) error {
	for len(s.ck.Runs) > maxFanIn-1 {
		in := s.ck.Runs[:maxFanIn]
		var readers []*runReader
		closeAll := func() {
			for _, rr := range readers {
				rr.Close()
			}
		}
		var srcs []mergeSource
		for _, r := range in {
			rr, err := openRun(filepath.Join(s.dir, r.Name), s.s.Codec, &st.RunBytesRead)
			if err != nil {
				closeAll()
				return err
			}
			readers = append(readers, rr)
			srcs = append(srcs, rr)
		}
		st.PeakReaders = max(st.PeakReaders, int64(len(readers)))
		name := fmt.Sprintf("run-%05d.bin", s.next)
		s.next++
		rw, err := createRun(filepath.Join(s.dir, name), s.s.Codec)
		if err != nil {
			closeAll()
			return err
		}
		next := mergeSources(srcs)
		for {
			r, err := next()
			if err == nil && r != nil {
				err = rw.add(r)
			}
			if err != nil {
				rw.f.Close()
				closeAll()
				return err
			}
			if r == nil {
				break
			}
		}
		closeAll()
		if err := rw.close(); err != nil {
			return err
		}
		if err := dataset.SyncDir(s.dir); err != nil {
			return err
		}
		st.RunBytesWrote += rw.n
		st.Cascades++
		var inBytes int64
		for _, r := range in {
			inBytes += r.Bytes
		}
		s.live += rw.n
		st.PeakStaging = max(st.PeakStaging, s.live)
		s.ck.Runs = append([]runFile{{Name: name, Rows: rw.rows, Bytes: rw.n}}, s.ck.Runs[maxFanIn:]...)
		b, err := json.Marshal(s.ck)
		if err != nil {
			return err
		}
		if err := saveCheckpoint(s.dir, b); err != nil {
			return err
		}
		for _, r := range in {
			if err := os.Remove(filepath.Join(s.dir, r.Name)); err != nil {
				return err
			}
		}
		s.live -= inBytes
	}
	return nil
}

func msDuration(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// From sortout (7d99970), for schema.Row.

func pointers(chunks [][]row) []*row {
	n := 0
	for _, c := range chunks {
		n += len(c)
	}
	ptrs := make([]*row, 0, n)
	for _, c := range chunks {
		for i := range c {
			ptrs = append(ptrs, &c[i])
		}
	}
	return ptrs
}

// SortPtrs sorts row pointers by unsigned key bytes: P chunks are sorted
// concurrently, then merged pairwise in parallel. It may return a different
// slice than its input.
func SortPtrs(order []*row, workers int) []*row {
	n := len(order)
	cmp := func(a, b *row) int { return strings.Compare(a.Key, b.Key) }
	if workers < 1 || n < 1<<14 {
		workers = 1
	}
	bounds := make([]int, workers+1)
	for i := range bounds {
		bounds[i] = i * n / workers
	}
	var wg sync.WaitGroup
	for c := 0; c < workers; c++ {
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			slices.SortFunc(order[lo:hi], cmp)
		}(bounds[c], bounds[c+1])
	}
	wg.Wait()
	if workers == 1 {
		return order
	}
	tmp := make([]*row, n)
	for len(bounds) > 2 {
		var nb []int
		for i := 0; i+2 < len(bounds); i += 2 {
			lo, mid, hi := bounds[i], bounds[i+1], bounds[i+2]
			wg.Add(1)
			go func() {
				defer wg.Done()
				mergeInto(tmp[lo:hi], order[lo:mid], order[mid:hi], cmp)
			}()
			nb = append(nb, lo)
		}
		if (len(bounds)-1)%2 == 1 { // odd chunk carried over
			lo := bounds[len(bounds)-2]
			copy(tmp[lo:], order[lo:])
			nb = append(nb, lo)
		}
		nb = append(nb, n)
		wg.Wait()
		order, tmp = tmp, order
		bounds = nb
	}
	return order
}

func mergeInto(dst, a, b []*row, cmp func(a, b *row) int) {
	i, j, k := 0, 0, 0
	for i < len(a) && j < len(b) {
		if cmp(a[i], b[j]) <= 0 {
			dst[k] = a[i]
			i++
		} else {
			dst[k] = b[j]
			j++
		}
		k++
	}
	k += copy(dst[k:], a[i:])
	copy(dst[k:], b[j:])
}

// mergeSource is one sorted input of the k-way merge.
type mergeSource interface {
	peek() *row // nil when exhausted
	advance() error
}

type memSource struct {
	order []*row
	i     int
}

func (m *memSource) peek() *row {
	if m.i >= len(m.order) {
		return nil
	}
	return m.order[m.i]
}
func (m *memSource) advance() error { m.i++; return nil }

type srcHeap []mergeSource

func (h srcHeap) Len() int           { return len(h) }
func (h srcHeap) Less(i, j int) bool { return h[i].peek().Key < h[j].peek().Key }
func (h srcHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *srcHeap) Push(x any)        { *h = append(*h, x.(mergeSource)) }
func (h *srcHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// mergeSources yields rows in key order. A key equal to the previous one
// is an error: the sort must never hide a duplicate.
func mergeSources(sources []mergeSource) func() (*row, error) {
	h := &srcHeap{}
	for _, s := range sources {
		if s.peek() != nil {
			*h = append(*h, s)
		}
	}
	heap.Init(h)
	var cur row
	first := true
	return func() (*row, error) {
		if h.Len() == 0 {
			return nil, nil
		}
		top := (*h)[0]
		prev := cur.Key
		cur = *top.peek()
		if !first && cur.Key <= prev {
			return nil, fmt.Errorf("merge: key %q after %q", cur.Key, prev)
		}
		first = false
		if err := top.advance(); err != nil {
			return nil, err
		}
		if top.peek() == nil {
			heap.Pop(h)
		} else {
			heap.Fix(h, 0)
		}
		return &cur, nil
	}
}
