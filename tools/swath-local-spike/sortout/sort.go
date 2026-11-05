// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sortout turns an unordered stream of rows into a globally key-sorted
// Parquet dataset with a run-based external sort: rows accumulate up to a
// row/byte bound, each full buffer is sorted and spilled to a temporary
// Parquet run under _staging/, and all runs (plus the in-memory tail) are
// k-way merged into byte-targeted final parts. One run skips the merge.
package sortout

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
)

type Options struct {
	Out             string
	SortBufferRows  int   // spill when the buffer holds this many rows
	SortBufferBytes int64 // or this many estimated bytes (0 = rows only)
	PartSize        int64 // final part byte target (soft, checked at page boundaries)
	Writer          pqout.Config
	SortWorkers     int    // goroutines for in-memory sorting (default GOMAXPROCS)
	EncodeScheme    string // SchemeColumn (default) or SchemePart; Writer.Encoders = N
}

func (o Options) partScheme() bool { return o.EncodeScheme == SchemePart }

// Stats describe the sort and write phases.
type Stats struct {
	Rows         int64         `json:"rows"`
	Runs         int           `json:"spill_runs"`
	SpillTime    time.Duration `json:"spill_ns"`      // cumulative, overlaps the walk
	SpillWait    time.Duration `json:"spill_wait_ns"` // walker time blocked on a busy spiller
	FinalSort    time.Duration `json:"final_sort_ns"`
	MergeWrite   time.Duration `json:"merge_write_ns"`
	Merged       bool          `json:"merged"`
	Parts        int           `json:"parts"`
	DatasetBytes int64         `json:"dataset_bytes"`
	PeakBufRows  int           `json:"peak_buffer_rows"`
}

// Sorter is a walk.Sink. Emit is safe for concurrent use.
type Sorter struct {
	opt      Options
	staging  string
	mu       sync.Mutex
	chunks   [][]record.Row // worker batches, owned (never copied)
	bufRows  int
	bufBytes int64
	spillCh  chan [][]record.Row
	spillWG  sync.WaitGroup
	runs     []string
	errMu    sync.Mutex
	err      error
	stats    Stats
}

func NewSorter(opt Options) (*Sorter, error) {
	if opt.SortBufferRows <= 0 {
		return nil, errors.New("sort buffer rows must be positive")
	}
	if opt.SortWorkers <= 0 {
		opt.SortWorkers = runtime.GOMAXPROCS(0)
	}
	s := &Sorter{opt: opt, staging: filepath.Join(opt.Out, "_staging"), spillCh: make(chan [][]record.Row, 1)}
	s.spillWG.Add(1)
	go s.spiller()
	return s, nil
}

func (s *Sorter) fail(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

// Emit takes ownership of a worker batch (no copy; the worker allocates a
// fresh one) and hands a full buffer to the spiller (backpressure: at most
// one buffer waits while one is spilling).
func (s *Sorter) Emit(rows []record.Row) []record.Row {
	var bytes int64
	if s.opt.SortBufferBytes > 0 {
		for i := range rows {
			bytes += rows[i].EstimatedBytes()
		}
	}
	s.mu.Lock()
	s.chunks = append(s.chunks, rows)
	s.bufRows += len(rows)
	s.bufBytes += bytes
	var full [][]record.Row
	if s.bufRows >= s.opt.SortBufferRows || (s.opt.SortBufferBytes > 0 && s.bufBytes >= s.opt.SortBufferBytes) {
		full = s.chunks
		s.stats.PeakBufRows = max(s.stats.PeakBufRows, s.bufRows)
		s.chunks, s.bufRows, s.bufBytes = nil, 0, 0
	}
	s.mu.Unlock()
	if full != nil {
		t := time.Now()
		s.spillCh <- full
		s.mu.Lock()
		s.stats.SpillWait += time.Since(t)
		s.mu.Unlock()
	}
	return nil
}

func (s *Sorter) spiller() {
	defer s.spillWG.Done()
	for rows := range s.spillCh {
		t := time.Now()
		if err := s.spill(rows); err != nil {
			s.fail(err)
		}
		s.mu.Lock()
		s.stats.SpillTime += time.Since(t)
		s.mu.Unlock()
	}
}

func (s *Sorter) spill(chunks [][]record.Row) error {
	if err := os.MkdirAll(s.staging, 0o755); err != nil {
		return err
	}
	order := SortPtrs(pointers(chunks), s.opt.SortWorkers)
	path := filepath.Join(s.staging, fmt.Sprintf("run-%05d.parquet", len(s.runs)))
	cfg := s.opt.Writer
	cfg.Fsync = false // internal scratch, never published
	w, err := pqout.Create(path, cfg, false, 0)
	if err != nil {
		return err
	}
	for _, r := range order {
		if err := w.Write(r); err != nil {
			w.Abort()
			return err
		}
	}
	if _, err := w.Close(false); err != nil {
		return err
	}
	s.runs = append(s.runs, path)
	return nil
}

// Finish sorts/merges everything into data/part-NNNNN.parquet.tmp files
// (renamed by Publish) and returns their metadata in key order.
func (s *Sorter) Finish() (parts []pqout.PartInfo, st Stats, err error) {
	close(s.spillCh)
	s.spillWG.Wait()
	var readers []*pqout.Reader
	defer func() {
		for _, rd := range readers {
			rd.Close()
		}
		if err != nil {
			// Nothing is published without _SUCCESS; drop the scratch too.
			CleanupFailed(s.opt.Out)
			parts = nil
		}
	}()
	if s.err != nil {
		return nil, s.stats, s.err
	}
	tail := s.chunks
	s.chunks = nil
	s.stats.PeakBufRows = max(s.stats.PeakBufRows, s.bufRows)
	s.stats.Runs = len(s.runs)
	t := time.Now()
	order := SortPtrs(pointers(tail), s.opt.SortWorkers)
	s.stats.FinalSort = time.Since(t)

	t = time.Now()
	sources := []source{&memSource{order: order}}
	for _, p := range s.runs {
		rd, err := pqout.Open(p)
		if err != nil {
			return nil, s.stats, err
		}
		readers = append(readers, rd)
		fs := &fileSource{rd: rd, buf: make([]record.Row, 0, 1024)}
		if err := fs.fill(); err != nil {
			return nil, s.stats, err
		}
		sources = append(sources, fs)
	}
	s.stats.Merged = len(s.runs) > 0
	if s.opt.partScheme() {
		parts, err = routeParts(s.opt, order, len(s.runs) == 0, mergeSources(sources))
	} else {
		parts, err = writeParts(s.opt, mergeSources(sources))
		if err == nil {
			var n int64
			for _, p := range parts {
				n += p.Rows
			}
			err = checkParts(parts, n)
		}
	}
	s.stats.MergeWrite = time.Since(t)
	if err != nil {
		return nil, s.stats, err
	}
	for _, rd := range readers {
		rd.Close()
	}
	readers = nil
	if err := os.RemoveAll(s.staging); err != nil {
		return nil, s.stats, err
	}
	s.stats.Parts = len(parts)
	for _, p := range parts {
		s.stats.Rows += p.Rows
		s.stats.DatasetBytes += p.Bytes
	}
	return parts, s.stats, nil
}

// PartName is Swath's sorted final naming: dense, zero-based, %05d.
func PartName(i int) string { return fmt.Sprintf("part-%05d.parquet", i) }

// roller writes ascending rows into byte-targeted parts. Rolls happen at
// page boundaries once the writer's estimate reaches PartSize; file_final is
// stamped only on the last part (known at finish).
type roller struct {
	opt   Options
	parts []pqout.PartInfo
	w     *pqout.Writer
}

func (p *roller) add(r *record.Row) error {
	var err error
	if p.w != nil && p.w.Rows()%int64(p.opt.Writer.PageRows) == 0 && p.w.EstimatedBytes() >= p.opt.PartSize {
		info, err := p.w.Close(false)
		p.w = nil
		if err != nil {
			return err
		}
		p.parts = append(p.parts, info)
	}
	if p.w == nil {
		path := filepath.Join(p.opt.Out, "data", PartName(len(p.parts))+".tmp")
		if p.w, err = pqout.Create(path, p.opt.Writer, true, len(p.parts)+1); err != nil {
			return err
		}
	}
	if err := p.w.Write(r); err != nil {
		p.w.Abort()
		p.w = nil
		return err
	}
	return nil
}

func (p *roller) finish() ([]pqout.PartInfo, error) {
	if p.w != nil {
		info, err := p.w.Close(true)
		p.w = nil
		if err != nil {
			return nil, err
		}
		p.parts = append(p.parts, info)
	}
	return p.parts, nil
}

func (p *roller) abort() {
	if p.w != nil {
		p.w.Abort()
		p.w = nil
	}
}

// routeParts feeds the part router: in-memory rows by pointer (no copy), or
// merged rows (whose iterator reuses one row) copied into 4096-row slabs.
func routeParts(opt Options, order []*record.Row, inMemory bool, next func() (*record.Row, error)) ([]pqout.PartInfo, error) {
	p := newPartRouter(opt)
	if inMemory {
		for _, r := range order {
			if err := p.add(r); err != nil {
				p.abort()
				return nil, err
			}
		}
		return p.finish()
	}
	var slab []record.Row
	for {
		r, err := next()
		if err != nil {
			p.abort()
			return nil, err
		}
		if r == nil {
			return p.finish()
		}
		if len(slab) == cap(slab) {
			slab = make([]record.Row, 0, 4096)
		}
		slab = append(slab, *r)
		if err := p.add(&slab[len(slab)-1]); err != nil {
			p.abort()
			return nil, err
		}
	}
}

// writeParts streams sorted rows from next into parts.
func writeParts(opt Options, next func() (*record.Row, error)) ([]pqout.PartInfo, error) {
	p := &roller{opt: opt}
	for {
		r, err := next()
		if err != nil {
			p.abort()
			return nil, err
		}
		if r == nil {
			return p.finish()
		}
		if err := p.add(r); err != nil {
			p.abort()
			return nil, err
		}
	}
}

// Streamer is a walk.Sink for walks that already emit keys in ascending
// order (--order dfs-*): rows go straight to the part writer, no sort, no
// spill. The writer still rejects any key that is not strictly ascending.
// Emit must be called from one goroutine; encoding runs on a separate
// goroutine fed through a small bounded channel (pipelining walk and encode).
type Streamer struct {
	p    roller
	pr   *partRouter // part scheme
	ch   chan [][]record.Row
	done chan struct{}
	err  error
}

func NewStreamer(opt Options) *Streamer {
	s := &Streamer{p: roller{opt: opt}, ch: make(chan [][]record.Row, 8), done: make(chan struct{})}
	add := s.p.add
	if opt.partScheme() {
		s.pr = newPartRouter(opt)
		add = s.pr.add
	}
	go func() {
		defer close(s.done)
		for segs := range s.ch {
			for _, rows := range segs {
				for i := range rows {
					if s.err != nil {
						break
					}
					s.err = add(&rows[i])
				}
			}
		}
	}()
	return s
}

// Emit takes ownership of rows.
func (s *Streamer) Emit(rows []record.Row) []record.Row {
	s.ch <- [][]record.Row{rows}
	return nil
}

// EmitSegments takes ownership of ascending row segments (walk.SegmentSink):
// rows are read in place by the encoder goroutine, never copied in between.
func (s *Streamer) EmitSegments(segs [][]record.Row) { s.ch <- segs }

// Finish closes the last part (stamped final) and returns all parts.
func (s *Streamer) Finish() ([]pqout.PartInfo, Stats, error) {
	close(s.ch)
	<-s.done
	if s.err != nil {
		s.p.abort()
		if s.pr != nil {
			s.pr.abort()
		}
		CleanupFailed(s.p.opt.Out)
		return nil, Stats{}, s.err
	}
	t := time.Now()
	var parts []pqout.PartInfo
	var err error
	if s.pr != nil {
		parts, err = s.pr.finish()
	} else {
		parts, err = s.p.finish()
		if err == nil {
			var n int64
			for _, p := range parts {
				n += p.Rows
			}
			err = checkParts(parts, n)
		}
	}
	if err != nil {
		CleanupFailed(s.p.opt.Out)
		return nil, Stats{}, err
	}
	st := Stats{MergeWrite: time.Since(t), Parts: len(parts)}
	for _, p := range parts {
		st.Rows += p.Rows
		st.DatasetBytes += p.Bytes
	}
	return parts, st, err
}

func pointers(chunks [][]record.Row) []*record.Row {
	n := 0
	for _, c := range chunks {
		n += len(c)
	}
	ptrs := make([]*record.Row, 0, n)
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
func SortPtrs(order []*record.Row, workers int) []*record.Row {
	n := len(order)
	cmp := func(a, b *record.Row) int { return strings.Compare(a.Key, b.Key) }
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
	tmp := make([]*record.Row, n)
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

func mergeInto(dst, a, b []*record.Row, cmp func(a, b *record.Row) int) {
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

// source is one sorted input of the k-way merge.
type source interface {
	peek() *record.Row // nil when exhausted
	advance() error
}

type memSource struct {
	order []*record.Row
	i     int
}

func (m *memSource) peek() *record.Row {
	if m.i >= len(m.order) {
		return nil
	}
	return m.order[m.i]
}
func (m *memSource) advance() error { m.i++; return nil }

type fileSource struct {
	rd  *pqout.Reader
	buf []record.Row
	i   int
	eof bool
}

func (f *fileSource) fill() error {
	var err error
	f.buf, err = f.rd.Read(f.buf)
	f.i = 0
	if errors.Is(err, io.EOF) {
		f.eof = len(f.buf) == 0
		return nil
	}
	return err
}

func (f *fileSource) peek() *record.Row {
	if f.i >= len(f.buf) {
		return nil
	}
	return &f.buf[f.i]
}

func (f *fileSource) advance() error {
	f.i++
	if f.i >= len(f.buf) && !f.eof {
		return f.fill()
	}
	return nil
}

type srcHeap []source

func (h srcHeap) Len() int           { return len(h) }
func (h srcHeap) Less(i, j int) bool { return h[i].peek().Key < h[j].peek().Key }
func (h srcHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *srcHeap) Push(x any)        { *h = append(*h, x.(source)) }
func (h *srcHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// mergeSources returns an iterator yielding rows in key order. The returned
// row is valid until the next call (copied by the writer).
func mergeSources(sources []source) func() (*record.Row, error) {
	h := &srcHeap{}
	for _, s := range sources {
		if s.peek() != nil {
			*h = append(*h, s)
		}
	}
	heap.Init(h)
	var cur record.Row
	return func() (*record.Row, error) {
		if h.Len() == 0 {
			return nil, nil
		}
		top := (*h)[0]
		cur = *top.peek()
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
