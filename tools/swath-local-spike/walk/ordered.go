// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package walk

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// Order modes.
const (
	OrderNone          = "none"           // parallel unordered walk; the caller sorts
	OrderDFSSequential = "dfs-sequential" // one goroutine, sorted depth-first emission
	OrderDFSParallel   = "dfs-parallel"   // parallel reads, reorder buffer, sorted emission
)

// Sorted depth-first emission yields globally sorted keys without a sort:
// within a directory, entries are ordered by their key bytes where a
// subdirectory's key is prefix+name+"/" (exactly its DIR row key) and a
// non-directory's is prefix+name. Every key in a subdirectory's subtree
// starts with prefix+name+"/", and a sibling that sorts after that string
// cannot share it as a prefix (names contain no '/'), so it sorts after the
// whole subtree. Emitting the DIR row and then recursing before the next
// sibling therefore produces strictly ascending unsigned-byte keys.

// kid places a subdirectory's subtree after rows[pos] (its DIR row).
type kid struct {
	pos  int32
	node *dirNode
}

// dirNode is one directory: its rows in sorted order (one slab) and the
// positions where child subtrees splice in. Splicing is by index: no row is
// copied when the drainer walks the tree.
type dirNode struct {
	job   dirJob
	rows  []record.Row
	kids  []kid
	state int8 // 0 queued, 1 claimed (being read), 2 done
}

// SegmentSink is an optional Sink extension for ordered walks: it receives
// contiguous runs of rows (sub-slices of per-directory slabs) in key order,
// without copying. It takes ownership of the slices; slabs are never reused.
type SegmentSink interface {
	EmitSegments(segs [][]record.Row)
}

// readNode reads one directory into sorted rows plus child splice points.
func readNode(w *worker, read dirReader, job dirJob) ([]record.Row, []kid) {
	w.stats.DirsRead++
	if fastReaderAvailable(w.opt) {
		if rows, kids, ok := readFast(w, job, statxMaskFor(w.opt)); ok {
			return rows, kids
		}
	}
	// Generic path (other engines, or DT_UNKNOWN entries): read via the
	// engine's reader, then sort rows and pair DIR rows with child jobs.
	w.batch = make([]record.Row, 0, 64)
	w.children = w.children[:0]
	read(w, job)
	rows := w.batch
	w.batch = nil
	jobs := make(map[string]dirJob, len(w.children))
	for _, c := range w.children {
		jobs[c.prefix] = c
	}
	slices.SortFunc(rows, func(a, b record.Row) int { return strings.Compare(a.Key, b.Key) })
	var kids []kid
	for i := range rows {
		if rows[i].Kind == record.KindDir {
			c, ok := jobs[rows[i].Key]
			if !ok {
				panic(fmt.Sprintf("directory pairing broken: %q", rows[i].Key))
			}
			kids = append(kids, kid{pos: int32(i), node: &dirNode{job: c}})
		}
	}
	return rows, kids
}

// orderedEmitter hands rows to the sink in emission order, as zero-copy
// segments when the sink supports it, batched to >= batchRows per call.
type orderedEmitter struct {
	sink    Sink
	seg     SegmentSink
	segs    [][]record.Row
	segRows int
	batch   []record.Row
	size    int
}

func newEmitter(sink Sink) *orderedEmitter {
	e := &orderedEmitter{sink: sink, size: 4096}
	e.seg, _ = sink.(SegmentSink)
	if e.seg == nil {
		e.batch = make([]record.Row, 0, e.size)
	}
	return e
}

func (e *orderedEmitter) emit(rows []record.Row) {
	if len(rows) == 0 {
		return
	}
	if e.seg != nil {
		e.segs = append(e.segs, rows)
		e.segRows += len(rows)
		if e.segRows >= e.size {
			e.flush()
		}
		return
	}
	for len(rows) > 0 {
		n := min(len(rows), e.size-len(e.batch))
		e.batch = append(e.batch, rows[:n]...)
		rows = rows[n:]
		if len(e.batch) >= e.size {
			e.flush()
		}
	}
}

func (e *orderedEmitter) flush() {
	if e.seg != nil {
		if len(e.segs) > 0 {
			e.seg.EmitSegments(e.segs)
			e.segs, e.segRows = nil, 0
		}
		return
	}
	if len(e.batch) > 0 {
		next := e.sink.Emit(e.batch)
		if next == nil {
			next = make([]record.Row, 0, e.size)
		}
		e.batch = next[:0]
	}
}

// drainNode emits n's rows with child subtrees spliced in, using visit for
// each child. It returns the number of rows consumed from n's slab.
func drainNode(n *dirNode, noDirs bool, em *orderedEmitter, visit func(*dirNode)) int {
	i := 0
	for _, k := range n.kids {
		p := int(k.pos)
		if noDirs {
			em.emit(n.rows[i:p])
		} else {
			em.emit(n.rows[i : p+1])
		}
		i = p + 1
		visit(k.node)
	}
	em.emit(n.rows[i:])
	return len(n.rows)
}

// WalkOrdered lists root emitting rows to sink in strictly ascending key
// order from a single goroutine. reorderRows bounds dfs-parallel buffering.
func WalkOrdered(root, mode string, opt Options, reorderRows int, sink Sink) (Stats, error) {
	if opt.Engine == EngineWalkDir {
		return Stats{}, fmt.Errorf("--order %s does not support engine walkdir", mode)
	}
	userNoDirs := opt.NoDirs
	opt.NoDirs = false   // DIR rows are needed to pair children; dropped at emission
	opt.SplitEntries = 0 // stat batches would bypass per-directory assembly
	opt.BatchRows = math.MaxInt
	if opt.Workers <= 0 {
		opt.Workers = DefaultWorkers()
	}
	if opt.Engine == "" {
		opt.Engine = defaultEngine
	}
	read, err := readerFor(opt)
	if err != nil {
		return Stats{}, err
	}
	if _, err := statRoot(root); err != nil {
		return Stats{}, err
	}
	em := newEmitter(sink)
	rootJob := dirJob{path: cleanRoot(root), prefix: ""}
	var st Stats
	switch mode {
	case OrderDFSSequential:
		w := &worker{opt: &opt}
		var visit func(n *dirNode)
		visit = func(n *dirNode) {
			n.rows, n.kids = readNode(w, read, n.job)
			drainNode(n, userNoDirs, em, visit)
			n.rows, n.kids = nil, nil
		}
		visit(&dirNode{job: rootJob})
		st = w.stats
	case OrderDFSParallel:
		if reorderRows <= 0 {
			reorderRows = 2_000_000
		}
		st = dfsParallel(rootJob, &opt, read, reorderRows, userNoDirs, em)
	default:
		return Stats{}, fmt.Errorf("unknown order %q", mode)
	}
	em.flush()
	return st, nil
}

// dfsParallel: workers read directories from a LIFO stack (children pushed in
// reverse sorted order so the next-needed directory is on top); the calling
// goroutine drains the tree depth-first and emits. Completed-but-early nodes
// are held until the drain reaches them. Workers stop taking new directories
// while buffered rows >= bound; the drainer reads a directory it needs itself
// if no worker has claimed it, so the bound cannot deadlock the walk.
//
// Wakeups are targeted: a completing worker wakes the drainer only if the
// drainer waits for that node, and wakes workers only when it pushed
// children; the drainer releases buffered rows in batches of >= 4096 and wakes
// workers only when that takes the buffer from at/over the bound to under it.
func dfsParallel(rootJob dirJob, opt *Options, read dirReader, bound int, noDirs bool, em *orderedEmitter) Stats {
	var mu sync.Mutex
	workCond := sync.NewCond(&mu)  // workers: stack empty or bound reached
	drainCond := sync.NewCond(&mu) // drainer: awaited node not done
	root := &dirNode{job: rootJob}
	stack := []*dirNode{root}
	buffered, maxBuffered := 0, 0
	var awaited *dirNode
	finished := false

	complete := func(n *dirNode, rows []record.Row, kids []kid) { // mu held
		n.rows, n.kids, n.state = rows, kids, 2
		buffered += len(rows)
		maxBuffered = max(maxBuffered, buffered)
		for i := len(kids) - 1; i >= 0; i-- {
			stack = append(stack, kids[i].node)
		}
		if n == awaited {
			drainCond.Signal()
		}
		if len(kids) > 0 && buffered < bound {
			workCond.Broadcast() // new work for idle workers
		}
	}

	workers := make([]*worker, opt.Workers)
	var wg sync.WaitGroup
	for i := range workers {
		w := &worker{opt: opt}
		workers[i] = w
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			for {
				for !finished && (len(stack) == 0 || buffered >= bound) {
					workCond.Wait()
				}
				if finished {
					break
				}
				n := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if n.state != 0 { // claimed by the drainer meanwhile
					continue
				}
				n.state = 1
				mu.Unlock()
				rows, kids := readNode(w, read, n.job)
				mu.Lock()
				complete(n, rows, kids)
			}
			mu.Unlock()
		}()
	}

	drainer := &worker{opt: opt}
	released := 0
	releaseEvery := max(1, min(4096, bound/16))
	releaseLocked := func() { // mu held
		wasFull := buffered >= bound
		buffered -= released
		released = 0
		if wasFull && buffered < bound {
			workCond.Broadcast()
		}
	}
	release := func(force bool) {
		if released == 0 || (!force && released < releaseEvery) {
			return
		}
		mu.Lock()
		releaseLocked()
		mu.Unlock()
	}
	var visit func(n *dirNode)
	visit = func(n *dirNode) {
		mu.Lock()
		for n.state != 2 {
			if n.state == 0 { // nobody has it: read it here
				n.state = 1
				mu.Unlock()
				rows, kids := readNode(drainer, read, n.job)
				mu.Lock()
				complete(n, rows, kids)
				break
			}
			if released > 0 {
				releaseLocked() // let workers proceed while we wait
			}
			awaited = n
			drainCond.Wait()
		}
		awaited = nil
		mu.Unlock()
		released += drainNode(n, noDirs, em, visit)
		n.rows, n.kids = nil, nil
		release(false)
	}
	visit(root)
	release(true)
	mu.Lock()
	finished = true
	workCond.Broadcast()
	mu.Unlock()
	wg.Wait()
	var total Stats
	total.add(&drainer.stats)
	for _, w := range workers {
		total.add(&w.stats)
	}
	total.ReorderMaxBuffered = int64(maxBuffered)
	return total
}
