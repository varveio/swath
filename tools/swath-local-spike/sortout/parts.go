// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortout

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// Encode schemes.
const (
	SchemePart   = "part"   // N goroutines, each writing whole parts serially (opt-in)
	SchemeColumn = "column" // one part at a time, columns of each page encoded in parallel (default)
)

// Per-part parallel encoding (after Swath's Java PartEncoders/PartSizer):
//
//   - The router alone fixes part boundaries, deterministically, from each
//     row's uncompressed logical size (logicalBytes). Part 0 is cut at
//     PartSize logical bytes and encoded first; the ratio encoded/logical of
//     part 0 is then frozen and every later part is cut at PartSize/ratio
//     logical bytes. Nothing is re-estimated, so boundaries do not depend on
//     timing or on the encoder count.
//   - Ordinals are dense (0..n-1). A full part is dispatched only once the
//     next row exists, so the router knows which part is last and sets
//     swath.sort.file_final itself.
//   - Part 0 is encoded with column-parallel pages (byte-identical to serial,
//     tested) so a one-part dataset still uses all cores. Parts 1.. go to N
//     encoder goroutines, each with its own serial parquet-go writer and its
//     own zstd encoder (concurrency 1, nothing shared).
//   - At most N parts are queued plus N being encoded plus 1 accumulating, so
//     rows retained by the writer are bounded by (2N+1) parts.
//   - checkParts gates publication: dense ordinals, strictly ascending
//     disjoint key ranges, rows written == rows routed.

// logicalBytes is the deterministic uncompressed size estimate of a row.
func logicalBytes(r *record.Row) int64 {
	n := int64(len(r.Key)+len(r.LinkTarget)+len(r.Kind.String())) + 1
	for _, p := range []record.Presence{record.HasSize, record.HasMtime, record.HasInode, record.HasDev, record.HasBlocks, record.HasCtime, record.HasAtime, record.HasBtime} {
		if r.Has(p) {
			n += 8
		}
	}
	for _, p := range []record.Presence{record.HasMode, record.HasUID, record.HasGID, record.HasNlink} {
		if r.Has(p) {
			n += 4
		}
	}
	return n
}

type partJob struct {
	ordinal int
	rows    []*record.Row
	final   bool
}

// partRouter cuts an ascending row stream into parts and runs the encoders.
type partRouter struct {
	opt     Options
	n       int
	jobs    chan partJob
	wg      sync.WaitGroup
	mu      sync.Mutex
	results map[int]pqout.PartInfo
	err     error

	cur        []*record.Row
	curLogical int64
	full       bool
	ordinal    int
	ratio      float64 // frozen after part 0
	limit      int64   // logical bytes per part after calibration
	rowsIn     int64
	maxQueued  int
}

func newPartRouter(opt Options) *partRouter {
	n := max(1, opt.Writer.Encoders)
	p := &partRouter{opt: opt, n: n, jobs: make(chan partJob, n), results: map[int]pqout.PartInfo{}}
	for i := 0; i < n; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for job := range p.jobs {
				info, err := p.encode(job, 1)
				p.record(job.ordinal, info, err)
			}
		}()
	}
	return p
}

func (p *partRouter) record(ordinal int, info pqout.PartInfo, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		if p.err == nil {
			p.err = err
		}
		return
	}
	p.results[ordinal] = info
}

// encode writes one whole part with its own writer and codec.
func (p *partRouter) encode(job partJob, columnEncoders int) (pqout.PartInfo, error) {
	cfg := p.opt.Writer
	cfg.Encoders = columnEncoders
	path := filepath.Join(p.opt.Out, "data", PartName(job.ordinal)+".tmp")
	w, err := pqout.Create(path, cfg, true, job.ordinal+1)
	if err != nil {
		return pqout.PartInfo{}, err
	}
	for _, r := range job.rows {
		if err := w.Write(r); err != nil {
			w.Abort()
			return pqout.PartInfo{}, err
		}
	}
	return w.Close(job.final)
}

// add routes one row; r must stay valid and unmodified until finish.
func (p *partRouter) add(r *record.Row) error {
	if p.full {
		if err := p.dispatch(false); err != nil {
			return err
		}
	}
	p.cur = append(p.cur, r)
	p.rowsIn++
	p.curLogical += logicalBytes(r)
	target := p.opt.PartSize
	if p.ratio > 0 {
		target = p.limit
	}
	if p.curLogical >= target {
		p.full = true // cut here once another row arrives
	}
	return nil
}

func (p *partRouter) dispatch(final bool) error {
	job := partJob{ordinal: p.ordinal, rows: p.cur, final: final}
	logical := p.curLogical
	p.ordinal++
	p.cur, p.curLogical, p.full = nil, 0, false
	if job.ordinal == 0 {
		// Calibration part: encoded now, column-parallel (same bytes as serial).
		info, err := p.encode(job, p.n)
		p.record(0, info, err)
		if err != nil {
			return err
		}
		if !final {
			p.ratio = float64(info.Bytes) / float64(max(1, logical))
			p.limit = max(1, int64(float64(p.opt.PartSize)/p.ratio))
		}
		return nil
	}
	p.mu.Lock()
	err := p.err
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.maxQueued = max(p.maxQueued, len(p.jobs)+1)
	p.jobs <- job
	return nil
}

// finish dispatches the last part (stamped final), waits for the encoders
// and returns the parts in ordinal order after the publication gate.
func (p *partRouter) finish() ([]pqout.PartInfo, error) {
	var err error
	if len(p.cur) > 0 {
		err = p.dispatch(true)
	}
	close(p.jobs)
	p.wg.Wait()
	if err != nil {
		return nil, err
	}
	if p.err != nil {
		return nil, p.err
	}
	parts := make([]pqout.PartInfo, p.ordinal)
	for i := range parts {
		info, ok := p.results[i]
		if !ok {
			return nil, fmt.Errorf("part ordinal %d missing", i)
		}
		parts[i] = info
	}
	return parts, checkParts(parts, p.rowsIn)
}

// abort stops the encoders (error path); unpublished files are removed by CleanupFailed.
func (p *partRouter) abort() {
	defer func() { recover() }() // jobs may already be closed
	close(p.jobs)
	p.wg.Wait()
}

// checkParts is the pre-publication gate (before the first rename).
func checkParts(parts []pqout.PartInfo, rowsIn int64) error {
	var rows int64
	for i, part := range parts {
		if filepath.Base(part.Path) != PartName(i)+".tmp" {
			return fmt.Errorf("part ordinals not dense: index %d holds %s", i, part.Path)
		}
		if part.Rows == 0 || part.MinKey > part.MaxKey {
			return fmt.Errorf("part %d: empty or inverted key range", i)
		}
		if i > 0 && !(parts[i-1].MaxKey < part.MinKey) {
			return fmt.Errorf("parts %d and %d overlap: %q !< %q", i-1, i, parts[i-1].MaxKey, part.MinKey)
		}
		rows += part.Rows
	}
	if rows != rowsIn {
		return fmt.Errorf("row count mismatch: routed %d, written %d", rowsIn, rows)
	}
	return nil
}
