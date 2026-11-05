// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/durable"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquet"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/s3"
)

// Resume errors.
var (
	ErrNothingToResume = errors.New("nothing to resume: no interrupted run in the output directory")
	ErrComplete        = errors.New("the run in the output directory is already complete")
)

// journal ties an S3 run to its durable journal. A nil *journal (no output,
// or -no-journal) publishes parts only at the end, as before.
type journal struct {
	out      string
	lock     *durable.Lock    // held from before Prepare until the run is over
	j        *durable.Journal // nil with -no-journal
	state    *durable.State   // the recovered journal on resume
	provider *s3.Provider
}

// openJournal prepares the output: a fresh journaled run (Prepare, then a
// new journal), a resume (identity check, recovery of the published
// parts), or no journal at all.
func openJournal(cfg Config, id durable.Identity) (*journal, error) {
	if cfg.Out == "" {
		if cfg.Resume {
			return nil, errors.New("-resume needs -out")
		}
		return nil, nil
	}
	// Ownership first: nothing in the output is read, removed or renamed
	// before this process holds its lock.
	lock, err := durable.TakeLock(cfg.Out)
	if err != nil {
		return nil, err
	}
	jr, err := openLocked(cfg, id, lock)
	if err != nil {
		lock.Release()
		return nil, err
	}
	return jr, nil
}

// openLocked does openJournal's work with the lock held.
func openLocked(cfg Config, id durable.Identity, lock *durable.Lock) (*journal, error) {
	if !cfg.Resume {
		if err := dataset.Prepare(cfg.Out, "", id.SchemaID, cfg.Replace); err != nil {
			if errors.Is(err, dataset.ErrNotEmpty) && dataset.Resumable(cfg.Out) {
				err = fmt.Errorf("%w (it holds an interrupted run: pass -resume to continue it)", err)
			}
			return nil, fmt.Errorf("prepare output: %w", err)
		}
		if cfg.S3.NoJournal {
			return &journal{out: cfg.Out, lock: lock}, nil
		}
		j, err := durable.Create(cfg.Out, id)
		if err != nil {
			return nil, err
		}
		return &journal{out: cfg.Out, lock: lock, j: j}, nil
	}
	if _, err := os.Stat(filepath.Join(cfg.Out, dataset.SuccessName)); err == nil {
		return nil, ErrComplete
	}
	st, err := durable.Read(cfg.Out)
	if errors.Is(err, durable.ErrNoJournal) {
		if !dataset.Resumable(cfg.Out) {
			return nil, ErrNothingToResume
		}
		// A journal file whose header never became durable: the run
		// published nothing, so it starts over.
		fresh := cfg
		fresh.Resume, fresh.Replace = false, true
		return openLocked(fresh, id, lock)
	}
	if err != nil {
		return nil, err
	}
	if err := durable.Check(st.Header.Identity, id); err != nil {
		return nil, err
	}
	if err := recoverParts(cfg.Out, st); err != nil {
		return nil, err
	}
	j, err := durable.Reopen(cfg.Out, st)
	if err != nil {
		return nil, err
	}
	return &journal{out: cfg.Out, lock: lock, j: j, state: st}, nil
}

// recoverParts makes data/ hold exactly the journaled parts: a part whose
// record is durable but whose rename did not happen is renamed now (its
// temporary file was synced before the record); every other file in data/
// (unrecorded temporary parts) is removed.
func recoverParts(out string, st *durable.State) error {
	data := filepath.Join(out, dataset.DataDir)
	// Refuse ambiguous damage before deleting anything: a part published
	// under its final name beyond the recorded ones means an acknowledged
	// record was lost (a damaged frame length, or bit rot in the last
	// frame), not a torn append, which is never followed by a rename.
	for i := len(st.Records); ; i++ {
		if _, err := os.Stat(filepath.Join(data, dataset.PartName(i))); err != nil {
			break
		}
		return fmt.Errorf("%w: part %d is published but the journal holds only %d records (%d damaged bytes); refusing to recover",
			durable.ErrCorrupt, i, len(st.Records), st.TornBytes)
	}
	keep := map[string]bool{}
	for i, rec := range st.Records {
		final := filepath.Join(data, dataset.PartName(i))
		keep[filepath.Base(final)] = true
		path := final
		if _, err := os.Stat(final); err != nil {
			path = dataset.TempPath(out, i)
			if _, err := os.Stat(path); err != nil || i != len(st.Records)-1 {
				return fmt.Errorf("resume: part %d is journaled but missing from %s", i, data)
			}
		}
		// Trust no retained part by existence: its size and digest must be
		// the journal's before the frontier moves past it.
		if err := verifyPart(path, rec.Part); err != nil {
			return fmt.Errorf("resume: part %d: %w", i, err)
		}
		if path != final {
			if _, err := dataset.PublishPart(out, i, rec.Part); err != nil {
				return fmt.Errorf("resume: %w", err)
			}
		}
	}
	ents, err := os.ReadDir(data)
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	for _, e := range ents {
		if !keep[e.Name()] {
			if err := os.Remove(filepath.Join(data, e.Name())); err != nil {
				return fmt.Errorf("resume: %w", err)
			}
		}
	}
	return nil
}

// ErrPartAltered reports a retained part that does not match its record.
var ErrPartAltered = errors.New("retained part does not match its journal record")

func verifyPart(path string, want dataset.Part) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := md5.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return err
	}
	if n != want.Bytes || hex.EncodeToString(h.Sum(nil)) != want.MD5 {
		return fmt.Errorf("%w: %s is %d bytes, md5 %x; the journal says %d bytes, md5 %s", ErrPartAltered, path, n, h.Sum(nil), want.Bytes, want.MD5)
	}
	return nil
}

func (jr *journal) last() *durable.Record {
	if jr == nil || jr.state == nil {
		return nil
	}
	return jr.state.Last()
}

// maxBoundsPerRecord caps the boundaries one record carries, so the
// journal grows with parts, not with splits.
const maxBoundsPerRecord = 4096

// complete reports that the final part is durable: the listing finished
// and a resume only has to publish.
func (jr *journal) complete() bool {
	last := jr.last()
	return last != nil && last.Final
}

func (jr *journal) providerResume() *s3.Resume {
	if last := jr.last(); last != nil {
		return &s3.Resume{After: last.Part.MaxKey, Boundaries: jr.state.Boundaries}
	}
	return nil
}

func (jr *journal) resumedParts() int {
	if jr == nil || jr.state == nil {
		return 0
	}
	return len(jr.state.Records)
}

func (jr *journal) resumedRows() int64 {
	if last := jr.last(); last != nil {
		return last.RowsTotal
	}
	return 0
}

// routerOptions makes the router publish each part as it closes (record,
// then rename) and continue after the recovered parts.
func (jr *journal) routerOptions(o *parquet.Options) {
	if jr == nil || jr.j == nil {
		return
	}
	if last := jr.last(); last != nil {
		parts := make([]dataset.Part, len(jr.state.Records))
		for i, rec := range jr.state.Records {
			parts[i] = rec.Part
			parts[i].Path = filepath.Join(jr.out, dataset.DataDir, dataset.PartName(i))
		}
		o.Resume = &parquet.Resume{Parts: parts, Limit: last.Limit, Rows: last.RowsTotal}
	}
	o.Publish = jr.publish
}

// publish is commit-before-emit at part granularity: the record (the part,
// the frozen limit, the boundaries since the last record) is synced before
// the part is renamed into the dataset.
func (jr *journal) publish(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
	if err := faults.At("part-closed"); err != nil {
		return p, err
	}
	var bounds []string
	if jr.provider != nil {
		bounds = jr.provider.TakeBoundaries()
	}
	if len(bounds) > maxBoundsPerRecord { // bounded growth: boundaries only shape the resume tiling
		bounds = bounds[len(bounds)-maxBoundsPerRecord:]
	}
	if err := jr.j.Append(durable.Record{Part: p, Ordinal: ordinal, Limit: limit, RowsTotal: rows, Boundaries: bounds, Final: final}); err != nil {
		return p, err
	}
	if err := faults.At("recorded"); err != nil {
		return p, err
	}
	p, err := dataset.PublishPart(jr.out, ordinal, p)
	if err != nil {
		return p, err
	}
	return p, faults.At("renamed")
}

// close closes the journal and releases the output lock; the run (with
// its cleanup and publication) is over.
func (jr *journal) close() {
	if jr == nil {
		return
	}
	if jr.j != nil {
		_ = jr.j.Close()
		jr.j = nil
	}
	jr.lock.Release()
}

// guard wraps the sink on resume: every row must sort after the last
// published key (the provider starts there; this checks it).
func (jr *journal) guard(sink order.Sink) order.Sink {
	last := jr.last()
	if last == nil {
		return sink
	}
	return afterSink{sink, last.Part.MaxKey}
}

type afterSink struct {
	order.Sink
	after string
}

func (a afterSink) Write(ctx context.Context, segs [][]schema.Row) error {
	for _, seg := range segs {
		if len(seg) > 0 && seg[0].Key <= a.after {
			return fmt.Errorf("resume: key %q is not after the last published key %q", seg[0].Key, a.after)
		}
	}
	return a.Sink.Write(ctx, segs)
}

// journaled reports whether the run keeps a journal (and so leaves its
// temporary parts for -resume to reconcile after a failure).
func (jr *journal) journaled() bool { return jr != nil && jr.j != nil }
