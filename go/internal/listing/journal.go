// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
)

// Resume errors.
var (
	ErrNothingToResume = errors.New("nothing to resume: no interrupted run in the output directory")
	ErrComplete        = errors.New("the run in the output directory is already complete")
)

// journal ties a run of a resumable source to its durable journal. A nil
// *runJournal (no output, or a source that cannot resume) and one without j
// (-no-journal) publish parts only at the end.
type runJournal struct {
	out   string
	lock  *journal.Lock    // held from before Prepare until the run is over
	j     *journal.Journal // nil with -no-journal
	state *journal.State   // the recovered journal on resume
	src   source.Resumable
}

// openJournal prepares the output: a fresh journaled run (Prepare, then a
// new journal), a resume (identity check, recovery of the published
// parts), or no journal at all. A source that cannot resume gets a plain
// Prepare, without the lock.
func openJournal(cfg Config, src registry.Source, id journal.Identity) (*runJournal, error) {
	if cfg.Output.Dir == "" {
		if cfg.Output.Resume {
			return nil, errors.New("resume needs an output directory")
		}
		return nil, nil
	}
	rs, resumable := src.(source.Resumable)
	if c, ok := src.(source.Capable); !ok || !c.Capabilities().Resume {
		resumable = false
	}
	if !resumable {
		if cfg.Output.Resume {
			return nil, fmt.Errorf("resume: a %s source cannot resume", src.Locator().Kind)
		}
		if err := dataset.Prepare(cfg.Output.Dir, src.Locator().Path, id.SchemaID, cfg.Output.Replace); err != nil {
			return nil, fmt.Errorf("prepare output: %w", err)
		}
		return nil, nil
	}
	if !journal.Enforced && cfg.Warn != nil {
		cfg.Warn("the output lock and directory syncs are not enforced on this platform: a concurrent run or a power loss can damage the journal")
	}
	// Ownership first: nothing in the output is read, removed or renamed
	// before this process holds its lock.
	lock, err := journal.TakeLock(cfg.Output.Dir)
	if err != nil {
		return nil, err
	}
	jr, err := openLocked(cfg, src.Locator().Path, id, lock)
	if err != nil {
		lock.Release()
		return nil, err
	}
	jr.src = rs
	return jr, nil
}

// openLocked does openJournal's work with the lock held.
func openLocked(cfg Config, path string, id journal.Identity, lock *journal.Lock) (*runJournal, error) {
	if !cfg.Output.Resume {
		if err := dataset.Prepare(cfg.Output.Dir, path, id.SchemaID, cfg.Output.Replace); err != nil {
			if errors.Is(err, dataset.ErrNotEmpty) && dataset.Resumable(cfg.Output.Dir) {
				err = fmt.Errorf("%w (it holds an interrupted run: swath resume continues it)", err)
			}
			return nil, fmt.Errorf("prepare output: %w", err)
		}
		if cfg.Output.NoJournal {
			return &runJournal{out: cfg.Output.Dir, lock: lock}, nil
		}
		j, err := journal.Create(cfg.Output.Dir, id, cfg.Argv)
		if err != nil {
			return nil, err
		}
		return &runJournal{out: cfg.Output.Dir, lock: lock, j: j}, nil
	}
	if dataset.Committed(cfg.Output.Dir) {
		return nil, ErrComplete
	}
	st, err := journal.Read(cfg.Output.Dir)
	if errors.Is(err, journal.ErrNoJournal) {
		if !dataset.Resumable(cfg.Output.Dir) {
			return nil, ErrNothingToResume
		}
		// A journal file whose header never became durable: the run
		// published nothing, so it starts over.
		fresh := cfg
		fresh.Output.Resume, fresh.Output.Replace = false, true
		return openLocked(fresh, path, id, lock)
	}
	if err != nil {
		return nil, err
	}
	if err := journal.Check(st.Header.Identity, id); err != nil {
		return nil, err
	}
	if err := recoverParts(cfg.Output.Dir, st); err != nil {
		return nil, err
	}
	j, err := journal.Reopen(cfg.Output.Dir, st)
	if err != nil {
		return nil, err
	}
	return &runJournal{out: cfg.Output.Dir, lock: lock, j: j, state: st}, nil
}

// recoverParts makes data/ hold exactly the journaled parts: a part whose
// record is durable but whose rename did not happen is renamed now (its
// temporary file was synced before the record); every other file in data/
// (unrecorded temporary parts) is removed.
func recoverParts(out string, st *journal.State) error {
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
			journal.ErrCorrupt, i, len(st.Records), st.TornBytes)
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
			if _, err := dataset.PublishPart(out, i, dataset.Part{Path: path, PartInfo: rec.Part}); err != nil {
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
	// Unrecorded parts: the resume writes them again.
	parts := filepath.Join(out, dataset.Workspace, dataset.PartsDir)
	if err := os.RemoveAll(parts); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if err := os.MkdirAll(parts, 0o755); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	return dataset.SyncDir(filepath.Join(out, dataset.Workspace))
}

// ErrPartAltered reports a retained part that does not match its record.
var ErrPartAltered = errors.New("retained part does not match its journal record")

func verifyPart(path string, want dataset.PartInfo) error {
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

func (jr *runJournal) last() *journal.Record {
	if jr == nil || jr.state == nil {
		return nil
	}
	return jr.state.Last()
}

// complete reports that the final part is durable: the listing finished
// and a resume only has to publish.
func (jr *runJournal) complete() bool {
	last := jr.last()
	return last != nil && last.Final
}

// listingStarted is when the listing began: the journal's creation for a
// resumed run, else now.
func (jr *runJournal) listingStarted(now time.Time) time.Time {
	if jr != nil && jr.state != nil {
		return jr.state.Header.Created
	}
	return now
}

func (jr *runJournal) resumedParts() int {
	if jr == nil || jr.state == nil {
		return 0
	}
	return len(jr.state.Records)
}

func (jr *runJournal) resumedRows() int64 {
	if last := jr.last(); last != nil {
		return last.RowsTotal
	}
	return 0
}

// routerOptions makes the router publish each part as it closes (record,
// then rename) and continue after the recovered parts.
func (jr *runJournal) routerOptions(o *parquetout.Options) {
	if jr == nil || jr.j == nil {
		return
	}
	if last := jr.last(); last != nil {
		parts := make([]dataset.Part, len(jr.state.Records))
		for i, rec := range jr.state.Records {
			parts[i] = dataset.Part{Path: filepath.Join(jr.out, dataset.DataDir, dataset.PartName(i)), PartInfo: rec.Part}
		}
		o.Resume = &parquetout.Resume{Parts: parts, Limit: last.Limit, Rows: last.RowsTotal}
	}
	o.Publish = jr.publish
}

// publish is commit-before-emit at part granularity: the record (the part
// and the writer state) is synced before the part is renamed into the
// dataset. The router calls it for one part at a time, in ordinal order: it
// is the one ordered committer.
func (jr *runJournal) publish(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
	if err := faults.At("part-closed"); err != nil {
		return p, err
	}
	if err := jr.j.Append(journal.Record{Part: p.PartInfo, Ordinal: ordinal, Limit: limit, RowsTotal: rows, Final: final}); err != nil {
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
func (jr *runJournal) close() {
	if jr == nil {
		return
	}
	if jr.j != nil {
		_ = jr.j.Close()
		jr.j = nil
	}
	jr.lock.Release()
}

// after is the resume's last published key: every row must sort after it
// (the provider starts there; the pipeline checks it). nil for a fresh run.
func (jr *runJournal) after() *string {
	last := jr.last()
	if last == nil {
		return nil
	}
	k := string(last.Part.MaxKey)
	return &k
}

// journaled reports whether the run keeps a journal (and so leaves its
// temporary parts for -resume to reconcile after a failure).
func (jr *runJournal) journaled() bool { return jr != nil && jr.j != nil }
