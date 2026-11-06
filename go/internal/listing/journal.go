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
	// ErrInterrupted: the output holds an interrupted run (a fresh run was
	// asked for).
	ErrInterrupted     = errors.New("it holds an interrupted run")
	ErrNothingToResume = errors.New("nothing to resume: no interrupted run in the output directory")
	ErrComplete        = errors.New("the run in the output directory is already complete")
	// ErrAmbiguousTail: the journal's last record reads as torn, so its
	// part was set aside (DIR/.swath/suspect), and the source no longer
	// reproduces that part. The record may have been acknowledged and
	// damaged since; relisting would replace output the run committed.
	ErrAmbiguousTail = errors.New("the journal's last record is damaged or torn and the source no longer reproduces its part")
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
	// suspect is the part of a record that read as torn, kept until the
	// resume reproduces it byte for byte (nil when there is none).
	suspect *suspect
}

// suspect is a temporary part set aside by recovery because its record
// read as torn: an append that never completed reads the same as an
// acknowledged record truncated or zeroed since, when the part was not yet
// renamed. Nothing on disk tells them apart, so the resume must reproduce
// the part exactly before it records the ordinal again; any difference
// (or no part at that ordinal) refuses with the part kept.
type suspect struct {
	ordinal int
	path    string
	info    dataset.PartInfo // bytes and MD5 only
}

// openJournal takes ownership of the output and prepares it: a fresh
// journaled run (Prepare, then a new journal), a resume (identity check,
// recovery of the published parts), or no journal at all. Every output is
// owned through its lock, journaled or not; refusals that need no lock
// (an output that may not be used, nothing to resume) come first, so a
// refused output is left as it was.
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
	path := src.Locator().Path
	switch {
	case cfg.Output.Resume && !resumable:
		return nil, fmt.Errorf("resume: a %s source cannot resume", src.Locator().Kind)
	case cfg.Output.Resume && dataset.Committed(cfg.Output.Dir):
		return nil, ErrComplete
	case cfg.Output.Resume:
		if _, err := os.Stat(filepath.Join(cfg.Output.Dir, dataset.Workspace)); err != nil {
			return nil, ErrNothingToResume
		}
	default:
		if _, err := dataset.Usable(cfg.Output.Dir, path, id.SchemaID, cfg.Output.Replace); err != nil {
			return nil, prepareError(cfg.Output.Dir, err)
		}
	}
	if resumable && !journal.Enforced && cfg.Warn != nil {
		cfg.Warn("the output lock and directory syncs are not enforced on this platform: a concurrent run or a power loss can damage the journal")
	}
	// Ownership first: nothing in the output is read, removed or renamed
	// before this process holds its lock.
	lock, err := journal.TakeLock(cfg.Output.Dir)
	if err != nil {
		return nil, err
	}
	cfg.Progress.own(true)
	var jr *runJournal
	if resumable {
		jr, err = openLocked(cfg, path, id, lock)
	} else if err = dataset.Prepare(cfg.Output.Dir, path, id.SchemaID, cfg.Output.Replace); err != nil {
		err = prepareError(cfg.Output.Dir, err)
	} else {
		jr = &runJournal{out: cfg.Output.Dir, lock: lock}
	}
	if err != nil {
		// Still owned: the caller settles the failure, then releases.
		return &runJournal{out: cfg.Output.Dir, lock: lock}, err
	}
	jr.src = rs
	return jr, nil
}

// settleUnowned settles a run that failed before it owned its output, when
// the invocation meant to continue or replace an unfinished run there:
// that run's earlier outcome must not stand as the current one. It takes
// the lock first; an output another process owns, a committed dataset and
// a directory without a workspace are left as they are (the caller reports
// the outcome itself).
func settleUnowned(cfg Config, rep Report, err error) {
	dir := cfg.Output.Dir
	if cfg.Settle == nil || dir == "" || !(cfg.Output.Resume || cfg.Output.Replace) || dataset.Committed(dir) {
		return
	}
	if fi, serr := os.Stat(filepath.Join(dir, dataset.Workspace)); serr != nil || !fi.IsDir() {
		return
	}
	lock, lerr := journal.TakeLock(dir)
	if lerr != nil {
		return
	}
	(&runJournal{out: dir, lock: lock}).close(cfg, rep, err)
}

// prepareError names an interrupted run when the output refused is one.
func prepareError(out string, err error) error {
	if errors.Is(err, dataset.ErrNotEmpty) && dataset.Resumable(out) {
		err = fmt.Errorf("%w: %w", err, ErrInterrupted)
	}
	return fmt.Errorf("prepare output: %w", err)
}

// openLocked does openJournal's work with the lock held.
func openLocked(cfg Config, path string, id journal.Identity, lock *journal.Lock) (*runJournal, error) {
	if !cfg.Output.Resume {
		if err := dataset.Prepare(cfg.Output.Dir, path, id.SchemaID, cfg.Output.Replace); err != nil {
			return nil, prepareError(cfg.Output.Dir, err)
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
	sus, err := recoverParts(cfg.Output.Dir, st)
	if err != nil {
		return nil, err
	}
	j, err := journal.Reopen(cfg.Output.Dir, st)
	if err != nil {
		return nil, err
	}
	return &runJournal{out: cfg.Output.Dir, lock: lock, j: j, state: st, suspect: sus}, nil
}

// recoverParts makes data/ hold exactly the journaled parts: a part whose
// record is durable but whose rename did not happen is renamed now (its
// temporary file was synced before the record); every other file in data/
// (unrecorded temporary parts) is removed. The temporary part of a record
// that read as torn is set aside as the suspect, returned.
func recoverParts(out string, st *journal.State) (*suspect, error) {
	if err := checkParts(out, st); err != nil {
		return nil, err
	}
	sus, err := keepSuspect(out, st)
	if err != nil {
		return nil, err
	}
	// Unrecorded parts: the resume writes them again.
	parts := filepath.Join(out, dataset.Workspace, dataset.PartsDir)
	if err := os.RemoveAll(parts); err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	if err := os.MkdirAll(parts, 0o755); err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	return sus, dataset.SyncDir(filepath.Join(out, dataset.Workspace))
}

// keepSuspect sets aside the temporary part of a last record that read as
// torn (moved into the suspect directory before the temporary parts are
// removed), and returns the suspect a resume must reproduce: that one, or
// one an earlier resume set aside and has not reproduced yet. A suspect
// whose ordinal was recorded since was reproduced (the record follows the
// check) and is removed.
func keepSuspect(out string, st *journal.State) (*suspect, error) {
	dir := filepath.Join(out, dataset.Workspace, dataset.SuspectDir)
	n := len(st.Records)
	name := dataset.PartName(n)
	if tmp := dataset.TempPath(out, n); st.TornBytes > 0 {
		if _, err := os.Stat(tmp); err == nil {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
			if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
			if err := dataset.SyncDir(dir); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
			if err := dataset.SyncDir(filepath.Join(out, dataset.Workspace)); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
		}
	}
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	var sus *suspect
	for _, e := range ents {
		path := filepath.Join(dir, e.Name())
		switch {
		case e.Name() < name: // recorded since: reproduced
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
		case e.Name() == name && !(n > 0 && st.Records[n-1].Final):
			info, err := measure(path)
			if err != nil {
				return nil, fmt.Errorf("resume: suspect part: %w", err)
			}
			sus = &suspect{ordinal: n, path: path, info: info}
		default:
			return nil, fmt.Errorf("%w: %s holds %s but the journal holds %d records", journal.ErrCorrupt, dir, e.Name(), n)
		}
	}
	return sus, nil
}

// checkParts is recoverParts up to the removal of the temporary parts.
func checkParts(out string, st *journal.State) error {
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
	// The accepted frames are acted on below (a part is renamed because of
	// its record), so they must be durable first: a whole frame that
	// survived a crash may never have been synced.
	if err := journal.Sync(out); err != nil {
		return fmt.Errorf("resume: %w", err)
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
	// Every recorded part's final name, found or made, must be durable
	// before the temporary copies, the fallback, are removed.
	if err := dataset.SyncDir(data); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	return faults.At("recovered")
}

// ErrPartAltered reports a retained part that does not match its record.
var ErrPartAltered = errors.New("retained part does not match its journal record")

func verifyPart(path string, want dataset.PartInfo) error {
	got, err := measure(path)
	if err != nil {
		return err
	}
	if got.Bytes != want.Bytes || got.MD5 != want.MD5 {
		return fmt.Errorf("%w: %s is %d bytes, md5 %s; the journal says %d bytes, md5 %s", ErrPartAltered, path, got.Bytes, got.MD5, want.Bytes, want.MD5)
	}
	return nil
}

// measure is a file's size and MD5 (hex), as a PartInfo records them.
func measure(path string) (dataset.PartInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return dataset.PartInfo{}, err
	}
	defer f.Close()
	h := md5.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return dataset.PartInfo{}, err
	}
	return dataset.PartInfo{Bytes: n, MD5: hex.EncodeToString(h.Sum(nil))}, nil
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
	sus := jr.suspect
	if sus != nil && ordinal == sus.ordinal && (p.Bytes != sus.info.Bytes || p.MD5 != sus.info.MD5) {
		return p, fmt.Errorf("%w: part %d is %d bytes, md5 %s now; the part set aside (%s) is %d bytes, md5 %s",
			ErrAmbiguousTail, ordinal, p.Bytes, p.MD5, sus.path, sus.info.Bytes, sus.info.MD5)
	}
	if err := jr.j.Append(journal.Record{Part: p.PartInfo, Ordinal: ordinal, Limit: limit, RowsTotal: rows, Final: final}); err != nil {
		return p, err
	}
	if sus != nil && ordinal == sus.ordinal {
		// Reproduced and recorded: the copy set aside is redundant (a
		// later recovery removes it if this does not).
		_ = os.Remove(sus.path)
		jr.suspect = nil
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

// close closes the journal, lets the caller record the outcome while the
// output is still owned (Config.Settle), and releases the output lock; the
// run (with its cleanup and publication) is over.
func (jr *runJournal) close(cfg Config, rep Report, err error) {
	if jr == nil {
		return
	}
	if jr.j != nil {
		_ = jr.j.Close()
		jr.j = nil
	}
	if cfg.Settle != nil {
		cfg.Settle(rep, err)
	}
	cfg.Progress.own(false)
	jr.lock.Release()
}

// unresolved refuses a finished resume that never reproduced its suspect
// part: the source no longer holds a part at that ordinal.
func (jr *runJournal) unresolved() error {
	if jr == nil || jr.suspect == nil {
		return nil
	}
	return fmt.Errorf("%w: the listing ended before part %d (set aside at %s)", ErrAmbiguousTail, jr.suspect.ordinal, jr.suspect.path)
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
