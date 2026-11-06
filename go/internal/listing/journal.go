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
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
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
	// ErrUnenforced: a journaled or resumed run where directory syncs are
	// not available (Windows): after a power loss the journal or a
	// published part's name may be missing although its record was
	// acknowledged.
	ErrUnenforced = errors.New("journaled output is not supported on this platform: directories cannot be synced here, so a power loss could lose acknowledged parts; use --no-journal (not resumable) or run on Linux or macOS")
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
	// acked counts this invocation's parts the journal acknowledged (the
	// append returned): durable whether or not the rename followed.
	acked struct {
		parts, bytes, rows atomic.Int64
	}
	// root is the locked output directory, opened once its identity was
	// verified: part files are created and renamed relative to it, never
	// by a pathname that may name another directory by then.
	root *os.Root
}

// rooted opens jr.root (once, before the router starts) and checks the
// identity after: the root is then the directory the lock is on.
func (jr *runJournal) rooted() (*os.Root, error) {
	if jr.root != nil {
		return jr.root, nil
	}
	r, err := os.OpenRoot(jr.out)
	if err != nil {
		return nil, err
	}
	if err := jr.lock.Verify(); err != nil {
		r.Close()
		return nil, err
	}
	jr.root = r
	return r, nil
}

// rel is path relative to the output (a path the run made under jr.out).
func (jr *runJournal) rel(path string) string {
	r, err := filepath.Rel(jr.out, path)
	if err != nil {
		return path
	}
	return r
}

// rename renames within the locked output directory.
func (jr *runJournal) rename(old, new string) error {
	if jr.root == nil {
		return os.Rename(old, new)
	}
	return jr.root.Rename(jr.rel(old), jr.rel(new))
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
	rel     string           // path relative to the output root
	info    dataset.PartInfo // bytes and MD5 only
	// rows is the claimed part's row count (its footer; 0 if unreadable):
	// the resume cuts the reproduced part there (parquetout.Options.Cut),
	// since its first cut may have been the part interval's, which a
	// resume's timing does not repeat.
	rows int64
}

// openJournal takes ownership of the output and prepares it: a fresh
// journaled run (Prepare, then a new journal), a resume (identity check,
// recovery of the published parts), or no journal at all. Every output is
// owned through its lock, journaled or not; refusals that need no lock
// (an output that may not be used, nothing to resume) come first, so a
// refused output is left as it was.
func openJournal(ctx context.Context, cfg Config, src registry.Source, id journal.Identity) (*runJournal, error) {
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
	case !dataset.DirSyncEnforced && (cfg.Output.Resume || (resumable && !cfg.Output.NoJournal)):
		return nil, ErrUnenforced
	case cfg.Output.Resume && !resumable:
		return nil, fmt.Errorf("resume: a %s source cannot resume", src.Locator().Kind)
	case cfg.Output.Resume && dataset.Committed(cfg.Output.Dir):
		return nil, ErrComplete
	case cfg.Output.Resume:
		// An unfinished run has a journal file (a workspace alone, empty
		// or holding only the lock, is nothing to resume, and is left as
		// it is: no lock is created).
		if !dataset.Resumable(cfg.Output.Dir) {
			return nil, ErrNothingToResume
		}
	default:
		if _, err := dataset.Usable(cfg.Output.Dir, path, id.SchemaID, cfg.Output.Replace); err != nil {
			return nil, prepareError(cfg.Output.Dir, err)
		}
	}
	if !journal.LockEnforced && cfg.Warn != nil {
		cfg.Warn("the output lock is not enforced on this platform: nothing stops a second run from writing the same output directory")
	}
	// Ownership first: nothing in the output is read, removed or renamed
	// before this process holds its lock.
	lock, err := journal.TakeLock(cfg.Output.Dir)
	if err != nil {
		return nil, err
	}
	cfg.Progress.own(lock)
	var jr *runJournal
	if resumable {
		jr, err = openLocked(ctx, cfg, path, id, lock)
	} else if err = lock.Verify(); err != nil {
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
// that run's earlier outcome must not stand as the current one
// (SettleUnfinished, writing through cfg.Settle). seen is the run's
// journal.Witness when the invocation began ("" when there was no
// unfinished run). It returns why it did not settle ("" when it did, or
// when there is no output or no settlement to make).
func settleUnowned(cfg Config, seen string, rep Report, err error) string {
	if cfg.Settle == nil || cfg.Output.Dir == "" {
		return ""
	}
	return SettleUnfinished(cfg.Output.Dir, seen, func() error {
		cfg.Settle(rep, err)
		return nil
	})
}

// SettleUnfinished is the one early settlement, for the listing and the
// command alike: an invocation that stopped before it owned dir records
// its outcome (write) over the unfinished run it saw there (seen, that
// run's journal.Witness), so that run's earlier outcome never stands as
// the current one. It takes the output lock first and writes only if,
// under it, the dataset is still uncommitted, the witness unchanged and the
// lock's path still the directory; a committed dataset, a directory
// without a run (nothing is created there, not even the lock), an output
// another process owns, and a run another process completed, replaced or
// continued since are left as they are. It returns why it did not record
// the outcome, never empty then; "" when write ran and succeeded.
func SettleUnfinished(dir, seen string, write func() error) (unrecorded string) {
	switch {
	case dir == "":
		return UnrecordedNoOutput
	case dataset.Committed(dir):
		return unrecordedComplete
	case seen == "":
		return unrecordedNoRun
	}
	if err := faults.At("settle-lock"); err != nil {
		return err.Error()
	}
	lock, err := journal.TakeLock(dir)
	if err != nil {
		return err.Error()
	}
	defer lock.Release()
	if dataset.Committed(dir) || journal.Witness(dir) != seen {
		return UnrecordedChanged
	}
	if err := lock.Verify(); err != nil {
		return err.Error()
	}
	if err := write(); err != nil {
		return err.Error()
	}
	return ""
}

// Reasons an invocation's outcome is not in the output's outcome file
// (Outcome.Unrecorded) when no settlement was attempted.
const (
	// UnrecordedChanged: the run this invocation saw was completed,
	// replaced or continued by another process before it held the lock.
	UnrecordedChanged  = "another process completed or replaced the run before this one held the lock"
	unrecordedComplete = "the dataset in the output is complete"
	unrecordedNoRun    = "the output holds no unfinished run this invocation continues or replaces"
	unrecordedLocked   = "another process owns the output"
	// UnrecordedComplete, UnrecordedNoRun and UnrecordedNoOutput, for the
	// command's own refusals before a run.
	UnrecordedComplete = unrecordedComplete
	UnrecordedNoRun    = unrecordedNoRun
	UnrecordedNoOutput = "the invocation has no output directory"
	unrecordedRefused  = "refused before owning the output, which is left as it was"
)

// unowned is why an invocation whose output could not be opened (err, from
// openJournal; jr nil when the lock was never taken) did not settle.
func unowned(jr *runJournal, err error) string {
	switch {
	case errors.Is(err, ErrComplete):
		return unrecordedComplete
	case errors.Is(err, ErrNothingToResume):
		return unrecordedNoRun
	case errors.Is(err, journal.ErrLocked):
		return unrecordedLocked
	case jr == nil:
		return unrecordedRefused
	}
	return ""
}

// prepareError names an interrupted run when the output refused is one.
func prepareError(out string, err error) error {
	if errors.Is(err, dataset.ErrNotEmpty) && dataset.Resumable(out) {
		err = fmt.Errorf("%w: %w", err, ErrInterrupted)
	}
	return fmt.Errorf("prepare output: %w", err)
}

// openLocked does openJournal's work with the lock held.
func openLocked(ctx context.Context, cfg Config, path string, id journal.Identity, lock *journal.Lock) (*runJournal, error) {
	if err := lock.Verify(); err != nil {
		return nil, err
	}
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
		return openLocked(ctx, fresh, path, id, lock)
	}
	if err != nil {
		return nil, err
	}
	if err := st.Resumable(); err != nil {
		return nil, err
	}
	if err := journal.Check(st.Header.Identity, id); err != nil {
		return nil, err
	}
	// Recovery acts through the output root, opened and then checked to be
	// the directory the lock is on (as runJournal.rooted); the run keeps it.
	r, err := os.OpenRoot(cfg.Output.Dir)
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	if err := lock.Verify(); err != nil {
		r.Close()
		return nil, err
	}
	sus, err := recoverParts(verifier{ctx, cfg.Progress}, r, cfg.Output.Dir, st, lock)
	if err != nil {
		r.Close()
		return nil, err
	}
	j, err := journal.Reopen(cfg.Output.Dir, st)
	if err != nil {
		r.Close()
		return nil, err
	}
	return &runJournal{out: cfg.Output.Dir, lock: lock, j: j, state: st, suspect: sus, root: r}, nil
}

// ErrUnsafeLayout: a path resume recovery reads, renames or removes is not
// what swath made there: data/, the workspace, its parts or suspect
// directory is a symbolic link or not a directory, or a retained or claimed
// part is a symbolic link or not a regular file. Recovery checks every such
// path without following links before it changes anything, and refuses
// (exit 1, class fatal, not resumable) with the output left as it was; the
// operator inspects it, or starts over with --overwrite. The refusal is a
// *LayoutError.
var ErrUnsafeLayout = errors.New("the output's layout is not one swath made")

// LayoutError is a refusal by ErrUnsafeLayout: Path, relative to the
// output, is Got where recovery needs Want.
type LayoutError struct {
	Path, Want, Got string
}

func (e *LayoutError) Error() string {
	return fmt.Sprintf("resume: %s is %s, not %s; refusing to recover: %v", e.Path, e.Got, e.Want, ErrUnsafeLayout)
}

func (e *LayoutError) Unwrap() error { return ErrUnsafeLayout }

// kind names a file mode for a LayoutError.
func kind(m fs.FileMode) string {
	switch {
	case m&fs.ModeSymlink != 0:
		return "a symbolic link"
	case m.IsDir():
		return "a directory"
	case m.IsRegular():
		return "a regular file"
	}
	return "a special file (" + m.Type().String() + ")"
}

// need checks rel (relative to r) without following a link: a directory
// when dir, else a regular file. A missing path is nil, nil when optional.
func need(r *os.Root, rel string, dir, optional bool) (fs.FileInfo, error) {
	fi, err := r.Lstat(rel)
	switch {
	case optional && errors.Is(err, fs.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("resume: %w", err)
	case dir && !fi.Mode().IsDir():
		return nil, &LayoutError{Path: rel, Want: "a directory", Got: kind(fi.Mode())}
	case !dir && !fi.Mode().IsRegular():
		return nil, &LayoutError{Path: rel, Want: "a regular file", Got: kind(fi.Mode())}
	}
	return fi, nil
}

// openPart opens part file rel through r, refusing anything but the regular
// file need found there (a link swapped in between the two is refused too).
func openPart(r *os.Root, rel string) (*os.File, error) {
	fi, err := need(r, rel, false, false)
	if err != nil {
		return nil, err
	}
	f, err := r.Open(rel)
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	if got, err := f.Stat(); err != nil || !os.SameFile(fi, got) {
		f.Close()
		return nil, &LayoutError{Path: rel, Want: "the regular file found there", Got: "another file once opened"}
	}
	return f, nil
}

// Paths recovery acts on, relative to the output root.
var (
	partsRel   = filepath.Join(dataset.Workspace, dataset.PartsDir)
	suspectRel = filepath.Join(dataset.Workspace, dataset.SuspectDir)
)

func partRel(i int) string  { return filepath.Join(dataset.DataDir, dataset.PartName(i)) }
func claimRel(i int) string { return filepath.Join(partsRel, dataset.PartName(i)+dataset.ClaimSuffix) }

// checkLayout is recovery's first step, before anything is renamed or
// removed: every directory it acts in is a directory, and every part it may
// keep, rename or set aside is a regular file, by lstat (a link is refused
// whatever it names). This closes the case of a link already in place when
// the resume starts (an interrupted output whose data/ was replaced by a
// link to an unrelated directory, whose files cleanup would otherwise
// remove). It does not close a replacement between this check and a later
// action; recovery then acts through r, the output root, so a link that
// leaves the output is refused by the root, and a link inside it is the
// pathname-replacement window durability.md documents.
func checkLayout(r *os.Root, st *journal.State) error {
	for _, d := range []struct {
		rel      string
		optional bool
	}{{dataset.Workspace, false}, {dataset.DataDir, false}, {partsRel, true}, {suspectRel, true}} {
		if _, err := need(r, d.rel, true, d.optional); err != nil {
			return err
		}
	}
	n := len(st.Records)
	for i := range n {
		fi, err := need(r, partRel(i), false, true)
		if err != nil {
			return err
		}
		if fi == nil && i == n-1 {
			if _, err := need(r, claimRel(i), false, true); err != nil {
				return err
			}
		}
	}
	_, err := need(r, claimRel(n), false, true)
	return err
}

// renameIn renames within r the paths under out (rename's arguments are
// pathnames the run made under out).
func renameIn(r *os.Root, out string) func(old, new string) error {
	return func(old, new string) error {
		o, err1 := filepath.Rel(out, old)
		n, err2 := filepath.Rel(out, new)
		if err := errors.Join(err1, err2); err != nil {
			return err
		}
		return r.Rename(o, n)
	}
}

// recoverParts makes data/ hold exactly the journaled parts: a part whose
// record is durable but whose rename did not happen is renamed now (its
// temporary file was synced before the record); every other file in data/
// (unrecorded temporary parts) is removed. The temporary part of a record
// that read as torn is set aside as the suspect, returned. r is the output
// root (out, opened after the lock was verified): recovery reads, renames
// and removes only through it, after checkLayout.
func recoverParts(v verifier, r *os.Root, out string, st *journal.State, lock *journal.Lock) (*suspect, error) {
	if err := checkLayout(r, st); err != nil {
		return nil, err
	}
	if err := checkParts(v, r, out, st, lock); err != nil {
		return nil, err
	}
	if err := lock.Verify(); err != nil {
		return nil, err
	}
	sus, err := keepSuspect(v, r, out, st)
	if err != nil {
		return nil, err
	}
	if err := lock.Verify(); err != nil {
		return nil, err
	}
	// Unrecorded parts: the resume writes them again.
	if err := r.RemoveAll(partsRel); err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	if err := r.MkdirAll(partsRel, 0o755); err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	return sus, dataset.SyncDirIn(r, dataset.Workspace)
}

// keepSuspect sets aside the claimed part of the ordinal after the last
// record (moved into the suspect directory before the temporary parts are
// removed), and returns the suspect a resume must reproduce: that one, or
// one an earlier resume set aside and has not reproduced yet. A claimed
// part is set aside whether or not the journal ends in a torn tail: a
// record acknowledged and then lost whole (the journal cut at the frame
// boundary before it) leaves a clean end, the same as an append that never
// began. Parts not claimed were never named by a record and are removed
// with the rest. A suspect whose ordinal was recorded since was reproduced
// (the record follows the check) and is removed. Suspects are compared by
// ordinal (dataset.PartOrdinal), never by name: part-99999 precedes
// part-100000; a name of another shape is corruption.
func keepSuspect(v verifier, r *os.Root, out string, st *journal.State) (*suspect, error) {
	dir := filepath.Join(out, suspectRel)
	n := len(st.Records)
	final := n > 0 && st.Records[n-1].Final
	name := dataset.PartName(n)
	_, held := r.Lstat(filepath.Join(suspectRel, name))
	if tmp := claimRel(n); !final && held != nil {
		// A suspect an earlier resume set aside stays: a later temporary
		// part at its ordinal was never compared with it (it crashed
		// before its record) and must not replace it.
		if _, err := r.Lstat(tmp); err == nil {
			if err := r.MkdirAll(suspectRel, 0o755); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
			if err := r.Rename(tmp, filepath.Join(suspectRel, name)); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
			if err := dataset.SyncDirIn(r, suspectRel); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
			if err := dataset.SyncDirIn(r, dataset.Workspace); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
		}
	}
	d, err := r.Open(suspectRel)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	ents, err := d.ReadDir(-1)
	d.Close()
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	var sus *suspect
	for _, e := range ents {
		rel := filepath.Join(suspectRel, e.Name())
		i, ok := dataset.PartOrdinal(e.Name())
		switch {
		case ok && i < n: // recorded since: reproduced
			if err := r.Remove(rel); err != nil {
				return nil, fmt.Errorf("resume: %w", err)
			}
		case ok && i == n && !final:
			if sus, err = measureSuspect(v, r, out, rel, n); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: %s holds %s but the journal holds %d records", journal.ErrCorrupt, dir, e.Name(), n)
		}
	}
	return sus, nil
}

// measureSuspect reads the suspect part rel (ordinal n) through r.
func measureSuspect(v verifier, r *os.Root, out, rel string, n int) (*suspect, error) {
	path := filepath.Join(out, rel)
	f, err := openPart(r, rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := v.measureFile(f, path)
	if err != nil {
		return nil, fmt.Errorf("resume: suspect part: %w", err)
	}
	rows, _ := parquetout.PartRows(f) // unreadable: no boundary, the comparison refuses
	return &suspect{ordinal: n, path: path, rel: rel, info: info, rows: rows}, nil
}

// checkParts is recoverParts up to the removal of the temporary parts.
func checkParts(v verifier, r *os.Root, out string, st *journal.State, lock *journal.Lock) error {
	data := filepath.Join(out, dataset.DataDir)
	// Refuse ambiguous damage before deleting anything: a part published
	// under its final name beyond the recorded ones means an acknowledged
	// record was lost (a damaged frame length, or bit rot in the last
	// frame), not a torn append, which is never followed by a rename.
	for i := len(st.Records); ; i++ {
		if _, err := r.Lstat(partRel(i)); err != nil {
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
		final := partRel(i)
		keep[dataset.PartName(i)] = true
		rel := final
		if _, err := r.Lstat(final); err != nil {
			rel = claimRel(i)
			if _, err := r.Lstat(rel); err != nil || i != len(st.Records)-1 {
				return fmt.Errorf("resume: part %d is journaled but missing from %s", i, data)
			}
		}
		// Trust no retained part by existence: its size and digest must be
		// the journal's before the frontier moves past it.
		if err := v.verifyPartIn(r, out, rel, rec.Part); err != nil {
			return fmt.Errorf("resume: part %d: %w", i, err)
		}
		if rel != final {
			if err := lock.Verify(); err != nil {
				return err
			}
			if _, err := dataset.PublishPartWith(renameIn(r, out), out, i, dataset.Part{Path: filepath.Join(out, rel), PartInfo: rec.Part}); err != nil {
				return fmt.Errorf("resume: %w", err)
			}
		}
	}
	d, err := r.Open(dataset.DataDir)
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	ents, err := d.ReadDir(-1)
	d.Close()
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if err := lock.Verify(); err != nil {
		return err
	}
	for _, e := range ents {
		// Removed through the root: an entry that is a link is removed
		// itself, never what it names.
		if !keep[e.Name()] {
			if err := r.Remove(filepath.Join(dataset.DataDir, e.Name())); err != nil {
				return fmt.Errorf("resume: %w", err)
			}
		}
	}
	// Every recorded part's final name, found or made, must be durable
	// before the temporary copies, the fallback, are removed.
	if err := dataset.SyncDirIn(r, dataset.DataDir); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	return faults.At("recovered")
}

// ErrPartAltered reports a retained part that does not match its record.
var ErrPartAltered = errors.New("retained part does not match its journal record")

// verifyPartIn checks retained part rel (under r, the root of out) against
// its record.
func (v verifier) verifyPartIn(r *os.Root, out, rel string, want dataset.PartInfo) error {
	path := filepath.Join(out, rel)
	f, err := openPart(r, rel)
	if err != nil {
		return err
	}
	defer f.Close()
	got, err := v.measureFile(f, path)
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
	return verifier{ctx: context.Background()}.measure(path)
}

// verifier reads retained parts for recovery: it stops when ctx is done
// (a signal, --max-duration) and reports the bytes read as progress, so a
// long verification of a large dataset is neither unstoppable nor taken
// for a stall.
type verifier struct {
	ctx context.Context
	pr  *Progress
}

// verifyChunk is the read size between cancellation checks and progress.
const verifyChunk = 4 << 20

func (v verifier) measure(path string) (dataset.PartInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return dataset.PartInfo{}, err
	}
	defer f.Close()
	return v.measureFile(f, path)
}

// measureFile is measure of the open file f (path names it in errors).
func (v verifier) measureFile(f *os.File, path string) (dataset.PartInfo, error) {
	h := md5.New()
	buf := make([]byte, verifyChunk)
	var n int64
	for {
		if err := v.ctx.Err(); err != nil {
			return dataset.PartInfo{}, fmt.Errorf("resume: verifying %s: %w", path, context.Cause(v.ctx))
		}
		k, err := f.Read(buf)
		h.Write(buf[:k])
		n += int64(k)
		if v.pr != nil {
			v.pr.verified.Add(int64(k))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return dataset.PartInfo{}, err
		}
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
	if sus := jr.suspect; sus != nil && sus.rows > 0 {
		o.Cut = &parquetout.Cut{Ordinal: sus.ordinal, Rows: sus.rows}
	}
}

// verifyOption makes the router check the output's identity before it
// creates each part file.
func (jr *runJournal) verifyOption(o *parquetout.Options) {
	if jr != nil && jr.lock != nil {
		o.Verify = jr.lock.Verify
		// Opened here, before any encoder runs (they share it).
		r, err := jr.rooted()
		o.Open = func(path string) (*os.File, error) {
			if err != nil {
				return nil, err
			}
			return r.OpenFile(jr.rel(path), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		}
	}
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
	// The claim: from here until the rename into data/, this part may be
	// named by an acknowledged record, and recovery keeps it (keepSuspect).
	// Its name must be durable before the record names it.
	claim := dataset.ClaimPath(jr.out, ordinal)
	if err := jr.verify(); err != nil {
		return p, err
	}
	if err := jr.rename(p.Path, claim); err != nil {
		return p, fmt.Errorf("claim part %d: %w", ordinal, err)
	}
	p.Path = claim
	if err := dataset.SyncDir(filepath.Dir(claim)); err != nil {
		return p, fmt.Errorf("claim part %d: %w", ordinal, err)
	}
	if err := jr.j.Append(journal.Record{Part: p.PartInfo, Ordinal: ordinal, Limit: limit, RowsTotal: rows, Final: final}); err != nil {
		return p, err
	}
	jr.acked.parts.Add(1)
	jr.acked.bytes.Add(p.Bytes)
	jr.acked.rows.Store(rows)
	if sus != nil && ordinal == sus.ordinal {
		// Reproduced and recorded: the copy set aside is redundant (a
		// later recovery removes it if this does not).
		if jr.root != nil {
			_ = jr.root.Remove(sus.rel)
		} else {
			_ = os.Remove(sus.path)
		}
		jr.suspect = nil
	}
	if err := faults.At("recorded"); err != nil {
		return p, err
	}
	if err := jr.verify(); err != nil {
		return p, err
	}
	p, err := dataset.PublishPartWith(jr.rename, jr.out, ordinal, p)
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
		if errors.Is(err, source.ErrProtocol) {
			// The service broke the listing contract: a resume would walk
			// back into it, so the journal marks the run unresumable.
			if merr := jr.j.MarkUnresumable("protocol", err.Error()); merr != nil && cfg.Warn != nil {
				cfg.Warn("the run is not marked unresumable: " + merr.Error())
			}
		}
		_ = jr.j.Close()
		jr.j = nil
	}
	// The outcome goes into the output only while its pathname is still
	// the directory this process locked.
	if cfg.Settle != nil && jr.verify() == nil {
		cfg.Settle(rep, err)
	}
	jr.release(cfg)
}

// verify is journal.Lock.Verify for the run's output (nil without one).
func (jr *runJournal) verify() error {
	if jr == nil {
		return nil
	}
	return jr.lock.Verify()
}

// release gives up the output without settling: nothing found under the
// lock is this invocation's to record (the run completed or was replaced
// by another process, or there is none).
func (jr *runJournal) release(cfg Config) {
	if jr == nil {
		return
	}
	if jr.j != nil {
		_ = jr.j.Close()
		jr.j = nil
	}
	cfg.Progress.own(nil)
	if jr.root != nil {
		jr.root.Close()
		jr.root = nil
	}
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
