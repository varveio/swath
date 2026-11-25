// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package localfs is the local filesystem listing provider. A range is a
// directory (or a slice of a large directory's entries); its entries are
// sorted by key, where a directory's key is prefix+name+"/" (its DIR row key)
// and anything else is prefix+name. Emitting a DIR row and then its subtree
// before the next sibling yields global unsigned-byte key order, because
// every key in the subtree starts with prefix+name+"/" and names contain no
// '/'.
package localfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/longpath"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader/listing"
)

// Fields selects how much metadata is read per entry.
type Fields uint8

const (
	Stat  Fields = iota // one fstatat per entry: every stat column
	Names               // no stat: key and kind from the directory entry type
	Statx               // one statx per entry (Linux, full mask): stat columns plus created_at
)

// ParseFields maps names, stat and statx to Fields.
func ParseFields(s string) (Fields, error) {
	switch s {
	case "stat":
		return Stat, nil
	case "names":
		return Names, nil
	case "statx":
		return Statx, nil
	}
	return 0, fmt.Errorf("unknown metadata tier %q (names, stat or statx)", s)
}

func (f Fields) String() string { return [...]string{"stat", "names", "statx"}[f] }

// Options configure a Provider.
type Options struct {
	Fields     Fields
	NoDirs     bool            // omit kind=dir rows (directories are still walked)
	LinkTarget bool            // read symlink targets (stat tiers)
	Skip       map[string]bool // the run's own output: keys neither emitted nor, for a directory ("a/b/"), walked
	Portable   bool            // use the portable engine even where a native one exists
	// OneFilesystem keeps boundary directory rows without walking their
	// contents: another device, or on macOS another containing volume.
	OneFilesystem bool
}

// Stats count what a listing saw. Per-entry problems are counted, never fatal.
type Stats struct {
	Files            int64    `json:"files"` // non-directory rows
	Dirs             int64    `json:"dirs"`  // directories below the root
	Bytes            int64    `json:"bytes"` // sum of size over non-directory rows
	DirsRead         int64    `json:"dirs_read"`
	OpenErrors       int64    `json:"open_errors"`
	ReadErrors       int64    `json:"read_errors"`
	StatErrors       int64    `json:"stat_errors"`
	Vanished         int64    `json:"vanished"` // gone between listing and stat
	Changed          int64    `json:"changed"`  // type changed between listing and stat; dropped
	ReadlinkErrors   int64    `json:"readlink_errors"`
	Skipped          int64    `json:"skipped_outputs"`             // entries left out as the run's own output (Options.Skip)
	Unsafe           int64    `json:"unsafe_dirs"`                 // a directory replaced by a symlink before open
	Chunks           int64    `json:"chunks"`                      // large-directory chunks statted in parallel
	ParallelSorts    int64    `json:"parallel_sorts"`              // large directories whose entries were sorted in parallel runs
	ReaddirplusHints int64    `json:"readdirplus_hints,omitempty"` // full directory reads followed by a few stats, so an NFS client fetches attributes with the next (readNames)
	Unsorted         int64    `json:"unsorted_dirs,omitempty"`     // directories a count read without sorting (Opened.Begin)
	Slabs            int64    `json:"count_slabs,omitempty"`       // directory slabs a count read before statting them (countSlab, Linux)
	Presized         int64    `json:"presized"`                    // large directories whose buffers were sized from st_size
	CountProcs       int64    `json:"count_procs,omitempty"`       // GOMAXPROCS a count ran with, when raised (Opened.Begin)
	Samples          []string `json:"error_samples,omitempty"`
	// OtherDevice counts directories listed and not walked, on another
	// device than the root (OneFilesystem).
	OtherDevice              int64  `json:"other_device_dirs"`
	OtherVolume              int64  `json:"other_volume_dirs"`             // same stat device, different macOS filesystem ID
	FilesystemIdentityErrors int64  `json:"filesystem_identity_errors"`    // subset of open_errors
	FilesystemBoundary       string `json:"filesystem_boundary,omitempty"` // selected mechanism, only with OneFilesystem
	PrunedDirs               int64  `json:"pruned_dirs"`
	// FilteredStatErrors counts entries whose stat failed that the filters
	// leave out by their key alone (Provider.leftOut): not unreadable.
	FilteredStatErrors int64           `json:"filtered_stat_errors"`
	Pruning            *filter.Pruning `json:"pruning,omitempty"`
	// KeyTooLong counts entries left out because their key passes
	// maxKeyBytes (a directory with its subtree): unreadable.
	KeyTooLong int64     `json:"key_too_long"`
	LongPaths  LongPaths `json:"long_paths"`
	DirOpens   DirOpens  `json:"dir_opens"`
	// unreadTo is the Expand's emitter, told what it did not read (unread);
	// nil when the emitter takes no reports.
	unreadTo source.Unreader
}

// DirOpens account for the directories below the root opened relative to
// an open directory (Linux, macOS: openRange). Each is opened one name
// down from its parent's descriptor, which the walk keeps for its queued
// subdirectories; past the limit on kept descriptors, a directory's
// subdirectories are opened from a kept ancestor's, several names down (a
// redescent). Elsewhere a directory is opened by its path and nothing is
// counted.
type DirOpens struct {
	Opened     int64 `json:"opened"`
	Redescents int64 `json:"redescents"` // opens attempted from an ancestor, more than one name down
	Refused    int64 `json:"refused"`    // directories the limit did not keep for their subdirectories
	// Failed counts the opens that failed, by reason, as LongPaths.Failed.
	Failed   map[string]int64 `json:"failed,omitempty"`
	PeakKept int64            `json:"peak_kept"` // the most descriptors of kept directories at once
	MaxKept  int64            `json:"max_kept"`  // the limit on them (keepLimit)
}

func (o *DirOpens) fail(reason string) {
	if o.Failed == nil {
		o.Failed = map[string]int64{}
	}
	o.Failed[reason]++
}

func (o *DirOpens) add(x *DirOpens) {
	o.Opened += x.Opened
	o.Redescents += x.Redescents
	o.Refused += x.Refused
	for k, v := range x.Failed {
		if o.Failed == nil {
			o.Failed = map[string]int64{}
		}
		o.Failed[k] += v
	}
}

// keptFDs bound the directory descriptors a walk keeps open (dirFD): a
// directory's, from its open until its chunks are read and its queued
// subdirectories opened relative to it. Past limit a directory is still
// read, whole by the worker that opened it, but not kept: its
// subdirectories are opened from a kept ancestor. The root is always
// kept.
type keptFDs struct {
	limit int64
	open  atomic.Int64
	peak  atomic.Int64
}

// take counts one more kept descriptor if the limit allows, or always.
func (k *keptFDs) take(always bool) bool {
	n := k.open.Add(1)
	if n > k.limit && !always {
		k.open.Add(-1)
		return false
	}
	for peak := k.peak.Load(); n > peak; peak = k.peak.Load() {
		if k.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	return true
}

// LongPaths account for the paths past the kernel's length limit
// (PATH_MAX: 4,096 bytes on Linux, 1,024 on macOS), which it refuses
// whole (ENAMETOOLONG): a directory there, opened relative to its parent
// as any below the root (openRange), and a file root, statted relative to
// its directory (atParent). A path that fits is never counted.
type LongPaths struct {
	Attempts int64 `json:"attempts"`
	Opened   int64 `json:"opened"`
	// Failed counts the attempts that failed, by reason: not_found,
	// permission, not_directory, symlink (a component replaced by a
	// symbolic link, never followed), name_too_long (one name past the
	// filesystem's limit), other_device (--one-file-system: a component
	// on another device; the directory is not walked) or other.
	Failed       map[string]int64 `json:"failed,omitempty"`
	MaxDepth     int64            `json:"max_depth"`      // the most names below the root opened for one directory
	MaxPathBytes int64            `json:"max_path_bytes"` // the longest path attempted
}

func (l *LongPaths) attempt(depth, path int) {
	l.Attempts++
	l.MaxDepth = max(l.MaxDepth, int64(depth))
	l.MaxPathBytes = max(l.MaxPathBytes, int64(path))
}

func (l *LongPaths) fail(reason string) {
	if l.Failed == nil {
		l.Failed = map[string]int64{}
	}
	l.Failed[reason]++
}

func (l *LongPaths) add(o *LongPaths) {
	l.Attempts += o.Attempts
	l.Opened += o.Opened
	for k, v := range o.Failed {
		if l.Failed == nil {
			l.Failed = map[string]int64{}
		}
		l.Failed[k] += v
	}
	l.MaxDepth = max(l.MaxDepth, o.MaxDepth)
	l.MaxPathBytes = max(l.MaxPathBytes, o.MaxPathBytes)
}

// maxKeyBytes is the longest key a listing emits. The kernel takes a path
// of at most PATH_MAX in one call, but a tree may go deeper (openLong); the
// cap keeps a key within what the rest of the pipeline is sized for (the
// writer cuts a page by its keys' bytes as well as its rows). A longer
// entry is left out and counted (KeyTooLong), a directory with its subtree.
const maxKeyBytes = 32 << 10

// Errors is the sum of the error counters.
func (s *Stats) Errors() int64 {
	return s.OpenErrors + s.ReadErrors + s.StatErrors + s.Vanished + s.Changed + s.ReadlinkErrors + s.Unsafe + s.KeyTooLong
}

// ErrorSamples are the first entries the listing could not read, each
// "path: cause": the command names them on stderr.
func (s *Stats) ErrorSamples() []string { return s.Samples }

const maxSamples = 8

// note counts an error; one that left the entry out (not a vanished entry,
// nor a link whose target was not read: Report) is a sample too.
func (s *Stats) note(counter *int64, path string, err error) {
	*counter++
	if len(s.Samples) < maxSamples && counter != &s.Vanished && counter != &s.ReadlinkErrors {
		s.Samples = append(s.Samples, path+": "+err.Error())
	}
}

func (s *Stats) add(o *Stats) {
	s.Files += o.Files
	s.Dirs += o.Dirs
	s.Bytes += o.Bytes
	s.DirsRead += o.DirsRead
	s.OpenErrors += o.OpenErrors
	s.ReadErrors += o.ReadErrors
	s.StatErrors += o.StatErrors
	s.Vanished += o.Vanished
	s.Changed += o.Changed
	s.ReadlinkErrors += o.ReadlinkErrors
	s.Skipped += o.Skipped
	s.Unsafe += o.Unsafe
	s.Chunks += o.Chunks
	s.ParallelSorts += o.ParallelSorts
	s.Unsorted += o.Unsorted
	s.Slabs += o.Slabs
	s.ReaddirplusHints += o.ReaddirplusHints
	s.Presized += o.Presized
	s.OtherDevice += o.OtherDevice
	s.OtherVolume += o.OtherVolume
	s.FilesystemIdentityErrors += o.FilesystemIdentityErrors
	s.PrunedDirs += o.PrunedDirs
	s.FilteredStatErrors += o.FilteredStatErrors
	s.KeyTooLong += o.KeyTooLong
	s.LongPaths.add(&o.LongPaths)
	s.DirOpens.add(&o.DirOpens)
	for _, x := range o.Samples {
		if len(s.Samples) < maxSamples {
			s.Samples = append(s.Samples, x)
		}
	}
}

// Range is a directory, or a chunk of a large directory's sorted entries.
type Range struct {
	path   string // on-disk path: for messages, and opened by it where directories are not opened relative to a descriptor (dirfd_other.go)
	prefix string // key prefix: "" for the root, "a/b/" below it
	chunk  *chunk
	// at is the open directory a directory below the root is opened from,
	// below names down (openRange), a reference the range holds until it
	// is expanded or disposed; nil for a root opened by its path, and where
	// directories are opened by their path.
	at    *dirFD
	below int
}

// Provider lists one root. It implements source.Provider[Range, Entry].
type Provider struct {
	root   string
	opt    Options
	native bool
	dev    int64    // the root's device (OneFilesystem)
	fsid   [2]int32 // the root's containing macOS volume, zero on other platforms
	// held is the directory the caller holds open (OpenDir), never
	// closed here, and prefix the key of the directory listed below it;
	// nil and "" for a root opened by its path (New). heldFile keeps the
	// caller's file reachable, so that no finalizer closes it.
	held     *dirFD
	heldFile *os.File
	prefix   string
	prune    func(string) bool // immutable directory predicate, set before workers start
	keyOut   func(string) bool // the filters' key-only exclusion (filter.Spec.KeyExcluded), set before workers start
	// unreadable is told the key prefix of a directory that could not be
	// opened (Opened.Begin's unreadable; nil: nobody asks), set before
	// workers start.
	unreadable func(string)
	// unreadTo is told each place the listing could not read (unread),
	// concurrently; set before workers start, by tests only.
	unreadTo func(key string, reason error)
	// unsorted: a count, whose rows nothing orders; a directory's entries
	// are not sorted (order.Options.Unordered). Set before listing starts.
	unsorted bool
	file     bool // the root is not a directory (expandFile)
	mu       sync.Mutex
	stats    Stats
	// warn tells the user of a directory left out (Open's warn); nil
	// outside a run. warnMu serialises the workers' calls.
	warn   func(code, msg string)
	warnMu sync.Mutex
	// space is the root's filesystem space, measured once (spaceOnce):
	// nil when the measurement failed or the platform has none. rootInfo
	// is the root New found, so a root replaced since is not measured.
	spaceOnce sync.Once
	space     *source.FilesystemSpace
	rootInfo  fs.FileInfo
	// keep bounds the directory descriptors the walk keeps (dirFD).
	keep keptFDs
}

// warnf passes a warning to the user, if the run takes them.
func (p *Provider) warnf(code, format string, a ...any) {
	if p.warn == nil {
		return
	}
	p.warnMu.Lock()
	defer p.warnMu.Unlock()
	p.warn(code, fmt.Sprintf(format, a...))
}

var _ source.Provider[Range, Entry] = (*Provider)(nil)

// New validates root and returns a provider for it. A root that is not a
// directory (a file, as du and ls take one) lists as its one entry, keyed
// by its name. A root directory that cannot be opened is an error, as one
// that does not exist: a listing of it would hold nothing. A root path
// past the kernel's length limit is resolved name by name (longpath).
func New(root string, opt Options) (*Provider, error) {
	fi, err := longpath.Stat(root)
	if err != nil {
		return nil, err
	}
	var d *os.File
	if fi.IsDir() {
		d, err = longpath.Open(root)
		if err != nil {
			return nil, err
		}
		defer d.Close()
		if fi, err = d.Stat(); err != nil {
			return nil, err
		}
	}
	if opt.Fields == Statx && (opt.Portable || !nativeEngine) {
		return nil, errors.New("the statx tier needs the Linux engine")
	}
	p := &Provider{root: filepath.Clean(root), opt: opt, native: nativeEngine && !opt.Portable, file: !fi.IsDir(), rootInfo: fi}
	p.keep.limit = keepLimit()
	if opt.OneFilesystem {
		if p.dev, err = rootDevice(fi, opt.Fields); err != nil {
			return nil, err
		}
		if d != nil {
			if p.fsid, err = readFilesystemID(int(d.Fd())); err != nil {
				return nil, fmt.Errorf("the root's filesystem identity: %w", err)
			}
		}
	}
	return p, nil
}

// Root is the root directory range: for a held root (OpenDir), the
// directory at prefix, opened from it by its names, "." for itself
// (descend: path.Base of "" is ".").
func (p *Provider) Root() Range {
	if p.held == nil {
		return Range{path: p.root}
	}
	names, _ := keyNames(p.prefix, true) // checked by OpenDir
	return Range{path: p.rootPath(), prefix: p.prefix, at: p.held, below: max(1, len(names))}
}

// rootPath names a held root's listed directory in messages: its key
// below the held directory, "." for that directory itself.
func (p *Provider) rootPath() string {
	if p.prefix == "" {
		return "."
	}
	return strings.TrimSuffix(p.prefix, "/")
}

// Stats returns the counters so far.
func (p *Provider) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	if p.opt.OneFilesystem {
		s.FilesystemBoundary = filesystemMechanism
	}
	s.Samples = slices.Clone(s.Samples)
	s.LongPaths.Failed = maps.Clone(s.LongPaths.Failed)
	s.DirOpens.Failed = maps.Clone(s.DirOpens.Failed)
	s.DirOpens.PeakKept, s.DirOpens.MaxKept = p.keep.peak.Load(), p.keep.limit
	return s
}

// Dispose releases what a range that will never be expanded holds (a
// stopped listing's). It implements source.Disposer.
func (p *Provider) Dispose(r Range) {
	p.release(r)
}

// release releases what a range holds once expanded or disposed: a chunk's
// share of its directory's descriptor, a directory's reference on the one
// it is opened from.
func (p *Provider) release(r Range) {
	disposeChunk(r)
	r.at.release()
}

// Expand lists one range.
func (p *Provider) Expand(ctx context.Context, r Range, e source.Emitter[Range, Entry]) error {
	// What the range holds is released however Expand ends, a cancellation
	// before any work included: a range a worker has taken is no longer
	// queued, so the order stage will not dispose it.
	defer p.release(r)
	var st Stats
	st.unreadTo, _ = e.(source.Unreader)
	if ctx.Err() != nil { // as if disposed
		return ctx.Err()
	}
	if t, ok := e.(source.Subtreer); ok && r.chunk == nil && !p.file {
		t.Subtree(r.prefix) // a directory's subtree is its subdirectories' ranges, its chunks and their continuations
	}
	var err error
	if p.file {
		err = p.expandFile(e, &st)
	} else if p.native {
		err = p.expandNative(ctx, r, e, &st)
	} else {
		err = p.expandPortable(ctx, r, e, &st)
	}
	p.mu.Lock()
	p.stats.add(&st)
	p.mu.Unlock()
	return err
}

// unread reports a place the listing could not read and went on past, as
// a worker meets it: the key of a directory or entry, and why, in the
// listing contract's class (unreadReason). It reaches the Expand's
// emitter (st.unreadTo: an order.UnreadSink's count passes it on), and
// p.unreadTo, nil outside tests.
func (p *Provider) unread(st *Stats, key string, reason error) {
	if st.unreadTo != nil {
		st.unreadTo.Unread(listing.Unread{Key: []byte(key), Reason: reason})
	}
	if p.unreadTo != nil {
		p.unreadTo(key, reason)
	}
}

// unreadReason is err, why an open, read or stat failed, beside its class
// in the listing contract: a refusal (EACCES, EPERM) listing.ErrAuth;
// another device than the root's listing.ErrOtherDevice; a symbolic link
// where a directory was, which is never followed, listing.ErrNotDirectory:
// what lies past it is not reached, never absent; a name the system
// refuses as too long listing.ErrUnsupported, which asking again does not
// change; an I/O error, and the tree changing under the listing (the entry
// gone, or no longer a directory), listing.ErrUnavailable.
func unreadReason(err error) error {
	class := listing.ErrUnavailable
	switch {
	case errors.Is(err, errSymlink):
		class = listing.ErrNotDirectory
	case errors.Is(err, errOtherDevice), errors.Is(err, errOtherVolume):
		class = listing.ErrOtherDevice
	case errors.Is(err, fs.ErrPermission):
		class = listing.ErrAuth
	case errors.Is(err, syscall.ENAMETOOLONG):
		class = listing.ErrUnsupported
	}
	return fmt.Errorf("%w: %w", class, err)
}

// errOtherDevice: a directory opened below the root is on another device
// than the root's (OneFilesystem).
var errOtherDevice = errors.New("on another device than the root")

// errSymlink: a name opened as a directory is a symbolic link, which the
// walk never follows.
var errSymlink = errors.New("a symbolic link, not followed")

// errKeyTooLong is the Reason of an entry left out because its key passes
// maxKeyBytes, a directory with its subtree.
var errKeyTooLong = fmt.Errorf("%w: its key passes the limit of %d bytes", listing.ErrUnsupported, maxKeyBytes)

// readFailed counts a directory read that failed: its enumeration ended
// there.
func (p *Provider) readFailed(r Range, st *Stats, err error) {
	st.note(&st.ReadErrors, r.path, err)
	p.unread(st, r.prefix, unreadReason(err))
}

// statFailed counts an entry left out because its stat failed (path names
// it), unless it vanished: an entry gone is no longer part of the tree.
func (p *Provider) statFailed(key, path string, st *Stats, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		st.note(&st.Vanished, path, err)
		return
	}
	st.note(&st.StatErrors, path, err)
	p.unread(st, key, unreadReason(err))
}

// emit hands a directory's sorted rows to e, splicing each subdirectory in
// as a child range right after its DIR row, opened from d, the directory
// read (dirFD.child). The run's own output (Skip) and an entry whose key
// passes maxKeyBytes are dropped, a directory with its subtree. A
// directory on another device (OneFilesystem) or pruned by the filters is
// listed and not walked.
func (p *Provider) emit(r Range, d *dirFD, rows []Row, e source.Emitter[Range, Entry], st *Stats) error {
	start := 0
	for i := range rows {
		row := &rows[i]
		output, long := len(p.opt.Skip) > 0 && p.opt.Skip[row.Key], len(row.Key) > maxKeyBytes
		if output || long {
			if long {
				name := strings.TrimSuffix(row.Key[len(r.prefix):], "/")
				st.note(&st.KeyTooLong, r.path+string(os.PathSeparator)+name, fmt.Errorf("its key of %d bytes passes the limit of %d", len(row.Key), maxKeyBytes))
				p.unread(st, row.Key, errKeyTooLong)
			} else {
				st.Skipped++
			}
			if err := e.Rows(rows[start:i]); err != nil {
				return err
			}
			start = i + 1
			continue
		}
		if row.Data.Kind != KindDir {
			st.Files++
			st.Bytes += row.Data.Size
			continue
		}
		st.Dirs++
		stop := p.otherDevice(row)
		switch {
		case stop:
			st.OtherDevice++
			p.unread(st, row.Key, unreadReason(errOtherDevice))
		case p.prune != nil && p.prune(row.Key):
			st.PrunedDirs++
			stop = true
		}
		if stop {
			if !p.opt.NoDirs {
				continue // listed with the rows after it; its subtree is not walked
			}
			if err := e.Rows(rows[start:i]); err != nil {
				return err
			}
			start = i + 1
			continue
		}
		end := i + 1
		if p.opt.NoDirs {
			end = i
		}
		// The child is taken from the row before the rows are handed over:
		// the emitter owns them then, and a filter may clear the DIR row.
		// The key is a view of the directory's key arena: the child's
		// prefix is a copy, so a queued subdirectory does not keep its
		// parent's names alive. Its reference on d is taken as it is handed
		// on: Child owns it then, even when it fails.
		key := row.Key
		child := Range{path: r.path + string(os.PathSeparator) + key[len(r.prefix):len(key)-1], prefix: strings.Clone(key)}
		if err := e.Rows(rows[start:end]); err != nil {
			return err
		}
		start = i + 1
		child.at, child.below = d.child()
		if err := e.Child(child); err != nil {
			return err
		}
	}
	return e.Rows(rows[start:])
}

// expandFile lists a root that is not a directory: its one row, keyed by
// its name, read as its directory's listing reads the entry: at the run's
// tier, a symbolic link as itself (New's stat followed it only to find
// that it does not name a directory). A path the kernel refuses by its
// length is read relative to its directory (statLong, readlinkLong).
func (p *Provider) expandFile(e source.Emitter[Range, Entry], st *Stats) error {
	row := Row{Key: filepath.Base(p.root)}
	var err error
	switch fi := fs.FileInfo(nil); {
	case p.opt.Fields == Names:
		if fi, err = os.Lstat(p.root); err == nil {
			row.Data.Kind = kindOf(fi.Mode().Type())
		}
	case p.native:
		err = statPath(p.root, p.opt.Fields, &row.Data)
	default:
		if fi, err = os.Lstat(p.root); err == nil {
			fromFileInfo(&row.Data, fi)
			fillRoot(p.root, &row.Data)
		}
	}
	if tooLong(err) {
		err = statLong(p.root, p.opt.Fields, &row.Data, st)
	}
	if err != nil {
		st.note(&st.StatErrors, p.root, err)
		return nil
	}
	if row.Data.Kind == KindSymlink && p.opt.LinkTarget && p.opt.Fields != Names {
		t, err := os.Readlink(p.root)
		if tooLong(err) {
			t, err = readlinkLong(p.root, st)
		}
		if err == nil {
			row.Data.LinkTarget, row.Data.Present = filepath.ToSlash(t), row.Data.Present|HasLink
		} else {
			st.note(&st.ReadlinkErrors, p.root, err)
		}
	}
	st.Files++
	st.Bytes += row.Data.Size
	return e.Rows([]Row{row})
}

func sortRows(rows []Row) {
	slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
}

// expandPortable lists a directory with os.File.ReadDir (readDir: on
// Windows its own read, which keeps what the enumeration says of each
// entry) and a stat per entry (statEntry).
func (p *Provider) expandPortable(ctx context.Context, r Range, e source.Emitter[Range, Entry], st *Stats) error {
	d, ok, err := p.openPortable(ctx, r, st)
	if !ok {
		return err
	}
	st.DirsRead++
	defer d.release() // statEntry and readlinkIn read relative to it (Linux, macOS); subdirectories take their own references
	ents, err := readDir(d.f)
	if err != nil {
		p.readFailed(r, st, err)
	}
	rows, err := p.portableRows(ctx, r, d.f, ents, st)
	if err != nil {
		return err
	}
	if p.unsorted {
		st.Unsorted++
	} else {
		sortRows(rows)
	}
	return p.emit(r, d, rows, e, st)
}

// portableRows stats ents, read from the directory f (statEntry), into
// rows, leaving out an entry whose stat failed.
func (p *Provider) portableRows(ctx context.Context, r Range, f *os.File, ents []fs.DirEntry, st *Stats) ([]Row, error) {
	rows := make([]Row, 0, len(ents))
	for i, d := range ents {
		if i%1024 == 1023 && ctx.Err() != nil { // stat-heavy loop: stay cancellable
			return nil, ctx.Err()
		}
		name := d.Name()
		row := Row{Key: r.prefix + name}
		path := r.path + string(os.PathSeparator) + name
		if p.opt.Fields == Names {
			row.Data.Kind = kindOf(d.Type())
		} else {
			if err := statEntry(f, d, &row.Data); err != nil {
				if p.leftOut(row.Key, d.IsDir(), true) {
					st.FilteredStatErrors++
					continue
				}
				key := row.Key
				if d.IsDir() {
					key += "/"
				}
				p.statFailed(key, path, st, err)
				continue
			}
			if dirBlocks && row.Data.Kind == KindDir {
				fillDirBlocks(path, &row.Data) // Windows: the enumeration gives a directory none
			}
			if row.Data.Kind == KindSymlink && p.opt.LinkTarget {
				if t, err := readlinkIn(f, path, name); err == nil {
					row.Data.LinkTarget, row.Data.Present = filepath.ToSlash(t), row.Data.Present|HasLink
				} else {
					st.note(&st.ReadlinkErrors, path, err)
				}
			}
		}
		if row.Data.Kind == KindUnknown && reparseLinks {
			p.reparsePoint(d, path, &row.Data)
		}
		if row.Data.Kind == KindDir {
			row.Key += "/"
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// leftOut says the filters leave out the entry at key (its name below the
// root, without a directory's trailing /; a directory if dir,
// either kind unless known) by its key alone, so that its failed stat
// leaves out nothing selected: a file's row excluded, or a directory's row
// excluded and every descendant pruned (an entry that is not statted is
// not walked either).
func (p *Provider) leftOut(key string, dir, known bool) bool {
	if p.keyOut == nil {
		return false
	}
	asFile := p.keyOut(key)
	asDir := p.keyOut(key+"/") && p.prune != nil && p.prune(key+"/")
	switch {
	case !known:
		return asFile && asDir
	case dir:
		return asDir
	}
	return asFile
}

// unreadableDir tells the run (Opened.Begin's unreadable) of a directory
// below the root that could not be opened.
func (p *Provider) unreadableDir(r Range) {
	if p.unreadable != nil && r.prefix != "" {
		p.unreadable(r.prefix)
	}
}

// kindOf maps Go file mode type bits to a kind.
func kindOf(m fs.FileMode) Kind {
	switch {
	case m.IsRegular():
		return KindFile
	case m&fs.ModeDir != 0:
		return KindDir
	case m&fs.ModeSymlink != 0:
		return KindSymlink
	case m&fs.ModeNamedPipe != 0:
		return KindFIFO
	case m&fs.ModeSocket != 0:
		return KindSocket
	case m&fs.ModeDevice != 0 && m&fs.ModeCharDevice != 0:
		return KindCharDevice
	case m&fs.ModeDevice != 0:
		return KindBlockDevice
	}
	return KindUnknown
}

// reparsePoint makes an entry of kind unknown that is a junction or a
// volume mount point (mountPoint: Windows) a symbolic link, with its
// target where the run reads targets (a stat tier and LinkTarget), from
// the one read that tells it apart.
func (p *Provider) reparsePoint(d fs.DirEntry, path string, r *Entry) {
	fi, err := d.Info()
	if err != nil {
		return
	}
	t, ok := mountPoint(fi, path)
	if !ok {
		return
	}
	r.Kind = KindSymlink
	if p.opt.LinkTarget && p.opt.Fields != Names {
		r.LinkTarget, r.Present = filepath.ToSlash(t), r.Present|HasLink
	}
}

// fromFileInfo fills a row from os.FileInfo plus the platform Sys() data.
func fromFileInfo(r *Entry, fi fs.FileInfo) {
	m := fi.Mode()
	r.Kind = kindOf(m)
	r.Size = fi.Size()
	r.Mtime = fi.ModTime().UnixMicro()
	r.Mode = uint32(m.Perm())
	if m&fs.ModeSetuid != 0 {
		r.Mode |= 0o4000
	}
	if m&fs.ModeSetgid != 0 {
		r.Mode |= 0o2000
	}
	if m&fs.ModeSticky != 0 {
		r.Mode |= 0o1000
	}
	r.Present |= HasSize | HasMtime | HasMode
	fillSys(r, fi)
}
