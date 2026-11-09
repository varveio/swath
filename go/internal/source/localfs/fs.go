// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package fs is the local filesystem listing provider. A range is a
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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/varveio/swath/go/internal/source"
)

// Fields selects how much metadata is read per entry.
type Fields uint8

const (
	Stat  Fields = iota // one fstatat per entry: every stat column
	Names               // no stat: key and kind from the directory entry type
	Statx               // one statx per entry (Linux, full mask): stat columns plus birth_time
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
	NoDirs     bool            // omit kind=DIR rows (directories are still walked)
	LinkTarget bool            // read symlink targets (stat tiers)
	Skip       map[string]bool // directory keys ("a/b/") neither emitted nor walked
	Portable   bool            // use the portable engine even where a native one exists
	// OneFilesystem lists a directory on another device than the root
	// (a mount point) but does not walk it, as du -x and find -xdev.
	OneFilesystem bool
}

// Stats count what a listing saw. Per-entry problems are counted, never fatal.
type Stats struct {
	Files          int64    `json:"files"` // non-directory rows
	Dirs           int64    `json:"dirs"`  // directories below the root
	Bytes          int64    `json:"bytes"` // sum of size over non-directory rows
	DirsRead       int64    `json:"dirs_read"`
	OpenErrors     int64    `json:"open_errors"`
	ReadErrors     int64    `json:"read_errors"`
	StatErrors     int64    `json:"stat_errors"`
	Vanished       int64    `json:"vanished"` // gone between listing and stat
	Changed        int64    `json:"changed"`  // type changed between listing and stat; dropped
	ReadlinkErrors int64    `json:"readlink_errors"`
	Skipped        int64    `json:"skipped_dirs"`
	Unsafe         int64    `json:"unsafe_dirs"`           // a directory replaced by a symlink before open
	Chunks         int64    `json:"chunks"`                // large-directory chunks statted in parallel
	ParallelSorts  int64    `json:"parallel_sorts"`        // large directories whose entries were sorted in parallel runs
	Presized       int64    `json:"presized"`              // large directories whose buffers were sized from st_size
	CountProcs     int64    `json:"count_procs,omitempty"` // GOMAXPROCS a count ran with, when raised (opened.Run)
	Samples        []string `json:"error_samples,omitempty"`
	// OtherDevice counts directories listed and not walked, on another
	// device than the root (OneFilesystem).
	OtherDevice int64 `json:"other_device_dirs"`
}

// Errors is the sum of the error counters.
func (s *Stats) Errors() int64 {
	return s.OpenErrors + s.ReadErrors + s.StatErrors + s.Vanished + s.Changed + s.ReadlinkErrors + s.Unsafe
}

const maxSamples = 8

func (s *Stats) note(counter *int64, path string, err error) {
	*counter++
	if len(s.Samples) < maxSamples {
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
	s.Presized += o.Presized
	s.OtherDevice += o.OtherDevice
	for _, x := range o.Samples {
		if len(s.Samples) < maxSamples {
			s.Samples = append(s.Samples, x)
		}
	}
}

// Range is a directory, or a chunk of a large directory's sorted entries.
type Range struct {
	path   string // on-disk path
	prefix string // key prefix: "" for the root, "a/b/" below it
	chunk  *chunk
}

// Provider lists one root. It implements source.Provider[Range, Entry].
type Provider struct {
	root   string
	opt    Options
	native bool
	dev    int64 // the root's device (OneFilesystem)
	mu     sync.Mutex
	stats  Stats
}

var _ source.Provider[Range, Entry] = (*Provider)(nil)

// ErrNotDir is returned by New when the root is not a directory.
var ErrNotDir = errors.New("not a directory")

// New validates root and returns a provider for it.
func New(root string, opt Options) (*Provider, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("%s: %w", root, ErrNotDir)
	}
	if opt.Fields == Statx && (opt.Portable || !nativeEngine) {
		return nil, errors.New("the statx tier needs the Linux engine")
	}
	p := &Provider{root: filepath.Clean(root), opt: opt, native: nativeEngine && !opt.Portable}
	if opt.OneFilesystem {
		if p.dev, err = rootDevice(fi, opt.Fields); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Root is the root directory range.
func (p *Provider) Root() Range { return Range{path: p.root} }

// Stats returns the counters so far.
func (p *Provider) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	s.Samples = slices.Clone(s.Samples)
	return s
}

// Expand lists one range.
// Dispose releases a range that will never be expanded (a chunk's share of
// its directory descriptor). It implements source.Disposer.
func (p *Provider) Dispose(r Range) { disposeChunk(r) }

func (p *Provider) Expand(ctx context.Context, r Range, e source.Emitter[Range, Entry]) error {
	// A chunk's share of its descriptor is released however Expand ends,
	// a cancellation before any work included: a range a worker has taken
	// is no longer queued, so the order stage will not dispose it.
	defer disposeChunk(r)
	if done(ctx) {
		return ctx.Err()
	}
	var st Stats
	var err error
	if p.native {
		err = p.expandNative(ctx, r, e, &st)
	} else {
		err = p.expandPortable(ctx, r, e, &st)
	}
	p.mu.Lock()
	p.stats.add(&st)
	p.mu.Unlock()
	return err
}

// emit hands a directory's sorted rows to e, splicing each subdirectory in
// as a child range right after its DIR row. Skipped directories are dropped.
func (p *Provider) emit(r Range, rows []Row, e source.Emitter[Range, Entry], st *Stats) error {
	start := 0
	for i := range rows {
		row := &rows[i]
		if row.Data.Kind != KindDir {
			st.Files++
			st.Bytes += row.Data.Size
			continue
		}
		if len(p.opt.Skip) > 0 && p.opt.Skip[row.Key] {
			st.Skipped++
			if err := e.Rows(rows[start:i]); err != nil {
				return err
			}
			start = i + 1
			continue
		}
		st.Dirs++
		if p.otherDevice(row) {
			st.OtherDevice++
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
		// parent's names alive.
		key := row.Key
		child := Range{path: r.path + string(os.PathSeparator) + key[len(r.prefix):len(key)-1], prefix: strings.Clone(key)}
		if err := e.Rows(rows[start:end]); err != nil {
			return err
		}
		start = i + 1
		if err := e.Child(child); err != nil {
			return err
		}
	}
	return e.Rows(rows[start:])
}

// done reports whether ctx is finished without ctx.Err(), which takes the
// context's mutex; a non-blocking receive on an open channel does not lock.
func done(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func sortRows(rows []Row) {
	slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
}

// expandPortable lists a directory with os.File.ReadDir and Lstat.
func (p *Provider) expandPortable(ctx context.Context, r Range, e source.Emitter[Range, Entry], st *Stats) error {
	f, ok := openChecked(r, st)
	if !ok {
		return nil
	}
	st.DirsRead++
	ents, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		st.note(&st.ReadErrors, r.path, err)
	}
	rows := make([]Row, 0, len(ents))
	for i, d := range ents {
		if i%1024 == 1023 && done(ctx) { // stat-heavy loop: stay cancellable
			return ctx.Err()
		}
		name := d.Name()
		row := Row{Key: r.prefix + name}
		path := r.path + string(os.PathSeparator) + name
		if p.opt.Fields == Names {
			row.Data.Kind = kindOf(d.Type())
		} else {
			fi, err := d.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					st.note(&st.Vanished, path, err)
				} else {
					st.note(&st.StatErrors, path, err)
				}
				continue
			}
			fromFileInfo(&row.Data, fi)
			if row.Data.Kind == KindSymlink && p.opt.LinkTarget {
				if t, err := os.Readlink(path); err == nil {
					row.Data.LinkTarget, row.Data.Present = filepath.ToSlash(t), row.Data.Present|HasLink
				} else {
					st.note(&st.ReadlinkErrors, path, err)
				}
			}
		}
		if row.Data.Kind == KindDir {
			row.Key += "/"
		}
		rows = append(rows, row)
	}
	sortRows(rows)
	return p.emit(r, rows, e, st)
}

// openChecked opens a directory by path. Below the root it then checks that
// the path is still a real directory (Lstat: not a symlink) and the same
// object as the opened file (fstat): best-effort protection against a
// directory swapped for a symlink between listing and open.
func openChecked(r Range, st *Stats) (*os.File, bool) {
	f, err := os.Open(r.path)
	if err != nil {
		st.note(&st.OpenErrors, r.path, err)
		return nil, false
	}
	if r.prefix == "" {
		return f, true
	}
	ffi, err1 := f.Stat()
	lfi, err2 := os.Lstat(r.path)
	if err1 != nil || err2 != nil || !ffi.IsDir() || !lfi.IsDir() || lfi.Mode()&fs.ModeSymlink != 0 || !os.SameFile(ffi, lfi) {
		f.Close()
		st.note(&st.Unsafe, r.path, errors.New("directory changed before open"))
		return nil, false
	}
	return f, true
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
