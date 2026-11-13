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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/tree"
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
	Skip       map[string]bool // the run's own output: keys neither emitted nor, for a directory ("a/b/"), walked
	Portable   bool            // use the portable engine even where a native one exists
	// OneFilesystem lists a directory on another device than the root
	// (a mount point) but does not walk it, as du -x and find -xdev.
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
	Unsorted         int64    `json:"unsorted_dirs,omitempty"`     // directories a count read without sorting (opened.Run)
	Slabs            int64    `json:"count_slabs,omitempty"`       // directory slabs a count read before statting them (countSlab, Linux)
	Presized         int64    `json:"presized"`                    // large directories whose buffers were sized from st_size
	CountProcs       int64    `json:"count_procs,omitempty"`       // GOMAXPROCS a count ran with, when raised (opened.Run)
	Samples          []string `json:"error_samples,omitempty"`
	// OtherDevice counts directories listed and not walked, on another
	// device than the root (OneFilesystem).
	OtherDevice int64           `json:"other_device_dirs"`
	PrunedDirs  int64           `json:"pruned_dirs"`
	Pruning     *filter.Pruning `json:"pruning,omitempty"`
}

// Errors is the sum of the error counters.
func (s *Stats) Errors() int64 {
	return s.OpenErrors + s.ReadErrors + s.StatErrors + s.Vanished + s.Changed + s.ReadlinkErrors + s.Unsafe
}

// ErrorSamples are the first entries the listing could not read, each
// "path: cause": the command names them on stderr.
func (s *Stats) ErrorSamples() []string { return s.Samples }

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
	s.Unsorted += o.Unsorted
	s.Slabs += o.Slabs
	s.ReaddirplusHints += o.ReaddirplusHints
	s.Presized += o.Presized
	for _, x := range o.Samples {
		if len(s.Samples) < maxSamples {
			s.Samples = append(s.Samples, x)
		}
	}
}

// Range is a directory (Dir its on-disk path), or a chunk of a large
// directory's sorted entries (Seg).
type Range = tree.Range[string, *chunk]

// Out receives a range's rows and chunks.
type Out = tree.Out[string, *chunk, Entry]

// Provider lists one root on the tree walker (tree.Walker): it is the
// walker's Reader, and the walker is the source.Provider[Range, Entry].
type Provider struct {
	*tree.Walker[string, *chunk, Entry]
	root   string
	opt    Options
	native bool
	dev    int64 // the root's device (OneFilesystem)
	// unsorted: a count, whose rows nothing orders; a directory's entries
	// are not sorted (order.Options.Unordered). Set before listing starts.
	unsorted bool
	file     bool // the root is not a directory (expandFile)
	mu       sync.Mutex
	stats    Stats
	// warn tells the user of a directory left out (registry.Request.Warn);
	// nil outside a run. warnMu serialises the workers' calls.
	warn   func(string)
	warnMu sync.Mutex
	// space is the root's filesystem space, measured once (spaceOnce):
	// nil when the measurement failed or the platform has none. rootInfo
	// is the root New found, so a root replaced since is not measured.
	spaceOnce sync.Once
	space     *source.FilesystemSpace
	rootInfo  fs.FileInfo
}

// warnf passes a warning to the user, if the run takes them.
func (p *Provider) warnf(format string, a ...any) {
	if p.warn == nil {
		return
	}
	p.warnMu.Lock()
	defer p.warnMu.Unlock()
	p.warn(fmt.Sprintf(format, a...))
}

var (
	_ source.Provider[Range, Entry]      = (*Provider)(nil)
	_ source.Disposer[Range]             = (*Provider)(nil)
	_ tree.Reader[string, *chunk, Entry] = walkReader{}
	_ tree.Disposer[*chunk]              = walkReader{}
)

// New validates root and returns a provider for it. A root that is not a
// directory (a file, as du and ls take one) lists as its one entry, keyed
// by its name.
func New(root string, opt Options) (*Provider, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if opt.Fields == Statx && (opt.Portable || !nativeEngine) {
		return nil, errors.New("the statx tier needs the Linux engine")
	}
	p := &Provider{root: filepath.Clean(root), opt: opt, native: nativeEngine && !opt.Portable, file: !fi.IsDir(), rootInfo: fi}
	if opt.OneFilesystem {
		if p.dev, err = rootDevice(fi, opt.Fields); err != nil {
			return nil, err
		}
	}
	p.Walker = tree.NewWalker(Range{Dir: p.root}, walkReader{p}, tree.Options[Entry]{NoDirs: opt.NoDirs, Size: entrySize})
	return p, nil
}

func entrySize(e *Entry) int64 { return e.Size }

// Stats returns the counters so far: the provider's and the walker's.
func (p *Provider) Stats() Stats {
	w := p.Walker.Stats()
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.stats
	s.Samples = slices.Clone(s.Samples)
	s.Files, s.Dirs, s.Bytes, s.Skipped, s.OtherDevice = w.Files, w.Dirs, w.Bytes, w.Omitted, w.Pruned
	s.PrunedDirs = w.Excluded
	return s
}

// walkReader is the provider as the walker's tree.Reader.
type walkReader struct{ p *Provider }

// Dispose releases a chunk that will never be read: its share of its
// directory descriptor.
func (rd walkReader) Dispose(c *chunk) { disposeChunk(Range{Seg: c, InSeg: true}) }

// Read lists one range.
func (rd walkReader) Read(ctx context.Context, r Range, out *Out) error {
	p := rd.p
	// A chunk's share of its descriptor is released however Read ends.
	defer disposeChunk(r)
	var st Stats
	var err error
	if p.file {
		err = p.expandFile(out, &st)
	} else if p.native {
		err = p.expandNative(ctx, r, out, &st)
	} else {
		err = p.expandPortable(ctx, r, out, &st)
	}
	p.mu.Lock()
	p.stats.add(&st)
	p.mu.Unlock()
	return err
}

// emit hands a directory's sorted rows to out, which splices each
// subdirectory in as a child range right after its DIR row. The run's own
// output (Skip) is dropped, a directory with its subtree, an entry of any
// other kind alone (tree.Omit); a directory on another device is listed
// and not walked (tree.Prune).
func (p *Provider) emit(r Range, rows []Row, out *Out) error {
	return out.Rows(rows, func(row *Row) (string, tree.Walk) {
		switch {
		case len(p.opt.Skip) > 0 && p.opt.Skip[row.Key]:
			return "", tree.Omit
		case row.Data.Kind != KindDir:
			return "", tree.Leaf
		case p.otherDevice(row):
			return "", tree.Prune
		}
		key := row.Key
		return r.Dir + string(os.PathSeparator) + key[len(r.Prefix):len(key)-1], tree.Descend
	})
}

// expandFile lists a root that is not a directory: its one row, keyed by
// its name, read as its directory's listing reads the entry: at the run's
// tier, a symbolic link as itself (New's stat followed it only to find
// that it does not name a directory).
func (p *Provider) expandFile(out *Out, st *Stats) error {
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
		}
	}
	if err != nil {
		st.note(&st.StatErrors, p.root, err)
		return nil
	}
	if row.Data.Kind == KindSymlink && p.opt.LinkTarget && p.opt.Fields != Names {
		if t, err := os.Readlink(p.root); err == nil {
			row.Data.LinkTarget, row.Data.Present = filepath.ToSlash(t), row.Data.Present|HasLink
		} else {
			st.note(&st.ReadlinkErrors, p.root, err)
		}
	}
	return p.emit(Range{}, []Row{row}, out)
}

func sortRows(rows []Row) {
	slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
}

// expandPortable lists a directory with os.File.ReadDir and Lstat.
func (p *Provider) expandPortable(ctx context.Context, r Range, out *Out, st *Stats) error {
	f, ok := openChecked(r, st)
	if !ok {
		return nil
	}
	st.DirsRead++
	defer f.Close() // statEntry stats relative to it on darwin
	ents, err := f.ReadDir(-1)
	if err != nil {
		st.note(&st.ReadErrors, r.Dir, err)
	}
	rows := make([]Row, 0, len(ents))
	for i, d := range ents {
		if i%1024 == 1023 && ctx.Err() != nil { // stat-heavy loop: stay cancellable
			return ctx.Err()
		}
		name := d.Name()
		row := Row{Key: r.Prefix + name}
		path := r.Dir + string(os.PathSeparator) + name
		if p.opt.Fields == Names {
			row.Data.Kind = kindOf(d.Type())
		} else {
			if err := statEntry(f, d, &row.Data); err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					st.note(&st.Vanished, path, err)
				} else {
					st.note(&st.StatErrors, path, err)
				}
				continue
			}
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
	if p.unsorted {
		st.Unsorted++
	} else {
		sortRows(rows)
	}
	return p.emit(r, rows, out)
}

// openChecked opens a directory by path. Below the root it then checks that
// the path is still a real directory (Lstat: not a symlink) and the same
// object as the opened file (fstat): best-effort protection against a
// directory swapped for a symlink between listing and open.
func openChecked(r Range, st *Stats) (*os.File, bool) {
	f, err := os.Open(r.Dir)
	if err != nil {
		st.note(&st.OpenErrors, r.Dir, err)
		return nil, false
	}
	if r.Prefix == "" {
		return f, true
	}
	ffi, err1 := f.Stat()
	lfi, err2 := os.Lstat(r.Dir)
	if err1 != nil || err2 != nil || !ffi.IsDir() || !lfi.IsDir() || lfi.Mode()&fs.ModeSymlink != 0 || !os.SameFile(ffi, lfi) {
		f.Close()
		st.note(&st.Unsafe, r.Dir, errors.New("directory changed before open"))
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
