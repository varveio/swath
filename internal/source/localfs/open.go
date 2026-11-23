// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/longpath"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/reader"
)

// workersPerCPU and maxWorkers bound the default --workers for a local tree:
// workersPerCPU per processor (GOMAXPROCS, which follows the CPU affinity and
// cgroup quota), at most maxWorkers, the object stores' default. On a 1-CPU,
// 512 MiB cgroup (ext4 on a persistent disk, cold, issue #388; 2 rounds) 4
// workers were fastest or tied on a nested tree of ~1M entries (28.3 s,
// 28.6 s at 64, 44.6 s at 1) and on 32 directories of 60,000 files (19.9 s
// and 90 MiB peak, 49.1 s and 261 MiB at 64). On one directory of 1M files
// 1 worker was fastest (119 s, 187 s at 4, 176 s at 64), every count there
// limited by the memory a whole directory holds. On a 4-vCPU machine
// (GOMAXPROCS 16), 1M files in one directory on XFS, ext4, btrfs and ZFS
// plateau from 4 to 16 workers, warm and cold.
const (
	workersPerCPU = 4
	maxWorkers    = 64
)

// DefaultWorkers is the default --workers for a local tree.
func DefaultWorkers() int { return min(maxWorkers, workersPerCPU*runtime.GOMAXPROCS(0)) }

// LinksRead says a listing at tier f reads each entry's link count, device
// and inode: a stat tier, where the platform's stat has them (statLinks:
// not Windows).
func LinksRead(f Fields) bool { return f != Names && statLinks }

// BlocksRead says a listing at tier f reads each entry's allocated blocks
// (allocated_bytes): a stat tier, where the platform's stat has them
// (statBlocks: not Windows).
func BlocksRead(f Fields) bool { return f != Names && statBlocks }

// Opened is a local tree opened by Open: the provider bound to its row
// schema (Bound), and what a run asks of an opened source besides its
// rows: the rows listed, the run's own output left out (Exclude), the
// locator, the report and the capabilities. Begin readies it for its one
// listing. It chooses no writer; the command joins it with one
// (internal/cli).
type Opened struct {
	bound bind.Bound[Entry]
	p     *Provider
}

// Open validates root, a directory or a file, and opens it with opt (New);
// warn (nil: no one) is told of a directory left out. A root that cannot
// be had is refused naming root as given.
func Open(root string, opt Options, warn func(code, msg string)) (*Opened, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	p, err := New(abs, opt)
	var pe *fs.PathError
	switch {
	case errors.As(err, &pe):
		// Named as given: nosuchdir: no such file or directory.
		return nil, fmt.Errorf("%s: %w", root, pe.Err)
	case err != nil:
		return nil, err
	}
	p.warn = warn
	b, err := bind.Provider[Range, Entry](p, &Schema)
	if err != nil {
		return nil, err
	}
	return &Opened{bound: b, p: p}, nil
}

// Bound is the provider bound to its row schema.
func (o *Opened) Bound() bind.Bound[Entry] { return o.bound }

// oneFilesystemFilter and noDirsFilter are --one-file-system and --no-dirs
// in the manifest's coverage.filters: options that take no value.
var (
	oneFilesystemFilter = reader.Filter{Filter: "one_file_system"}
	noDirsFilter        = reader.Filter{Filter: "no_dirs"}
)

// countProcsFactor is how many processors a count runs with, per CPU the
// process had, at a stat tier and at the names tier. A stat or a directory
// read that waits on the disk holds its processor until the scheduler hands
// it off, so with one processor per CPU a cold tree leaves CPUs idle; more
// processors keep more of them in flight. Measured on 4 CPUs: stat tier, cold,
// ~1M entries, 3.97 s at 4 processors, 3.07 s at 16, warm no slower; names
// tier, cold, 2.18 s at 4, 1.59 s at 8, 1.46 s at 16, warm no slower at 8 but
// up to 8 % slower at 16 (no stat to wait on, more contention), hence 2.
const (
	countProcsFactor      = 4
	namesCountProcsFactor = 2
)

// Begin readies o for its one listing, before it starts: the directories
// f prunes and the keys it leaves out by name alone
// (filter.Spec.PruneDirectories, KeyExcluded; f may be nil), and
// unreadable (nil: no one), told the key prefix of each directory that
// cannot be read. A count's rows are not ordered: neither engine sorts a
// directory's entries for one (names in a directory are distinct, and
// nothing consumes the order), so Begin sets opt.Unordered. A count runs
// with more processors (countProcsFactor per CPU at a stat tier,
// namesCountProcsFactor at the names tier, at most opt.Workers) until end
// restores the setting; a count has no encoders, reorder storage or
// readahead, the other readers of GOMAXPROCS. Every listing that writes
// output keeps the setting as it is.
func (o *Opened) Begin(f *filter.Spec, unreadable func(prefix string), count bool, opt *order.Options) (end func(), err error) {
	prune, plan, err := f.PruneDirectories()
	if err != nil {
		return nil, err
	}
	o.p.prune = prune
	if o.p.keyOut, err = f.KeyExcluded(); err != nil {
		return nil, err
	}
	o.p.mu.Lock()
	o.p.stats.Pruning = &plan
	o.p.mu.Unlock()
	o.p.unreadable = unreadable
	if !count {
		return func() {}, nil
	}

	o.p.unsorted = true
	opt.Unordered = true
	factor := countProcsFactor
	if o.p.opt.Fields == Names {
		factor = namesCountProcsFactor
	}
	prev := runtime.GOMAXPROCS(0)
	procs := min(prev*factor, max(prev, opt.Workers))
	if procs <= prev {
		return func() {}, nil
	}
	runtime.GOMAXPROCS(procs)
	o.p.mu.Lock()
	o.p.stats.CountProcs = int64(procs)
	o.p.mu.Unlock()
	return func() { runtime.GOMAXPROCS(prev) }, nil
}

// Locator records the root, the local path it is.
func (o *Opened) Locator() source.Locator {
	return source.Locator{Kind: "local", Root: o.p.root, Path: o.p.root}
}

// Listed is the rows emitted so far: every file, and every directory but
// with NoDirs.
func (o *Opened) Listed() int64 {
	o.p.mu.Lock()
	defer o.p.mu.Unlock()
	if o.p.opt.NoDirs {
		return o.p.stats.Files
	}
	return o.p.stats.Files + o.p.stats.Dirs
}

// Exclude leaves out path, an output the run writes, when it lies inside
// the root; it is called before listing starts. A directory (the dataset,
// a text file's scratch) is located by identity, where the run writes into
// it, and left out with its subtree; any other path (a text file, its
// temporary) is left out by its name in its directory, which a commit
// renames over, existing yet or not. A held root (OpenDir), which has no
// path, refuses it.
func (o *Opened) Exclude(path string) error {
	if o.p.held != nil {
		return errors.New("localfs: a held root has no path to locate an output in")
	}
	key, err := o.locate(path)
	if err != nil {
		return fmt.Errorf("locate output: %w", err)
	}
	if key != "" {
		if o.p.opt.Skip == nil {
			o.p.opt.Skip = map[string]bool{}
		}
		o.p.opt.Skip[key] = true
	}
	return nil
}

// locate is path's key below the root ("a/b/" for a directory, "a/b"
// otherwise), "" when it is not below it.
func (o *Opened) locate(path string) (string, error) {
	if runtime.GOOS == "windows" {
		// Windows removes "." and ".." from a path by its spelling
		// (GetFullPathName, which Abs calls) before it follows any link:
		// link\.. is the directory holding link there, unlike POSIX.
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
	}
	if fi, err := os.Stat(path); err == nil && fi.IsDir() {
		rel, inside, err := dataset.LocateInside(o.p.root, path)
		if !inside {
			return "", err
		}
		return rel + "/", err
	}
	dir, name := splitParent(path)
	name = storedName(dir, name)
	rfi, err := longpath.Stat(o.p.root)
	if err != nil {
		return "", err
	}
	if dfi, err := os.Stat(dir); err == nil && os.SameFile(dfi, rfi) {
		return name, nil // directly in the root
	}
	rel, inside, err := dataset.LocateInside(o.p.root, dir)
	if !inside {
		return "", err
	}
	return rel + "/" + name, err
}

// splitParent splits path after its last separator into the directory as
// spelled, which the system resolves (not lexically cleaned: on POSIX
// link/.. is the parent of link's target, not the directory holding
// link; locate has spelled a Windows path as Windows resolves it), and
// the name in it: dir+name is path, and dir+n names n there.
func splitParent(path string) (dir, name string) {
	i := len(path) - 1
	for i >= len(filepath.VolumeName(path)) && !os.IsPathSeparator(path[i]) {
		i--
	}
	dir, name = path[:i+1], path[i+1:]
	if dir == "" {
		dir = "." + string(os.PathSeparator)
	}
	return dir, name
}

// storedName is the spelling under which dir holds an existing entry
// named name, which the listing emits: name itself, unless the directory
// ignores case (macOS's default APFS, NTFS) and holds no entry spelled
// name but exactly one spelled otherwise that equals it without case and
// is the same entry by identity. A hard link to the file under another
// name, or a name only equal without case, is never taken for it.
func storedName(dir, name string) string {
	fi, err := os.Lstat(dir + name)
	if err != nil {
		return name // nothing there yet: the commit creates name
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return name
	}
	stored := ""
	for _, e := range ents {
		switch {
		case e.Name() == name:
			return name
		case strings.EqualFold(e.Name(), name):
			if efi, err := os.Lstat(dir + e.Name()); err == nil && os.SameFile(fi, efi) {
				if stored != "" {
					return name // two candidates: neither is taken
				}
				stored = e.Name()
			}
		}
	}
	if stored == "" {
		return name
	}
	return stored
}

// Report: an entry counts as unreadable when an error kept it (or its
// subtree) out; a link whose target could not be read is still listed, and
// an entry that vanished is no longer part of the tree.
func (o *Opened) Report() source.Report {
	st := o.p.Stats()
	unreadable := st.Errors() - st.ReadlinkErrors - st.Vanished
	cov := source.Coverage{Target: o.p.root, Complete: unreadable == 0, StopReason: source.StopEndOfScope, Unreadable: unreadable}
	cov.FilesystemBoundaries = o.p.filesystemBoundaries()
	cov.Exclusions = slices.Sorted(maps.Keys(o.p.opt.Skip))
	if o.p.opt.OneFilesystem {
		// The tree is listed only as far as its filesystem reaches; the
		// manifest records it so an application can tell.
		cov.Filters = append(cov.Filters, oneFilesystemFilter)
	}
	if o.p.opt.NoDirs {
		// The directories' rows are left out; the manifest records it so
		// an application can tell.
		cov.Filters = append(cov.Filters, noDirsFilter)
	}
	var collected []string
	if o.p.opt.LinkTarget && o.p.opt.Fields != Names {
		collected = []string{"link_target"}
	}
	return source.Report{Name: "local", Stats: &st, Fields: o.p.opt.Fields.String(), Collected: collected, Coverage: cov, Filesystem: o.p.filesystemSpace(),
		Brief: fmt.Sprintf("files=%d dirs=%d bytes=%d errors=%d", st.Files, st.Dirs, st.Bytes, st.Errors())}
}

// Capabilities: a directory is read whole and sorted; there is no seek and
// no resume.
func (o *Opened) Capabilities() source.Capabilities {
	return source.Capabilities{Seek: source.SeekNone, Order: source.OrderNone}
}

var (
	_ source.Capable  = (*Opened)(nil)
	_ source.Excluder = (*Opened)(nil)
	_ source.Reporter = (*Opened)(nil)
)
