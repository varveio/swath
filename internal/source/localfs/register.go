// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/totals"
)

// The local filesystem takes roots without a scheme.
func init() {
	registry.Register(registry.Factory{EncoderBytes: parquetout.EncoderBytes(&Schema), Scheme: "", Flags: flags, Defaults: func() any { return &Options{LinkTarget: true} }, Open: open, Workers: defaultWorkers, CheckTotals: checkTotals, CheckFilter: checkFilter,
		Title: "Local", Example: "/data", Usage: "a local directory or file"})
}

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

// defaultWorkers is the default --workers for a local tree.
func defaultWorkers() int { return min(maxWorkers, workersPerCPU*runtime.GOMAXPROCS(0)) }

// Set, String and Type make Fields a flag value.
func (f *Fields) Set(s string) error {
	v, err := ParseFields(s)
	if err == nil {
		*f = v
	}
	return err
}

func (f *Fields) Type() string { return "string" }

func flags(f *pflag.FlagSet, _ bool, _ source.SharedValues) any {
	o := &Options{LinkTarget: true}
	source.Define(f, source.Flag{Name: "metadata", Value: &o.Fields, Metavar: "names|stat|statx", Default: o.Fields.String(),
		Usage: "what is read per entry (statx adds birth_time; Linux)"})
	source.Define(f, source.Flag{Name: "no-dirs", Value: source.Switch(&o.NoDirs), Usage: "omit the directories' rows (kind DIR)"})
	source.Define(f, source.Flag{Name: "no-link-target", Value: source.Off(&o.LinkTarget), Usage: "do not read symbolic links' targets"})
	source.Define(f, source.Flag{Name: "one-file-system", Familiar: []string{"one-filesystem"}, Value: source.Switch(&o.OneFilesystem),
		Usage: "do not walk into a mount point below the root, as du -x"})
	return o
}

// checkTotals refuses totals the names tier cannot give
// (registry.Factory.CheckTotals): the allocated bytes and the hard links
// are read from each entry's stat.
func checkTotals(options any, t totals.Options) error {
	if o, ok := options.(*Options); ok && o.Fields == Names && (t.BytesColumn == totals.AllocatedBytes || t.LinksOnce) {
		return errors.New("--allocated and --count-links-once read each entry's stat: not with --metadata names")
	}
	return nil
}

// checkFilter refuses filters on what a local tree does not read
// (registry.Factory.CheckFilter): storage classes ever, sizes and times at
// the names tier.
func checkFilter(options any, f *filter.Spec) error {
	size, mtime, class := f.Reads()
	switch o, _ := options.(*Options); {
	case class:
		return errors.New("--storage-class: a local tree has no storage classes")
	case (size || mtime) && o != nil && o.Fields == Names:
		return errors.New("--min-size, --max-size, --modified-since and --modified-until read each entry's stat: not with --metadata names")
	}
	return nil
}

// open validates the root. SWATH_LOCALFS_ENGINE=portable selects the
// portable engine (comparisons).
func open(_ context.Context, req registry.Request) (registry.Source, error) {
	o, ok := req.Options.(*Options)
	if !ok {
		return nil, fmt.Errorf("localfs: options are %T, not *localfs.Options", req.Options)
	}
	opt := *o
	if os.Getenv("SWATH_LOCALFS_ENGINE") == "portable" {
		opt.Portable = true
	}
	root, err := filepath.Abs(req.Root)
	if err != nil {
		return nil, err
	}
	p, err := New(root, opt)
	var pe *fs.PathError
	switch {
	case errors.As(err, &pe):
		// Named as given: nosuchdir: no such file or directory.
		return nil, fmt.Errorf("%s: %w", req.Root, pe.Err)
	case err != nil:
		return nil, err
	}
	p.warn = req.Warn
	return &opened{Bound: registry.Bind[Range, Entry](p, &Schema), p: p}, nil
}

// oneFilesystemFilter and noDirsFilter are --one-file-system and --no-dirs
// in the manifest's coverage.filters.
const (
	oneFilesystemFilter = "one_filesystem"
	noDirsFilter        = "no_dirs"
)

type opened struct {
	registry.Bound[Entry]
	p *Provider
}

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

// Run lists the tree. A count runs with more processors (countProcsFactor
// per CPU at a stat tier, namesCountProcsFactor at the names tier, at most
// the workers) and restores the setting when it returns; a count has no
// encoders, reorder storage or readahead, the other readers of GOMAXPROCS.
// Every run that writes output keeps the setting as it is. A count's rows
// are not ordered: neither engine sorts a directory's entries (names in a
// directory are distinct, and nothing consumes the order).
func (o *opened) Run(ctx context.Context, env registry.Env) (order.Stats, parquetout.Encoder, error) {
	prune, plan, err := env.Filter.PruneDirectories()
	if err != nil {
		return order.Stats{}, nil, err
	}
	o.p.prune = prune
	o.p.mu.Lock()
	o.p.stats.Pruning = &plan
	o.p.mu.Unlock()
	if env.Count {
		o.p.unsorted = true
		env.Order.Unordered = true
		factor := countProcsFactor
		if o.p.opt.Fields == Names {
			factor = namesCountProcsFactor
		}
		prev := runtime.GOMAXPROCS(0)
		if procs := min(prev*factor, max(prev, env.Order.Workers)); procs > prev {
			runtime.GOMAXPROCS(procs)
			defer runtime.GOMAXPROCS(prev)
			o.p.mu.Lock()
			o.p.stats.CountProcs = int64(procs)
			o.p.mu.Unlock()
		}
	}
	return o.Bound.Run(ctx, env)
}

func (o *opened) Locator() source.Locator {
	return source.Locator{Kind: "local-fs", Root: o.p.root, Path: o.p.root}
}

func (o *opened) Listed() int64 {
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
// renames over, existing yet or not.
func (o *opened) Exclude(path string) error {
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
func (o *opened) locate(path string) (string, error) {
	if fi, err := os.Stat(path); err == nil && fi.IsDir() {
		rel, inside, err := dataset.LocateInside(o.p.root, path)
		if !inside {
			return "", err
		}
		return rel + "/", err
	}
	dir, name := splitParent(path)
	rfi, err := os.Stat(o.p.root)
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

// splitParent splits path at its last separator into the directory as
// spelled, which the system resolves (not lexically cleaned:
// link/.. is the parent of link's target, not the directory holding
// link), and the name in it.
func splitParent(path string) (dir, name string) {
	i := len(path) - 1
	for i >= len(filepath.VolumeName(path)) && !os.IsPathSeparator(path[i]) {
		i--
	}
	dir, name = path[:i+1], path[i+1:]
	if dir == "" {
		dir = "."
	}
	return dir, name
}

// Report: an entry counts as skipped when an error kept it (or its subtree)
// out; a link whose target could not be read is still listed, and an entry
// that vanished is no longer part of the tree.
func (o *opened) Report() source.Report {
	st := o.p.Stats()
	cov := source.Coverage{Target: o.p.root, Complete: true, StopReason: "completed", Skipped: st.Errors() - st.ReadlinkErrors - st.Vanished}
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
	return source.Report{Name: "local", Stats: &st, Errors: st.Errors(), Fields: o.p.opt.Fields.String(), Coverage: cov, Filesystem: o.p.filesystemSpace(),
		Brief: fmt.Sprintf("files=%d dirs=%d bytes=%d errors=%d", st.Files, st.Dirs, st.Bytes, st.Errors())}
}

// Capabilities: a directory is read whole and sorted; there is no seek and
// no resume.
func (o *opened) Capabilities() source.Capabilities {
	return source.Capabilities{Seek: source.SeekNone, Order: source.OrderNone}
}

var (
	_ source.Capable  = (*opened)(nil)
	_ source.Excluder = (*opened)(nil)
	_ source.Reporter = (*opened)(nil)
)
