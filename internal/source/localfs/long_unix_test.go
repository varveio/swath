// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"
)

// kernelPathMax is the length from which the kernel refuses a path: PATH_MAX
// (4,096 on Linux, 1,024 on macOS) counts the terminating NUL.
var kernelPathMax = map[string]int{"linux": 4096, "darwin": 1024}[runtime.GOOS]

// longName is level i's directory name: 200 bytes, distinct per level.
func longName(i int) string { return fmt.Sprintf("%03d", i) + strings.Repeat("x", 197) }

// deepTree builds a chain of levels directories below root through
// directory handles (mkdirat, openat), past any path length limit. The
// directory at each level holds a file s (5 bytes) and the next level; the
// last holds a file f, a symbolic link l to f, and a directory d holding a
// file g. It returns the chain's names and every key, sorted.
func deepTree(t *testing.T, root string, levels int) (names, keys []string) {
	t.Helper()
	dir, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	prefix := ""
	for i := range levels {
		name := longName(i)
		writeAt(t, dir, "s")
		if err := unix.Mkdirat(dir, name, 0o755); err != nil {
			t.Fatal(err)
		}
		next, err := unix.Openat(dir, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		unix.Close(dir)
		if err != nil {
			t.Fatal(err)
		}
		dir = next
		names = append(names, name)
		keys = append(keys, prefix+"s", prefix+name+"/")
		prefix += name + "/"
	}
	defer unix.Close(dir)
	writeAt(t, dir, "f")
	if err := unix.Symlinkat("f", dir, "l"); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkdirat(dir, "d", 0o755); err != nil {
		t.Fatal(err)
	}
	d := atLevel(t, dir, "d")
	writeAt(t, d, "g")
	unix.Close(d)
	keys = append(keys, prefix+"f", prefix+"l", prefix+"d/", prefix+"d/g")
	slices.Sort(keys)
	return names, keys
}

// writeAt creates the file name in dir holding "hello".
func writeAt(t *testing.T, dir int, name string) {
	t.Helper()
	fd, err := unix.Openat(dir, name, unix.O_CREAT|unix.O_WRONLY|unix.O_CLOEXEC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if _, err := unix.Write(fd, []byte("hello")); err != nil {
		t.Fatal(err)
	}
}

// atLevel opens the directory names leads to from dir, one name at a time.
func atLevel(t *testing.T, dir int, names ...string) int {
	t.Helper()
	fd, err := unix.Dup(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		next, err := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if err != nil {
			t.Fatal(err)
		}
		fd = next
	}
	return fd
}

// rootFD opens root for atLevel.
func rootFD(t *testing.T, root string) int {
	t.Helper()
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(fd) })
	return fd
}

// levelPath is the path of the directory names[:k] leads to.
func levelPath(root string, names []string, k int) string {
	return root + "/" + strings.Join(names[:k], "/")
}

// rangeAt walks p from its root to the directory names lead to, one
// Expand a level, and returns that directory's range, which holds its
// reference on the directory it is opened from: the caller expands or
// disposes it. p's counters are cleared after the walk.
func rangeAt(t *testing.T, p *Provider, names []string) Range {
	t.Helper()
	r := p.Root()
	for i := range names {
		var x expansion
		if err := p.Expand(context.Background(), r, &x); err != nil {
			t.Fatal(err)
		}
		want := strings.Join(names[:i+1], "/") + "/"
		next := slices.IndexFunc(x.kids, func(k Range) bool { return k.chunk == nil && k.prefix == want })
		if next < 0 {
			t.Fatalf("level %d: no range for %.40q among %d", i+1, want, len(x.kids))
		}
		r = x.kids[next]
		disposeAll(p, slices.Delete(x.kids, next, next+1))
	}
	p.mu.Lock()
	p.stats = Stats{}
	p.mu.Unlock()
	return r
}

// disposeAll releases ranges that will not be expanded.
func disposeAll(p *Provider, rs []Range) {
	for _, r := range rs {
		p.Dispose(r)
	}
}

// expansion collects one Expand's rows and child ranges.
type expansion struct {
	rows []Row
	kids []Range
}

func (x *expansion) Rows(rows []Row) error { x.rows = append(x.rows, rows...); return nil }
func (x *expansion) Child(r Range) error   { x.kids = append(x.kids, r); return nil }

// TestLongPaths: a tree whose paths pass PATH_MAX, to a little under the
// key cap, lists whole with exact keys, sizes and link targets and no
// error, with either engine at the stat and names tiers, each directory
// opened from its parent's descriptor or, with no descriptor kept, from
// the root's through every name above it. Exactly the directories whose
// path the kernel would refuse are counted (long_paths), every one opened.
func TestLongPaths(t *testing.T) {
	root := t.TempDir()
	names, keys := deepTree(t, root, 160)
	long := 0 // the directories whose path is refused: the root's descendants at least kernelPathMax long
	for k := 1; k <= len(names); k++ {
		if len(levelPath(root, names, k)) >= kernelPathMax {
			long++
		}
	}
	if len(levelPath(root, names, len(names)))+len("/d") >= kernelPathMax {
		long++
	}
	last := strings.Join(names, "/") + "/"
	if len(last)+len("d/g") > maxKeyBytes || long < 140 {
		t.Fatalf("fixture: %d bytes, %d long directories", len(last), long)
	}
	dirs := int64(len(names) + 1) // the chain and d
	for _, kept := range []bool{true, false} {
		for _, portable := range []bool{false, true} {
			for _, fields := range []Fields{Stat, Names} {
				t.Run(fmt.Sprintf("kept=%v/portable=%v/%v", kept, portable, fields), func(t *testing.T) {
					p, err := New(root, Options{Fields: fields, Portable: portable, LinkTarget: true})
					if err != nil {
						t.Fatal(err)
					}
					if !kept {
						p.keep.limit = 0
					}
					rows, st := walk(t, p, 4, 1<<20)
					var got []string
					for _, r := range rows {
						got = append(got, r.Key)
					}
					if !slices.Equal(got, keys) {
						t.Fatalf("%d keys, want %d", len(got), len(keys))
					}
					if st.Errors() != 0 || len(st.Samples) != 0 {
						t.Fatalf("errors %d: %q", st.Errors(), st.Samples)
					}
					lp := st.LongPaths
					if lp.Attempts != int64(long) || lp.Opened != lp.Attempts || len(lp.Failed) != 0 ||
						lp.MaxDepth != int64(len(names)+1) || lp.MaxPathBytes != int64(len(root)+1+len(last)+len("d")) {
						t.Fatalf("long_paths %+v, want %d attempts, depth %d", lp, long, len(names)+1)
					}
					// Not kept, every directory below the root is refused and
					// each below the first level opened again from the root.
					do, redescents, refused := st.DirOpens, int64(0), int64(0)
					if !kept {
						redescents, refused = dirs-1, dirs
					}
					if do.Opened != dirs || do.Redescents != redescents || do.Refused != refused || len(do.Failed) != 0 ||
						do.MaxKept != p.keep.limit || do.PeakKept < 1 || (!kept && do.PeakKept != 1) || p.keep.open.Load() != 0 {
						t.Fatalf("dir_opens %+v, want %d opened, %d redescents, %d refused", do, dirs, redescents, refused)
					}
					if fields == Names {
						return
					}
					for _, r := range rows {
						switch r.Key {
						case last + "f", last + "d/g":
							if r.Data.Kind != KindFile || r.Data.Size != 5 || !r.Data.Has(HasStat) {
								t.Fatalf("%s: %+v", r.Key[len(last):], r.Data)
							}
						case last + "l":
							if r.Data.Kind != KindSymlink || r.Data.LinkTarget != "f" || !r.Data.Has(HasLink) {
								t.Fatalf("l: %+v", r.Data)
							}
						}
					}
				})
			}
		}
	}
}

// TestKeyTooLong: an entry whose key passes the cap is left out, counted
// and named (key_too_long, unreadable), and a directory's subtree with it;
// the entries within the cap beside it are listed.
func TestKeyTooLong(t *testing.T) {
	root := t.TempDir()
	_, keys := deepTree(t, root, 165)
	var want []string
	for _, k := range keys {
		if len(k) <= maxKeyBytes {
			want = append(want, k)
		}
	}
	for _, portable := range []bool{false, true} {
		rows, st := list(t, root, Options{Portable: portable}, 4, 1<<20)
		var got []string
		for _, r := range rows {
			got = append(got, r.Key)
		}
		if !slices.Equal(got, want) || len(got) == len(keys) {
			t.Fatalf("portable=%v: %d keys, want %d of %d", portable, len(got), len(want), len(keys))
		}
		if st.KeyTooLong != 1 || st.Errors() != 1 || len(st.Samples) != 1 || !strings.Contains(st.Samples[0], "passes the limit of 32768") {
			t.Fatalf("portable=%v: key_too_long %d, errors %d, samples %.300q", portable, st.KeyTooLong, st.Errors(), st.Samples)
		}
	}
}

// TestLongPathSwaps: a component of a long path replaced since its parent
// was listed is never traversed. A directory opened from its parent's
// descriptor does not pass through the names above it, so a replacement
// there leaves its listing whole. Opened again from the root (no
// descriptor kept), it refuses each: a symbolic link to the same directory
// (symlink, unsafe_dirs), a file (not_directory) and nothing there
// (not_found, open_errors), naming the level refused. The directory itself
// replaced is refused either way. Each refusal counts as a directory that
// could not be opened.
func TestLongPathSwaps(t *testing.T) {
	const levels, target = 40, 35 // the range's directory: past PATH_MAX
	cases := []struct {
		name   string
		at     int // the component replaced
		swap   func(dir int, name string) error
		reason string
	}{
		{"symlink-intermediate", 29, func(dir int, name string) error { return unix.Symlinkat("moved", dir, name) }, "symlink"},
		{"symlink-final", target - 1, func(dir int, name string) error { return unix.Symlinkat("moved", dir, name) }, "symlink"},
		{"file-intermediate", 29, func(dir int, name string) error {
			fd, err := unix.Openat(dir, name, unix.O_CREAT|unix.O_WRONLY|unix.O_CLOEXEC, 0o644)
			if err == nil {
				unix.Close(fd)
			}
			return err
		}, "not_directory"},
		{"removed", 29, func(int, string) error { return nil }, "not_found"},
	}
	for _, c := range cases {
		for _, kept := range []bool{true, false} {
			for _, portable := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/kept=%v/portable=%v", c.name, kept, portable), func(t *testing.T) {
					root := t.TempDir()
					names, _ := deepTree(t, root, levels)
					p, err := New(root, Options{Portable: portable})
					if err != nil {
						t.Fatal(err)
					}
					if !kept {
						p.keep.limit = 0
					}
					r := rangeAt(t, p, names[:target])
					if len(r.path) < kernelPathMax {
						t.Fatalf("fixture: %d bytes", len(r.path))
					}
					parent := atLevel(t, rootFD(t, root), names[:c.at]...)
					defer unix.Close(parent)
					if err := unix.Renameat(parent, names[c.at], parent, "moved"); err != nil {
						t.Fatal(err)
					}
					if err := c.swap(parent, names[c.at]); err != nil {
						t.Fatal(err)
					}
					var x expansion
					if err := p.Expand(context.Background(), r, &x); err != nil {
						t.Fatal(err)
					}
					disposeAll(p, x.kids)
					st := p.Stats()
					if kept && c.at < target-1 {
						if len(x.rows) != 2 || st.Errors() != 0 || st.DirOpens.Opened != 1 {
							t.Fatalf("opened from its parent: %d rows, stats %+v", len(x.rows), st)
						}
						return
					}
					unsafe, open := int64(0), int64(1)
					if c.reason == "symlink" {
						unsafe, open = 1, 0
					}
					if len(x.rows) != 0 || len(x.kids) != 0 || st.Unsafe != unsafe || st.OpenErrors != open ||
						st.LongPaths.Failed[c.reason] != 1 || len(st.LongPaths.Failed) != 1 || st.DirOpens.Failed[c.reason] != 1 {
						t.Fatalf("%d rows, %d children, unsafe %d, open errors %d, long_paths %+v, dir_opens %+v, samples %.200q",
							len(x.rows), len(x.kids), st.Unsafe, st.OpenErrors, st.LongPaths, st.DirOpens, st.Samples)
					}
					if want := fmt.Sprintf("level %d below the root", c.at+1); !kept && !strings.Contains(st.Samples[0], want) {
						t.Fatalf("sample %.200q, want %q", st.Samples[0][len(st.Samples[0])-120:], want)
					}
				})
			}
		}
	}
}

// TestLongPathOneFilesystem: with --one-file-system each directory opened
// below the root is checked against the root's device, and every
// component of a descent from an ancestor: the first on another device
// stops it, the directory is not walked, and that is no error
// (other_device). The other device is simulated by giving the provider
// another root device once the directory's range is queued.
func TestLongPathOneFilesystem(t *testing.T) {
	root := t.TempDir()
	names, _ := deepTree(t, root, 40)
	for _, kept := range []bool{true, false} {
		for _, other := range []bool{false, true} {
			p, err := New(root, Options{OneFilesystem: true})
			if err != nil {
				t.Fatal(err)
			}
			if !kept {
				p.keep.limit = 0
			}
			r := rangeAt(t, p, names[:35])
			if other {
				p.dev++
				// The descent from the root stops at its first name.
				if _, err := p.descend(context.Background(), r); !kept && (!errors.Is(err, errOtherDevice) || !strings.Contains(err.Error(), "level 1 below the root")) {
					t.Fatalf("a descent from the root: %v", err)
				}
			}
			var x expansion
			if err := p.Expand(context.Background(), r, &x); err != nil {
				t.Fatal(err)
			}
			disposeAll(p, x.kids)
			st := p.Stats()
			if other {
				if len(x.rows) != 0 || st.OtherDevice != 1 || st.Errors() != 0 || st.LongPaths.Failed["other_device"] != 1 || st.DirOpens.Failed["other_device"] != 1 {
					t.Fatalf("kept=%v, other device: %d rows, stats %+v", kept, len(x.rows), st)
				}
			} else if len(x.rows) != 2 || st.OtherDevice != 0 || st.LongPaths.Opened != 1 {
				t.Fatalf("kept=%v, same device: %d rows, stats %+v", kept, len(x.rows), st)
			}
			if n := p.keep.open.Load(); n != 0 {
				t.Fatalf("kept=%v: %d descriptors kept after", kept, n)
			}
		}
	}
}

// stopAfter is a context whose Err reports cancellation from its n+1st call.
type stopAfter struct {
	context.Context
	n atomic.Int64
}

func (c *stopAfter) Err() error {
	if c.n.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

// TestLongPathCancelled: a run stopped at any step of a descent from the
// root returns the context's error, counts no failure and holds no
// descriptor after.
func TestLongPathCancelled(t *testing.T) {
	root := t.TempDir()
	names, _ := deepTree(t, root, 40)
	p, err := New(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	p.keep.limit = 0
	r := rangeAt(t, p, names[:35])
	defer p.Dispose(r)
	if r.below != 35 {
		t.Fatalf("a descent of %d names, want 35", r.below)
	}
	before := countFDs(t)
	for k := range 35 {
		ctx := &stopAfter{Context: context.Background()}
		ctx.n.Store(int64(k))
		var st Stats
		if d, ok, err := p.openRange(ctx, r, &st); ok || !errors.Is(err, context.Canceled) {
			if ok {
				d.release()
			}
			t.Fatalf("stopped after %d steps: opened %v, %v", k, ok, err)
		}
		if st.Errors() != 0 || st.LongPaths.Attempts != 1 || st.LongPaths.Opened != 0 || len(st.LongPaths.Failed) != 0 || len(st.DirOpens.Failed) != 0 {
			t.Fatalf("stopped after %d steps: %+v", k, st)
		}
		if n := countFDs(t); n != before {
			t.Fatalf("stopped after %d steps: %d descriptors open, %d before", k, n, before)
		}
	}
}

// countFDs counts the process's open descriptors.
func countFDs(t *testing.T) int {
	t.Helper()
	fd, err := unix.Open("/dev/fd", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	buf := make([]byte, 1<<16)
	n := 0
	for {
		got, err := unix.ReadDirent(fd, buf)
		if err != nil {
			t.Fatal(err)
		}
		if got == 0 {
			return n // the descriptor reading it included (ParseDirent leaves out . and ..)
		}
		_, c, _ := unix.ParseDirent(buf[:got], -1, nil)
		n += c
	}
}

// TestLongPathLowDescriptorLimit: a walk of a deep tree holds a few
// descriptors per worker, not one per level: it lists whole under a limit
// a few above what the process holds, and releases them all.
func TestLongPathLowDescriptorLimit(t *testing.T) {
	root := t.TempDir()
	_, keys := deepTree(t, root, 60)
	var lim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		t.Fatal(err)
	}
	before := countFDs(t)
	low := lim
	low.Cur = uint64(before + 24)
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &low); err != nil {
		t.Skip(err)
	}
	rows, st := list(t, root, Options{}, 4, 1<<20)
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(keys) || st.Errors() != 0 || st.LongPaths.Opened == 0 {
		t.Fatalf("%d rows of %d, errors %d %.200q", len(rows), len(keys), st.Errors(), st.Samples)
	}
	if n := countFDs(t); n != before {
		t.Fatalf("%d descriptors open, %d before", n, before)
	}
}

// TestLongRoot: a root whose own path passes PATH_MAX lists as any root,
// keys below it; a file root there reads as its directory's listing reads
// it, a symbolic link as itself; a root with one name past the
// filesystem's limit stays an error.
func TestLongRoot(t *testing.T) {
	base := t.TempDir()
	names, keys := deepTree(t, base, 40)
	rel := strings.Join(names[:25], "/") + "/"
	root := base + "/" + strings.TrimSuffix(rel, "/")
	if len(root) < kernelPathMax {
		t.Fatalf("fixture: %d bytes", len(root))
	}
	var want []string
	for _, k := range keys {
		if after, ok := strings.CutPrefix(k, rel); ok && after != "" {
			want = append(want, after)
		}
	}
	last := root + "/" + strings.Join(names[25:], "/")
	for _, portable := range []bool{false, true} {
		for _, fields := range []Fields{Stat, Names} {
			opt := Options{Portable: portable, Fields: fields, LinkTarget: true}
			rows, st := list(t, root, opt, 4, 1<<20)
			var got []string
			for _, r := range rows {
				got = append(got, r.Key)
			}
			if !slices.Equal(got, want) || st.Errors() != 0 || st.LongPaths.Opened != st.LongPaths.Attempts {
				t.Fatalf("portable=%v %v: %d keys, want %d; errors %d %.200q", portable, fields, len(got), len(want), st.Errors(), st.Samples)
			}
			dir, _ := list(t, last, opt, 2, 1<<20)
			for _, name := range []string{"f", "l"} {
				rows, st := list(t, last+"/"+name, opt, 2, 100)
				i := slices.IndexFunc(dir, func(r Row) bool { return r.Key == name })
				if len(rows) == 1 && i >= 0 {
					rows[0].Data.Atime = dir[i].Data.Atime // reading a link's target touches it
				}
				if len(rows) != 1 || i < 0 || rows[0] != dir[i] || st.Errors() != 0 || st.LongPaths.Opened == 0 {
					t.Fatalf("portable=%v %v %s: file root %+v, listed %+v, stats %+v", portable, fields, name, rows, dir, st)
				}
			}
		}
	}
	for _, r := range []string{base + "/" + strings.Repeat("y", 300), root + "/" + strings.Repeat("y", 300)} {
		if _, err := New(r, Options{}); !errors.Is(err, unix.ENAMETOOLONG) {
			t.Fatalf("a name of 300 bytes at %d: %v", len(r), err)
		}
	}
}
