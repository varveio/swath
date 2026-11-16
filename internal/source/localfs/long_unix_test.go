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

// levelRange is the range of the directory names[:k] leads to.
func levelRange(root string, names []string, k int) Range {
	rel := strings.Join(names[:k], "/")
	return Range{path: root + "/" + rel, prefix: rel + "/"}
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
// error, with either engine at the stat and names tiers. Exactly the
// directories whose path the kernel refuses are opened below the root
// (long_paths), every one of them successfully.
func TestLongPaths(t *testing.T) {
	root := t.TempDir()
	names, keys := deepTree(t, root, 160)
	long := 0 // the directories whose path is refused: the root's descendants at least kernelPathMax long
	for k := 1; k <= len(names); k++ {
		if len(levelRange(root, names, k).path) >= kernelPathMax {
			long++
		}
	}
	if len(levelRange(root, names, len(names)).path)+len("/d") >= kernelPathMax {
		long++
	}
	last := strings.Join(names, "/") + "/"
	if len(last)+len("d/g") > maxKeyBytes || long < 140 {
		t.Fatalf("fixture: %d bytes, %d long directories", len(last), long)
	}
	for _, portable := range []bool{false, true} {
		for _, fields := range []Fields{Stat, Names} {
			t.Run(fmt.Sprintf("portable=%v/%v", portable, fields), func(t *testing.T) {
				rows, st := list(t, root, Options{Fields: fields, Portable: portable, LinkTarget: true}, 4, 1<<20)
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
// was listed is never traversed: a symbolic link to the same directory, in
// the middle of the path or at its end, is refused (symlink), as are a
// file there (not_directory) and nothing there (not_found); each counts as
// a directory that could not be opened.
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
		for _, portable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/portable=%v", c.name, portable), func(t *testing.T) {
				root := t.TempDir()
				names, _ := deepTree(t, root, levels)
				r := levelRange(root, names, target)
				if len(r.path) < kernelPathMax {
					t.Fatalf("fixture: %d bytes", len(r.path))
				}
				p, err := New(root, Options{Portable: portable})
				if err != nil {
					t.Fatal(err)
				}
				var before expansion
				if err := p.Expand(context.Background(), r, &before); err != nil || len(before.rows) != 2 {
					t.Fatalf("before the swap: %v, %d rows", err, len(before.rows))
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
				st := p.Stats()
				if len(x.rows) != 0 || len(x.kids) != 0 || st.OpenErrors != 1 || st.LongPaths.Failed[c.reason] != 1 || len(st.LongPaths.Failed) != 1 {
					t.Fatalf("%d rows, %d children, open errors %d, long_paths %+v, samples %.200q", len(x.rows), len(x.kids), st.OpenErrors, st.LongPaths, st.Samples)
				}
				if want := fmt.Sprintf("level %d below the root", c.at+1); !strings.Contains(st.Samples[0], want) {
					t.Fatalf("sample %.200q, want %q", st.Samples[0][len(st.Samples[0])-120:], want)
				}
			})
		}
	}
}

// TestLongPathOneFilesystem: with --one-file-system each component opened
// below the root is checked against the root's device, and the first on
// another device stops the descent: the directory is not walked, and that
// is no error (other_device). The other device is simulated by giving the
// provider another root device.
func TestLongPathOneFilesystem(t *testing.T) {
	root := t.TempDir()
	names, _ := deepTree(t, root, 40)
	r := levelRange(root, names, 35)
	for _, other := range []bool{false, true} {
		p, err := New(root, Options{OneFilesystem: true})
		if err != nil {
			t.Fatal(err)
		}
		if other {
			p.dev++
		}
		var x expansion
		if err := p.Expand(context.Background(), r, &x); err != nil {
			t.Fatal(err)
		}
		st := p.Stats()
		if other {
			if len(x.rows) != 0 || st.OtherDevice != 1 || st.Errors() != 0 || st.LongPaths.Failed["other_device"] != 1 {
				t.Fatalf("other device: %d rows, stats %+v", len(x.rows), st)
			}
		} else if len(x.rows) != 2 || st.OtherDevice != 0 || st.LongPaths.Opened != 1 {
			t.Fatalf("same device: %d rows, stats %+v", len(x.rows), st)
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

// TestLongPathCancelled: a run stopped at any step of the descent below
// the root returns the context's error, counts no failure and holds no
// descriptor after.
func TestLongPathCancelled(t *testing.T) {
	root := t.TempDir()
	names, _ := deepTree(t, root, 40)
	r := levelRange(root, names, 35)
	p, err := New(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	before := countFDs(t)
	for k := range 35 {
		ctx := &stopAfter{Context: context.Background()}
		ctx.n.Store(int64(k))
		var st Stats
		if fd, ok, err := p.openLong(ctx, r, &st); ok || !errors.Is(err, context.Canceled) {
			if ok {
				unix.Close(fd)
			}
			t.Fatalf("stopped after %d steps: opened %v, %v", k, ok, err)
		}
		if st.Errors() != 0 || st.LongPaths.Attempts != 1 || st.LongPaths.Opened != 0 || len(st.LongPaths.Failed) != 0 {
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
