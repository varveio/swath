// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
)

type collect struct {
	mu   sync.Mutex
	rows []Row
}

// unicodeNames: the file system refuses names that are not UTF-8 (NTFS names
// are UTF-16; APFS requires UTF-8).
var unicodeNames = runtime.GOOS == "windows" || runtime.GOOS == "darwin"

// permissionBits: chmod 0 makes a directory unreadable (Windows keeps only a
// read-only attribute, which does not stop a directory being listed).
var permissionBits = runtime.GOOS != "windows"

func (c *collect) Write(_ context.Context, segs [][]Row) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		c.rows = append(c.rows, s...)
	}
	return nil
}

func mk(t *testing.T, root, rel string, dir bool) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	var err error
	if dir {
		err = os.MkdirAll(p, 0o755)
	} else {
		err = os.WriteFile(p, []byte("hello"), 0o644)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// buildTree has the names that make the trailing-slash rule matter (a-b,
// a.b, a, a0, ab, a b) as files in one directory and as directories in
// another, bytes above 0x7f, symlinks (file, dir, dangling), a FIFO, a
// non-UTF-8 name, an unreadable directory and a directory large enough to be
// chunked.
func buildTree(t *testing.T) string {
	root := t.TempDir()
	tricky := []string{"a-b", "a.b", "a", "a0", "ab", "a b", "z", "~", "\x7f", "é", "日本"}
	for i, n := range tricky {
		mk(t, root, n, i%2 == 1)
		mk(t, root, "mixed/"+n, i%2 == 0)
		if i%2 == 1 {
			mk(t, root, n+"/inner.txt", false)
		} else {
			mk(t, root, "mixed/"+n+"/sub/leaf", false)
		}
	}
	mk(t, root, "empty", true)
	for i := range 5000 {
		mk(t, root, "big/f"+strconv.Itoa(i), false)
	}
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.Symlink("z", filepath.Join(root, "link-file")))
	must(os.Symlink("mixed", filepath.Join(root, "link-dir")))
	must(os.Symlink("/nonexistent", filepath.Join(root, "dangling")))
	if runtime.GOOS != "windows" {
		must(mkfifo(filepath.Join(root, "mixed", "fifo")))
	}
	if !unicodeNames {
		must(os.WriteFile(filepath.Join(root, "bad\xffname"), nil, 0o644))
		mk(t, root, "bad\xffdir/hidden", false)
	}
	mk(t, root, "locked/secret", false)
	must(os.Chmod(filepath.Join(root, "locked"), 0))
	t.Cleanup(func() { os.Chmod(filepath.Join(root, "locked"), 0o755) })
	return root
}

// oracle lists root with filepath.WalkDir and sorts the keys.
func oracle(t *testing.T, root string, dirs bool) []string {
	var keys []string
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if dirs {
				keys = append(keys, rel+"/")
			}
			return nil
		}
		keys = append(keys, rel)
		return nil
	})
	slices.Sort(keys)
	return keys
}

func list(t *testing.T, root string, opt Options, workers, budget int) ([]Row, Stats) {
	t.Helper()
	p, err := New(root, opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range, Entry](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: int64(budget) * 256}); err != nil {
		t.Fatal(err)
	}
	return c.rows, p.Stats()
}

func TestListingEqualsSortedOracle(t *testing.T) {
	root := buildTree(t)
	for _, portable := range []bool{false, true} {
		for _, fields := range []Fields{Names, Stat, Statx} {
			if fields == Statx && (portable || !nativeEngine) {
				continue
			}
			for _, noDirs := range []bool{false, true} {
				for _, wb := range [][2]int{{1, 1}, {4, 7}, {8, 1 << 20}} {
					name := fmt.Sprintf("portable=%v/%s/nodirs=%v/w%d_b%d", portable, fields, noDirs, wb[0], wb[1])
					t.Run(name, func(t *testing.T) {
						rows, st := list(t, root, Options{Fields: fields, NoDirs: noDirs, LinkTarget: true, Portable: portable}, wb[0], wb[1])
						keys := make([]string, len(rows))
						for i := range rows {
							keys[i] = rows[i].Key
						}
						want := oracle(t, root, !noDirs)
						if !slices.Equal(keys, want) {
							t.Fatalf("sequence differs: got %d keys, want %d", len(keys), len(want))
						}
						// Names that are not UTF-8 are listed with their exact bytes.
						if !unicodeNames && (!slices.Contains(keys, "bad\xffname") || !slices.Contains(keys, "bad\xffdir/hidden")) {
							t.Error("names that are not UTF-8 were not listed byte for byte")
						}
						if permissionBits && os.Geteuid() != 0 && st.OpenErrors == 0 {
							t.Error("unreadable directory not counted")
						}
						if !portable && nativeEngine && fields != Names && st.Chunks == 0 {
							t.Error("5000-entry directory was not chunked")
						}
						// The 5000-entry directory takes more than one read: a stat tier hints between them.
						if !portable && nativeEngine && (st.ReaddirplusHints > 0) != (fields != Names) {
							t.Errorf("readdirplus_hints %d at the %s tier", st.ReaddirplusHints, fields)
						}
						checkRows(t, rows, fields, portable)
					})
				}
			}
		}
	}
}

func checkRows(t *testing.T, rows []Row, fields Fields, portable bool) {
	t.Helper()
	by := map[string]Entry{}
	for _, r := range rows {
		by[r.Key] = r.Data
	}
	want := map[string]Kind{"z": KindFile, "link-file": KindSymlink, "link-dir": KindSymlink, "dangling": KindSymlink}
	if runtime.GOOS != "windows" {
		want["mixed/fifo"] = KindFIFO
	}
	for k, kind := range want {
		if by[k].Kind != kind {
			t.Errorf("%s: kind %v, want %v", k, by[k].Kind, kind)
		}
	}
	z := by["z"]
	if fields == Names {
		if z.Present != 0 {
			t.Errorf("names tier filled values: %b", z.Present)
		}
		return
	}
	if z.Size != 5 || !z.Has(HasSize|HasMtime|HasMode) {
		t.Errorf("z: size %d presence %b", z.Size, z.Present)
	}
	if (runtime.GOOS == "linux" || runtime.GOOS == "darwin") && (!z.Has(HasStat) || z.Inode == 0 || z.Nlink != 1) {
		t.Errorf("z: stat fields missing: %+v", z)
	}
	if runtime.GOOS == "darwin" && !z.Has(HasBtime) {
		t.Errorf("z: no birth time on darwin: %+v", z)
	}
	if l := by["link-file"]; l.LinkTarget != "z" || !l.Has(HasLink) {
		t.Errorf("link target %q", l.LinkTarget)
	}
	if fields == Statx && !z.Has(HasBtime) {
		t.Log("filesystem reports no birth time") // tmpfs, older kernels
	}
}

func TestSkipDirectory(t *testing.T) {
	root := t.TempDir()
	for _, f := range []string{"a/1", "a/out/data/x", "b"} {
		mk(t, root, f, false)
	}
	for _, portable := range []bool{false, true} {
		rows, st := list(t, root, Options{Skip: map[string]bool{"a/out/": true}, Portable: portable}, 2, 100)
		var keys []string
		for _, r := range rows {
			keys = append(keys, r.Key)
		}
		if !slices.Equal(keys, []string{"a/", "a/1", "b"}) || st.Skipped != 1 {
			t.Fatalf("portable=%v: keys %q skipped %d", portable, keys, st.Skipped)
		}
	}
}

func TestOpenCheckedRejectsSymlinkedChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks need privileges on Windows")
	}
	base := t.TempDir()
	real := filepath.Join(base, "real")
	link := filepath.Join(base, "link")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	var st Stats
	if f, ok := openChecked(Range{path: link, prefix: "x/"}, &st); ok {
		f.Close()
		t.Fatal("symlinked child directory was opened")
	}
	if st.Unsafe != 1 {
		t.Fatalf("unsafe count %d", st.Unsafe)
	}
	if f, ok := openChecked(Range{path: link}, &st); !ok { // the root may be a symlink
		t.Fatal("symlinked root refused")
	} else {
		f.Close()
	}
}

func TestCancelledExpand(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "a", false)
	p, err := New(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := order.Run[Range, Entry](ctx, p, &collect{}, &Schema, order.Options{Workers: 2, Budget: 10 * 256}); err == nil {
		t.Fatal("expected cancellation")
	}
}

// TestCancelReleasesChunkFDs: a run that fails while large-directory chunks
// are still queued releases their shared directory descriptor (the order
// stage disposes ranges it never expands).
func TestCancelReleasesChunkFDs(t *testing.T) {
	if !nativeEngine {
		t.Skip("chunks are a getdents-engine feature")
	}
	root := t.TempDir()
	for d := range 6 {
		dir := filepath.Join(root, fmt.Sprintf("big%d", d))
		os.Mkdir(dir, 0o755)
		for i := range 5000 {
			os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%05d", i)), nil, 0o644)
		}
	}
	fds := func() int { ents, _ := os.ReadDir("/proc/self/fd"); return len(ents) }
	before := -1
	for i := range 6 { // the first run warms up the runtime's own descriptors (poller)
		if i == 1 {
			before = fds()
		}
		p, err := New(root, Options{Fields: Stat})
		if err != nil {
			t.Fatal(err)
		}
		c := &failAfter{n: 3000}
		if _, err := order.Run[Range, Entry](context.Background(), p, c, &Schema, order.Options{Workers: 4, Budget: 2000 * 256}); err == nil {
			t.Fatal("sink failure not reported")
		}
	}
	if after := fds(); after > before+2 {
		ents, _ := os.ReadDir("/proc/self/fd")
		var targets []string
		for _, e := range ents {
			l, _ := os.Readlink("/proc/self/fd/" + e.Name())
			targets = append(targets, l)
		}
		t.Fatalf("descriptors leaked: %d before, %d after: %v", before, after, targets)
	}
}

type failAfter struct{ n, seen int }

func (f *failAfter) Write(_ context.Context, segs [][]Row) error {
	for _, s := range segs {
		f.seen += len(s)
	}
	if f.seen >= f.n {
		return errors.New("sink failed")
	}
	return nil
}

// TestUnsortedCountEqualsSortedListing: a count does not sort a
// directory's entries (opened.Run sets unsorted and order's Unordered), yet
// it sees the same keys, sizes and counts as a sorted listing, at every
// tier and with a chunked directory.
func TestUnsortedCountEqualsSortedListing(t *testing.T) {
	root := buildTree(t)
	for _, portable := range []bool{false, true} {
		for _, fields := range []Fields{Names, Stat, Statx} {
			if fields == Statx && (portable || !nativeEngine) {
				continue
			}
			t.Run(fmt.Sprintf("portable=%v/%s", portable, fields), func(t *testing.T) {
				opt := Options{Fields: fields, LinkTarget: true, Portable: portable}
				sorted, want := list(t, root, opt, 4, 7)
				src, err := open(context.Background(), registry.Request{Root: root, Options: &opt, Workers: 4})
				if err != nil {
					t.Fatal(err)
				}
				p := src.(*opened).p
				st, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 4}})
				if err != nil {
					t.Fatal(err)
				}
				got := p.Stats()
				if st.Rows != int64(len(sorted)) || got.Files != want.Files || got.Dirs != want.Dirs || got.Bytes != want.Bytes {
					t.Fatalf("count: rows %d files %d dirs %d bytes %d; listing: rows %d files %d dirs %d bytes %d",
						st.Rows, got.Files, got.Dirs, got.Bytes, len(sorted), want.Files, want.Dirs, want.Bytes)
				}
				if got.Unsorted != got.DirsRead {
					t.Fatalf("unsorted_dirs %d, want every directory read (%d)", got.Unsorted, got.DirsRead)
				}
				// The Linux engine streams: every directory read is a batch at least, the 5000-entry
				// directory several, and nothing is chunked or held for a sort.
				if !portable && nativeEngine && (got.StreamedBatches <= got.DirsRead || got.Chunks != 0 || got.ParallelSorts != 0) {
					t.Fatalf("streamed_batches %d for %d directories, chunks %d, parallel_sorts %d", got.StreamedBatches, got.DirsRead, got.Chunks, got.ParallelSorts)
				}
				// The same keys, through a sink on the provider the run left unsorted.
				c := &collect{}
				if _, err := order.CountWithSink[Range, Entry](context.Background(), p, nil, order.Options{Workers: 4, Unordered: true}, c); err != nil {
					t.Fatal(err)
				}
				keys := make([]string, len(c.rows))
				for i := range c.rows {
					keys[i] = c.rows[i].Key
				}
				slices.Sort(keys)
				if !slices.Equal(keys, oracle(t, root, true)) {
					t.Fatalf("count saw %d keys, want the listing's %d", len(keys), len(sorted))
				}
			})
		}
	}
}
