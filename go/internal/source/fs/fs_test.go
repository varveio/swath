// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package fs

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
)

type collect struct {
	mu   sync.Mutex
	rows []schema.Row
}

func (c *collect) Write(_ context.Context, segs [][]schema.Row) error {
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
		if !utf8.ValidString(rel) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
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

func list(t *testing.T, root string, opt Options, workers, budget int) ([]schema.Row, Stats) {
	t.Helper()
	p, err := New(root, opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range](context.Background(), p, c, order.Options{Workers: workers, Budget: budget}); err != nil {
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
						if runtime.GOOS != "windows" && st.InvalidUTF8 != 2 {
							t.Errorf("invalid UTF-8 count %d, want 2", st.InvalidUTF8)
						}
						if os.Geteuid() != 0 && st.OpenErrors == 0 {
							t.Error("unreadable directory not counted")
						}
						if !portable && nativeEngine && fields != Names && st.Chunks == 0 {
							t.Error("5000-entry directory was not chunked")
						}
						checkRows(t, rows, fields, portable)
					})
				}
			}
		}
	}
}

func checkRows(t *testing.T, rows []schema.Row, fields Fields, portable bool) {
	t.Helper()
	by := map[string]schema.Row{}
	for _, r := range rows {
		by[r.Key] = r
	}
	want := map[string]schema.Kind{"z": schema.KindFile, "link-file": schema.KindSymlink, "link-dir": schema.KindSymlink, "dangling": schema.KindSymlink}
	if runtime.GOOS != "windows" {
		want["mixed/fifo"] = schema.KindFIFO
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
	if z.Size != 5 || !z.Has(schema.HasSize|schema.HasMtime|schema.HasMode) {
		t.Errorf("z: size %d presence %b", z.Size, z.Present)
	}
	if runtime.GOOS == "linux" && (!z.Has(schema.HasStat) || z.Inode == 0 || z.Nlink != 1) {
		t.Errorf("z: stat fields missing: %+v", z)
	}
	if l := by["link-file"]; l.LinkTarget != "z" || !l.Has(schema.HasLink) {
		t.Errorf("link target %q", l.LinkTarget)
	}
	if fields == Statx && !z.Has(schema.HasBtime) {
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
	if _, err := order.Run[Range](ctx, p, &collect{}, order.Options{Workers: 2, Budget: 10}); err == nil {
		t.Fatal("expected cancellation")
	}
}
