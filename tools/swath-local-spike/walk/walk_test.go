// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package walk

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

type collect struct {
	mu   sync.Mutex
	rows []record.Row
}

func (c *collect) Emit(rows []record.Row) []record.Row {
	c.mu.Lock()
	c.rows = append(c.rows, rows...)
	c.mu.Unlock()
	return rows[:0]
}

// buildTree makes nested dirs, files, symlinks (file, dir, dangling), a fifo,
// an unreadable dir, a non-UTF-8 name and one large directory.
func buildTree(t *testing.T) (root string, unreadable string) {
	t.Helper()
	root = t.TempDir()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range []string{"a/b/c", "a/b2", "x y/é", "big"} {
		must(os.MkdirAll(filepath.Join(root, d), 0o755))
	}
	for _, f := range []string{"top.txt", "a/1", "a/b/2", "a/b/c/3", "a/b2/4", "x y/é/ü.txt", "a.b", "a-b"} {
		must(os.WriteFile(filepath.Join(root, f), []byte("hello"), 0o644))
	}
	for i := 0; i < 5000; i++ {
		must(os.WriteFile(filepath.Join(root, "big", "f"+itoa(i)), nil, 0o644))
	}
	must(os.Symlink("top.txt", filepath.Join(root, "link-file")))
	must(os.Symlink("a", filepath.Join(root, "link-dir")))
	must(os.Symlink("/nonexistent", filepath.Join(root, "dangling")))
	if runtime.GOOS != "windows" {
		must(mkfifo(filepath.Join(root, "a", "fifo")))
		must(os.WriteFile(filepath.Join(root, "bad\xffname"), nil, 0o644))
	}
	unreadable = filepath.Join(root, "locked")
	must(os.MkdirAll(filepath.Join(unreadable, "inner"), 0o755))
	must(os.WriteFile(filepath.Join(unreadable, "secret"), nil, 0o644))
	must(os.Chmod(unreadable, 0o000))
	t.Cleanup(func() { os.Chmod(unreadable, 0o755) })
	return root, unreadable
}

func itoa(i int) string { return fmtInt(i) }

func fmtInt(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	return string(b)
}

// expected keys via filepath.WalkDir (directories with a trailing slash),
// skipping non-UTF-8 names and, when unreadable as this user, the locked dir's contents.
func expected(t *testing.T, root string, dirs bool) []string {
	var keys []string
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == root {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if !utf8Valid(rel) {
			return nil
		}
		if d.IsDir() {
			if dirs {
				keys = append(keys, rel+"/")
			}
			return nil // an unreadable dir yields err on the second callback; contents absent
		}
		keys = append(keys, rel)
		return nil
	})
	slices.Sort(keys)
	return keys
}

func utf8Valid(s string) bool { return string([]rune(s)) == s }

func engines() []string {
	e := []string{EngineParallelLstat, EnginePortable, EngineWalkDir}
	if runtime.GOOS == "linux" {
		e = append(e, EngineFstatat, EngineGetdents)
	}
	return e
}

func TestWalkMatchesWalkDir(t *testing.T) {
	root, _ := buildTree(t)
	for _, dirs := range []bool{true, false} {
		want := expected(t, root, dirs)
		for _, eng := range engines() {
			tiers := []Fields{FieldsNames, FieldsStat}
			if eng == EngineFstatat || eng == EngineGetdents {
				tiers = append(tiers, FieldsStatx)
			}
			for _, tier := range tiers {
				for _, workers := range []int{1, 3} {
					c := &collect{}
					st, err := Walk(root, Options{Engine: eng, Workers: workers, Fields: tier, NoDirs: !dirs, LinkTarget: true, SplitEntries: 1000, InoSort: eng == EngineGetdents}, c)
					if err != nil {
						t.Fatal(err)
					}
					var got []string
					for _, r := range c.rows {
						got = append(got, r.Key)
					}
					slices.Sort(got)
					if !slices.Equal(got, want) {
						t.Fatalf("%s/%s/w%d dirs=%v: got %d keys, want %d\nfirst diff: %v", eng, tier, workers, dirs, len(got), len(want), firstDiff(got, want))
					}
					if runtime.GOOS != "windows" && st.InvalidUTF8 != 1 {
						t.Errorf("%s/%s: invalid utf8 count %d", eng, tier, st.InvalidUTF8)
					}
					if (eng == EngineFstatat || eng == EngineGetdents) && tier != FieldsNames && st.SplitBatches == 0 {
						t.Errorf("%s/%s: 5000-entry dir was not split", eng, tier)
					}
					if os.Geteuid() != 0 && st.OpenErrors == 0 {
						t.Errorf("%s/%s: unreadable dir not counted", eng, tier)
					}
					checkKinds(t, eng, tier, c.rows)
				}
			}
		}
	}
}

func checkKinds(t *testing.T, eng string, tier Fields, rows []record.Row) {
	t.Helper()
	by := map[string]record.Row{}
	for _, r := range rows {
		by[r.Key] = r
	}
	want := map[string]record.Kind{"top.txt": record.KindFile, "link-file": record.KindSymlink, "link-dir": record.KindSymlink, "dangling": record.KindSymlink}
	if runtime.GOOS != "windows" {
		want["a/fifo"] = record.KindFIFO
	}
	for k, kind := range want {
		if by[k].Kind != kind {
			t.Errorf("%s/%s: %s kind %v want %v", eng, tier, k, by[k].Kind, kind)
		}
	}
	if d, ok := by["a/b/"]; ok && d.Kind != record.KindDir {
		t.Errorf("a/b/ kind %v", d.Kind)
	}
	top := by["top.txt"]
	switch tier {
	case FieldsNames:
		if top.Present != 0 {
			t.Errorf("%s names tier filled presence %b", eng, top.Present)
		}
	default:
		if top.Size != 5 || !top.Has(record.HasSize|record.HasMtime|record.HasMode) {
			t.Errorf("%s/%s: top.txt size %d presence %b", eng, tier, top.Size, top.Present)
		}
		if runtime.GOOS == "linux" && (!top.Has(record.HasStat) || top.Inode == 0 || top.Nlink != 1) {
			t.Errorf("%s/%s: top.txt stat fields missing: %+v", eng, tier, top)
		}
		if l := by["link-file"]; l.LinkTarget != "top.txt" || !l.Has(record.HasLink) {
			t.Errorf("%s/%s: link target %q", eng, tier, l.LinkTarget)
		}
	}
}

func firstDiff(a, b []string) string {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return a[i] + " vs " + b[i]
		}
	}
	return "length"
}

func TestRootMustBeDir(t *testing.T) {
	f := filepath.Join(t.TempDir(), "f")
	os.WriteFile(f, nil, 0o644)
	if _, err := Walk(f, Options{}, &collect{}); err == nil {
		t.Fatal("expected error for file root")
	}
}

func TestSplitEntriesOneRejected(t *testing.T) {
	root := t.TempDir()
	os.WriteFile(filepath.Join(root, "f"), nil, 0o644)
	if _, err := Walk(root, Options{SplitEntries: 1}, &collect{}); err == nil {
		t.Fatal("SplitEntries 1 must be rejected")
	}
	if _, err := WalkOrdered(root, OrderDFSSequential, Options{SplitEntries: 1}, 0, &collect{}); err != nil {
		t.Fatal(err) // ordered modes force splitting off
	}
}

// Review L3: path-based engines refuse a child directory that is (now) a symlink.
func TestOpenDirCheckedRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on Windows")
	}
	base := t.TempDir()
	real := filepath.Join(base, "real")
	os.Mkdir(real, 0o755)
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	w := &worker{opt: &Options{}}
	if f, ok := openDirChecked(w, dirJob{path: link, prefix: "x/"}); ok {
		f.Close()
		t.Fatal("symlinked child directory was opened")
	}
	if w.stats.UnsafeDirs != 1 {
		t.Fatalf("unsafe count %d", w.stats.UnsafeDirs)
	}
	f, ok := openDirChecked(w, dirJob{path: real, prefix: "x/"})
	if !ok {
		t.Fatal("real directory refused")
	}
	f.Close()
	if f, ok := openDirChecked(w, dirJob{path: link, prefix: ""}); !ok { // the root may be a symlink
		t.Fatal("symlinked root refused")
	} else {
		f.Close()
	}
}
