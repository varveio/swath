// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source/tree"
	"github.com/varveio/swath/internal/textout"
)

// pruneReference deliberately walks every subtree, using the pre-existing
// row filter as its oracle. It never calls PruneDirectories.
func pruneReference(t *testing.T, root string, opt Options, spec filter.Spec) []Row {
	t.Helper()
	rows, _ := list(t, root, opt, 4, 7)
	keep, err := filter.Compile(&spec, &Schema)
	if err != nil {
		t.Fatal(err)
	}
	if keep != nil {
		rows = slices.DeleteFunc(rows, func(r Row) bool { return !keep(&r) })
	}
	return rows
}

// Compare every output column except atime: opening directories and reading
// symlink targets can change that value between the two walks.
func pruneText(t *testing.T, dst *bytes.Buffer) *textout.Out {
	t.Helper()
	var columns []string
	for _, c := range Schema.Specs() {
		if c.Name != "atime" {
			columns = append(columns, c.Name)
		}
	}
	out, err := textout.Open(textout.Config{Format: textout.JSONL, Stdout: dst, Columns: columns})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { out.Abort() })
	return out
}

func pruneReferenceText(t *testing.T, rows []Row) []byte {
	t.Helper()
	var dst bytes.Buffer
	out := pruneText(t, &dst)
	sink, err := textout.NewSink(out, &Schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), [][]Row{rows}); err != nil {
		t.Fatal(err)
	}
	if err := out.Commit(); err != nil {
		t.Fatal(err)
	}
	return dst.Bytes()
}

// Keep the byte comparison authoritative; decode only its first differing
// row to make a key/order difference distinguishable from metadata drift.
func pruneTextDifference(got, want []byte) string {
	actual, expected := bytes.Split(got, []byte{'\n'}), bytes.Split(want, []byte{'\n'})
	for i := range max(len(actual), len(expected)) {
		if i >= len(actual) || i >= len(expected) {
			return fmt.Sprintf("row %d: output ended, got %d lines, want %d", i+1, len(actual), len(expected))
		}
		if bytes.Equal(actual[i], expected[i]) {
			continue
		}
		var a, b map[string]json.RawMessage
		if json.Unmarshal(actual[i], &a) != nil || json.Unmarshal(expected[i], &b) != nil {
			return fmt.Sprintf("row %d: got %.1024q, want %.1024q", i+1, actual[i], expected[i])
		}
		var columns, changes []string
		for name := range a {
			columns = append(columns, name)
		}
		for name := range b {
			columns = append(columns, name)
		}
		slices.Sort(columns)
		for _, name := range slices.Compact(columns) {
			if !bytes.Equal(a[name], b[name]) {
				changes = append(changes, fmt.Sprintf("%s: got %.256s, want %.256s", name, a[name], b[name]))
			}
		}
		if len(changes) == 0 {
			return fmt.Sprintf("row %d: encoding differs: got %.1024q, want %.1024q", i+1, actual[i], expected[i])
		}
		return fmt.Sprintf("row %d key got=%.256s want=%.256s: %s", i+1, a["key"], b["key"], strings.Join(changes, "; "))
	}
	return "no differing row"
}

func pruneOpen(t *testing.T, root string, opt Options) registry.Source {
	t.Helper()
	factory, err := registry.Lookup(root)
	if err != nil {
		t.Fatal(err)
	}
	src, err := factory.Open(context.Background(), registry.Request{Root: root, Options: &opt, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func pruneRun(t *testing.T, root string, opt Options, spec filter.Spec, count bool, want []Row) Stats {
	t.Helper()
	src := pruneOpen(t, root, opt)
	var filtered, ordered atomic.Int64
	var dst bytes.Buffer
	env := registry.Env{Filter: &spec, Filtered: &filtered, Ordered: &ordered,
		Count: count, Order: order.Options{Workers: 4, Budget: 7 * 256}}
	if !count {
		env.Text = pruneText(t, &dst)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, _, err := src.Run(ctx, env)
	if err != nil {
		t.Fatal(err)
	}
	if !count {
		if err := env.Text.Commit(); err != nil {
			t.Fatal(err)
		}
		if expected := pruneReferenceText(t, want); !bytes.Equal(dst.Bytes(), expected) {
			t.Fatalf("ordered row payload differs: got %d bytes (%d rows), want %d bytes (%d rows); %s", dst.Len(), st.Rows, len(expected), len(want), pruneTextDifference(dst.Bytes(), expected))
		}
	}
	if st.Rows != int64(len(want)) || ordered.Load() != st.Rows {
		t.Fatalf("emitted rows=%d ordered=%d, want %d", st.Rows, ordered.Load(), len(want))
	}
	if src.Listed() != st.Rows+filtered.Load() {
		t.Fatalf("listed=%d, emitted=%d filtered=%d", src.Listed(), st.Rows, filtered.Load())
	}
	report := src.(*opened).Report()
	stats := *report.Stats.(*Stats)
	if stats.Errors() != 0 || report.Coverage.Skipped != 0 || !report.Coverage.Complete {
		t.Fatalf("intentional pruning damaged coverage: %+v stats %+v", report.Coverage, stats)
	}
	return stats
}

// Finish fixture mutations before either walk. Explicit times make fixture
// mtimes deterministic and distinct; the payload comparison keeps all its columns.
func pruneFinalizeTimes(t *testing.T, root string) {
	t.Helper()
	at := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Reverse preorder finalizes children before parents; distinct times also
	// catch metadata accidentally paired with another key on Windows.
	for i := len(paths) - 1; i >= 0; i-- {
		stamp := at.Add(time.Duration(i) * time.Second)
		if err := os.Chtimes(paths[i], stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
}

func pruneTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{
		"dir/hidden/deep.txt", "dir/keep.txt", "dir0/visible.txt", "dir-name/visible.txt",
		"outer/dir/visible.txt", "node_modules/pkg/node_modules/deeper/file.txt",
		"outer/node_modules/pkg/file.txt", "outer/node_modules-other/visible.txt",
		"outer/deep/node_modules/pkg/file.txt", "outer/keep.txt", "root.txt",
		"cache/file.tmp", "cache/nested/file.txt", "nested/cache/visible.txt",
		"é/inner.txt",
	} {
		mk(t, root, name, false)
	}
	mk(t, root, "empty", true)
	if runtime.GOOS != "windows" {
		mk(t, root, "odd\nname/inner.txt", false)
		if err := os.Symlink("dir", filepath.Join(root, "link-dir")); err != nil {
			t.Fatal(err)
		}
	}
	if !unicodeNames {
		mk(t, root, "bad\xffdir/inner.txt", false)
	}
	pruneFinalizeTimes(t, root)
	return root
}

// Each case is differential over complete ordered payloads and the count
// path. In particular a rejected DIR row says nothing about its children.
func TestPruneDifferential(t *testing.T) {
	root := pruneTree(t)
	cases := []struct {
		name   string
		spec   filter.Spec
		pruned bool
	}{
		{"directory-row-only", filter.Spec{Exclude: []string{"dir/"}}, false},
		{"basename-row-only", filter.Spec{Exclude: []string{"dir"}}, false},
		{"nested-row-only", filter.Spec{Exclude: []string{"**/node_modules/"}}, false},
		{"descendants", filter.Spec{Exclude: []string{"dir/**"}}, true},
		{"row-and-descendants", filter.Spec{Exclude: []string{"dir/", "dir/**"}}, true},
		{"root-anchor", filter.Spec{Exclude: []string{"/cache/**"}}, true},
		{"brace-and-wildcard", filter.Spec{Exclude: []string{"{dir,cache}/**", "outer/node_*/**"}}, true},
		{"repeated-segments", filter.Spec{Exclude: []string{"**/node_modules/**"}}, true},
		{"include-inside-excluded", filter.Spec{Include: []string{"**/*.txt"}, IncludeRegex: []string{"^root\\.txt$"}, Exclude: []string{"dir/**", "**/node_modules/**", "*.tmp"}, ExcludeRegex: []string{"^outer/keep\\.txt$"}}, true},
		{"regex-is-row-only", filter.Spec{ExcludeRegex: []string{"^dir/$"}}, false},
		{"regex-descendants", filter.Spec{ExcludeRegex: []string{"^dir/.+"}}, false},
		{"include-does-not-prune", filter.Spec{Include: []string{"dir/hidden/**"}}, false},
	}
	for _, portable := range []bool{false, true} {
		for _, fields := range []Fields{Names, Stat} {
			for _, noDirs := range []bool{false, true} {
				opt := Options{Fields: fields, NoDirs: noDirs, LinkTarget: true, Portable: portable}
				for _, tc := range cases {
					t.Run(fmt.Sprintf("portable=%v/%s/nodirs=%v/%s", portable, fields, noDirs, tc.name), func(t *testing.T) {
						want := pruneReference(t, root, opt, tc.spec)
						for _, count := range []bool{false, true} {
							stats := pruneRun(t, root, opt, tc.spec, count, want)
							if (stats.PrunedDirs > 0) != tc.pruned {
								t.Errorf("count=%v: pruned_dirs=%d, pruning expected=%v", count, stats.PrunedDirs, tc.pruned)
							}
							if tc.pruned && stats.Pruning == nil {
								t.Error("engaged pruning missing configuration classification")
							}
						}
					})
				}
			}
		}
	}
}

// A small explicit oracle protects the trailing-slash contract independently
// of the row-filter implementation used by the differential tests.
func TestPruneDirectoryRowSemantics(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "dir/sub/file", false)
	mk(t, root, "dir0/file", false)
	for _, tc := range []struct {
		exclude []string
		want    []string
	}{
		{[]string{"dir/"}, []string{"dir/sub/", "dir/sub/file", "dir0/", "dir0/file"}},
		{[]string{"dir/**"}, []string{"dir/", "dir0/", "dir0/file"}},
		{[]string{"dir/", "dir/**"}, []string{"dir0/", "dir0/file"}},
	} {
		spec := filter.Spec{Exclude: tc.exclude}
		rows := pruneReference(t, root, Options{Fields: Names}, spec)
		var got []string
		for _, row := range rows {
			got = append(got, row.Key)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("exclude %q: keys %q, want %q", tc.exclude, got, tc.want)
		}
		pruneRun(t, root, Options{Fields: Names}, spec, false, rows)
	}
}

// The emitter assertion is deterministic: reading fewer directories is not
// inferred from permissions, timing, or a subtree's eventual absence.
func TestPruneDoesNotSubmitChild(t *testing.T) {
	for _, noDirs := range []bool{false, true} {
		p, err := New(t.TempDir(), Options{NoDirs: noDirs})
		if err != nil {
			t.Fatal(err)
		}
		spec := filter.Spec{Exclude: []string{"drop/**"}}
		prune, _, err := spec.PruneDirectories()
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{{Key: "drop/", Data: Entry{Kind: KindDir}}, {Key: "drop0/", Data: Entry{Kind: KindDir}}, {Key: "tail", Data: Entry{Kind: KindFile}}}
		em := &pruneEmitter{}
		p.Walker = tree.NewWalker(p.Root(), pruneReader{p, rows}, tree.Options[Entry]{NoDirs: noDirs, Size: entrySize})
		p.SetPruneDirectories(prune)
		if err := p.Expand(context.Background(), p.Root(), em); err != nil {
			t.Fatal(err)
		}
		stats := p.Stats()
		if !slices.Equal(em.children, []string{"drop0/"}) {
			t.Fatalf("nodirs=%v: submitted children %q", noDirs, em.children)
		}
		want := []string{"drop/", "drop0/", "tail"}
		if noDirs {
			want = []string{"tail"}
		}
		if !slices.Equal(em.keys, want) || stats.PrunedDirs != 1 || stats.Dirs != 2 || stats.Files != 1 {
			t.Fatalf("nodirs=%v: rows %q stats %+v", noDirs, em.keys, stats)
		}
	}
}

// Supply rows through the same Reader/Walker seam used by directory reads.
type pruneReader struct {
	p    *Provider
	rows []Row
}

func (r pruneReader) Read(_ context.Context, dir Range, out *Out) error {
	return r.p.emit(dir, r.rows, out)
}

type pruneEmitter struct{ keys, children []string }

func (e *pruneEmitter) Rows(rows []Row) error {
	for _, row := range rows {
		e.keys = append(e.keys, row.Key)
	}
	return nil
}
func (e *pruneEmitter) Child(r Range) error { e.children = append(e.children, r.Prefix); return nil }

func TestPruneWideDirectory(t *testing.T) {
	root := t.TempDir()
	// Beyond the native engine's chunk and slab boundaries. Prunable
	// children occur before, within and after the run of ordinary files.
	for i := range 8500 {
		mk(t, root, fmt.Sprintf("wide/f%05d", i), false)
	}
	for _, name := range []string{"wide/a-drop/sub/file", "wide/f04000-drop/sub/file", "wide/z-drop/sub/file", "wide/z-drop0/keep"} {
		mk(t, root, name, false)
	}
	pruneFinalizeTimes(t, root)
	spec := filter.Spec{Exclude: []string{"wide/*-drop/**"}}
	for _, portable := range []bool{false, true} {
		for _, fields := range []Fields{Names, Stat} {
			t.Run(fmt.Sprintf("portable=%v/%s", portable, fields), func(t *testing.T) {
				opt := Options{Fields: fields, Portable: portable}
				want := pruneReference(t, root, opt, spec)
				for _, count := range []bool{false, true} {
					stats := pruneRun(t, root, opt, spec, count, want)
					if stats.PrunedDirs != 3 || stats.DirsRead != 3 {
						t.Fatalf("count=%v: pruned=%d read=%d, want 3 each", count, stats.PrunedDirs, stats.DirsRead)
					}
					if nativeEngine && !portable {
						if fields == Stat && stats.Chunks == 0 {
							t.Error("wide stat run did not exercise chunks")
						}
						if count && stats.Slabs == 0 {
							t.Error("wide count did not exercise slabs")
						}
					}
				}
			})
		}
	}
}

func TestPruneSkipAndOneFilesystem(t *testing.T) {
	root := pruneTree(t)
	mk(t, root, "out/parts/data", false)
	pruneFinalizeTimes(t, root)
	for _, portable := range []bool{false, true} {
		for _, oneFS := range []bool{false, true} {
			if oneFS && runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
				continue
			}
			opt := Options{Fields: Stat, Portable: portable, OneFilesystem: oneFS, Skip: map[string]bool{"out/": true}}
			spec := filter.Spec{Exclude: []string{"dir/**", "out/**"}}
			want := pruneReference(t, root, opt, spec)
			stats := pruneRun(t, root, opt, spec, false, want)
			if stats.Skipped != 1 || stats.PrunedDirs != 1 || stats.OtherDevice != 0 {
				t.Fatalf("portable=%v onefs=%v: %+v", portable, oneFS, stats)
			}
		}
	}
}

func TestPruneAllChildrenAndCancellation(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "a/deep/file", false)
	mk(t, root, "b/deep/file", false)
	spec := filter.Spec{Exclude: []string{"a/**", "b/**"}}
	for _, portable := range []bool{false, true} {
		opt := Options{Fields: Names, Portable: portable, NoDirs: true}
		for _, count := range []bool{false, true} {
			stats := pruneRun(t, root, opt, spec, count, nil)
			if stats.PrunedDirs != 2 || stats.DirsRead != 1 {
				t.Fatalf("portable=%v count=%v: %+v", portable, count, stats)
			}
		}
		src := pruneOpen(t, root, opt)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := src.Run(ctx, registry.Env{Filter: &spec, Count: true, Order: order.Options{Workers: 4}})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("portable=%v: canceled run: %v", portable, err)
		}
		if src.Listed() != 0 {
			t.Fatalf("canceled run listed %d rows", src.Listed())
		}
	}
}

// Exclude is how the output destination is installed by the command. It
// drops both its directory row and subtree, independently of row filtering.
func TestPruneOutputExclusion(t *testing.T) {
	root := t.TempDir()
	mk(t, root, "out/parts/data", false)
	mk(t, root, "drop/deep/file", false)
	mk(t, root, "keep", false)
	for _, portable := range []bool{false, true} {
		src := pruneOpen(t, root, Options{Fields: Names, Portable: portable})
		opened := src.(*opened)
		if err := opened.Exclude(filepath.Join(root, "out")); err != nil {
			t.Fatal(err)
		}
		spec := filter.Spec{Exclude: []string{"drop/**", "out/**"}}
		var filtered atomic.Int64
		st, _, err := src.Run(context.Background(), registry.Env{Count: true, Filter: &spec, Filtered: &filtered, Order: order.Options{Workers: 4}})
		if err != nil {
			t.Fatal(err)
		}
		stats := opened.p.Stats()
		if st.Rows != 2 || src.Listed() != 2 || filtered.Load() != 0 || stats.Skipped != 1 || stats.PrunedDirs != 1 || stats.DirsRead != 1 {
			t.Fatalf("portable=%v rows=%d listed=%d filtered=%d stats=%+v", portable, st.Rows, src.Listed(), filtered.Load(), stats)
		}
		if exclusions := opened.Report().Coverage.Exclusions; !slices.Equal(exclusions, []string{"out/"}) {
			t.Fatalf("output exclusions %q, want out/", exclusions)
		}
	}
}

// A metadata filter can reject a directory's own row while accepting a
// descendant. Only the unrelated recursive key exclude authorizes pruning;
// neither engine may make traversal depend on the row predicate's result.
func TestPruneMetadataRejectedParent(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"old-parent/future-file", "future-parent/old-file", "drop/hidden-file"} {
		mk(t, root, name, false)
	}
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	future := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	for name, at := range map[string]time.Time{
		"old-parent": old, "old-parent/future-file": future,
		"future-parent": future, "future-parent/old-file": old,
		"drop": future, "drop/hidden-file": future,
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	spec := filter.Spec{Since: "2040-01-01", Exclude: []string{"drop/**"}}
	for _, portable := range []bool{false, true} {
		t.Run(fmt.Sprintf("portable=%v", portable), func(t *testing.T) {
			opt := Options{Fields: Stat, Portable: portable}
			want := pruneReference(t, root, opt, spec)
			var keys []string
			for _, row := range want {
				keys = append(keys, row.Key)
			}
			if expected := []string{"drop/", "future-parent/", "old-parent/future-file"}; !slices.Equal(keys, expected) {
				t.Fatalf("metadata reference keys %q, want %q", keys, expected)
			}
			for _, count := range []bool{false, true} {
				stats := pruneRun(t, root, opt, spec, count, want)
				if stats.PrunedDirs != 1 || stats.DirsRead != 3 {
					t.Fatalf("count=%v: pruned=%d read=%d, want 1 and 3", count, stats.PrunedDirs, stats.DirsRead)
				}
			}
		})
	}
}

// An inaccessible descendant would make a full walk incomplete. A proven
// recursive exclude must stop before that open, without counting an error.
func TestPruneUnreadableDescendant(t *testing.T) {
	if !permissionBits || os.Geteuid() == 0 {
		t.Skip("requires enforced directory permissions and an unprivileged user")
	}
	root := t.TempDir()
	mk(t, root, "dir/locked/x", false)
	locked := filepath.Join(root, "dir", "locked")
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(locked, 0o755) })
	for _, portable := range []bool{false, true} {
		t.Run(fmt.Sprintf("portable=%v", portable), func(t *testing.T) {
			opt := Options{Fields: Stat, Portable: portable}
			control := pruneOpen(t, root, opt)
			if _, _, err := control.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 4}}); err != nil {
				t.Fatal(err)
			}
			report := control.(*opened).Report()
			stats := report.Stats.(*Stats)
			if stats.OpenErrors != 1 || report.Coverage.Skipped != 1 {
				t.Fatalf("control open errors=%d skipped=%d, want 1 each", stats.OpenErrors, report.Coverage.Skipped)
			}
			spec := filter.Spec{Exclude: []string{"dir/**"}}
			want := pruneReference(t, root, opt, spec)
			for _, count := range []bool{false, true} {
				stats := pruneRun(t, root, opt, spec, count, want)
				if stats.PrunedDirs != 1 || stats.Errors() != 0 {
					t.Fatalf("count=%v: pruned=%d errors=%d, want 1 and 0", count, stats.PrunedDirs, stats.Errors())
				}
			}
		})
	}
}

// A device boundary already prevents traversal. Even with a matching
// recursive exclude, the boundary owns the engagement count and retains
// its directory row unless NoDirs requests otherwise.
func TestPruneOtherDevicePrecedence(t *testing.T) {
	for _, noDirs := range []bool{false, true} {
		p, err := New(t.TempDir(), Options{NoDirs: noDirs})
		if err != nil {
			t.Fatal(err)
		}
		// Supply the device metadata directly so no mount or platform's
		// stat implementation is needed to exercise the shared emitter.
		p.opt.OneFilesystem, p.dev = true, 17
		spec := filter.Spec{Exclude: []string{"mount/**"}}
		prune, _, err := spec.PruneDirectories()
		if err != nil {
			t.Fatal(err)
		}
		rows := []Row{
			{Key: "mount/", Data: Entry{Kind: KindDir, Dev: 18, Present: HasDev}},
			{Key: "tail", Data: Entry{Kind: KindFile}},
		}
		em := &pruneEmitter{}
		p.Walker = tree.NewWalker(p.Root(), pruneReader{p, rows}, tree.Options[Entry]{NoDirs: noDirs, Size: entrySize})
		p.SetPruneDirectories(prune)
		if err := p.Expand(context.Background(), p.Root(), em); err != nil {
			t.Fatal(err)
		}
		stats := p.Stats()
		want := []string{"mount/", "tail"}
		if noDirs {
			want = []string{"tail"}
		}
		if !slices.Equal(em.keys, want) || len(em.children) != 0 || stats.OtherDevice != 1 || stats.PrunedDirs != 0 {
			t.Fatalf("nodirs=%v rows=%q children=%q stats=%+v", noDirs, em.keys, em.children, stats)
		}
	}
}
