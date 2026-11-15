// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// datasetKeys reads every key of a committed local dataset in order,
// repeats included.
func datasetKeys(t testing.TB, dir string) []string {
	t.Helper()
	var m dataset.Manifest
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	var keys []string
	for _, p := range m.Parts {
		b, err := os.ReadFile(filepath.Join(dir, p.Path))
		if err != nil {
			t.Fatal(err)
		}
		pf, err := pq.OpenFile(bytes.NewReader(b), int64(len(b)))
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]pq.Row, pf.NumRows())
		r := pq.NewReader(pf)
		n, _ := r.ReadRows(rows)
		r.Close()
		for _, row := range rows[:n] {
			keys = append(keys, string(row[0].ByteArray()))
		}
	}
	return keys
}

// indexCuts is the cut file of a committed local dataset made from its
// parts' key page index, as a resume reads its retained parts.
func indexCuts(t testing.TB, dir string, m dataset.Manifest) []byte {
	t.Helper()
	c := dataset.NewCuts(0)
	for _, p := range m.Parts {
		pages, err := parquetout.KeyPages(filepath.Join(dir, p.Path))
		if err != nil {
			t.Fatal(err)
		}
		ends, err := parquetout.PageEnds(pages, p.Rows)
		if err != nil {
			t.Fatal(err)
		}
		c.Part(ends)
	}
	b, err := c.File()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// checkCuts checks a committed local dataset's cut file, if it holds one
// (false: none), against keys, the dataset's keys in order: the
// --seed-from reader takes it (datasetCuts: counted, ascending, the last
// cut the last key, the counts the manifest's rows), every cut is a key
// and its count the rows of the keys after the previous cut through it
// (so no cut falls inside a key's versions), and it is the file the parts'
// key page index makes.
func checkCuts(t testing.TB, dir string, keys []string) bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, dataset.CutsName))
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	var rows int64
	for _, p := range m.Parts {
		rows += p.Rows
	}
	cuts, err := datasetCuts(dir, &m, rows)
	if err != nil || cuts == nil {
		t.Fatalf("%s: the cut file is not the dataset's: %v", dir, err)
	}
	at := 0
	for i, c := range cuts {
		from := at
		for at < len(keys) && keys[at] <= c.Key {
			at++
		}
		if at == from || keys[at-1] != c.Key || int64(at-from) != c.Rows {
			t.Fatalf("%s: cut %d %.64q counts %d rows; %d keys after the previous cut through it", dir, i+1, c.Key, c.Rows, at-from)
		}
	}
	if at != len(keys) {
		t.Fatalf("%s: the cut file covers %d of %d rows", dir, at, len(keys))
	}
	if want := indexCuts(t, dir, m); !bytes.Equal(b, want) {
		t.Fatalf("%s: the cut file is not the one the key page index makes:\n%q\n%q", dir, b, want)
	}
	return true
}

// sameCuts: got's cut file, when it holds one, is ref's byte for byte (got
// holds ref's parts).
func sameCuts(t testing.TB, name, ref, got string) {
	t.Helper()
	g, err := os.ReadFile(filepath.Join(got, dataset.CutsName))
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	r, rerr := os.ReadFile(filepath.Join(ref, dataset.CutsName))
	if err != nil || rerr != nil || !bytes.Equal(g, r) {
		t.Fatalf("%s: the cut file is not the reference's (%v, %v)", name, err, rerr)
	}
	checkCuts(t, got, datasetKeys(t, got))
}

// coarsened is a cut file written at every page end (R = 1) as its writer
// writes it under a bound of at most lines lines (dataset.Cuts): R the
// least power of two that fits, a cut at the first line where the rows
// reach each multiple of R, the last key closing the last line.
func coarsened(t testing.TB, b []byte, lines int) []byte {
	t.Helper()
	type cut struct {
		key  string
		rows int64
	}
	var all []cut
	var at int64
	for l := range strings.SplitSeq(strings.TrimSuffix(string(b), "\n"), "\n") {
		i := strings.LastIndexByte(l, '\t')
		n, err := strconv.ParseInt(l[i+1:], 10, 64)
		if i < 0 || err != nil {
			t.Fatalf("cut file line %q", l)
		}
		at += n
		all = append(all, cut{l[:i], at})
	}
	for unit := int64(1); ; unit *= 2 {
		var kept []cut
		prev := int64(0)
		for _, c := range all {
			if c.rows/unit > prev/unit {
				kept = append(kept, c)
			}
			prev = c.rows
		}
		if last := all[len(all)-1]; kept[len(kept)-1] != last {
			kept = append(kept, last)
		}
		if len(kept) > lines {
			continue
		}
		var out []byte
		prev = 0
		for _, c := range kept {
			out = fmt.Appendf(out, "%s\t%d\n", c.key, c.rows-prev)
			prev = c.rows
		}
		return out
	}
}

// requireCuts: dir holds a cut file.
func requireCuts(t testing.TB, name, dir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, dataset.CutsName)); err != nil {
		t.Fatalf("%s: no cut file: %v", name, err)
	}
}

// warnings collects a run's warnings.
type warnings struct {
	mu  sync.Mutex
	got []string
}

func (w *warnings) warn(_, s string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.got = append(w.got, s)
}

func (w *warnings) about(s string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, g := range w.got {
		if strings.Contains(g, s) {
			out = append(out, g)
		}
	}
	return out
}

// TestCutsFile: a committed dataset carries its cut file beside the
// manifest, every key page end a cut at this size (checkCuts), with spill
// off and on. A cut file that cannot be written is one warning, the run
// stays committed with its run report, and its manifest and parts are the
// other run's byte for byte: the cut file is outside them.
func TestCutsFile(t *testing.T) {
	url := crashBucket(t)
	dir := t.TempDir()
	var ref string
	for _, spill := range []bool{false, true} {
		out := filepath.Join(dir, fmt.Sprintf("spill-%v", spill))
		cfg := crashConfig(url, out, false)
		cfg.Limits.Spill.Off = !spill
		if _, err := Run(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
		checkContract(t, out)
		requireCuts(t, out, out)
		if ref == "" {
			ref = out
		} else {
			sameDataset(t, out, ref, out)
		}
	}
	defer faults.Set(func(p string) error {
		if p == "cuts" {
			return errors.New("no space left on device")
		}
		return nil
	})()
	out := filepath.Join(dir, "unwritten")
	cfg := crashConfig(url, out, false)
	var w warnings
	cfg.Warn = w.warn
	sum, err := Run(context.Background(), cfg)
	if err != nil || sum.Outcome.Status != StatusCommitted {
		t.Fatalf("%v, %+v", err, sum.Outcome)
	}
	if got := w.about(dataset.CutsName); len(got) != 1 || !strings.Contains(got[0], "no space left") {
		t.Fatalf("warnings %q", w.got)
	}
	if _, err := os.Stat(filepath.Join(out, dataset.CutsName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a cut file that failed is there: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, dataset.ReportName)); err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "unwritten", ref, out)
}

// TestCutsFileKeyBytes: keys holding tabs, carriage returns, digits after
// a tab and multibyte characters are cuts as any other; a key holding a
// line feed (two in five here) is not written (its rows count toward the next line), so the
// --seed-from reader never splits one. A dataset whose last key holds a
// line feed has no cut file, and says so once.
func TestCutsFileKeyBytes(t *testing.T) {
	var keys []string
	for i := range 3001 { // the last without a line feed
		k := fmt.Sprintf("d%04d/", i)
		switch i % 5 {
		case 0:
			k += "a\tb"
		case 1:
			k += "line\nfeed"
		case 2:
			k += "cr\r"
		case 3:
			k += "tab\t12"
		default:
			k += "日本\n"
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := runScripted(t, &scripted{keys: keys, chunk: 50}, out, false); err != nil {
		t.Fatal(err)
	}
	if !checkCuts(t, out, checkContract(t, out)) {
		t.Fatal("no cut file")
	}
	m := manifestOf(t, out)
	feeds := 0
	for _, p := range m.Parts[:len(m.Parts)-1] {
		if strings.Contains(string(p.MaxKey), "\n") {
			feeds++
		}
	}
	if feeds == 0 {
		t.Fatal("no part ends at a key holding a line feed: the fixture proves nothing")
	}
	keys = append(keys, "d9999/last\nkey")
	out = filepath.Join(t.TempDir(), "last")
	scriptMu.Lock()
	scriptNext = &scripted{keys: keys, chunk: 50}
	scriptMu.Unlock()
	var w warnings
	sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()},
		Limits: Limits{PartSize: 2 << 10, Workers: 4, RowBudget: 128 << 10, PartInterval: -1}, Warn: w.warn})
	if err != nil || sum.Outcome.Status != StatusCommitted {
		t.Fatal(err)
	}
	if got := w.about(dataset.CutsName); len(got) != 1 || !strings.Contains(got[0], "last key") {
		t.Fatalf("warnings %q", w.got)
	}
	if checkCuts(t, out, checkContract(t, out)) {
		t.Fatal("a cut file closed at a key holding a line feed")
	}
}

// drainStream reads a dataset prior's page index stream whole.
func drainStream(t *testing.T, s source.CutStream) []source.Cut {
	t.Helper()
	stop := s.Start(context.Background(), "", func() {})
	defer stop()
	var cuts []source.Cut
	for {
		b, err := s.Next()
		switch {
		case errors.Is(err, io.EOF):
			return cuts
		case err != nil:
			t.Fatal(err)
		case b != nil:
			cuts = append(cuts, b.Cuts...)
		}
	}
}

// TestSeedFromDatasetCuts: a dataset's cut file is its prior when it is
// the dataset's own: at this size every key page end is a cut, so its
// cuts are the page index stream's, cut for cut (the plan is a function
// of the cuts in order, whatever their batches), and the run seeded from
// it plans the same tiles and writes the same parts as one seeded from
// the page index (the dataset without its cut file). A cut file that is
// not the dataset's (a count changed, its last line dropped, another
// dataset's, without counts, unreadable) is refused once, reported, and
// the page index read instead.
func TestSeedFromDatasetCuts(t *testing.T) {
	url := fakeS3(t, 6000)
	dir := t.TempDir()
	first := filepath.Join(dir, "first")
	if _, err := listS3(t, url, "s3://bkt", first, ""); err != nil {
		t.Fatal(err)
	}
	requireCuts(t, "first", first)
	index := filepath.Join(dir, "index")
	if err := os.CopyFS(index, os.DirFS(first)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(index, dataset.CutsName)); err != nil {
		t.Fatal(err)
	}
	loc := source.Locator{Kind: "s3", Root: "s3://bkt", Container: "bkt"}
	fromFile, side, err := datasetPrior(first, loc, true, true)
	if err != nil || side.state != "read" || fromFile.Stream != nil {
		t.Fatalf("%v, %+v", err, side)
	}
	fromIndex, side, err := datasetPrior(index, loc, true, true)
	if err != nil || side.state != "absent" || fromIndex.Stream == nil {
		t.Fatalf("%v, %+v", err, side)
	}
	if streamed := drainStream(t, fromIndex.Stream); !slices.Equal(fromFile.Cuts, streamed) {
		t.Fatalf("%d cuts from the cut file, %d from the page index", len(fromFile.Cuts), len(streamed))
	}
	seeded := func(name, prior string) (Report, string) {
		t.Helper()
		out := filepath.Join(dir, name)
		sum, err := listS3(t, url, "s3://bkt", out, prior)
		if err != nil {
			t.Fatal(err)
		}
		return sum, out
	}
	sumFile, outFile := seeded("from-file", first)
	sumIndex, outIndex := seeded("from-index", index)
	sameDataset(t, "from-file", first, outFile)
	sameDataset(t, "from-index", first, outIndex)
	cf, ci := s3Stats(sumFile).Counters, s3Stats(sumIndex).Counters
	for _, n := range []string{"seed.from.cuts", "seed.from.tiles", "seed.from.expected_keys", "seed.requests"} {
		if cf.Value(n) != ci.Value(n) {
			t.Fatalf("%s: %d from the cut file, %d from the page index", n, cf.Value(n), ci.Value(n))
		}
	}
	if cf.Value("seed.from.indexed_parts") != 0 || ci.Value("seed.from.indexed_parts") == 0 || cf.Value("seed.from.tiles") < 2 {
		t.Fatalf("counters %v, %v", cf, ci)
	}
	if p := sumFile.Invocation.Prior; p == nil || p.Source != "dataset" || p.CutsFile != "read" || p.Cuts != len(fromFile.Cuts) || p.CutsRefused != "" {
		t.Fatalf("report prior %+v", p)
	}
	if p := sumIndex.Invocation.Prior; p == nil || p.CutsFile != "absent" || p.Cuts != 0 {
		t.Fatalf("report prior %+v", p)
	}

	good, err := os.ReadFile(filepath.Join(first, dataset.CutsName))
	if err != nil {
		t.Fatal(err)
	}
	scoped := filepath.Join(dir, "scoped")
	if _, err := listS3(t, url, "s3://bkt/logs/03/", scoped, ""); err != nil {
		t.Fatal(err)
	}
	other, err := os.ReadFile(filepath.Join(scoped, dataset.CutsName))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(good), "\n")
	recount := strings.Replace(string(good), "\t1024\n", "\t1023\n", 1)
	for name, tc := range map[string]struct {
		body []byte // nil: a directory in its place
		why  string
	}{
		"recounted":  {[]byte(recount), "sum to"},
		"truncated":  {[]byte(strings.Join(lines[:len(lines)-2], "")), "ends at"},
		"another":    {other, "ends at"},
		"uncounted":  {[]byte("logs/00/\nlogs/16/obj-05998\n"), "no counts"},
		"unreadable": {nil, ""},
	} {
		prior := filepath.Join(dir, "prior-"+name)
		if err := os.CopyFS(prior, os.DirFS(index)); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(prior, dataset.CutsName)
		if tc.body == nil {
			err = os.Mkdir(path, 0o755)
		} else {
			err = os.WriteFile(path, tc.body, 0o644)
		}
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(dir, "out-"+name)
		var w warnings
		sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: "s3://bkt", SeedFrom: prior,
			Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Seed: true}}},
			Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 16 << 10, Workers: 8}, Warn: w.warn})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		p, c := sum.Invocation.Prior, s3Stats(sum).Counters
		if p == nil || p.CutsFile != "refused" || p.CutsRefused == "" || !strings.Contains(p.CutsRefused, tc.why) || c.Value("seed.from.indexed_parts") == 0 {
			t.Fatalf("%s: prior %+v, counters %v", name, p, c)
		}
		if got := w.about(dataset.CutsName); len(got) != 1 {
			t.Fatalf("%s: warnings %q", name, w.got)
		}
		sameDataset(t, name, first, out)
	}
}

// TestSeedFromDatasetBadCuts: a cut file that passes the dataset's checks
// (ascending from its first key, ending at its last, the counts summing to
// its rows) but holds a key the engine cannot list (not UTF-8, longer than
// a key may be) is refused as one that fails them, and so is one whose
// counts are not each positive and within the manifest's rows on the way
// (huge counts that wrap to the total, a zero): the run warns once,
// reports it and reads the page index, and lists the same keys.
func TestSeedFromDatasetBadCuts(t *testing.T) {
	s3test.NoCredentials(t)
	keys := []string{"a", "b"}
	for i := range 97 {
		keys = append(keys, fmt.Sprintf("c%03d", i))
	}
	keys = append(keys, "z")
	url := s3test.Bucket(t, keys)
	dir := t.TempDir()
	first := filepath.Join(dir, "first")
	if _, err := listS3(t, url, "s3://bkt", first, ""); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(first, dataset.CutsName)); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct{ body, why string }{
		"utf8":     {"a\xff\t99\nz\t1\n", "UTF-8"},
		"long":     {"a\t1\nc" + strings.Repeat("x", 1100) + "\t98\nz\t1\n", "longer than"},
		"overflow": {"a\t9223372036854775807\nb\t9223372036854775807\nz\t102\n", "counts 9223372036854775807 rows"},
		"zero":     {"b\t0\nz\t100\n", "counts 0 rows"},
	} {
		prior := filepath.Join(dir, "prior-"+name)
		if err := os.CopyFS(prior, os.DirFS(first)); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(prior, dataset.CutsName), []byte(tc.body), 0o644); err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(dir, "out-"+name)
		var w warnings
		sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: "s3://bkt", SeedFrom: prior,
			Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Seed: true}}},
			Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 16 << 10, Workers: 8}, Warn: w.warn})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		p, c := sum.Invocation.Prior, s3Stats(sum).Counters
		if p == nil || p.CutsFile != "refused" || !strings.Contains(p.CutsRefused, tc.why) || c.Value("seed.from.indexed_parts") == 0 {
			t.Fatalf("%s: prior %+v, counters %v", name, p, c)
		}
		if got := w.about(dataset.CutsName); len(got) != 1 {
			t.Fatalf("%s: warnings %q", name, w.got)
		}
		if got := checkContract(t, out); !slices.Equal(got, keys) {
			t.Fatalf("%s: %d keys listed, want %d", name, len(got), len(keys))
		}
	}
}
