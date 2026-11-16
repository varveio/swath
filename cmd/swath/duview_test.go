// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// viewTerminal makes stdout a terminal of width columns and height lines
// for the test.
func viewTerminal(t *testing.T, width, height int) {
	tty, size := stdoutTerminal, stdoutSize
	stdoutTerminal = func() bool { return true }
	stdoutSize = func() (int, int) { return width, height }
	t.Cleanup(func() { stdoutTerminal, stdoutSize = tty, size })
}

// viewTree is a local tree whose bytes sit in data/raw/, with ten small
// logs and a one-byte file at the top.
func viewTree(t *testing.T) string {
	root := t.TempDir()
	files := map[string]int{"data/raw/a.bin": 6000, "data/raw/b.bin": 3000, "data/c.bin": 500, "x": 1}
	for i := range 10 {
		files[fmt.Sprintf("logs/%d.log", i)] = 10
	}
	for name, n := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, bytes.Repeat([]byte("x"), n), 0o644); err != nil {
			t.Fatal(err)
		}
		at := viewTime
		if name == "data/c.bin" {
			at = at.Add(time.Hour) // the newest, under data/ and the total
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// viewTime is viewTree's files' time but data/c.bin's, an hour later.
var viewTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// TestDuView: bare swath du on a terminal draws the view under a
// heading: the total first under SOURCE, the largest nodes at any depth by
// size down to a floor (here logs/ and x, under it, stay in a fold), each with
// its count of files, its share, a bar, its newest date and its path, each
// parent's rest folded into one "(N more)" line; the columns go as the
// width shrinks (the bar, then the share, then the date), color as --color
// and NO_COLOR say; the line under it gives the total. -d, --sort, an
// explicit --format and a pipe keep the rows.
func TestDuView(t *testing.T) {
	root := viewTree(t)
	viewTerminal(t, 200, 11) // the temporary directory's name is not cut; a heading and eight lines, the figures line and the prompt
	t.Setenv("NO_COLOR", "1")
	t.Setenv("TERM", "xterm") // not dumb, which turns color off
	stderr := captureStderr(t)
	var out bytes.Buffer
	if err := runDu(context.Background(), []string{root, "--no-stats"}, &out); err != nil {
		t.Fatal(err)
	}
	// The dates are local, the zone named once.
	zone, _ := time.Now().Zone()
	at, later := viewTime.Local().Format(time.DateOnly), viewTime.Add(time.Hour).Local().Format(time.DateOnly)
	pad := strings.Repeat(" ", 1+len(zone))
	want := "" +
		"    size  files  share                        newest" + strings.Repeat(" ", len(pad)+4) + "  path\n" +
		"9.38 KiB     14   100%  ████████████████████  " + later + " " + zone + "  ROOT\n" +
		"9.28 KiB      3  98.9%  ███████████████████▊  " + later + pad + "    data/\n" +
		"8.79 KiB      2  93.7%  ██████████████████▋   " + at + pad + "      raw/\n" +
		"5.86 KiB      1  62.5%  ████████████▍         " + at + pad + "        a.bin\n" +
		"2.93 KiB      1  31.2%  ██████▏               " + at + pad + "        b.bin\n" +
		"   500 B      1   5.2%  █                     " + later + pad + "      c.bin\n" +
		"   101 B     11   1.1%  ▏                     " + at + pad + "    (2 more)\n"
	if got := strings.Replace(out.String(), root, "ROOT", 1); got != want {
		t.Fatalf("view\n%s\nwant\n%s", got, want)
	}
	if !strings.HasPrefix(stderr.String(), "  9.38 KiB in 14 files · ") {
		t.Fatalf("stderr %q", stderr.String())
	}

	// The width: from 100 columns a bar of 20 cells, at 80 of up to 10,
	// at 60 none, at 40 neither the share nor the date; the size, the
	// count and the path always; no line wider than the terminal.
	for _, tc := range []struct {
		width            int
		bar, share, date bool
	}{{40, false, false, false}, {60, false, true, true}, {80, true, true, true}, {100, true, true, true}, {120, true, true, true}} {
		viewTerminal(t, tc.width, 24)
		out.Reset()
		if err := runDu(context.Background(), []string{root, "--no-stats", "-q"}, &out); err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSuffix(strings.Replace(out.String(), root, "ROOT", 1), "\n"), "\n")
		for _, l := range lines {
			if n := utf8.RuneCountInString(l); n > tc.width {
				t.Errorf("width %d: %q is %d wide", tc.width, l, n)
			}
		}
		head := lines[0]
		cells := strings.Count(lines[1], "█")
		if len(lines) != 8 || cells > 0 != tc.bar || tc.width >= 100 && cells != 20 || tc.width == 80 && cells > 10 ||
			strings.Contains(head, "share") != tc.share || strings.Contains(head, "newest") != tc.date || !strings.HasSuffix(head, "  path") {
			t.Errorf("width %d: %d lines, bar %d cells\n%s", tc.width, len(lines), cells, out.String())
		}
	}

	// Color: --color always, or auto on a terminal without NO_COLOR;
	// never with --color never.
	for _, tc := range []struct {
		noColor string
		args    []string
		color   bool
	}{{"1", []string{"--color", "always"}, true}, {"", nil, true}, {"", []string{"--color", "never"}, false}} {
		t.Setenv("NO_COLOR", tc.noColor)
		out.Reset()
		if err := runDu(context.Background(), append([]string{root, "--no-stats", "-q"}, tc.args...), &out); err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(out.String(), "\x1b["); got != tc.color {
			t.Errorf("NO_COLOR %q %q: color %v\n%q", tc.noColor, tc.args, got, out.String())
		}
	}

	// A short terminal: fewer lines, still adding up.
	viewTerminal(t, 80, 10)
	out.Reset()
	if err := runDu(context.Background(), []string{root, "--no-stats", "-q", "--color", "never"}, &out); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(out.String(), "\n"); n > 9 { // 10 rows less the prompt (-q)
		t.Errorf("a 10-line terminal got %d lines", n)
	}

	// -d, --sort, --format table and --format tsv keep the rows on a
	// terminal.
	viewTerminal(t, 80, 24)
	for _, args := range [][]string{{"-d", "1"}, {"-d", "0"}, {"--format", "table"}, {"--sort", "size"}, {"--sort", "name"}, {"--max-depth", "1"}} {
		out.Reset()
		if err := runDu(context.Background(), append([]string{root, "--no-stats", "-q"}, args...), &out); err != nil || !strings.HasSuffix(strings.SplitN(out.String(), "\n", 2)[0], "  path") {
			t.Errorf("%q: %v\n%s", args, err, out.String())
		}
	}
	out.Reset()
	if err := runDu(context.Background(), []string{root, "--no-stats", "--format", "tsv"}, &out); err != nil || !strings.HasPrefix(out.String(), "row_type\t") {
		t.Errorf("--format tsv: %v\n%s", err, out.String())
	}
	stdoutTerminal = func() bool { return false }
	out.Reset()
	if err := runDu(context.Background(), []string{root, "--no-stats"}, &out); err != nil || !strings.HasPrefix(out.String(), "row_type\t") {
		t.Errorf("a pipe: %v\n%s", err, out.String())
	}
}

// TestDuViewFromDataset: the view of a saved dataset is the live view of
// its source, line for line, for a local tree and a bucket; the run report
// accounts for the view.
func TestDuViewFromDataset(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	keys := s3test.Keys(400, 7)
	b := s3test.Backend(t, keys)
	for i, n := range []int{9000, 4000, 2500} {
		k := fmt.Sprintf("big/part-%d", i)
		if _, err := b.PutObject(s3test.Name, k, nil, bytes.NewReader(make([]byte, n)), int64(n), nil); err != nil {
			t.Fatal(err)
		}
	}
	bucket := []string{"s3://" + s3test.Name, "--anonymous", "--endpoint-url", s3test.Serve(t, http.Handler(s3test.Handler(b)))}
	// A local tree of more small directories than --max-prefixes beside
	// one large file: each directory completes when its listing ends, so
	// the live view keeps every level, as the dataset's does.
	wide := t.TempDir()
	files := map[string]int{"a/big": 1_000_000}
	for i := range 120 {
		files[fmt.Sprintf("d%03d/f", i)] = 1
	}
	for name, n := range files {
		p := filepath.Join(wide, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Two directories and a budget of three: the dataset's rows, read in
	// order, hold a node or two at each run's edge, which count against
	// nothing, as the live listing's do not.
	small := t.TempDir()
	for name, n := range map[string]int{"a/f": 1000, "b/f": 100} {
		p := filepath.Join(small, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		src, du []string // the source, and du's options for both
	}{{[]string{viewTree(t)}, nil}, {bucket, nil}, {[]string{wide}, []string{"--max-prefixes", "100"}}, {[]string{small}, []string{"--max-prefixes", "3"}}} {
		src := tc.src
		viewTerminal(t, 200, 30) // room for the temporary directories' names, not cut
		report := filepath.Join(t.TempDir(), "r.json")
		var live, saved bytes.Buffer
		if err := runDu(context.Background(), append(append(slices.Clone(src), tc.du...), "--no-stats", "-q", "--report", report), &live); err != nil {
			t.Fatal(err)
		}
		var r listing.Report
		readJSON(t, report, &r)
		if v := r.TotalsView; v == nil || v.Runs < 1 || v.Completed == 0 || v.DepthKept != -1 {
			t.Fatalf("%s: report view %+v", src[0], v)
		}
		dir := filepath.Join(t.TempDir(), "ds")
		if err := run(context.Background(), append(append(slices.Clone(src), "-o", dir, "--no-stats", "-q"), unjournaled...), &bytes.Buffer{}, diagList); err != nil {
			t.Fatal(err)
		}
		if err := runDu(context.Background(), append([]string{"--from-dataset", dir, "--no-stats", "-q"}, tc.du...), &saved); err != nil {
			t.Fatal(err)
		}
		lh, lv, _ := strings.Cut(live.String(), "\n") // the heading, then the total's line
		sh, sv, _ := strings.Cut(saved.String(), "\n")
		l, lrest, _ := strings.Cut(lv, "\n")
		s, srest, _ := strings.Cut(sv, "\n")
		if !strings.HasSuffix(l, src[0]) || !strings.HasSuffix(s, dir) ||
			lh != sh || lrest != srest || strings.Count(live.String(), "\n") < 4 {
			t.Fatalf("%s: live\n%s\nsaved\n%s", src[0], live.String(), saved.String())
		}
	}
}

// TestDuViewNames: a name is escaped for the terminal as the command's other
// human lines escape it (a bidirectional control drawn as \u202e), and cut
// in the middle where the line would pass the terminal's width.
func TestDuViewNames(t *testing.T) {
	root := t.TempDir()
	evil := "evil\u202etxt.bin"
	long := strings.Repeat("l", 40) + "-end.parquet"
	for name, n := range map[string]int{evil: 4000, long: 3000, "x": 10} {
		if err := os.WriteFile(filepath.Join(root, name), make([]byte, n), 0o644); err != nil {
			t.Skipf("this filesystem refuses the name %q: %v", name, err)
		}
	}
	t.Setenv("NO_COLOR", "1")
	for _, width := range []int{40, 160} {
		viewTerminal(t, width, 24)
		var out bytes.Buffer
		if err := runDu(context.Background(), []string{root, "--no-stats", "-q"}, &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "\u202e") || !strings.Contains(out.String(), `evil\u202etxt.bin`) {
			t.Errorf("width %d: the control is not escaped\n%s", width, out.String())
		}
		for _, l := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
			if n := utf8.RuneCountInString(l); n > width {
				t.Errorf("width %d: %q is %d wide", width, l, n)
			}
		}
		below := strings.SplitN(out.String(), "\n", 3)[2] // past the heading and the total's line, which names the temporary directory and may be cut
		if cut := strings.Contains(below, "…"); cut != (width == 40) {
			t.Errorf("width %d: cut %v\n%s", width, cut, out.String())
		}
	}
}

// TestDuViewFromDatasetSource: --from-dataset DIR SOURCE, SOURCE a local
// directory below the dataset's, draws the live view of that directory.
func TestDuViewFromDatasetSource(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	root := viewTree(t)
	viewTerminal(t, 200, 30)
	dir := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), append([]string{root, "-o", dir, "--no-stats", "-q"}, unjournaled...), &bytes.Buffer{}, diagList); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(root, "data")
	var live, saved bytes.Buffer
	if err := runDu(context.Background(), []string{sub, "--no-stats", "-q"}, &live); err != nil {
		t.Fatal(err)
	}
	if err := runDu(context.Background(), []string{"--from-dataset", dir, sub, "--no-stats", "-q"}, &saved); err != nil {
		t.Fatal(err)
	}
	_, lv, _ := strings.Cut(live.String(), "\n") // past the heading
	_, sv, _ := strings.Cut(saved.String(), "\n")
	l, lrest, _ := strings.Cut(lv, "\n")
	s, srest, _ := strings.Cut(sv, "\n")
	if !strings.HasSuffix(l, sub) || !strings.HasSuffix(s, sub) || lrest != srest || !strings.Contains(lrest, "raw/") || strings.Contains(lrest, "logs/") {
		t.Fatalf("live\n%s\nsaved\n%s", live.String(), saved.String())
	}
}

// TestDuViewWideBucket: a bucket of 2,000 top-level prefixes, four of them
// large, opens the large prefixes and leaves the small ones, each under the
// floor, in one fold: the view ends shorter than the terminal rather than
// list them.
func TestDuViewWideBucket(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var keys []string
	for i := range 2000 {
		keys = append(keys, fmt.Sprintf("p%04d/obj-%d", i, i)) // a body of its key: sizes differ
	}
	b := s3test.Backend(t, keys)
	for i := range 4 {
		k := fmt.Sprintf("p%04d/big", 500*i)
		if _, err := b.PutObject(s3test.Name, k, nil, bytes.NewReader(make([]byte, 20_000)), 20_000, nil); err != nil {
			t.Fatal(err)
		}
	}
	viewTerminal(t, 100, 30)
	var out bytes.Buffer
	src := []string{"s3://" + s3test.Name, "--anonymous", "--endpoint-url", s3test.Serve(t, http.Handler(s3test.Handler(b)))}
	if err := runDu(context.Background(), append(src, "--no-stats", "-q"), &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	opened := 0
	for _, l := range lines {
		if strings.HasSuffix(l, "  big") {
			opened++
		}
		if strings.Contains(l, "obj-1") || strings.Contains(l, "p1001/") {
			t.Fatalf("a small prefix listed:\n%s", out.String())
		}
	}
	if opened != 4 || len(lines) >= 29 || !strings.HasSuffix(lines[len(lines)-1], "(1,996 more)") {
		t.Fatalf("%d lines, %d large prefixes opened\n%s", len(lines), opened, out.String())
	}
}
