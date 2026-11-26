// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"
)

// viewRows is a view of a bucket: a prefix, its largest object, the fold
// of its other children and the fold of the total's.
func viewRows() []Row {
	at := time.Date(2025, 11, 15, 12, 38, 30, 0, time.UTC)
	ago := func(h int) *time.Time { t := at.Add(-time.Duration(h) * time.Hour); return &t }
	return []Row{
		{Name: "s3://bucket", Bytes: 2048 << 30, Objects: 37_100_000, Newest: &at},
		{Name: "logs/", Level: 1, Bytes: 1536 << 30, Objects: 12_000, Newest: ago(48)},
		{Name: "2025/app-events-archive.tar", Level: 2, Bytes: 1024 << 30, Objects: 1, Newest: ago(48)},
		{Fold: true, Level: 2, Bytes: 512 << 30, Objects: 11_999, More: 1234, Newest: ago(720)},
		{Fold: true, Level: 1, Bytes: 512 << 30, Objects: 37_088_000, More: 9},
	}
}

// checkWidth fails a line of out that passes width terminal columns, its
// ANSI escapes aside.
func checkWidth(t *testing.T, out string, width int) {
	t.Helper()
	plain := regexp.MustCompile("\x1b\\[[0-9;]*m").ReplaceAllString(out, "")
	for _, l := range strings.Split(strings.TrimSuffix(plain, "\n"), "\n") {
		if Cells(l) > width {
			t.Errorf("width %d: %q is %d columns", width, l, Cells(l))
		}
	}
}

// TestRenderColumns: the caller's columns sit after the date, in the
// order given, each aligned as asked; a fold takes the caller's cell
// (the first fold's cost) or stays blank (the second's). As the width
// shrinks the bar goes, then the share, then the date, then the caller's
// columns from the last to the first, and no line passes a width that
// holds the size, the count and the paths' ellipses.
func TestRenderColumns(t *testing.T) {
	rows := viewRows()
	cost := Column{Heading: "cost/mo", Align: Right, Cells: []string{"$47.10", "$35.30", "$23.50", "$11.80", ""}}
	owner := Column{Heading: "owner", Cells: []string{"", "ops", "data-eng", "", ""}}
	want := map[int]string{
		120: "" +
			"    size  objects  share                        newest          cost/mo  owner     path\n" +
			"2.00 TiB    37.1M   100%  ████████████████████  2025-11-15 UTC   $47.10            s3://bucket\n" +
			"1.50 TiB    12.0k  75.0%  ███████████████       2025-11-13       $35.30  ops         logs/\n" +
			"1.00 TiB        1  50.0%  ██████████            2025-11-13       $23.50  data-eng      2025/app-events-archive.tar\n" +
			" 512 GiB    12.0k  25.0%  █████                 2025-10-16       $11.80                (1,234 more)\n" +
			" 512 GiB    37.1M  25.0%  █████                                                      (9 more)\n",
		80: "" +
			"    size  objects  share  newest          cost/mo  owner     path\n" +
			"2.00 TiB    37.1M   100%  2025-11-15 UTC   $47.10            s3://bucket\n" +
			"1.50 TiB    12.0k  75.0%  2025-11-13       $35.30  ops         logs/\n" +
			"1.00 TiB        1  50.0%  2025-11-13       $23.50  data-eng      2025/ap…ive.tar\n" +
			" 512 GiB    12.0k  25.0%  2025-10-16       $11.80                (1,234 more)\n" +
			" 512 GiB    37.1M  25.0%                                       (9 more)\n",
		72: "" +
			"    size  objects  newest          cost/mo  owner     path\n" +
			"2.00 TiB    37.1M  2025-11-15 UTC   $47.10            s3://bucket\n" +
			"1.50 TiB    12.0k  2025-11-13       $35.30  ops         logs/\n" +
			"1.00 TiB        1  2025-11-13       $23.50  data-eng      2025/a…ive.tar\n" +
			" 512 GiB    12.0k  2025-10-16       $11.80                (1,234 more)\n" +
			" 512 GiB    37.1M                                       (9 more)\n",
		60: "" +
			"    size  objects  cost/mo  owner     path\n" +
			"2.00 TiB    37.1M   $47.10            s3://bucket\n" +
			"1.50 TiB    12.0k   $35.30  ops         logs/\n" +
			"1.00 TiB        1   $23.50  data-eng      2025/app…chive.tar\n" +
			" 512 GiB    12.0k   $11.80                (1,234 more)\n" +
			" 512 GiB    37.1M                       (9 more)\n",
		50: "" +
			"    size  objects  cost/mo  path\n" +
			"2.00 TiB    37.1M   $47.10  s3://bucket\n" +
			"1.50 TiB    12.0k   $35.30    logs/\n" +
			"1.00 TiB        1   $23.50      2025/app…chive.tar\n" +
			" 512 GiB    12.0k   $11.80      (1,234 more)\n" +
			" 512 GiB    37.1M             (9 more)\n",
		40: "" +
			"    size  objects  path\n" +
			"2.00 TiB    37.1M  s3://bucket\n" +
			"1.50 TiB    12.0k    logs/\n" +
			"1.00 TiB        1      2025/app…hive.tar\n" +
			" 512 GiB    12.0k      (1,234 more)\n" +
			" 512 GiB    37.1M    (9 more)\n",
	}
	for width, w := range want {
		var b bytes.Buffer
		if err := Render(&b, Layout{Width: width, Loc: time.UTC}, rows, cost, owner); err != nil {
			t.Fatal(err)
		}
		if b.String() != w {
			t.Errorf("width %d:\n%s\nwant\n%s", width, b.String(), w)
		}
	}
	for width := 24; width <= 140; width++ { // narrower, the size and the count alone pass it
		var b bytes.Buffer
		if err := Render(&b, Layout{Width: width, Color: true, Loc: time.UTC}, rows, cost, owner); err != nil {
			t.Fatal(err)
		}
		checkWidth(t, b.String(), width)
	}
}

// TestRenderWideColumns: names and the caller's cells of East Asian wide
// characters (two columns each) and combining marks (none) are measured in
// terminal columns: the caller's column stays aligned, and no line passes
// the width.
func TestRenderWideColumns(t *testing.T) {
	rows := []Row{
		{Name: "s3://b", Bytes: 300, Objects: 3},
		{Name: "写真/", Level: 1, Bytes: 200, Objects: 2},
		{Name: strings.Repeat("界", 12) + ".bin", Level: 2, Bytes: 150, Objects: 1},
		{Fold: true, Level: 2, Bytes: 50, Objects: 1, More: 1},
		{Name: "e\u0301te\u0301.txt", Level: 1, Bytes: 100, Objects: 1},
	}
	owner := Column{Heading: "所有者", Cells: []string{"", "経理部", "経理", "", "ops"}}
	want := map[int]string{
		100: "" +
			" size  objects  share                        所有者  path\n" +
			"300 B        3   100%  ████████████████████          s3://b\n" +
			"200 B        2  66.7%  █████████████▎        経理部    写真/\n" +
			"150 B        1  50.0%  ██████████            経理        界界界界界界界界界界界界.bin\n" +
			" 50 B        1  16.7%  ███▎                              (1 more)\n" +
			"100 B        1  33.3%  ██████▋               ops       e\u0301te\u0301.txt\n",
		44: "" +
			" size  objects  所有者  path\n" +
			"300 B        3          s3://b\n" +
			"200 B        2  経理部    写真/\n" +
			"150 B        1  経理        界界界…界界.bin\n" +
			" 50 B        1              (1 more)\n" +
			"100 B        1  ops       e\u0301te\u0301.txt\n",
	}
	for width, w := range want {
		var b bytes.Buffer
		if err := Render(&b, Layout{Width: width}, rows, owner); err != nil {
			t.Fatal(err)
		}
		if b.String() != w {
			t.Errorf("width %d:\n%s\nwant\n%s", width, b.String(), w)
		}
	}
	for width := 21; width <= 120; width++ {
		var b bytes.Buffer
		if err := Render(&b, Layout{Width: width}, rows, owner); err != nil {
			t.Fatal(err)
		}
		checkWidth(t, b.String(), width)
	}
}

// TestRenderLayout: the headings the caller names, the first row's name
// without a label, color dimming a fold with the caller's cell in it, and
// a column with a cell too many or too few refused before anything is
// written.
func TestRenderLayout(t *testing.T) {
	rows := viewRows()
	var b bytes.Buffer
	cost := Column{Heading: "cost/mo", Align: Right, Cells: []string{"$47.10", "$35.30", "$23.50", "$11.80", "$11.80"}}
	if err := Render(&b, Layout{Width: 120, Color: true, Loc: time.UTC, SizeHeading: "on disk", CountHeading: "files"}, rows, cost); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(b.String(), "\n")
	plain := regexp.MustCompile("\x1b\\[[0-9;]*m").ReplaceAllString(b.String(), "")
	if h := strings.SplitN(plain, "\n", 2)[0]; h != " on disk  files  share                        newest          cost/mo  path" ||
		!strings.Contains(lines[1], "s3://bucket") || !strings.HasPrefix(lines[5], "\x1b[2m") || !strings.HasSuffix(lines[5], "$11.80    (9 more)\x1b[0m") {
		t.Errorf("layout:\n%q", b.String())
	}
	b.Reset()
	for _, n := range []int{len(rows) - 1, len(rows) + 1} {
		short := Column{Heading: "x", Cells: make([]string, n)}
		if err := Render(&b, Layout{}, rows, short); err == nil || b.Len() > 0 {
			t.Errorf("%d cells for %d rows: %v, wrote %q", n, len(rows), err, b.String())
		}
	}
}
