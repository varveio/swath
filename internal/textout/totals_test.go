// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/totals"
)

// TestIEC: a table's size keeps at most three significant digits across
// the unit boundaries, moving to the next unit before a fourth digit; an
// unknown figure is "-", a partial sum a lower bound.
func TestIEC(t *testing.T) {
	const k, m = 1024, 1024 * 1024
	for n, want := range map[int64]string{
		0: "0 B", 1023: "1023 B", k: "1.00 KiB", 10 * k: "10.0 KiB", 100 * k: "100 KiB",
		999 * k: "999 KiB", 999*k + k/2: "0.98 MiB", 1000 * k: "0.98 MiB", 1023 * k: "1.00 MiB", m: "1.00 MiB",
		9996 * m / 1000: "10.0 MiB", 9996 * m / 100: "100 MiB", 9994 * m / 1000: "9.99 MiB", 1 << 62: "4.00 EiB",
	} {
		if got := string(appendTableFigure(nil, &totals.Prefix{Objects: 1, Size: n, HasSize: true}, totals.Size)); got != want {
			t.Errorf("%d: %q, want %q", n, got, want)
		}
	}
	for want, r := range map[string]totals.Prefix{
		"-":           {Objects: 1, SizeMissing: 1},
		">= 5 B":      {Objects: 2, Size: 5, SizeMissing: 1, HasSize: true},
		"0 B":         {HasSize: true},
		">= 12.0 KiB": {Objects: 3, Size: 12 * k, SizeMissing: 2, HasSize: true},
	} {
		if got := string(appendTableFigure(nil, &r, totals.Size)); got != want {
			t.Errorf("%+v: %q, want %q", r, got, want)
		}
	}
}

// TestTotalsTSVEscapes: a TSV prefix takes the rows' grammar: a byte that
// is not UTF-8 is \xHH, never the raw byte, and decodes back; a missing
// time and a source without allocated bytes are \N.
func TestTotalsTSVEscapes(t *testing.T) {
	rows := []totals.Prefix{{Prefix: "bad\xff\t/", Depth: 1, Objects: 1, Size: 1, HasSize: true, AllocatedMissing: 1}}
	var b bytes.Buffer
	if err := WriteTotals(&b, TSV, TotalsLayout{Sort: "name"}, rows); err != nil {
		t.Fatal(err)
	}
	want := "row_type\tdepth\tobjects\tsize\tsize_missing\tallocated_bytes\tallocated_bytes_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\n" +
		"prefix\t1\t1\t1\t0\t\\N\t1\t\\N\tsize\tname\tfalse\tbad\\xff\\t/\n"
	if b.String() != want {
		t.Fatalf("%q\nwant %q", b.String(), want)
	}
	f := strings.Split(strings.TrimSuffix(b.String(), "\n"), "\t")
	if p, ok, err := UnescapeTSV(f[len(f)-1]); err != nil || !ok || p != rows[0].Prefix {
		t.Fatalf("%q %v %v", p, ok, err)
	}
}

// TestTotalsJSONL: every line is JSON; a prefix that is not UTF-8 is
// prefix_base64 alone, its exact bytes; both figures are there, each with
// its missing objects; a missing time is null; every row carries its type
// and the run's primary figure, order and hard-link policy.
func TestTotalsJSONL(t *testing.T) {
	rows := []totals.Prefix{
		{Prefix: "", Objects: 3, Size: 7, SizeMissing: 1, HasSize: true, Allocated: 8192, AllocatedMissing: 1, HasAllocated: true, Newest: 1, HasNewest: true},
		{Prefix: "caf\xc3\xa9/\"q\"\t/", Depth: 2, Objects: 1, Size: 2, HasSize: true, Allocated: 4096, HasAllocated: true},
		{Prefix: "bad\xff/", Depth: 1, Objects: 1, SizeMissing: 1, AllocatedMissing: 1},
	}
	var b bytes.Buffer
	if err := WriteTotals(&b, JSONL, TotalsLayout{Local: true, Primary: totals.AllocatedBytes, Sort: "size", LinksOnce: true}, rows); err != nil {
		t.Fatal(err)
	}
	var got []map[string]any
	for l := range strings.Lines(b.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("%q: %v", l, err)
		}
		got = append(got, m)
	}
	if len(got) != 3 || got[0][FieldDepth] != 0.0 || got[0][FieldRowType] != RowTotal || got[1][FieldDepth] != 2.0 || got[1][FieldRowType] != RowPrefix ||
		got[2][FieldDepth] != 1.0 || got[1][FieldPrefix] != rows[1].Prefix || got[1][FieldSize] != 2.0 || got[1][FieldAllocatedBytes] != 4096.0 ||
		got[1][FieldMaxLastModified] != nil || got[0][FieldMaxLastModified] != "1970-01-01T00:00:00.000001Z" {
		t.Fatalf("%v", got)
	}
	if want := `{"row_type":"total","depth":0,"prefix":"","objects":3,"dirs":0,"size":7,"size_missing":1,"allocated_bytes":8192,"allocated_bytes_missing":1,` +
		`"max_last_modified":"1970-01-01T00:00:00.000001Z","primary":"allocated_bytes","sort":"size","links_once":true}`; strings.Split(b.String(), "\n")[0] != want {
		t.Fatalf("member order: %q", b.String())
	}
	raw, err := base64.StdEncoding.DecodeString(got[2][FieldPrefixBase64].(string))
	if err != nil || string(raw) != rows[2].Prefix || got[2][FieldPrefix] != nil || got[2][FieldSize] != nil || got[2][FieldSizeMissing] != 1.0 ||
		got[2][FieldAllocatedBytes] != nil || got[2][FieldAllocatedBytesMissing] != 1.0 {
		t.Fatalf("%v (%v)", got[2], err)
	}
}

// TestTotalsMissing: a figure some objects lacked is the sum of the others
// beside their count in TSV and JSONL, and a lower bound in the table; one
// every object lacked is null and "-"; an empty prefix's is 0.
func TestTotalsMissing(t *testing.T) {
	rows := []totals.Prefix{
		{Objects: 2, Size: 7, SizeMissing: 1, HasSize: true, AllocatedMissing: 2},
		{Prefix: "e/", Depth: 1, HasSize: true, HasAllocated: true},
	}
	l := TotalsLayout{Local: true, Sort: "name", Loc: time.UTC}
	for format, want := range map[string]string{
		TSV: "row_type\tdepth\tobjects\tdirs\tsize\tsize_missing\tallocated_bytes\tallocated_bytes_missing\tmax_last_modified\tprimary\tsort\tlinks_once\tprefix\n" +
			"total\t0\t2\t0\t7\t1\t\\N\t2\t\\N\tsize\tname\tfalse\t\n" +
			"prefix\t1\t0\t0\t0\t0\t0\t0\t\\N\tsize\tname\tfalse\te/\n",
		JSONL: `{"row_type":"total","depth":0,"prefix":"","objects":2,"dirs":0,"size":7,"size_missing":1,"allocated_bytes":null,"allocated_bytes_missing":2,"max_last_modified":null,"primary":"size","sort":"name","links_once":false}` + "\n" +
			`{"row_type":"prefix","depth":1,"prefix":"e/","objects":0,"dirs":0,"size":0,"size_missing":0,"allocated_bytes":0,"allocated_bytes_missing":0,"max_last_modified":null,"primary":"size","sort":"name","links_once":false}` + "\n",
		Table: "  size  files  dirs  last_modified (UTC)  path\n" +
			"   0 B      0     0  -                    e/\n" +
			">= 7 B      2     0  -                    (total)\n",
	} {
		var b bytes.Buffer
		if err := WriteTotals(&b, format, l, rows); err != nil || b.String() != want {
			t.Errorf("%s: %v\n%q\nwant %q", format, err, b.String(), want)
		}
	}
	l.Primary = totals.AllocatedBytes
	var b bytes.Buffer
	if err := WriteTotals(&b, Table, l, rows); err != nil || b.String() != "on disk  files  dirs  last_modified (UTC)  path\n    0 B      0     0  -                    e/\n      -      2     0  -                    (total)\n" {
		t.Errorf("on disk: %v %q", err, b.String())
	}
}

// TestTotalsOrder: TSV and JSONL keep the rows in the order given (the
// tally's byte order, the total first); the table puts the total last,
// found by its Depth, not its place, and leaves the rows as they were.
func TestTotalsOrder(t *testing.T) {
	rows := []totals.Prefix{{Prefix: "logs/", Objects: 3, Size: 3, HasSize: true}, {Prefix: "logs/a/", Depth: 1, Objects: 1, Size: 1, HasSize: true},
		{Prefix: "logs/a/b/", Depth: 2, Objects: 1, Size: 1, HasSize: true}, {Prefix: "logs/c/", Depth: 1, Objects: 2, Size: 2, HasSize: true}}
	var b bytes.Buffer
	if err := WriteTotals(&b, TSV, TotalsLayout{NoHeader: true}, rows); err != nil {
		t.Fatal(err)
	}
	var tsv []string
	for l := range strings.Lines(b.String()) {
		f := strings.Split(strings.TrimSuffix(l, "\n"), "\t")
		tsv = append(tsv, f[0]+" "+f[1]+" "+f[len(f)-1])
	}
	if got := strings.Join(tsv, ", "); got != "total 0 logs/, prefix 1 logs/a/, prefix 2 logs/a/b/, prefix 1 logs/c/" {
		t.Fatalf("tsv %q", got)
	}
	b.Reset()
	if err := WriteTotals(&b, JSONL, TotalsLayout{}, rows); err != nil {
		t.Fatal(err)
	}
	var depths []string
	for l := range strings.Lines(b.String()) {
		var r struct{ Depth int }
		if err := json.Unmarshal([]byte(l), &r); err != nil {
			t.Fatal(err)
		}
		depths = append(depths, strconv.Itoa(r.Depth))
	}
	if got := strings.Join(depths, ""); got != "0121" {
		t.Fatalf("jsonl order %q", got)
	}
	// The total displaced in the input: still last in the table, the input
	// untouched.
	displaced := []totals.Prefix{rows[1], rows[0], rows[2], rows[3]}
	before := slices.Clone(displaced)
	b.Reset()
	if err := WriteTotals(&b, Table, TotalsLayout{}, displaced); err != nil {
		t.Fatal(err)
	}
	var prefixes []string
	for l := range strings.Lines(b.String()) {
		f := strings.Fields(l)
		prefixes = append(prefixes, f[len(f)-1])
	}
	if got := strings.Join(prefixes, " "); got != "prefix logs/a/ logs/a/b/ logs/c/ (total)" || !slices.Equal(displaced, before) {
		t.Fatalf("table order %q", got)
	}
}

// TestTotalsTable: the table has one figure: the size, or the allocated
// bytes, on disk, when they are the primary figure, then the counts, the
// newest time in the table's zone, named in its heading ("-" when no
// object had one), and the paths, which keep their whole text, indented
// two blanks a level below the first.
func TestTotalsTable(t *testing.T) {
	const mib = 1 << 20
	newest := time.Date(2026, 11, 15, 9, 30, 5, 999999000, time.UTC).UnixMicro() // to the second, not rounded
	older := time.Date(2026, 7, 1, 23, 0, 0, 0, time.UTC).UnixMicro()
	cet := time.FixedZone("CET", 3600)
	rows := []totals.Prefix{
		{Objects: 3, Dirs: 5, Size: 64*mib + 5000, HasSize: true, Allocated: 12288, HasAllocated: true, Newest: newest, HasNewest: true},
		{Prefix: "archive/", Depth: 1, Objects: 2, Dirs: 1, Size: 64 * mib, HasSize: true, Allocated: 4096, HasAllocated: true, Newest: older, HasNewest: true},
		{Prefix: "archive/vm/", Depth: 2, Objects: 1, Size: 64 * mib, HasSize: true, HasAllocated: true},
	}
	for name, c := range map[string]struct {
		l    TotalsLayout
		want string
	}{
		"local": {TotalsLayout{Local: true, Loc: cet}, "" +
			"    size  files  dirs  last_modified (CET)  path\n" +
			"64.0 MiB      2     1  2026-07-02 00:00:00  archive/\n" +
			"64.0 MiB      1     0  -                      archive/vm/\n" +
			"64.0 MiB      3     5  2026-11-15 10:30:05  (total)\n"},
		"allocated": {TotalsLayout{Local: true, Primary: totals.AllocatedBytes, Loc: cet}, "" +
			" on disk  files  dirs  last_modified (CET)  path\n" +
			"4.00 KiB      2     1  2026-07-02 00:00:00  archive/\n" +
			"     0 B      1     0  -                      archive/vm/\n" +
			"12.0 KiB      3     5  2026-11-15 10:30:05  (total)\n"},
		"bucket": {TotalsLayout{Loc: time.UTC}, "" +
			"    size  objects  last_modified (UTC)  prefix\n" +
			"64.0 MiB        2  2026-07-01 23:00:00  archive/\n" +
			"64.0 MiB        1  -                      archive/vm/\n" +
			"64.0 MiB        3  2026-11-15 09:30:05  (total)\n"},
	} {
		var b bytes.Buffer
		if err := WriteTotals(&b, Table, c.l, rows); err != nil || b.String() != c.want {
			t.Errorf("%s: %v\n%s\nwant\n%s", name, err, b.String(), c.want)
		}
	}
}

// TestTableCounts: the table groups a count's digits by three; TSV and
// JSONL keep them plain.
func TestTableCounts(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 60010: "60,010", 1234567: "1,234,567", -1000: "-1,000"} {
		if got := string(appendGrouped(nil, n)); got != want {
			t.Errorf("%d: %q", n, got)
		}
	}
	rows := []totals.Prefix{{Objects: 60010, Size: 1, HasSize: true}}
	var b bytes.Buffer
	if err := WriteTotals(&b, Table, TotalsLayout{Loc: time.UTC}, rows); err != nil || b.String() != "size  objects  last_modified (UTC)  prefix\n 1 B   60,010  -                    (total)\n" {
		t.Fatalf("%v %q", err, b.String())
	}
	b.Reset()
	if err := WriteTotals(&b, TSV, TotalsLayout{NoHeader: true}, rows); err != nil || !strings.HasPrefix(b.String(), "total\t0\t60010\t") {
		t.Fatalf("%v %q", err, b.String())
	}
}

// TestTableTotalAndUnreadable: the table's total row names the source
// (TotalsLayout.Total, escaped as a prefix is) before "(total)", and a
// directory that could not be read shows "-" for its figure and counts.
func TestTableTotalAndUnreadable(t *testing.T) {
	rows := []totals.Prefix{
		{Objects: 3, Dirs: 2, Size: 10, HasSize: true},
		{Prefix: "locked/", Depth: 1, HasSize: true, Unreadable: true},
		{Prefix: "ok/", Depth: 1, Objects: 3, Size: 10, HasSize: true},
	}
	var b bytes.Buffer
	l := TotalsLayout{Local: true, Loc: time.UTC, Total: "t/\x1bphotos"}
	want := "size  files  dirs  last_modified (UTC)  path\n" +
		"   -      -     -  -                    locked/\n" +
		"10 B      3     0  -                    ok/\n" +
		"10 B      3     2  -                    t/\\x1bphotos (total)\n"
	if err := WriteTotals(&b, Table, l, rows); err != nil || b.String() != want {
		t.Fatalf("%v\n%q\nwant\n%q", err, b.String(), want)
	}
}
