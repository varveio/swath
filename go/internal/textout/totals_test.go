// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/totals"
)

// TestIEC: a table's size keeps at most three significant digits across
// the unit boundaries, moving to the next unit before a fourth digit.
func TestIEC(t *testing.T) {
	const k, m = 1024, 1024 * 1024
	for n, want := range map[int64]string{
		0: "0 B", 1023: "1023 B", k: "1.00 KiB", 10 * k: "10.0 KiB", 100 * k: "100 KiB",
		999 * k: "999 KiB", 999*k + k/2: "0.98 MiB", 1000 * k: "0.98 MiB", 1023 * k: "1.00 MiB", m: "1.00 MiB",
		9996 * m / 1000: "10.0 MiB", 9996 * m / 100: "100 MiB", 9994 * m / 1000: "9.99 MiB", 1 << 62: "4.00 EiB",
	} {
		if got := string(appendPrefixSize(nil, &totals.Prefix{Bytes: n})); got != want {
			t.Errorf("%d: %q, want %q", n, got, want)
		}
	}
	if got := string(appendPrefixSize(nil, &totals.Prefix{Bytes: 5, Unsized: 1})); got != "-" {
		t.Errorf("unsized: %q", got)
	}
}

// TestTotalsTSVEscapes: a TSV prefix takes the rows' grammar: a byte that
// is not UTF-8 is \xHH, never the raw byte, and decodes back; a missing
// time is \N.
func TestTotalsTSVEscapes(t *testing.T) {
	rows := []totals.Prefix{{Prefix: "bad\xff\t/", Depth: 1, Objects: 1, Bytes: 1}}
	var b bytes.Buffer
	if err := WriteTotals(&b, TSV, TotalsLayout{}, rows); err != nil {
		t.Fatal(err)
	}
	want := "depth\tobjects\tsize\tlast_modified\tprefix\n1\t1\t1\t\\N\tbad\\xff\\t/\n"
	if b.String() != want {
		t.Fatalf("%q\nwant %q", b.String(), want)
	}
	f := strings.Split(strings.TrimSuffix(b.String(), "\n"), "\t")
	if p, ok, err := UnescapeTSV(f[len(f)-1]); err != nil || !ok || p != rows[0].Prefix {
		t.Fatalf("%q %v %v", p, ok, err)
	}
}

// TestTotalsJSONL: every line is JSON; a prefix that is not UTF-8 is
// prefix_base64 alone, its exact bytes; size is the sized objects' bytes
// and a local tree's unsized the others; a missing time is null.
func TestTotalsJSONL(t *testing.T) {
	rows := []totals.Prefix{
		{Prefix: "", Objects: 3, Bytes: 7, Newest: 1, HasNewest: true},
		{Prefix: "caf\xc3\xa9/\"q\"\t/", Depth: 2, Objects: 1, Bytes: 2},
		{Prefix: "bad\xff/", Depth: 1, Objects: 1, Unsized: 1},
	}
	var b bytes.Buffer
	if err := WriteTotals(&b, JSONL, TotalsLayout{Local: true}, rows); err != nil {
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
	if len(got) != 3 || got[0][FieldDepth] != 0.0 || got[1][FieldDepth] != 2.0 || got[2][FieldDepth] != 1.0 ||
		got[1][FieldPrefix] != rows[1].Prefix || got[1][FieldSize] != 2.0 || got[1][FieldLastModified] != nil {
		t.Fatalf("%v", got)
	}
	if !strings.HasPrefix(strings.Split(b.String(), "\n")[2], `{"depth":1,"prefix_base64":`) {
		t.Fatalf("member order: %q", b.String())
	}
	raw, err := base64.StdEncoding.DecodeString(got[2][FieldPrefixBase64].(string))
	if err != nil || string(raw) != rows[2].Prefix || got[2][FieldPrefix] != nil || got[2][FieldSize] != 0.0 || got[2][FieldUnsized] != 1.0 {
		t.Fatalf("%v (%v)", got[2], err)
	}
}

// TestTotalsPartlySized: a prefix with some files sized and some not keeps
// the known bytes in TSV and JSONL beside Unsized; the table, for reading,
// shows its size as unknown.
func TestTotalsPartlySized(t *testing.T) {
	rows := []totals.Prefix{{Objects: 2, Bytes: 7, Unsized: 1}}
	for format, want := range map[string]string{
		TSV:   "depth\tobjects\tdirs\tsize\tunsized\tlast_modified\tprefix\n0\t2\t0\t7\t1\t\\N\t\n",
		JSONL: `{"depth":0,"prefix":"","objects":2,"dirs":0,"size":7,"unsized":1,"last_modified":null}` + "\n",
		Table: "Size  Files  Dirs  LastModified (local)  Path\n   -      2     0                        (total)\n",
	} {
		var b bytes.Buffer
		if err := WriteTotals(&b, format, TotalsLayout{Local: true}, rows); err != nil || b.String() != want {
			t.Errorf("%s: %v\n%q\nwant %q", format, err, b.String(), want)
		}
	}
}

// TestTotalsOrder: TSV and JSONL keep the rows in the order given (the
// tally's byte order, the total first); the table puts the total last,
// found by its Depth, not its place, and leaves the rows as they were.
func TestTotalsOrder(t *testing.T) {
	rows := []totals.Prefix{{Prefix: "logs/", Objects: 3, Bytes: 3}, {Prefix: "logs/a/", Depth: 1, Objects: 1, Bytes: 1},
		{Prefix: "logs/a/b/", Depth: 2, Objects: 1, Bytes: 1}, {Prefix: "logs/c/", Depth: 1, Objects: 2, Bytes: 2}}
	var b bytes.Buffer
	if err := WriteTotals(&b, TSV, TotalsLayout{}, rows); err != nil {
		t.Fatal(err)
	}
	if want := "depth\tobjects\tsize\tlast_modified\tprefix\n0\t3\t3\t\\N\tlogs/\n1\t1\t1\t\\N\tlogs/a/\n2\t1\t1\t\\N\tlogs/a/b/\n1\t2\t2\t\\N\tlogs/c/\n"; b.String() != want {
		t.Fatalf("tsv %q", b.String())
	}
	b.Reset()
	if err := WriteTotals(&b, JSONL, TotalsLayout{}, rows); err != nil {
		t.Fatal(err)
	}
	var depths []string
	for l := range strings.Lines(b.String()) {
		depths = append(depths, l[len(`{"depth":`):len(`{"depth":`)+1])
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
	if got := strings.Join(prefixes, " "); got != "Prefix logs/a/ logs/a/b/ logs/c/ (total)" || !slices.Equal(displaced, before) {
		t.Fatalf("table order %q", got)
	}
}

// TestTotalsTableLocalTime: the table's times are in the reader's zone (the
// layout's), under a heading that says so; TSV's stay UTC.
func TestTotalsTableLocalTime(t *testing.T) {
	rows := []totals.Prefix{{Objects: 1, Bytes: 1, Newest: 0, HasNewest: true}}
	var b bytes.Buffer
	if err := WriteTotals(&b, Table, TotalsLayout{Loc: time.FixedZone("X", 2*3600)}, rows); err != nil {
		t.Fatal(err)
	}
	want := "Size  Objects  LastModified (local)  Prefix\n" +
		" 1 B        1  1970-01-01 02:00:00   (total)\n"
	if b.String() != want {
		t.Fatalf("%q\nwant %q", b.String(), want)
	}
	b.Reset()
	if err := WriteTotals(&b, TSV, TotalsLayout{Loc: time.FixedZone("X", 2*3600)}, rows); err != nil || !strings.HasSuffix(b.String(), "\t1970-01-01T00:00:00.000000Z\t\n") {
		t.Fatalf("%v %q", err, b.String())
	}
}
