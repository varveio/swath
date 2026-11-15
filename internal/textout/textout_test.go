// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/totals"
)

type obj struct {
	size, mtime        int64
	etag, class, owner string
}

func nonEmpty(f func(*obj) string) func(*obj) (string, bool) {
	return func(o *obj) (string, bool) { v := f(o); return v, v != "" }
}

// testSchema is a schema of string, integer and time columns, some null.
var testSchema = &schema.Schema[obj]{Info: schema.Info{ID: "test", Key: schema.String},
	Heap: func(o *obj) int64 { return int64(len(o.etag) + len(o.class) + len(o.owner)) }, Columns: []schema.Column[obj]{
		{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(o *obj) (int64, bool) { return o.size, true }},
		{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: func(o *obj) (int64, bool) { return o.mtime, o.mtime != 0 }},
		{Spec: schema.Spec{Name: "etag", Type: schema.String}, Str: nonEmpty(func(o *obj) string { return o.etag })},
		{Spec: schema.Spec{Name: "storage_class", Type: schema.String}, Str: nonEmpty(func(o *obj) string { return o.class })},
		{Spec: schema.Spec{Name: "owner_id", Type: schema.String}, Str: nonEmpty(func(o *obj) string { return o.owner })},
		{Spec: schema.Spec{Name: "generation", Type: schema.Int64}, Int: func(o *obj) (int64, bool) { return 7, true }},
	}}

var rows = [][]schema.Row[obj]{{
	{Key: "a/b.txt", Data: obj{size: 12, mtime: 1_700_000_000_000_000, etag: "e1", class: "STANDARD"}},
	{Key: "tab\there\nnl\\x<&>\"\x1b\x7fµ", Data: obj{size: 0}},
	{Key: "µ", Data: obj{size: 1 << 40, mtime: 1_700_000_000_000_120, etag: "e", owner: "o"}},
}}

func write(t *testing.T, cfg Config) []byte {
	t.Helper()
	var buf bytes.Buffer
	if cfg.Path == "" {
		cfg.Stdout = &buf
	}
	o, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	k, err := NewSink(o, testSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.Write(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if err := o.Commit(); err != nil {
		t.Fatal(err)
	}
	if o.Rows() != 3 || o.Bytes() == 0 {
		t.Fatalf("rows %d bytes %d", o.Rows(), o.Bytes())
	}
	if cfg.Path != "" {
		b, err := os.ReadFile(cfg.Path)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	return buf.Bytes()
}

// TestRowContract: the three formats over a schema's columns in its order
// under its names: TSV with \N for a null and the TSV escapes, JSONL with
// null and JSON's escapes, every time with six fraction digits in UTC; the
// table's time in its zone, named in the heading, the size in binary
// units, a null "-"; TSV's header line left out on request.
func TestRowContract(t *testing.T) {
	tsv := "key\tsize\tlast_modified\tetag\tstorage_class\towner_id\tgeneration\n" +
		"a/b.txt\t12\t2023-11-14T22:13:20.000000Z\te1\tSTANDARD\t\\N\t7\n" +
		"tab\\there\\nnl\\\\x<&>\"\\x1b\\x7fµ\t0\t\\N\t\\N\t\\N\t\\N\t7\n" +
		"µ\t1099511627776\t2023-11-14T22:13:20.000120Z\te\t\\N\to\t7\n"
	if got := string(write(t, Config{Format: TSV})); got != tsv {
		t.Fatalf("tsv\n%q\nwant\n%q", got, tsv)
	}
	if got := string(write(t, Config{Format: TSV, NoHeader: true})); got != tsv[strings.IndexByte(tsv, '\n')+1:] {
		t.Fatalf("tsv without its header\n%q", got)
	}
	jsonl := `{"key":"a/b.txt","size":12,"last_modified":"2023-11-14T22:13:20.000000Z","etag":"e1","storage_class":"STANDARD","owner_id":null,"generation":7}` + "\n" +
		`{"key":"tab\there\nnl\\x<&>\"\u001b\u007fµ","size":0,"last_modified":null,"etag":null,"storage_class":null,"owner_id":null,"generation":7}` + "\n" +
		`{"key":"µ","size":1099511627776,"last_modified":"2023-11-14T22:13:20.000120Z","etag":"e","storage_class":null,"owner_id":"o","generation":7}` + "\n"
	if got := string(write(t, Config{Format: JSONL})); got != jsonl {
		t.Fatalf("jsonl\n%s\nwant\n%s", got, jsonl)
	}
	table := "last_modified (CET)      size  key\n" +
		"2023-11-14 23:13:20      12 B  a/b.txt\n" +
		"-                         0 B  tab\\there\\nnl\\\\x<&>\"\\x1b\\x7fµ\n" +
		"2023-11-14 23:13:20  1.00 TiB  µ\n"
	if got := string(write(t, Config{Format: Table, Loc: time.FixedZone("CET", 3600)})); got != table {
		t.Fatalf("table\n%q\nwant\n%q", got, table)
	}
}

// TestTSVGrammar: every byte, alone, doubled and between plain runs and
// valid UTF-8, round-trips through the TSV escaping, which writes only
// valid UTF-8 without a control byte; \N is null and differs from the
// escaped value \N; a malformed escape is refused.
func TestTSVGrammar(t *testing.T) {
	for c := 0; c < 256; c++ {
		b := string([]byte{byte(c)})
		for _, s := range []string{b, b + b, "ab" + b + "é" + b + b + "\\N", "\xe2\x82" + b} {
			e := string(appendTSV(nil, s))
			if !utf8.ValidString(e) || strings.ContainsFunc(e, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
				t.Fatalf("%q escaped as %q", s, e)
			}
			got, ok, err := UnescapeTSV(e)
			if err != nil || !ok || got != s {
				t.Fatalf("%q -> %q -> %q %v %v", s, e, got, ok, err)
			}
		}
	}
	if _, ok, err := UnescapeTSV(`\N`); ok || err != nil {
		t.Fatalf("null: %v %v", ok, err)
	}
	if got, ok, err := UnescapeTSV(`\\N`); !ok || err != nil || got != `\N` {
		t.Fatalf("escaped \\N: %q %v %v", got, ok, err)
	}
	for _, bad := range []string{`a\`, `\x4`, `\xzz`, `\u0001`, `\b`} {
		if _, _, err := UnescapeTSV(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// TestFileCompressedAndAtomic: a file is written beside its destination
// and renamed when complete (an aborted one leaves the destination as it
// was and no temporary); a .jsonl.gz and a .jsonl.zst decode to the plain
// rows.
func TestFileCompressedAndAtomic(t *testing.T) {
	dir := t.TempDir()
	plain := write(t, Config{Format: JSONL})
	for comp, dec := range map[string]func(io.Reader) (io.Reader, error){
		Gzip: func(r io.Reader) (io.Reader, error) { return gzip.NewReader(r) },
		Zstd: func(r io.Reader) (io.Reader, error) { d, err := zstd.NewReader(r); return d, err },
	} {
		path := filepath.Join(dir, "out.jsonl."+comp)
		r, err := dec(bytes.NewReader(write(t, Config{Format: JSONL, Path: path, Compression: comp})))
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := io.ReadAll(r); !bytes.Equal(got, plain) {
			t.Fatalf("%s: %q", comp, got)
		}
	}
	path := filepath.Join(dir, "keep.tsv")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	o, err := Open(Config{Format: TSV, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	o.Abort()
	if b, _ := os.ReadFile(path); string(b) != "old" {
		t.Fatalf("aborted output replaced the file: %q", b)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 3 {
		t.Fatalf("temporaries left: %v", ents)
	}
	if _, err := Open(Config{Format: TSV, Path: dir}); err == nil {
		t.Fatal("a directory destination accepted")
	}
}

// TestWriteView: the view's lines: the share with one decimal, the bar in
// eighths of a cell, a fold line's count grouped or "(more)" when unknown,
// names indented two blanks a level, no bar where the width leaves too
// little, color only when asked.
func TestWriteView(t *testing.T) {
	rows := []totals.ViewRow{
		{Name: "s3://b", Bytes: 2048},
		{Name: "logs/", Level: 1, Bytes: 1536},
		{Name: "a", Level: 2, Bytes: 1024},
		{Fold: true, Level: 2, Bytes: 512, More: 1234},
		{Fold: true, Level: 1, Bytes: 512, More: -1},
	}
	var b bytes.Buffer
	if err := WriteView(&b, ViewLayout{Label: "s3://b", Width: 60}, rows); err != nil {
		t.Fatal(err)
	}
	want := "" +
		"2.00 KiB   100%  █████████████████████████  s3://b\n" +
		"1.50 KiB  75.0%  ██████████████████▊          logs/\n" +
		"1.00 KiB  50.0%  ████████████▌                  a\n" +
		"   512 B  25.0%  ██████▎                        (1,234 more)\n" +
		"   512 B  25.0%  ██████▎                      (more)\n"
	if b.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", b.String(), want)
	}
	b.Reset()
	if err := WriteView(&b, ViewLayout{Label: "s3://b", Width: 40, Color: true}, rows); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "█") || !strings.Contains(b.String(), "\x1b[2m(more)\x1b[0m") || !strings.HasPrefix(b.String(), "2.00 KiB   100%  s3://b\n") {
		t.Fatalf("narrow, colored:\n%q", b.String())
	}
}

// TestWriteViewTimes: each node's newest time, to the second in the zone
// given, which the total's line names once; a fold line has none, a node
// without one "-". As the width shrinks the bar goes first, then the time.
func TestWriteViewTimes(t *testing.T) {
	at := int64(1_763_210_310_000_000) // 2025-11-15T12:38:30Z
	rows := []totals.ViewRow{
		{Name: "s3://b", Bytes: 2048, Newest: at, HasNewest: true},
		{Name: "logs/", Level: 1, Bytes: 1536, Newest: at - 3_600_000_000, HasNewest: true},
		{Name: "a", Level: 1, Bytes: 0},
		{Fold: true, Level: 1, Bytes: 512, More: 3},
	}
	write := func(width int) string {
		var b bytes.Buffer
		if err := WriteView(&b, ViewLayout{Label: "s3://b", Width: width, Loc: time.UTC}, rows); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	want := map[int]string{
		120: "" +
			"2.00 KiB   100%  ██████████████████████████████  2025-11-15 12:38:30 UTC  s3://b\n" +
			"1.50 KiB  75.0%  ██████████████████████▌         2025-11-15 11:38:30        logs/\n" +
			"     0 B   0.0%                                  -                          a\n" +
			"   512 B  25.0%  ███████▌                                                   (3 more)\n",
		60: "" +
			"2.00 KiB   100%  2025-11-15 12:38:30 UTC  s3://b\n" +
			"1.50 KiB  75.0%  2025-11-15 11:38:30        logs/\n" +
			"     0 B   0.0%  -                          a\n" +
			"   512 B  25.0%                             (3 more)\n",
		40: "" +
			"2.00 KiB   100%  s3://b\n" +
			"1.50 KiB  75.0%    logs/\n" +
			"     0 B   0.0%    a\n" +
			"   512 B  25.0%    (3 more)\n",
	}
	for width, w := range want {
		if got := write(width); got != w {
			t.Errorf("width %d:\n%s\nwant\n%s", width, got, w)
		}
	}
}

// TestWriteViewClips: no line passes the width: a long name is cut in the
// middle, its end kept, the total's label too.
func TestWriteViewClips(t *testing.T) {
	long := strings.Repeat("n", 40) + "-tail.parquet"
	rows := []totals.ViewRow{
		{Name: "s3://bucket/" + long, Bytes: 100},
		{Name: long, Level: 1, Bytes: 60},
		{Fold: true, Level: 1, Bytes: 40, More: 2},
	}
	for _, width := range []int{40, 80} {
		var b bytes.Buffer
		if err := WriteView(&b, ViewLayout{Label: rows[0].Name, Width: width, Color: true}, rows); err != nil {
			t.Fatal(err)
		}
		plain := regexp.MustCompile("\x1b\\[[0-9;]*m").ReplaceAllString(b.String(), "")
		for _, l := range strings.Split(strings.TrimSuffix(plain, "\n"), "\n") {
			if n := utf8.RuneCountInString(l); n > width || !strings.Contains(l, "more") && !strings.HasSuffix(l, "tail.parquet") {
				t.Errorf("width %d: %q (%d)", width, l, n)
			}
		}
		if width == 40 && !strings.Contains(plain, "…") {
			t.Errorf("width 40: nothing cut\n%s", plain)
		}
	}
	if got := clip("abcdefghij.txt", 9); got != "abcd….txt" {
		t.Errorf("clip: %q", got)
	}
	// Columns, not runes: an East Asian wide character takes two, a
	// combining mark none.
	wide := strings.Repeat("界", 30) + ".bin"
	for _, room := range []int{10, 21, 40} {
		if got := clip(wide, room); cells(got) > room || !strings.HasSuffix(got, ".bin") || !strings.Contains(got, "…") {
			t.Errorf("clip %d: %q, %d columns", room, got, cells(got))
		}
	}
	if n := cells("e\u0301界a"); n != 4 {
		t.Errorf("cells: %d", n)
	}
	var b bytes.Buffer
	rows = []totals.ViewRow{{Name: "s3://b", Bytes: 2}, {Name: wide, Level: 1, Bytes: 1}, {Fold: true, Level: 1, Bytes: 1, More: 1}}
	if err := WriteView(&b, ViewLayout{Label: "s3://b", Width: 40}, rows); err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n") {
		if cells(l) > 40 {
			t.Errorf("width 40: %q is %d columns", l, cells(l))
		}
	}
}
