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
	"github.com/varveio/swath/reader/du/tree"
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
	table := "    size  last_modified (CET)  key\n" +
		"    12 B  2023-11-14 23:13:20  a/b.txt\n" +
		"     0 B  -                    tab\\there\\nnl\\\\x<&>\"\\x1b\\x7fµ\n" +
		"1.00 TiB  2023-11-14 23:13:20  µ\n"
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

// TestWriteView: the view's lines under a heading: the size, the count of
// objects (three significant digits), the share, a bar of 20 cells from
// 100 columns and up to 10 at 80, the newest date (the zone named on the
// total's line; a fold's date, none when unknown) and the path indented
// two blanks a level. As the width shrinks the bar goes, then the share,
// then the date; the path is cut so that no line passes the width.
func TestWriteView(t *testing.T) {
	at := int64(1_763_210_310_000_000) // 2025-11-15T12:38:30Z
	when := func(micros int64) *time.Time { t := time.UnixMicro(micros); return &t }
	rows := []tree.Row{
		{Name: "s3://bucket", Bytes: 2048 << 30, Objects: 37_100_000, Newest: when(at)},
		{Name: "logs/", Level: 1, Bytes: 1536 << 30, Objects: 12_000, Newest: when(at - 3_600_000_000*48)},
		{Name: "2025/app-events-archive.tar", Level: 2, Bytes: 1024 << 30, Objects: 1, Newest: when(at - 3_600_000_000*48)},
		{Fold: true, Level: 2, Bytes: 512 << 30, Objects: 11_999, More: 1234, Newest: when(at - 3_600_000_000*24*30)},
		{Fold: true, Level: 1, Bytes: 512 << 30, Objects: 37_088_000, More: 9},
	}
	want := map[int]string{
		40: "" +
			"    size  objects  path\n" +
			"2.00 TiB    37.1M  s3://bucket\n" +
			"1.50 TiB    12.0k    logs/\n" +
			"1.00 TiB        1      2025/app…hive.tar\n" +
			" 512 GiB    12.0k      (1,234 more)\n" +
			" 512 GiB    37.1M    (9 more)\n",
		60: "" +
			"    size  objects  share  newest          path\n" +
			"2.00 TiB    37.1M   100%  2025-11-15 UTC  s3://bucket\n" +
			"1.50 TiB    12.0k  75.0%  2025-11-13        logs/\n" +
			"1.00 TiB        1  50.0%  2025-11-13          2025/a…ive.tar\n" +
			" 512 GiB    12.0k  25.0%  2025-10-16          (1,234 more)\n" +
			" 512 GiB    37.1M  25.0%                    (9 more)\n",
		80: "" +
			"    size  objects  share              newest          path\n" +
			"2.00 TiB    37.1M   100%  ██████████  2025-11-15 UTC  s3://bucket\n" +
			"1.50 TiB    12.0k  75.0%  ███████▌    2025-11-13        logs/\n" +
			"1.00 TiB        1  50.0%  █████       2025-11-13          2025/app-e…archive.tar\n" +
			" 512 GiB    12.0k  25.0%  ██▌         2025-10-16          (1,234 more)\n" +
			" 512 GiB    37.1M  25.0%  ██▌                           (9 more)\n",
		100: "" +
			"    size  objects  share                        newest          path\n" +
			"2.00 TiB    37.1M   100%  ████████████████████  2025-11-15 UTC  s3://bucket\n" +
			"1.50 TiB    12.0k  75.0%  ███████████████       2025-11-13        logs/\n" +
			"1.00 TiB        1  50.0%  ██████████            2025-11-13          2025/app-events-archive.tar\n" +
			" 512 GiB    12.0k  25.0%  █████                 2025-10-16          (1,234 more)\n" +
			" 512 GiB    37.1M  25.0%  █████                                   (9 more)\n",
		120: "" +
			"    size  objects  share                        newest          path\n" +
			"2.00 TiB    37.1M   100%  ████████████████████  2025-11-15 UTC  s3://bucket\n" +
			"1.50 TiB    12.0k  75.0%  ███████████████       2025-11-13        logs/\n" +
			"1.00 TiB        1  50.0%  ██████████            2025-11-13          2025/app-events-archive.tar\n" +
			" 512 GiB    12.0k  25.0%  █████                 2025-10-16          (1,234 more)\n" +
			" 512 GiB    37.1M  25.0%  █████                                   (9 more)\n",
	}
	for width, w := range want {
		var b bytes.Buffer
		if err := WriteView(&b, ViewLayout{Label: "s3://bucket", Width: width, Loc: time.UTC}, rows); err != nil {
			t.Fatal(err)
		}
		if b.String() != w {
			t.Errorf("width %d:\n%s\nwant\n%s", width, b.String(), w)
		}
		for _, l := range strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n") {
			if tree.Cells(l) > width {
				t.Errorf("width %d: %q is %d columns", width, l, tree.Cells(l))
			}
		}
	}
	// A local tree counts files; color dims the heading and the folds and
	// paints the bar.
	var b bytes.Buffer
	if err := WriteView(&b, ViewLayout{Label: "/data", Local: true, Width: 120, Color: true, Loc: time.UTC}, rows); err != nil {
		t.Fatal(err)
	}
	plain := regexp.MustCompile("\x1b\\[[0-9;]*m").ReplaceAllString(b.String(), "")
	lines := strings.Split(b.String(), "\n")
	if h := strings.SplitN(plain, "\n", 2)[0]; h != "    size  files  share                        newest          path" ||
		!strings.HasPrefix(lines[0], "\x1b[2m") || !strings.Contains(b.String(), "\x1b[36m") ||
		!strings.HasPrefix(lines[5], "\x1b[2m") || !strings.HasSuffix(lines[5], "(9 more)\x1b[0m") {
		t.Errorf("local, colored:\n%q", b.String())
	}
}

// TestWriteViewClips: no line passes the width: a long name is cut in the
// middle, its end kept, the total's label too.
func TestWriteViewClips(t *testing.T) {
	long := strings.Repeat("n", 40) + "-tail.parquet"
	rows := []tree.Row{
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
		for _, l := range strings.Split(strings.TrimSuffix(plain, "\n"), "\n")[1:] { // past the heading
			if n := utf8.RuneCountInString(l); n > width || !strings.Contains(l, "more") && !strings.HasSuffix(l, "parquet") {
				t.Errorf("width %d: %q (%d)", width, l, n)
			}
		}
		if width == 40 && !strings.Contains(plain, "…") {
			t.Errorf("width 40: nothing cut\n%s", plain)
		}
	}
	// Columns, not runes: an East Asian wide character takes two.
	wide := strings.Repeat("界", 30) + ".bin"
	var b bytes.Buffer
	rows = []tree.Row{{Name: "s3://b", Bytes: 2}, {Name: wide, Level: 1, Bytes: 1}, {Fold: true, Level: 1, Bytes: 1, More: 1}}
	if err := WriteView(&b, ViewLayout{Label: "s3://b", Width: 40}, rows); err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n") {
		if tree.Cells(l) > 40 {
			t.Errorf("width 40: %q is %d columns", l, tree.Cells(l))
		}
	}
}
