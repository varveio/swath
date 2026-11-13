// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/schema"
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
// table's time in its zone, the size in binary units, a null blank.
func TestRowContract(t *testing.T) {
	tsv := "key\tsize\tlast_modified\tetag\tstorage_class\towner_id\tgeneration\n" +
		"a/b.txt\t12\t2023-11-14T22:13:20.000000Z\te1\tSTANDARD\t\\N\t7\n" +
		"tab\\there\\nnl\\\\x<&>\"\\x1b\\x7fµ\t0\t\\N\t\\N\t\\N\t\\N\t7\n" +
		"µ\t1099511627776\t2023-11-14T22:13:20.000120Z\te\t\\N\to\t7\n"
	if got := string(write(t, Config{Format: TSV})); got != tsv {
		t.Fatalf("tsv\n%q\nwant\n%q", got, tsv)
	}
	jsonl := `{"key":"a/b.txt","size":12,"last_modified":"2023-11-14T22:13:20.000000Z","etag":"e1","storage_class":"STANDARD","owner_id":null,"generation":7}` + "\n" +
		`{"key":"tab\there\nnl\\x<&>\"\u001b\u007fµ","size":0,"last_modified":null,"etag":null,"storage_class":null,"owner_id":null,"generation":7}` + "\n" +
		`{"key":"µ","size":1099511627776,"last_modified":"2023-11-14T22:13:20.000120Z","etag":"e","storage_class":null,"owner_id":"o","generation":7}` + "\n"
	if got := string(write(t, Config{Format: JSONL})); got != jsonl {
		t.Fatalf("jsonl\n%s\nwant\n%s", got, jsonl)
	}
	table := "last_modified (local)      size  key\n" +
		"2023-11-14 23:13:20        12 B  a/b.txt\n" +
		"                            0 B  tab\\there\\nnl\\\\x<&>\"\\x1b\\x7fµ\n" +
		"2023-11-14 23:13:20    1.00 TiB  µ\n"
	if got := string(write(t, Config{Format: Table, Loc: time.FixedZone("X", 3600)})); got != table {
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
