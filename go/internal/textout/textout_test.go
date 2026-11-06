// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

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

var testSchema = &schema.Schema[obj]{Info: schema.Info{ID: "test", Key: schema.String}, Columns: []schema.Column[obj]{
	{Spec: schema.Spec{Name: "size"}, Int: func(o *obj) (int64, bool) { return o.size, true }},
	{Spec: schema.Spec{Name: "last_modified"}, Int: func(o *obj) (int64, bool) { return o.mtime, o.mtime != 0 }},
	{Spec: schema.Spec{Name: "etag"}, Str: nonEmpty(func(o *obj) string { return o.etag })},
	{Spec: schema.Spec{Name: "storage_class"}, Str: nonEmpty(func(o *obj) string { return o.class })},
	{Spec: schema.Spec{Name: "owner_id"}, Str: nonEmpty(func(o *obj) string { return o.owner })},
}}

var rows = [][]schema.Row[obj]{{
	{Key: "a/b.txt", Data: obj{size: 12, mtime: 1_700_000_000_000_000, etag: "e1", class: "STANDARD"}},
	{Key: "tab\there\nnl\\x<&>", Data: obj{size: 0, mtime: 1_700_000_000_123_000}},
	{Key: "µ", Data: obj{size: 1 << 40, mtime: 1_700_000_000_123_456, etag: "e\x7f", owner: "o"}},
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

// TestRowContract: Java's TSV (header, \xNN control escaping, last_modified
// with 0, 3 or 6 fraction digits, OBJECT), JSONL (field order, absent
// fields omitted, no HTML escaping) and table (14 and 24 columns).
func TestRowContract(t *testing.T) {
	tsv := "key\tsize\tlast_modified\tetag\tstorage_class\trow_type\n" +
		"a/b.txt\t12\t2023-11-14T22:13:20Z\te1\tSTANDARD\tOBJECT\n" +
		"tab\\x09here\\x0anl\\x<&>\t0\t2023-11-14T22:13:20.123Z\t\t\tOBJECT\n" +
		"µ\t1099511627776\t2023-11-14T22:13:20.123456Z\te\\x7f\t\tOBJECT\n"
	if got := string(write(t, Config{Format: TSV})); got != tsv {
		t.Fatalf("tsv\n%q\nwant\n%q", got, tsv)
	}
	jsonl := `{"key":"a/b.txt","size":12,"last_modified":"2023-11-14T22:13:20Z","etag":"e1","storage_class":"STANDARD","row_type":"OBJECT"}` + "\n" +
		`{"key":"tab\there\nnl\\x<&>","size":0,"last_modified":"2023-11-14T22:13:20.123Z","row_type":"OBJECT"}` + "\n" +
		`{"key":"µ","size":1099511627776,"last_modified":"2023-11-14T22:13:20.123456Z","etag":"e` + "\x7f" + `","owner_id":"o","row_type":"OBJECT"}` + "\n"
	if got := string(write(t, Config{Format: JSONL})); got != jsonl {
		t.Fatalf("jsonl\n%s\nwant\n%s", got, jsonl)
	}
	table := "            12  2023-11-14T22:13:20Z      a/b.txt\n" +
		"             0  2023-11-14T22:13:20.123Z  tab\\x09here\\x0anl\\x<&>\n" +
		" 1099511627776  2023-11-14T22:13:20.123456Z  µ\n"
	if got := string(write(t, Config{Format: Table})); got != table {
		t.Fatalf("table\n%q\nwant\n%q", got, table)
	}
}

// TestFileCompressedAndAtomic: a file is written beside its destination
// and renamed when complete (an aborted one leaves the destination as it
// was and no temporary); gzip and zstd decode to the plain rows.
func TestFileCompressedAndAtomic(t *testing.T) {
	dir := t.TempDir()
	plain := write(t, Config{Format: TSV})
	for comp, dec := range map[string]func(io.Reader) (io.Reader, error){
		Gzip: func(r io.Reader) (io.Reader, error) { return gzip.NewReader(r) },
		Zstd: func(r io.Reader) (io.Reader, error) { d, err := zstd.NewReader(r); return d, err },
	} {
		path := filepath.Join(dir, "out.tsv."+comp)
		r, err := dec(bytes.NewReader(write(t, Config{Format: TSV, Path: path, Compression: comp})))
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
	bin := &schema.Schema[obj]{Info: schema.Info{ID: "bin", Key: schema.Binary}}
	if _, err := NewSink(o, bin); err == nil {
		t.Fatal("a byte-key schema accepted")
	}
}
