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

// testSchema has a provider's extra column (owner_id stands in, with no
// owner_display_name) and an unquoted etag (GCS-like).
var testSchema = &schema.Schema[obj]{Info: schema.Info{ID: "test", Key: schema.String},
	Heap: func(o *obj) int64 { return int64(len(o.etag) + len(o.class) + len(o.owner)) }, Columns: []schema.Column[obj]{
		{Spec: schema.Spec{Name: "size"}, Int: func(o *obj) (int64, bool) { return o.size, true }},
		{Spec: schema.Spec{Name: "last_modified"}, Int: func(o *obj) (int64, bool) { return o.mtime, o.mtime != 0 }},
		{Spec: schema.Spec{Name: "etag"}, Str: nonEmpty(func(o *obj) string { return o.etag })},
		{Spec: schema.Spec{Name: "storage_class"}, Str: nonEmpty(func(o *obj) string { return o.class })},
		{Spec: schema.Spec{Name: "owner_id"}, Str: nonEmpty(func(o *obj) string { return o.owner })},
		{Spec: schema.Spec{Name: "generation"}, Int: func(o *obj) (int64, bool) { return 7, true }},
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

// TestRowContract: the three formats over a schema with fewer fields than
// S3's (absent fields omitted; a provider's other columns not written),
// JSON escaping as jq prints it (the same rule, minus the quote escape,
// in TSV and the table), and LastModified with a fraction only when it
// has one.
func TestRowContract(t *testing.T) {
	tsv := "Key\tLastModified\tETag\tSize\tStorageClass\tOwner.ID\n" +
		"a/b.txt\t2023-11-14T22:13:20+00:00\te1\t12\tSTANDARD\t\n" +
		"tab\\there\\nnl\\\\x<&>\"\\u001b\\u007fµ\t\t\t0\t\t\n" +
		"µ\t2023-11-14T22:13:20.000120+00:00\te\t1099511627776\t\to\n"
	if got := string(write(t, Config{Format: TSV})); got != tsv {
		t.Fatalf("tsv\n%q\nwant\n%q", got, tsv)
	}
	jsonl := `{"Key":"a/b.txt","LastModified":"2023-11-14T22:13:20+00:00","ETag":"e1","Size":12,"StorageClass":"STANDARD"}` + "\n" +
		`{"Key":"tab\there\nnl\\x<&>\"\u001b\u007fµ","Size":0}` + "\n" +
		`{"Key":"µ","LastModified":"2023-11-14T22:13:20.000120+00:00","ETag":"e","Size":1099511627776,"Owner":{"ID":"o"}}` + "\n"
	if got := string(write(t, Config{Format: JSONL})); got != jsonl {
		t.Fatalf("jsonl\n%s\nwant\n%s", got, jsonl)
	}
	table := "2023-11-14 22:13:20         12 a/b.txt\n" +
		"                             0 tab\\there\\nnl\\\\x<&>\"\\u001b\\u007fµ\n" +
		"2023-11-14 22:13:20 1099511627776 µ\n"
	if got := string(write(t, Config{Format: Table})); got != table {
		t.Fatalf("table\n%q\nwant\n%q", got, table)
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
	bin := &schema.Schema[obj]{Info: schema.Info{ID: "bin", Key: schema.Binary}}
	if _, err := NewSink(o, bin); err == nil {
		t.Fatal("a byte-key schema accepted")
	}
}
