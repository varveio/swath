// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/s3"
)

// source adapts an Opener to seekparquet.Source, whose Open returns its own
// file interface; an objfile.Object satisfies it.
type source struct{ objfile.Opener }

func (s source) Open(ctx context.Context, name string) (seekparquet.SourceFile, error) {
	f, err := s.Opener.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return f, nil
}

var _ seekparquet.ContextBinder = objfile.Object(nil)

type row struct {
	Key  string `parquet:"key"`
	Size int64  `parquet:"size"`
}

// rowReader yields rows in turn.
type rowReader struct{ rows []parquet.Row }

func (r *rowReader) ReadRows(out []parquet.Row) (int, error) {
	n := copy(out, r.rows)
	r.rows = r.rows[n:]
	if len(r.rows) == 0 {
		return n, io.EOF
	}
	return n, nil
}

// TestDataset: a dataset written by the module's local writer and uploaded to
// the fake bucket reads through objfile/s3 as it reads from disk.
func TestDataset(t *testing.T) {
	ctx := context.Background()
	schema := parquet.SchemaOf(new(row))
	var all []parquet.Row
	for d := range 17 { // in key order
		for i := d; i < 3000; i += 17 {
			all = append(all, schema.Deconstruct(nil, &row{Key: fmt.Sprintf("logs/%02d/obj-%05d", d, i), Size: int64(i)}))
		}
	}
	dir := filepath.Join(t.TempDir(), "ds")
	err := local.WriteDir(ctx, dir, schema, &rowReader{rows: all}, local.DatasetOptions{
		Part:      write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: "objfile/test", RowGroupRows: 500, PageRows: 100},
		PartBytes: 4 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	f := newFake(t, ifMatch)
	uploaded := 0
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		f.put(t, "datasets/ds/"+filepath.ToSlash(rel), data)
		uploaded++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if uploaded < 3 {
		t.Fatalf("the dataset has %d files; want a manifest and several parts", uploaded)
	}

	want, err := seekparquet.OpenDir(ctx, dir, seekparquet.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer want.Close()
	src := source{objfile.Limit(objfile.Root(s3.New(f.client, bucket), "datasets/ds"), 4)}
	got, err := seekparquet.OpenDataset(ctx, src, seekparquet.Options{VerifyOnOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()

	rng := seekparquet.Range{Prefix: []byte("logs/03/"), Start: []byte("logs/03/obj-01000"), Limit: 50, Columns: []string{"key", "size"}}
	wantRows, err := want.Range(ctx, rng)
	if err != nil {
		t.Fatal(err)
	}
	gotRows, err := got.Range(ctx, rng)
	if err != nil {
		t.Fatal(err)
	}
	if len(wantRows) == 0 || !reflect.DeepEqual(gotRows, wantRows) {
		t.Fatalf("Range: %d rows remote, %d local, or they differ", len(gotRows), len(wantRows))
	}

	list := seekparquet.List{Prefix: []byte("logs/"), Delimiter: '/', Limit: 100}
	wantEntries, err := want.List(ctx, list)
	if err != nil {
		t.Fatal(err)
	}
	gotEntries, err := got.List(ctx, list)
	if err != nil {
		t.Fatal(err)
	}
	if len(wantEntries) != 17 || !reflect.DeepEqual(gotEntries, wantEntries) {
		t.Fatalf("List: %d entries remote, %d local, or they differ", len(gotEntries), len(wantEntries))
	}
}
