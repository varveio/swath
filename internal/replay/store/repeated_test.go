// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/source/s3"
)

// TestRepeatedKeysAreRefused: a SeekParquet dataset whose keys repeat (the
// S3 versions schema, key.unique false) is refused at open with
// ErrRepeatedKeys. The routes continue a listing after a key, never inside
// its group, so serving it would drop the rest of any group a page ends
// in; the versions routes (issue #279) come first.
func TestRepeatedKeysAreRefused(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := parquetout.NewRouter(parquetout.Options{Out: dir, PartSize: 64 << 20, Encoders: 1}, &s3.VersionsSchema)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]s3.VersionRow, 3000)
	for i := range rows {
		rows[i] = s3.VersionRow{Key: "k", Data: s3.Version{Object: s3.Object{Size: int64(i)}, VersionID: "v"}}
	}
	if err := r.Write(context.Background(), [][]s3.VersionRow{rows}); err != nil {
		t.Fatal(err)
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	meta := dataset.Meta{SchemaID: s3.VersionsSchema.ID, Groups: true, Source: dataset.Source{Kind: "s3", URI: "s3://b"}, Scope: dataset.Scope{Kind: "whole"}}
	if err := dataset.Publish(dir, meta, parts, nil); err != nil {
		t.Fatal(err)
	}
	st, _, err := Open(context.Background(), dir, Options{})
	if !errors.Is(err, ErrRepeatedKeys) {
		if st != nil {
			_ = st.Close()
		}
		t.Fatalf("open: %v, want ErrRepeatedKeys", err)
	}
}
