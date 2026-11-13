// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/runtrace"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

// TestTreeDatasetTotals: a dataset written from a hierarchical namespace
// holds the flat listing's schema, which the factory declares
// (registry.SchemaInfo), so du --from-dataset (registry.DatasetTotals)
// totals it as a live du of the account does, directory rows counted as
// objects in both.
func TestTreeDatasetTotals(t *testing.T) {
	ctx := context.Background()
	store := azblobtest.NewTreeStore(hnsCorpus(200))
	dir := filepath.Join(t.TempDir(), "ds")
	if _, err := listing.Run(ctx, listing.Config{
		Source: listing.SourceConfig{URI: "az://acct/c", Options: &Options{Client: store}},
		Output: listing.OutputConfig{Dir: dir, NoJournal: true},
	}); err != nil {
		t.Fatal(err)
	}
	opt := totals.Options{Depth: 3}
	live := totals.New(opt)
	o := openTree(t, store, "", 2, 0)
	if _, _, err := o.Run(ctx, registry.Env{Count: true, Totals: live, Filter: &filter.Spec{}, Order: order.Options{Workers: 2}}); err != nil {
		t.Fatal(err)
	}
	d, err := reader.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if id := string(d.Metadata().SchemaID); id != Schema.Info.ID {
		t.Fatalf("schema %s, want %s", id, Schema.Info.ID)
	}
	saved := totals.New(opt)
	if _, err := registry.DatasetTotals(ctx, d, "", nil, saved, 2); err != nil {
		t.Fatal(err)
	}
	got, want := saved.Result(), live.Result()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("from the dataset %+v, live %+v", got, want)
	}
	if n := int64(len(store.Keys())); want[0].Objects != n {
		t.Fatalf("total %d objects, want every file and directory, %d", want[0].Objects, n)
	}
}

// TestTreeTraceRefused: --trace is refused on a hierarchical namespace
// (the tree source is not source.Traced) with a reason that names it,
// before any listing request.
func TestTreeTraceRefused(t *testing.T) {
	store := azblobtest.NewTreeStore([]string{"a/b"})
	tr := runtrace.New(io.Discard)
	defer tr.Close(0, "", "")
	_, err := listing.Run(context.Background(), listing.Config{
		Source: listing.SourceConfig{URI: "az://acct/c", Options: &Options{Client: store}},
		Output: listing.OutputConfig{Dir: filepath.Join(t.TempDir(), "ds"), NoJournal: true},
		Trace:  tr,
	})
	var ce *listing.ConfigError
	if !errors.As(err, &ce) || !strings.Contains(err.Error(), "hierarchical namespace") {
		t.Fatalf("%v", err)
	}
	if n := len(store.Log()); n != 0 {
		t.Fatalf("%d listing requests before the refusal", n)
	}
}
